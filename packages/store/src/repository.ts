import * as crypto from 'crypto';
import { PoolClient } from 'pg';
import { NewWorkflowEvent, WorkflowEvent, WorkflowEventType } from '@ratchet/core';
import { Database } from './db';

export type WorkflowStatus = 'running' | 'waiting' | 'completed' | 'failed' | 'cancelled';

export interface WorkflowRecord<TInput = any, TOutput = any> {
  id: string;
  type: string;
  status: WorkflowStatus;
  input: TInput;
  output: TOutput | null;
  error: string | null;
  idempotencyKey: string | null;
  parentId: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface TaskRecord {
  id: string;
  workflowId: string;
  kind: 'workflow' | 'step';
  stepSeq: number | null;
  stepName: string | null;
  status: 'pending' | 'leased' | 'done' | 'dead';
  runAt: Date;
  attempt: number;
  maxAttempts: number;
  leaseToken: string | null;
  leaseExpires: Date | null;
  lastError: string | null;
}

export class RatchetRepository {
  constructor(private readonly db: Database) {}

  async startWorkflow<TInput>(params: {
    type: string;
    input: TInput;
    idempotencyKey?: string;
    parentId?: string;
  }): Promise<WorkflowRecord<TInput>> {
    return this.db.withTransaction(async (client) => {
      if (params.idempotencyKey) {
        const existing = await client.query(
          `SELECT * FROM workflows WHERE idempotency_key = $1`,
          [params.idempotencyKey],
        );
        if (existing.rowCount && existing.rowCount > 0) {
          return this.mapWorkflowRow(existing.rows[0]);
        }
      }

      const wfRes = await client.query(
        `INSERT INTO workflows (type, status, input, idempotency_key, parent_id)
         VALUES ($1, 'running', $2::jsonb, $3, $4)
         RETURNING *`,
        [
          params.type,
          JSON.stringify(params.input ?? {}),
          params.idempotencyKey ?? null,
          params.parentId ?? null,
        ],
      );
      const workflow = this.mapWorkflowRow(wfRes.rows[0]);

      await this.appendEventsTx(client, workflow.id, [
        {
          stepSeq: null,
          type: 'WorkflowStarted',
          name: params.type,
          payload: { input: params.input ?? {} },
        },
      ]);

      await this.enqueueWorkflowTickTx(client, workflow.id);
      return workflow;
    });
  }

  async getWorkflow(workflowId: string): Promise<WorkflowRecord | null> {
    const res = await this.db.query(`SELECT * FROM workflows WHERE id = $1`, [
      workflowId,
    ]);
    return res.rowCount ? this.mapWorkflowRow(res.rows[0]) : null;
  }

  async getWorkflowHistory(workflowId: string): Promise<WorkflowEvent[]> {
    const res = await this.db.query(
      `SELECT workflow_id, seq, step_seq, type, name, payload, created_at
       FROM workflow_events
       WHERE workflow_id = $1
       ORDER BY seq ASC`,
      [workflowId],
    );
    return res.rows.map((r) => ({
      workflowId: r.workflow_id,
      seq: Number(r.seq),
      stepSeq: r.step_seq !== null ? Number(r.step_seq) : null,
      type: r.type as WorkflowEventType,
      name: r.name,
      payload: r.payload,
      createdAt: r.created_at,
    }));
  }

  //Claims the next due task using FOR UPDATE SKIP LOCKED and assigns a fresh lease_token.
  async claimNextTask(leaseDurationSeconds = 30): Promise<TaskRecord | null> {
    const leaseToken = crypto.randomUUID();
    const res = await this.db.query(
      `UPDATE tasks
       SET status = 'leased',
           lease_token = $1::uuid,
           lease_expires = now() + ($2 || ' seconds')::interval,
           attempt = attempt + 1,
           updated_at = now()
       WHERE id = (
         SELECT id FROM tasks
         WHERE status = 'pending' AND run_at <= now()
         ORDER BY run_at ASC, created_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       )
       RETURNING *`,
      [leaseToken, leaseDurationSeconds],
    );

    return res.rowCount ? this.mapTaskRow(res.rows[0]) : null;
  }

  // Commits the outcome of a workflow decision tick atomically with lease fencing.
  async completeWorkflowTick(params: {
    taskId: string;
    leaseToken: string;
    workflowId: string;
    newEvents: NewWorkflowEvent[];
    outcome:
      | { status: 'completed'; output: unknown }
      | { status: 'failed'; error: string }
      | {
          status: 'suspended_step';
          stepSeq: number;
          stepName: string;
          alreadyScheduled: boolean;
        };
  }): Promise<boolean> {
    return this.db.withTransaction(async (client) => {
      // 1. Fence check on the workflow task lease
      const fence = await client.query(
        `UPDATE tasks
         SET status = 'done', updated_at = now()
         WHERE id = $1 AND lease_token = $2::uuid AND status = 'leased'`,
        [params.taskId, params.leaseToken],
      );
      if (fence.rowCount !== 1) {
        return false;
      }

      // 2. Append any new events produced during the replay tick
      await this.appendEventsTx(client, params.workflowId, params.newEvents);

      // 3. Apply state transition
      if (params.outcome.status === 'completed') {
        await client.query(
          `UPDATE workflows
           SET status = 'completed', output = $2::jsonb, updated_at = now()
           WHERE id = $1`,
          [params.workflowId, JSON.stringify(params.outcome.output ?? null)],
        );
      } else if (params.outcome.status === 'failed') {
        await client.query(
          `UPDATE workflows
           SET status = 'failed', error = $2, updated_at = now()
           WHERE id = $1`,
          [params.workflowId, params.outcome.error],
        );
      } else if (params.outcome.status === 'suspended_step') {
        await client.query(
          `INSERT INTO tasks (workflow_id, kind, step_seq, step_name, status)
           VALUES ($1, 'step', $2, $3, 'pending')
           ON CONFLICT (workflow_id, step_seq)
           WHERE kind = 'step' AND status IN ('pending', 'leased')
           DO NOTHING`,
          [params.workflowId, params.outcome.stepSeq, params.outcome.stepName],
        );
      }

      return true;
    });
  }

  //Atomically records a StepCompleted event (fenced by lease_token and deduplicated by step_seq)
  //and schedules a follow-up workflow replay tick.
  async completeStepTask(params: {
    taskId: string;
    leaseToken: string;
    workflowId: string;
    stepSeq: number;
    stepName: string;
    result: unknown;
  }): Promise<boolean> {
    return this.db.withTransaction(async (client) => {
      const fence = await client.query(
        `UPDATE tasks
         SET status = 'done', updated_at = now()
         WHERE id = $1 AND lease_token = $2::uuid AND status = 'leased'`,
        [params.taskId, params.leaseToken],
      );
      if (fence.rowCount !== 1) {
        return false;
      }

      await this.appendEventsTx(client, params.workflowId, [
        {
          stepSeq: params.stepSeq,
          type: 'StepCompleted',
          name: params.stepName,
          payload: { result: params.result ?? null },
        },
      ]);

      await this.enqueueWorkflowTickTx(client, params.workflowId);
      return true;
    });
  }

  // Handles step failure: either re-queues with backoff or marks dead + records StepFailed.
  async failStepTask(params: {
    taskId: string;
    leaseToken: string;
    workflowId: string;
    stepSeq: number;
    stepName: string;
    attempt: number;
    maxAttempts: number;
    error: string;
    retryDelayMs?: number;
  }): Promise<boolean> {
    return this.db.withTransaction(async (client) => {
      if (params.attempt < params.maxAttempts) {
        const delayMs = params.retryDelayMs ?? Math.min(1000 * 2 ** (params.attempt - 1), 30_000);
        const fence = await client.query(
          `UPDATE tasks
           SET status = 'pending',
               lease_token = NULL,
               lease_expires = NULL,
               last_error = $3,
               run_at = now() + ($4 || ' milliseconds')::interval,
               updated_at = now()
           WHERE id = $1 AND lease_token = $2::uuid AND status = 'leased'`,
          [params.taskId, params.leaseToken, params.error, delayMs],
        );
        return fence.rowCount === 1;
      }

      // Permanent failure: mark task dead, record StepFailed, wake workflow to fail cleanly
      const fence = await client.query(
        `UPDATE tasks
         SET status = 'dead', last_error = $3, updated_at = now()
         WHERE id = $1 AND lease_token = $2::uuid AND status = 'leased'`,
        [params.taskId, params.leaseToken, params.error],
      );
      if (fence.rowCount !== 1) {
        return false;
      }

      await this.appendEventsTx(client, params.workflowId, [
        {
          stepSeq: params.stepSeq,
          type: 'StepFailed',
          name: params.stepName,
          payload: { error: params.error, attempts: params.attempt },
        },
      ]);

      await this.enqueueWorkflowTickTx(client, params.workflowId);
      return true;
    });
  }

  private async appendEventsTx(
    client: PoolClient,
    workflowId: string,
    events: NewWorkflowEvent[],
  ): Promise<void> {
    if (events.length === 0) return;

    // Lock the workflow row to serialize physical `seq` allocation per workflow
    await client.query(`SELECT id FROM workflows WHERE id = $1 FOR UPDATE`, [
      workflowId,
    ]);

    const seqRes = await client.query<{ next_seq: string }>(
      `SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
       FROM workflow_events
       WHERE workflow_id = $1`,
      [workflowId],
    );
    let nextSeq = Number(seqRes.rows[0].next_seq);

    for (const evt of events) {
      const inserted = await client.query(
        `INSERT INTO workflow_events (workflow_id, seq, step_seq, type, name, payload)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb)
         ON CONFLICT (workflow_id, step_seq, type) WHERE step_seq IS NOT NULL
         DO NOTHING
         RETURNING seq`,
        [
          workflowId,
          nextSeq,
          evt.stepSeq,
          evt.type,
          evt.name,
          JSON.stringify(evt.payload ?? {}),
        ],
      );
      if (inserted.rowCount && inserted.rowCount > 0) {
        nextSeq += 1;
      }
    }
  }

  private async enqueueWorkflowTickTx(
    client: PoolClient,
    workflowId: string,
  ): Promise<void> {
    await client.query(
      `INSERT INTO tasks (workflow_id, kind, status)
       VALUES ($1, 'workflow', 'pending')
       ON CONFLICT (workflow_id)
       WHERE kind = 'workflow' AND status IN ('pending', 'leased')
       DO NOTHING`,
      [workflowId],
    );
  }

  private mapWorkflowRow(row: any): WorkflowRecord {
    return {
      id: row.id,
      type: row.type,
      status: row.status,
      input: row.input,
      output: row.output,
      error: row.error,
      idempotencyKey: row.idempotency_key,
      parentId: row.parent_id,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  private mapTaskRow(row: any): TaskRecord {
    return {
      id: row.id,
      workflowId: row.workflow_id,
      kind: row.kind,
      stepSeq: row.step_seq !== null ? Number(row.step_seq) : null,
      stepName: row.step_name,
      status: row.status,
      runAt: row.run_at,
      attempt: Number(row.attempt),
      maxAttempts: Number(row.max_attempts),
      leaseToken: row.lease_token,
      leaseExpires: row.lease_expires,
      lastError: row.last_error,
    };
  }
}