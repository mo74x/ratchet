import * as crypto from 'crypto';
import { PoolClient } from 'pg';
import {
  calculateBackoffWithJitter,
  NewWorkflowEvent,
  WorkflowEvent,
  WorkflowEventType,
} from '@ratchet/core';
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
  timeoutMs: number | null;
  leaseToken: string | null;
  leaseExpires: Date | null;
  lastError: string | null;
}

export interface TimerRecord {
  id: string;
  workflowId: string;
  stepSeq: number;
  fireAt: Date;
  fired: boolean;
}

export class RatchetRepository {
  constructor(private readonly db: Database) { }

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
           lease_expires = now() + ($2::text || ' seconds')::interval,
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
      timeoutMs?: number;
      maxAttempts?: number;
    }
    | {
      status: 'suspended_sleep';
      stepSeq: number;
      durationMs: number;
    }
    | {
      status: 'suspended_signal';
      stepSeq: number;
      signalName: string;
      timeoutMs?: number;
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
      const { outcome } = params;
      if (outcome.status === 'completed') {
        await client.query(
          `UPDATE workflows
           SET status = 'completed', output = $2::jsonb, updated_at = now()
           WHERE id = $1`,
          [params.workflowId, JSON.stringify(outcome.output ?? null)],
        );
      } else if (outcome.status === 'failed') {
        await client.query(
          `UPDATE workflows
           SET status = 'failed', error = $2, updated_at = now()
           WHERE id = $1`,
          [params.workflowId, outcome.error],
        );
      } else if (outcome.status === 'suspended_step') {
        await client.query(
          `UPDATE workflows SET status = 'running', updated_at = now() WHERE id = $1`,
          [params.workflowId],
        );
        await client.query(
          `INSERT INTO tasks (workflow_id, kind, step_seq, step_name, status, timeout_ms, max_attempts)
           VALUES ($1, 'step', $2, $3, 'pending', $4, COALESCE($5, 5))
           ON CONFLICT (workflow_id, step_seq)
           WHERE kind = 'step' AND status IN ('pending', 'leased')
           DO NOTHING`,
          [
            params.workflowId,
            outcome.stepSeq,
            outcome.stepName,
            outcome.timeoutMs ?? null,
            outcome.maxAttempts ?? null,
          ],
        );
      } else if (outcome.status === 'suspended_sleep') {
        await client.query(
          `INSERT INTO timers (workflow_id, step_seq, fire_at)
           VALUES ($1, $2, now() + ($3 || ' milliseconds')::interval)
           ON CONFLICT (workflow_id, step_seq) DO NOTHING`,
          [params.workflowId, outcome.stepSeq, outcome.durationMs],
        );
        await client.query(
          `UPDATE workflows SET status = 'waiting', updated_at = now() WHERE id = $1`,
          [params.workflowId],
        );
      } else if (outcome.status === 'suspended_signal') {
        // Check if an early signal is ALREADY waiting in the mailbox!
        const earlySignal = await client.query(
          `SELECT id, payload FROM signals
           WHERE workflow_id = $1 AND name = $2 AND NOT consumed
           ORDER BY created_at ASC
           FOR UPDATE SKIP LOCKED
           LIMIT 1`,
          [params.workflowId, outcome.signalName],
        );

        if (earlySignal.rowCount && earlySignal.rowCount > 0) {
          const sigRow = earlySignal.rows[0];
          const recorded = await this.appendTerminalResolutionEventTx(
            client,
            params.workflowId,
            {
              stepSeq: outcome.stepSeq,
              type: 'SignalReceived',
              name: outcome.signalName,
              payload: { signalId: sigRow.id, payload: sigRow.payload },
            },
          );

          if (recorded) {
            await client.query(
              `UPDATE signals
               SET consumed = true, consumed_by_seq = $2
               WHERE id = $1`,
              [sigRow.id, outcome.stepSeq],
            );
            // Cancel any timeout timer for this stepSeq if one existed
            await client.query(
              `UPDATE timers SET fired = true WHERE workflow_id = $1 AND step_seq = $2`,
              [params.workflowId, outcome.stepSeq],
            );
          }

          await client.query(
            `UPDATE workflows SET status = 'running', updated_at = now() WHERE id = $1`,
            [params.workflowId],
          );
          await this.enqueueWorkflowTickTx(client, params.workflowId);
        } else {
          // No signal in mailbox yet -> schedule optional timeout timer and enter 'waiting'
          if (outcome.timeoutMs !== undefined) {
            await client.query(
              `INSERT INTO timers (workflow_id, step_seq, fire_at)
               VALUES ($1, $2, now() + ($3 || ' milliseconds')::interval)
               ON CONFLICT (workflow_id, step_seq) DO NOTHING`,
              [params.workflowId, outcome.stepSeq, outcome.timeoutMs],
            );
          }
          await client.query(
            `UPDATE workflows SET status = 'waiting', updated_at = now() WHERE id = $1`,
            [params.workflowId],
          );
        }
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
        const delayMs =
          params.retryDelayMs ??
          calculateBackoffWithJitter(params.attempt, {
            baseDelayMs: 200,
            maxDelayMs: 10_000,
          });
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
      timeoutMs: row.timeout_ms !== null && row.timeout_ms !== undefined ? Number(row.timeout_ms) : null,
    };
  }

  // Renamed from updateLeaseExpiration to heartbeatTask to be more descriptive
  async heartbeatTask(
    taskId: string,
    leaseToken: string,
    leaseDurationSeconds = 30,
  ): Promise<boolean> {
    const res = await this.db.query(
      `UPDATE tasks
       SET lease_expires = now() + ($3::text || ' seconds')::interval,
           updated_at = now()
       WHERE id = $1
         AND lease_token = $2::uuid
         AND status = 'leased'`,
      [taskId, leaseToken, leaseDurationSeconds],
    );
    return res.rowCount === 1;
  }

  //Renamed from reaper to reapExpiredLeases for brevity and clarity
  async reapExpiredLeases(batchSize = 100): Promise<{
    requeued: number;
    deadLettered: number;
  }> {
    return this.db.withTransaction(async (client) => {
      const expiredRes = await client.query(
        `SELECT * FROM tasks
         WHERE status = 'leased' AND lease_expires <= now()
         ORDER BY lease_expires ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $1`,
        [batchSize],
      );

      let requeued = 0;
      let deadLettered = 0;

      for (const row of expiredRes.rows) {
        const task = this.mapTaskRow(row);

        // Workflow replay ticks are always idempotent and re-queued;
        // Step tasks are re-queued if attempt < maxAttempts.
        if (task.kind === 'workflow' || task.attempt < task.maxAttempts) {
          await client.query(
            `UPDATE tasks
             SET status = 'pending',
                 lease_token = NULL,
                 lease_expires = NULL,
                 last_error = COALESCE(last_error, 'Lease expired before task completion'),
                 run_at = now(),
                 updated_at = now()
             WHERE id = $1`,
            [task.id],
          );
          requeued += 1;
        } else {
          // Step task exhausted maxAttempts via repeated lease expirations
          const errMsg =
            task.lastError ??
            `Step lease expired after ${task.attempt} attempts`;
          await client.query(
            `UPDATE tasks
             SET status = 'dead',
                 lease_token = NULL,
                 lease_expires = NULL,
                 last_error = $2,
                 updated_at = now()
             WHERE id = $1`,
            [task.id, errMsg],
          );

          await this.appendEventsTx(client, task.workflowId, [
            {
              stepSeq: task.stepSeq,
              type: 'StepFailed',
              name: task.stepName,
              payload: { error: errMsg, attempts: task.attempt },
            },
          ]);

          await this.enqueueWorkflowTickTx(client, task.workflowId);
          deadLettered += 1;
        }
      }

      return { requeued, deadLettered };
    });
  }
  /**
   * Sends a named external signal to a workflow.
   * - If the workflow is currently waiting on this signal name, binds it immediately,
   *   cancels any timeout timer on that step_seq, and wakes the workflow.
   * - Otherwise, buffers the signal in the `signals` mailbox (`consumed = false`)
   *   so it is delivered as soon as the workflow reaches `waitForSignal(name)`.
   */
  async sendSignal(params: {
    workflowId: string;
    name: string;
    payload?: unknown;
  }): Promise<{ signalId: string; deliveredImmediately: boolean }> {
    return this.db.withTransaction(async (client) => {
      // Lock the workflow row to coordinate with concurrent ticks or timers
      const wfRes = await client.query(
        `SELECT id, status FROM workflows WHERE id = $1 FOR UPDATE`,
        [params.workflowId],
      );
      if (!wfRes.rowCount) {
        throw new Error(`Workflow ${params.workflowId} not found`);
      }
      const wfStatus = wfRes.rows[0].status as WorkflowStatus;

      // Check if the workflow has an active unresolved SignalWaiting event for this signal name
      const waitingEvt = await client.query<{ step_seq: number }>(
        `SELECT w.step_seq
         FROM workflow_events w
         WHERE w.workflow_id = $1
           AND w.type = 'SignalWaiting'
           AND w.name = $2
           AND NOT EXISTS (
             SELECT 1 FROM workflow_events t
             WHERE t.workflow_id = w.workflow_id
               AND t.step_seq = w.step_seq
               AND t.type IN ('SignalReceived', 'TimerFired')
           )
         ORDER BY w.step_seq ASC
         LIMIT 1`,
        [params.workflowId, params.name],
      );

      if (
        waitingEvt.rowCount &&
        waitingEvt.rowCount > 0 &&
        (wfStatus === 'waiting' || wfStatus === 'running')
      ) {
        const targetStepSeq = Number(waitingEvt.rows[0].step_seq);
        const sigInsert = await client.query<{ id: string }>(
          `INSERT INTO signals (workflow_id, name, payload, consumed, consumed_by_seq)
           VALUES ($1, $2, $3::jsonb, true, $4)
           RETURNING id`,
          [
            params.workflowId,
            params.name,
            JSON.stringify(params.payload ?? null),
            targetStepSeq,
          ],
        );
        const signalId = sigInsert.rows[0].id;

        const recorded = await this.appendTerminalResolutionEventTx(
          client,
          params.workflowId,
          {
            stepSeq: targetStepSeq,
            type: 'SignalReceived',
            name: params.name,
            payload: { signalId, payload: params.payload ?? null },
          },
        );

        if (recorded) {
          // Cancel any pending timeout timer for this waitForSignal step_seq
          await client.query(
            `UPDATE timers SET fired = true WHERE workflow_id = $1 AND step_seq = $2`,
            [params.workflowId, targetStepSeq],
          );
          await client.query(
            `UPDATE workflows SET status = 'running', updated_at = now() WHERE id = $1`,
            [params.workflowId],
          );
          await this.enqueueWorkflowTickTx(client, params.workflowId);
          return { signalId, deliveredImmediately: true };
        }
      }

      // Otherwise buffer the signal as unconsumed in the mailbox (early signal)
      const buffered = await client.query<{ id: string }>(
        `INSERT INTO signals (workflow_id, name, payload, consumed)
         VALUES ($1, $2, $3::jsonb, false)
         RETURNING id`,
        [params.workflowId, params.name, JSON.stringify(params.payload ?? null)],
      );

      return {
        signalId: buffered.rows[0].id,
        deliveredImmediately: false,
      };
    });
  }

  /**
   * Claims and fires due timers using FOR UPDATE SKIP LOCKED so multiple Timer Service
   * instances can poll concurrently without ever firing a timer twice.
   */
  async fireDueTimers(batchSize = 100): Promise<number> {
    return this.db.withTransaction(async (client) => {
      const dueRes = await client.query(
        `SELECT * FROM timers
         WHERE NOT fired AND fire_at <= now()
         ORDER BY fire_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $1`,
        [batchSize],
      );

      let firedCount = 0;

      for (const row of dueRes.rows) {
        const timer: TimerRecord = {
          id: row.id,
          workflowId: row.workflow_id,
          stepSeq: Number(row.step_seq),
          fireAt: row.fire_at,
          fired: row.fired,
        };

        await client.query(`UPDATE timers SET fired = true WHERE id = $1`, [
          timer.id,
        ]);

        const wfRes = await client.query(
          `SELECT status FROM workflows WHERE id = $1 FOR UPDATE`,
          [timer.workflowId],
        );
        if (!wfRes.rowCount) continue;
        const wfStatus = wfRes.rows[0].status as WorkflowStatus;
        if (wfStatus !== 'waiting' && wfStatus !== 'running') continue;

        const recorded = await this.appendTerminalResolutionEventTx(
          client,
          timer.workflowId,
          {
            stepSeq: timer.stepSeq,
            type: 'TimerFired',
            name: 'timer',
            payload: { timerId: timer.id, firedAt: new Date().toISOString() },
          },
        );

        if (recorded) {
          await client.query(
            `UPDATE workflows SET status = 'running', updated_at = now() WHERE id = $1`,
            [timer.workflowId],
          );
          await this.enqueueWorkflowTickTx(client, timer.workflowId);
          firedCount += 1;
        }
      }

      return firedCount;
    });
  }

  /**
   * Appends a terminal resolution event (`SignalReceived` or `TimerFired`) guarded by
   * `workflow_events_single_terminal_outcome_idx`. Returns true if this event won the race,
   * or false if the step_seq was already resolved by a competing event.
   */
  private async appendTerminalResolutionEventTx(
    client: PoolClient,
    workflowId: string,
    evt: NewWorkflowEvent,
  ): Promise<boolean> {
    await client.query(`SELECT id FROM workflows WHERE id = $1 FOR UPDATE`, [
      workflowId,
    ]);

    const seqRes = await client.query<{ next_seq: string }>(
      `SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
       FROM workflow_events
       WHERE workflow_id = $1`,
      [workflowId],
    );
    const nextSeq = Number(seqRes.rows[0].next_seq);

    const inserted = await client.query(
      `INSERT INTO workflow_events (workflow_id, seq, step_seq, type, name, payload)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb)
       ON CONFLICT DO NOTHING
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

    return Boolean(inserted.rowCount && inserted.rowCount > 0);
  }
}