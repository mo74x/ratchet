import {
  resolveStepHandler,
  runWorkflowTick,
  WorkflowRegistry,
} from '@ratchet/core';
import { RatchetRepository, TaskRecord } from '@ratchet/store';

export interface WorkerOptions {
  pollIntervalMs?: number;
  leaseDurationSeconds?: number;
  onStepExecuted?: (info: {
    workflowId: string;
    stepSeq: number;
    stepName: string;
  }) => void | Promise<void>;
}

export class RatchetWorker {
  private running = false;
  private loopPromise: Promise<void> | null = null;
  private readonly pollIntervalMs: number;
  private readonly leaseDurationSeconds: number;

  constructor(
    private readonly repo: RatchetRepository,
    private readonly registry: WorkflowRegistry,
    private readonly options: WorkerOptions = {},
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 100;
    this.leaseDurationSeconds = options.leaseDurationSeconds ?? 30;
  }

  /**
   * Claims and executes a single due task (either a 'workflow' replay tick or a 'step' execution).
   * Returns true if a task was processed, or false if the queue was empty.
   */
  async runNextTask(): Promise<boolean> {
    const task = await this.repo.claimNextTask(this.leaseDurationSeconds);
    if (!task || !task.leaseToken) {
      return false;
    }

    if (task.kind === 'workflow') {
      await this.handleWorkflowTask(task);
    } else if (task.kind === 'step') {
      await this.handleStepTask(task);
    }

    return true;
  }

  private async handleWorkflowTask(task: TaskRecord): Promise<void> {
    const workflow = await this.repo.getWorkflow(task.workflowId);
    if (!workflow || workflow.status !== 'running') {
      return;
    }

    const history = await this.repo.getWorkflowHistory(workflow.id);
    const workflowFn = this.registry.get(workflow.type);

    const tick = await runWorkflowTick({
      workflowId: workflow.id,
      workflowFn,
      input: workflow.input,
      history,
    });

    if (tick.status === 'completed') {
      await this.repo.completeWorkflowTick({
        taskId: task.id,
        leaseToken: task.leaseToken!,
        workflowId: workflow.id,
        newEvents: tick.newEvents,
        outcome: { status: 'completed', output: tick.output },
      });
    } else if (tick.status === 'failed') {
      await this.repo.completeWorkflowTick({
        taskId: task.id,
        leaseToken: task.leaseToken!,
        workflowId: workflow.id,
        newEvents: tick.newEvents,
        outcome: { status: 'failed', error: tick.error.message },
      });
    } else if (
      tick.status === 'suspended' &&
      tick.suspension.kind === 'step'
    ) {
      await this.repo.completeWorkflowTick({
        taskId: task.id,
        leaseToken: task.leaseToken!,
        workflowId: workflow.id,
        newEvents: tick.newEvents,
        outcome: {
          status: 'suspended_step',
          stepSeq: tick.suspension.stepSeq,
          stepName: tick.suspension.stepName,
          alreadyScheduled: tick.suspension.alreadyScheduled,
        },
      });
    }
  }

  private async handleStepTask(task: TaskRecord): Promise<void> {
    const workflow = await this.repo.getWorkflow(task.workflowId);
    if (!workflow || workflow.status !== 'running') {
      return;
    }

    const stepSeq = task.stepSeq!;
    const stepName = task.stepName!;
    const history = await this.repo.getWorkflowHistory(workflow.id);
    const workflowFn = this.registry.get(workflow.type);

    try {
      const handler = await resolveStepHandler({
        workflowId: workflow.id,
        workflowFn,
        input: workflow.input,
        history,
        targetStepSeq: stepSeq,
        targetStepName: stepName,
      });

      const idempotencyKey = `${workflow.id}:${stepSeq}`;
      const result = await handler(idempotencyKey);

      await this.repo.completeStepTask({
        taskId: task.id,
        leaseToken: task.leaseToken!,
        workflowId: workflow.id,
        stepSeq,
        stepName,
        result,
      });

      await this.options.onStepExecuted?.({
        workflowId: workflow.id,
        stepSeq,
        stepName,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.repo.failStepTask({
        taskId: task.id,
        leaseToken: task.leaseToken!,
        workflowId: workflow.id,
        stepSeq,
        stepName,
        attempt: task.attempt,
        maxAttempts: task.maxAttempts,
        error: message,
      });
    }
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.pollLoop();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.loopPromise) {
      await this.loopPromise;
      this.loopPromise = null;
    }
  }

  private async pollLoop(): Promise<void> {
    while (this.running) {
      const processed = await this.runNextTask();
      if (!processed && this.running) {
        await new Promise((r) => setTimeout(r, this.pollIntervalMs));
      }
    }
  }
}