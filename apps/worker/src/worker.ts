import {
  resolveStepHandler,
  runWorkflowTick,
  WorkflowRegistry,
  calculateBackoffWithJitter,
  StepTimeoutError,
} from '@ratchet/core';
import { RatchetRepository, TaskRecord } from '@ratchet/store';

export interface WorkerOptions {
  workerId?: string;
  pollIntervalMs?: number;
  leaseDurationSeconds?: number;
  heartbeatIntervalMs?: number;
  reapIntervalMs?: number;
  retryBaseDelayMs?: number;
  retryMaxDelayMs?: number;
  onStepExecuted?: (info: {
    workerId: string;
    workflowId: string;
    stepSeq: number;
    stepName: string;
    attempt: number;
    committed: boolean;
  }) => void | Promise<void>;
  beforeCommitStep?: (info: {
    workerId: string;
    task: TaskRecord;
    result: unknown;
  }) => void | Promise<void>;
}
export class RatchetWorker {
  public readonly workerId: string;
  private running = false;
  private loopPromise: Promise<void> | null = null;
  private reaperTimer: NodeJS.Timeout | null = null;

  private readonly pollIntervalMs: number;
  private readonly leaseDurationSeconds: number;
  private readonly heartbeatIntervalMs: number;
  private readonly reapIntervalMs: number;
  private readonly retryBaseDelayMs: number;
  private readonly retryMaxDelayMs: number;

  constructor(
    private readonly repo: RatchetRepository,
    private readonly registry: WorkflowRegistry,
    private readonly options: WorkerOptions = {},
  ) {
    this.workerId =
      options.workerId ?? `worker-${Math.random().toString(36).slice(2, 8)}`;
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
    this.leaseDurationSeconds = options.leaseDurationSeconds ?? 30;
    this.heartbeatIntervalMs =
      options.heartbeatIntervalMs ??
      Math.max(50, Math.floor((this.leaseDurationSeconds * 1000) / 3));
    this.reapIntervalMs = options.reapIntervalMs ?? 500;
    this.retryBaseDelayMs = options.retryBaseDelayMs ?? 200;
    this.retryMaxDelayMs = options.retryMaxDelayMs ?? 10_000;
  }

  /**
     * Claims and executes a single due task with active lease heartbeating.
     */
  async runNextTask(): Promise<boolean> {
    const task = await this.repo.claimNextTask(this.leaseDurationSeconds);
    if (!task || !task.leaseToken) {
      return false;
    }

    const stopHeartbeat = this.startHeartbeat(task.id, task.leaseToken);
    try {
      if (task.kind === 'workflow') {
        await this.handleWorkflowTask(task);
      } else if (task.kind === 'step') {
        await this.handleStepTask(task);
      }
    } finally {
      stopHeartbeat();
    }

    return true;
  }

  private startHeartbeat(taskId: string, leaseToken: string): () => void {
    let active = true;
    const timer = setInterval(async () => {
      if (!active) return;
      try {
        const ok = await this.repo.heartbeatTask(
          taskId,
          leaseToken,
          this.leaseDurationSeconds,
        );
        if (!ok) {
          active = false;
          clearInterval(timer);
        }
      } catch {
        // Ignore transient DB errors during heartbeat; fence will guard commit
      }
    }, this.heartbeatIntervalMs);

    timer.unref?.();

    return () => {
      active = false;
      clearInterval(timer);
    };
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
    }else if (tick.status === 'suspended') {
      const { suspension } = tick;
      if (suspension.kind === 'step') {
        await this.repo.completeWorkflowTick({
          taskId: task.id,
          leaseToken: task.leaseToken!,
          workflowId: workflow.id,
          newEvents: tick.newEvents,
          outcome: {
            status: 'suspended_step',
            stepSeq: suspension.stepSeq,
            stepName: suspension.stepName,
            alreadyScheduled: suspension.alreadyScheduled,
            timeoutMs: suspension.timeoutMs,
            maxAttempts: suspension.maxAttempts,
          },
        });
      } else if (suspension.kind === 'sleep') {
        await this.repo.completeWorkflowTick({
          taskId: task.id,
          leaseToken: task.leaseToken!,
          workflowId: workflow.id,
          newEvents: tick.newEvents,
          outcome: {
            status: 'suspended_sleep',
            stepSeq: suspension.stepSeq,
            durationMs: suspension.durationMs,
          },
        });
      } else if (suspension.kind === 'signal') {
        await this.repo.completeWorkflowTick({
          taskId: task.id,
          leaseToken: task.leaseToken!,
          workflowId: workflow.id,
          newEvents: tick.newEvents,
          outcome: {
            status: 'suspended_signal',
            stepSeq: suspension.stepSeq,
            signalName: suspension.signalName,
            timeoutMs: suspension.timeoutMs,
          },
        });
      }
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
      const result = await this.executeWithTimeout(
        () => handler(idempotencyKey),
        stepName,
        task.timeoutMs,
      );

      if (this.options.beforeCommitStep) {
        await this.options.beforeCommitStep({
          workerId: this.workerId,
          task,
          result,
        });
      }

      const committed = await this.repo.completeStepTask({
        taskId: task.id,
        leaseToken: task.leaseToken!,
        workflowId: workflow.id,
        stepSeq,
        stepName,
        result,
      });

      await this.options.onStepExecuted?.({
        workerId: this.workerId,
        workflowId: workflow.id,
        stepSeq,
        stepName,
        attempt: task.attempt,
        committed,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const retryDelayMs = calculateBackoffWithJitter(task.attempt, {
        baseDelayMs: this.retryBaseDelayMs,
        maxDelayMs: this.retryMaxDelayMs,
      });

      await this.repo.failStepTask({
        taskId: task.id,
        leaseToken: task.leaseToken!,
        workflowId: workflow.id,
        stepSeq,
        stepName,
        attempt: task.attempt,
        maxAttempts: task.maxAttempts,
        error: message,
        retryDelayMs,
      });
    }
  }

  start(): void {
    if (this.running) return;
    this.running = true;

    this.reaperTimer = setInterval(async () => {
      if (!this.running) return;
      try {
        await this.repo.reapExpiredLeases();
      } catch {
        // Ignore transient reaper contention
      }
    }, this.reapIntervalMs);
    this.reaperTimer.unref?.();

    this.loopPromise = this.pollLoop();
  }

  //used in testing
  async stop(): Promise<void> {
    this.running = false;
    if (this.reaperTimer) {
      clearInterval(this.reaperTimer);
      this.reaperTimer = null;
    }
    if (this.loopPromise) {
      await this.loopPromise;
      this.loopPromise = null;
    }
  }

  //used in testing
  killImmediate(): void {
    this.running = false;
    if (this.reaperTimer) {
      clearInterval(this.reaperTimer);
      this.reaperTimer = null;
    }
    this.loopPromise = null;
  }
  
  private async executeWithTimeout<T>(
    fn: () => Promise<T> | T,
    stepName: string,
    timeoutMs: number | null,
  ): Promise<T> {
    if (!timeoutMs || timeoutMs <= 0) {
      return await fn();
    }

    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        Promise.resolve(fn()),
        new Promise<T>((_, reject) => {
          timer = setTimeout(
            () => reject(new StepTimeoutError(stepName, timeoutMs)),
            timeoutMs,
          );
          timer.unref?.();
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private async pollLoop(): Promise<void> {
    while (this.running) {
      try {
        const processed = await this.runNextTask();
        if (!processed && this.running) {
          await new Promise((r) => setTimeout(r, this.pollIntervalMs));
        }
      } catch {
        if (this.running) {
          await new Promise((r) => setTimeout(r, this.pollIntervalMs));
        }
      }
    }
  }
}