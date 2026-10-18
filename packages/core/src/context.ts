import * as crypto from 'crypto';
import { NewWorkflowEvent, WorkflowEvent } from './events';
import {
  NonDeterminismError,
  StepExecutionError,
  WorkflowExplicitFailureError,
  WorkflowSuspendedSignal,
} from './errors';

export type StepHandler<T> = (idempotencyKey: string) => Promise<T> | T;


export interface WorkflowContext {
  readonly workflowId: string;
  step<T>(name: string, fn: StepHandler<T>): Promise<T>;
  now(): Promise<number>;
  random(): Promise<number>;
  uuid(): Promise<string>;
  fail(reason: string): never;
}

// Helper types for indexing
interface IndexedStepEvents {
  scheduled?: WorkflowEvent;
  completed?: WorkflowEvent;
  failed?: WorkflowEvent;
  valueRecorded?: WorkflowEvent;
}

// Replay Workflow Context
export class ReplayWorkflowContext implements WorkflowContext {
  private currentStepSeq = 0;
  private readonly historyByStepSeq = new Map<number, IndexedStepEvents>();
  public readonly pendingEvents: NewWorkflowEvent[] = [];

  constructor(
    public readonly workflowId: string,
    history: WorkflowEvent[],
  ) {
    this.indexHistory(history);
  }

  private indexHistory(history: WorkflowEvent[]): void {
    for (const evt of history) {
      if (evt.stepSeq === null || evt.stepSeq === undefined) continue;
      const bucket = this.historyByStepSeq.get(evt.stepSeq) ?? {};
      switch (evt.type) {
        case 'StepScheduled':
          bucket.scheduled = evt;
          break;
        case 'StepCompleted':
          bucket.completed = evt;
          break;
        case 'StepFailed':
          bucket.failed = evt;
          break;
        case 'ValueRecorded':
          bucket.valueRecorded = evt;
          break;
      }
      this.historyByStepSeq.set(evt.stepSeq, bucket);
    }
  }

  private nextStepSeq(): number {
    this.currentStepSeq += 1;
    return this.currentStepSeq;
  }

  async step<T>(name: string, fn: StepHandler<T>): Promise<T> {
    const stepSeq = this.nextStepSeq();
    const recorded = this.historyByStepSeq.get(stepSeq);

    // 1. Determinism Guard: Check if this stepSeq was previously used for a side-effect value (now/random/uuid)
    if (recorded?.valueRecorded) {
      throw new NonDeterminismError({
        workflowId: this.workflowId,
        stepSeq,
        expected: `ValueRecorded:${recorded.valueRecorded.name}`,
        actual: `Step:${name}`,
      });
    }

    // 2. Determinism Guard: Verify step name matches any recorded StepScheduled / StepCompleted / StepFailed
    const existingStepEvt =
      recorded?.completed ?? recorded?.failed ?? recorded?.scheduled;
    if (existingStepEvt && existingStepEvt.name !== name) {
      throw new NonDeterminismError({
        workflowId: this.workflowId,
        stepSeq,
        expected: `Step:${existingStepEvt.name}`,
        actual: `Step:${name}`,
      });
    }

    // 3. Fast-forward if already completed
    if (recorded?.completed) {
      return recorded.completed.payload.result as T;
    }

    // 4. Re-throw recorded permanent step failure if step exhausted retries
    if (recorded?.failed) {
      throw new StepExecutionError(
        name,
        stepSeq,
        recorded.failed.payload?.error ?? 'Step failed permanently',
      );
    }

    // 5. Not completed yet: emit StepScheduled if not already scheduled, then suspend
    const alreadyScheduled = Boolean(recorded?.scheduled);
    if (!alreadyScheduled) {
      this.pendingEvents.push({
        stepSeq,
        type: 'StepScheduled',
        name,
        payload: {},
      });
    }

    throw new WorkflowSuspendedSignal({
      kind: 'step',
      stepSeq,
      stepName: name,
      alreadyScheduled,
      handler: fn,
    });
  }

  async now(): Promise<number> {
    return this.recordInlineValue('ctx.now', () => Date.now());
  }

  async random(): Promise<number> {
    return this.recordInlineValue('ctx.random', () => Math.random());
  }

  async uuid(): Promise<string> {
    return this.recordInlineValue('ctx.uuid', () => crypto.randomUUID());
  }

  fail(reason: string): never {
    throw new WorkflowExplicitFailureError(reason);
  }

  private recordInlineValue<T>(name: string, generator: () => T): T {
    const stepSeq = this.nextStepSeq();
    const recorded = this.historyByStepSeq.get(stepSeq);

    // Determinism Guard: Ensure this stepSeq wasn't previously a Step
    const existingStepEvt =
      recorded?.scheduled ?? recorded?.completed ?? recorded?.failed;
    if (existingStepEvt) {
      throw new NonDeterminismError({
        workflowId: this.workflowId,
        stepSeq,
        expected: `Step:${existingStepEvt.name}`,
        actual: `ValueRecorded:${name}`,
      });
    }

    if (recorded?.valueRecorded) {
      if (recorded.valueRecorded.name !== name) {
        throw new NonDeterminismError({
          workflowId: this.workflowId,
          stepSeq,
          expected: `ValueRecorded:${recorded.valueRecorded.name}`,
          actual: `ValueRecorded:${name}`,
        });
      }
      return recorded.valueRecorded.payload.value as T;
    }

    const value = generator();
    this.pendingEvents.push({
      stepSeq,
      type: 'ValueRecorded',
      name,
      payload: { value },
    });
    return value;
  }
}