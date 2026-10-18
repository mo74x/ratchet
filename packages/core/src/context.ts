import * as crypto from 'crypto';
import { DurationInput, parseDurationMs } from './duration';
import { NewWorkflowEvent, WorkflowEvent } from './events';
import {
  NonDeterminismError,
  StepExecutionError,
  StepOptions,
  WorkflowExplicitFailureError,
  WorkflowSuspendedSignal,
} from './errors';

export type StepHandler<T> = (idempotencyKey: string) => Promise<T> | T;

export interface WaitForSignalOptions {
  timeout?: DurationInput;
}

export interface WorkflowContext {
  readonly workflowId: string;
  step<T>(name: string, fn: StepHandler<T>, options?: StepOptions): Promise<T>;
  sleep(duration: DurationInput): Promise<void>;
  /**
   * Waits for an external signal with `name`.
   * Returns the signal payload, or `null` if the optional `timeout` expires first.
   */
  waitForSignal<T = any>(
    name: string,
    options?: WaitForSignalOptions,
  ): Promise<T | null>;
  now(): Promise<number>;
  random(): Promise<number>;
  uuid(): Promise<string>;
  fail(reason: string): never;
}

interface IndexedStepEvents {
  scheduled?: WorkflowEvent;
  completed?: WorkflowEvent;
  failed?: WorkflowEvent;
  valueRecorded?: WorkflowEvent;
  timerStarted?: WorkflowEvent;
  timerFired?: WorkflowEvent;
  signalWaiting?: WorkflowEvent;
  signalReceived?: WorkflowEvent;
}

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
        case 'TimerStarted':
          bucket.timerStarted = evt;
          break;
        case 'TimerFired':
          bucket.timerFired = evt;
          break;
        case 'SignalWaiting':
          bucket.signalWaiting = evt;
          break;
        case 'SignalReceived':
          bucket.signalReceived = evt;
          break;
      }
      this.historyByStepSeq.set(evt.stepSeq, bucket);
    }
  }

  private nextStepSeq(): number {
    this.currentStepSeq += 1;
    return this.currentStepSeq;
  }

  private describeRecorded(bucket?: IndexedStepEvents): string | null {
    if (!bucket) return null;
    if (bucket.scheduled || bucket.completed || bucket.failed) {
      const evt = bucket.completed ?? bucket.failed ?? bucket.scheduled!;
      return `Step:${evt.name}`;
    }
    if (bucket.valueRecorded) {
      return `ValueRecorded:${bucket.valueRecorded.name}`;
    }
    if (bucket.signalWaiting || bucket.signalReceived) {
      const evt = bucket.signalReceived ?? bucket.signalWaiting!;
      return `Signal:${evt.name}`;
    }
    if (bucket.timerStarted || bucket.timerFired) {
      return `Sleep:timer`;
    }
    return null;
  }

  async step<T>(
    name: string,
    fn: StepHandler<T>,
    options?: StepOptions,
  ): Promise<T> {
    const stepSeq = this.nextStepSeq();
    const recorded = this.historyByStepSeq.get(stepSeq);
    const expectedSignature = `Step:${name}`;
    const existingSignature = this.describeRecorded(recorded);

    if (existingSignature && existingSignature !== expectedSignature) {
      throw new NonDeterminismError({
        workflowId: this.workflowId,
        stepSeq,
        expected: existingSignature,
        actual: expectedSignature,
      });
    }

    if (recorded?.completed) {
      return recorded.completed.payload.result as T;
    }

    if (recorded?.failed) {
      throw new StepExecutionError(
        name,
        stepSeq,
        recorded.failed.payload?.error ?? 'Step failed permanently',
      );
    }

    const timeoutMs =
      options?.startToCloseTimeout !== undefined
        ? parseDurationMs(options.startToCloseTimeout)
        : undefined;

    const alreadyScheduled = Boolean(recorded?.scheduled);
    if (!alreadyScheduled) {
      this.pendingEvents.push({
        stepSeq,
        type: 'StepScheduled',
        name,
        payload: {
          timeoutMs: timeoutMs ?? null,
          maxAttempts: options?.maxAttempts ?? null,
        },
      });
    }

    throw new WorkflowSuspendedSignal({
      kind: 'step',
      stepSeq,
      stepName: name,
      alreadyScheduled,
      handler: fn,
      timeoutMs,
      maxAttempts: options?.maxAttempts,
    });
  }

  async sleep(duration: DurationInput): Promise<void> {
    const stepSeq = this.nextStepSeq();
    const durationMs = parseDurationMs(duration);
    const recorded = this.historyByStepSeq.get(stepSeq);
    const existingSignature = this.describeRecorded(recorded);

    if (existingSignature && existingSignature !== 'Sleep:timer') {
      throw new NonDeterminismError({
        workflowId: this.workflowId,
        stepSeq,
        expected: existingSignature,
        actual: 'Sleep:timer',
      });
    }

    if (recorded?.timerFired) {
      return;
    }

    const alreadyScheduled = Boolean(recorded?.timerStarted);
    if (!alreadyScheduled) {
      this.pendingEvents.push({
        stepSeq,
        type: 'TimerStarted',
        name: 'sleep',
        payload: { durationMs },
      });
    }

    throw new WorkflowSuspendedSignal({
      kind: 'sleep',
      stepSeq,
      durationMs,
      alreadyScheduled,
    });
  }

  async waitForSignal<T = any>(
    name: string,
    options?: WaitForSignalOptions,
  ): Promise<T | null> {
    const stepSeq = this.nextStepSeq();
    const recorded = this.historyByStepSeq.get(stepSeq);
    const expectedSignature = `Signal:${name}`;
    const existingSignature = this.describeRecorded(recorded);

    if (existingSignature && existingSignature !== expectedSignature) {
      throw new NonDeterminismError({
        workflowId: this.workflowId,
        stepSeq,
        expected: existingSignature,
        actual: expectedSignature,
      });
    }

    // 1. Signal arrived and was bound to this stepSeq
    if (recorded?.signalReceived) {
      return (recorded.signalReceived.payload?.payload ?? null) as T;
    }

    // 2. Timeout fired before signal arrived -> deterministic timeout branch returns null
    if (recorded?.timerFired) {
      return null;
    }

    const timeoutMs =
      options?.timeout !== undefined
        ? parseDurationMs(options.timeout)
        : undefined;

    const alreadyScheduled = Boolean(recorded?.signalWaiting);
    if (!alreadyScheduled) {
      this.pendingEvents.push({
        stepSeq,
        type: 'SignalWaiting',
        name,
        payload: { timeoutMs: timeoutMs ?? null },
      });
    }

    throw new WorkflowSuspendedSignal({
      kind: 'signal',
      stepSeq,
      signalName: name,
      timeoutMs,
      alreadyScheduled,
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
    const expectedSignature = `ValueRecorded:${name}`;
    const existingSignature = this.describeRecorded(recorded);

    if (existingSignature && existingSignature !== expectedSignature) {
      throw new NonDeterminismError({
        workflowId: this.workflowId,
        stepSeq,
        expected: existingSignature,
        actual: expectedSignature,
      });
    }

    if (recorded?.valueRecorded) {
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