export interface StepOptions {
  /** Maximum time a single step execution attempt may run (e.g. '5s', 5000) */
  startToCloseTimeout?: number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`;
  /** Override default max_attempts (default: 5) */
  maxAttempts?: number;
}
export class NonDeterminismError extends Error {
  public readonly workflowId: string;
  public readonly stepSeq: number;
  public readonly expected: string;
  public readonly actual: string;

  constructor(params: {
    workflowId: string;
    stepSeq: number;
    expected: string;
    actual: string;
  }) {
    super(
      `[NonDeterminismError] Workflow ${params.workflowId} diverged at step_seq=${params.stepSeq}: ` +
        `recorded history has (${params.expected}), but code executed (${params.actual}).`,
    );
    this.name = 'NonDeterminismError';
    this.workflowId = params.workflowId;
    this.stepSeq = params.stepSeq;
    this.expected = params.expected;
    this.actual = params.actual;
  }
}

// Workflow Explicit Failure Error
export class WorkflowExplicitFailureError extends Error {
  constructor(public readonly reason: string) {
    super(`Workflow explicitly failed: ${reason}`);
    this.name = 'WorkflowExplicitFailureError';
  }
}

// Step Execution Errors
export class StepExecutionError extends Error {
  constructor(
    public readonly stepName: string,
    public readonly stepSeq: number,
    message: string,
  ) {
    super(`Step "${stepName}" (step_seq=${stepSeq}) failed: ${message}`);
    this.name = 'StepExecutionError';
  }
}

// Workflow Suspension Reasons
export type SuspensionReason =
  | {
      kind: 'step';
      stepSeq: number;
      stepName: string;
      alreadyScheduled: boolean;
      handler: (idempotencyKey: string) => Promise<unknown> | unknown;
      timeoutMs?: number;
      maxAttempts?: number;
    }
  | {
      kind: 'sleep';
      stepSeq: number;
      durationMs: number;
      alreadyScheduled: boolean;
    }
  | {
      kind: 'signal';
      stepSeq: number;
      signalName: string;
      timeoutMs?: number;
      alreadyScheduled: boolean;
    };

// Internal control-flow exception thrown by WorkflowContext to unwind
export class WorkflowSuspendedSignal extends Error {
  public readonly isRatchetControlSignal = true;

  constructor(public readonly reason: SuspensionReason) {
    super(`Workflow suspended on ${reason.kind} at step_seq=${reason.stepSeq}`);
    this.name = 'WorkflowSuspendedSignal';
  }
}

// Helper to identify ratchet control signals
export function isRatchetControlSignal(err: unknown): boolean {
  return (
    err instanceof WorkflowSuspendedSignal ||
    err instanceof NonDeterminismError ||
    err instanceof WorkflowExplicitFailureError ||
    (typeof err === 'object' &&
      err !== null &&
      (err as any).isRatchetControlSignal === true)
  );
}

export class StepTimeoutError extends Error {
  constructor(
    public readonly stepName: string,
    public readonly timeoutMs: number,
  ) {
    super(`Step "${stepName}" timed out after ${timeoutMs}ms (startToCloseTimeout)`);
    this.name = 'StepTimeoutError';
  }
}

