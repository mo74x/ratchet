import { ReplayWorkflowContext, StepHandler, WorkflowContext } from './context';
import {
  NonDeterminismError,
  SuspensionReason,
  WorkflowExplicitFailureError,
  WorkflowSuspendedSignal,
} from './errors';
import { NewWorkflowEvent, WorkflowEvent } from './events';

export type WorkflowFunction<TInput = any, TOutput = any> = (
  ctx: WorkflowContext,
  input: TInput,
) => Promise<TOutput>;

export type WorkflowTickOutcome<TOutput = any> =
  | {
      status: 'completed';
      output: TOutput;
      newEvents: NewWorkflowEvent[];
    }
  | {
      status: 'suspended';
      suspension: SuspensionReason;
      newEvents: NewWorkflowEvent[];
    }
  | {
      status: 'failed';
      error: Error;
      isNonDeterministic: boolean;
      newEvents: NewWorkflowEvent[];
    };

// Executes a single deterministic replay tick of a workflow function.
export async function runWorkflowTick<TInput, TOutput>(params: {
  workflowId: string;
  workflowFn: WorkflowFunction<TInput, TOutput>;
  input: TInput;
  history: WorkflowEvent[];
}): Promise<WorkflowTickOutcome<TOutput>> {
  const ctx = new ReplayWorkflowContext(params.workflowId, params.history);

  try {
    const output = await params.workflowFn(ctx, params.input);
    ctx.pendingEvents.push({
      stepSeq: null,
      type: 'WorkflowCompleted',
      name: null,
      payload: { output: output ?? null },
    });

    return {
      status: 'completed',
      output,
      newEvents: ctx.pendingEvents,
    };
  } catch (err) {
    if (err instanceof WorkflowSuspendedSignal) {
      return {
        status: 'suspended',
        suspension: err.reason,
        newEvents: ctx.pendingEvents,
      };
    }

    const errorObj = err instanceof Error ? err : new Error(String(err));
    const isNonDeterministic = err instanceof NonDeterminismError;

    ctx.pendingEvents.push({
      stepSeq: null,
      type: 'WorkflowFailed',
      name: errorObj.name,
      payload: {
        error: errorObj.message,
        isNonDeterministic,
        isExplicitFailure: err instanceof WorkflowExplicitFailureError,
      },
    });

    return {
      status: 'failed',
      error: errorObj,
      isNonDeterministic,
      newEvents: ctx.pendingEvents,
    };
  }
}

// Replays the workflow up to `targetStepSeq` using recorded history
export async function resolveStepHandler<TInput>(params: {
  workflowId: string;
  workflowFn: WorkflowFunction<TInput, any>;
  input: TInput;
  history: WorkflowEvent[];
  targetStepSeq: number;
  targetStepName: string;
}): Promise<StepHandler<unknown>> {
  // Filter out any completion/failure at or after targetStepSeq so replay suspends at targetStepSeq
  const truncatedHistory = params.history.filter(
    (evt) =>
      evt.stepSeq === null ||
      evt.stepSeq < params.targetStepSeq ||
      (evt.stepSeq === params.targetStepSeq && evt.type === 'StepScheduled'),
  );

  const outcome = await runWorkflowTick({
    workflowId: params.workflowId,
    workflowFn: params.workflowFn,
    input: params.input,
    history: truncatedHistory,
  });

  if (
    outcome.status === 'suspended' &&
    outcome.suspension.kind === 'step' &&
    outcome.suspension.stepSeq === params.targetStepSeq
  ) {
    if (outcome.suspension.stepName !== params.targetStepName) {
      throw new NonDeterminismError({
        workflowId: params.workflowId,
        stepSeq: params.targetStepSeq,
        expected: `Step:${params.targetStepName}`,
        actual: `Step:${outcome.suspension.stepName}`,
      });
    }
    return outcome.suspension.handler;
  }

  throw new Error(
    `Failed to resolve step handler for workflow=${params.workflowId} step_seq=${params.targetStepSeq} (${params.targetStepName}). Outcome was: ${outcome.status}`,
  );
}