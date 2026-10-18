import {
  NonDeterminismError,
  resolveStepHandler,
  runWorkflowTick,
  WorkflowContext,
  WorkflowEvent,
} from '../index';

describe('@ratchet/core replay engine', () => {
  const workflowId = '11111111-2222-3333-4444-555555555555';

  it('suspends at step 1 on empty history and emits StepScheduled', async () => {
    const step1Spy = jest.fn(() => 'step-1-result');

    const wf = async (ctx: WorkflowContext, input: { email: string }) => {
      const a = await ctx.step('step-1', step1Spy);
      return `${input.email}:${a}`;
    };

    const tick = await runWorkflowTick({
      workflowId,
      workflowFn: wf,
      input: { email: 'dev@ratchet.io' },
      history: [],
    });

    expect(tick.status).toBe('suspended');
    if (tick.status === 'suspended' && tick.suspension.kind === 'step') {
      expect(tick.suspension.stepSeq).toBe(1);
      expect(tick.suspension.stepName).toBe('step-1');
      expect(tick.suspension.alreadyScheduled).toBe(false);
    }
    // The step handler must NOT run inside the workflow decision tick
    expect(step1Spy).not.toHaveBeenCalled();
    expect(tick.newEvents).toEqual([
      {
        stepSeq: 1,
        type: 'StepScheduled',
        name: 'step-1',
        payload: {
          timeoutMs: null,
          maxAttempts: null,
        },
      },
    ]);
  });

  it('fast-forwards steps 1-3 from history without re-running them and resumes at step 4 with hydrated closure state', async () => {
    const spy1 = jest.fn(() => 'r1');
    const spy2 = jest.fn(() => 'r2');
    const spy3 = jest.fn(() => 'r3');
    const spy4 = jest.fn((idempotencyKey: string) => `r4-key:${idempotencyKey}`);
    const spy5 = jest.fn(() => 'r5');

    const fiveStepWorkflow = async (ctx: WorkflowContext, input: { seed: string }) => {
      const s1 = await ctx.step('step-1', spy1);
      const s2 = await ctx.step('step-2', spy2);
      const s3 = await ctx.step('step-3', spy3);
      const s4 = await ctx.step('step-4', (key) => {
        // Closure uses results from steps 1, 2, 3
        return `${input.seed}|${s1}|${s2}|${s3}|${spy4(key)}`;
      });
      const s5 = await ctx.step('step-5', spy5);
      return `${s4}|${s5}`;
    };

    // Simulate crash after step 3 completed
    const historyAfterStep3: WorkflowEvent[] = [
      { workflowId, seq: 1, stepSeq: 1, type: 'StepScheduled', name: 'step-1', payload: {} },
      { workflowId, seq: 2, stepSeq: 1, type: 'StepCompleted', name: 'step-1', payload: { result: 'recorded-1' } },
      { workflowId, seq: 3, stepSeq: 2, type: 'StepScheduled', name: 'step-2', payload: {} },
      { workflowId, seq: 4, stepSeq: 2, type: 'StepCompleted', name: 'step-2', payload: { result: 'recorded-2' } },
      { workflowId, seq: 5, stepSeq: 3, type: 'StepScheduled', name: 'step-3', payload: {} },
      { workflowId, seq: 6, stepSeq: 3, type: 'StepCompleted', name: 'step-3', payload: { result: 'recorded-3' } },
    ];

    const tick = await runWorkflowTick({
      workflowId,
      workflowFn: fiveStepWorkflow,
      input: { seed: 'init' },
      history: historyAfterStep3,
    });

    // Steps 1-3 must never be called during replay
    expect(spy1).not.toHaveBeenCalled();
    expect(spy2).not.toHaveBeenCalled();
    expect(spy3).not.toHaveBeenCalled();

    // Workflow suspends at step 4
    expect(tick.status).toBe('suspended');
    if (tick.status === 'suspended' && tick.suspension.kind === 'step') {
      expect(tick.suspension.stepSeq).toBe(4);
      expect(tick.suspension.stepName).toBe('step-4');
    }

    // Now resolve and execute step 4's handler as a worker would
    const step4Handler = await resolveStepHandler({
      workflowId,
      workflowFn: fiveStepWorkflow,
      input: { seed: 'init' },
      history: historyAfterStep3,
      targetStepSeq: 4,
      targetStepName: 'step-4',
    });

    const step4Result = await step4Handler(`${workflowId}:4`);
    expect(step4Result).toBe(
      `init|recorded-1|recorded-2|recorded-3|r4-key:${workflowId}:4`,
    );
  });

  it('triggers NonDeterminismError when step order is modified mid-flight', async () => {
    const mutatedWorkflow = async (ctx: WorkflowContext) => {
      // Developer swapped step-1 and step-2 in code while workflow was in-flight!
      await ctx.step('step-2', () => 'two');
      await ctx.step('step-1', () => 'one');
    };

    const existingHistory: WorkflowEvent[] = [
      { workflowId, seq: 1, stepSeq: 1, type: 'StepScheduled', name: 'step-1', payload: {} },
      { workflowId, seq: 2, stepSeq: 1, type: 'StepCompleted', name: 'step-1', payload: { result: 'one' } },
    ];

    const tick = await runWorkflowTick({
      workflowId,
      workflowFn: mutatedWorkflow,
      input: {},
      history: existingHistory,
    });

    expect(tick.status).toBe('failed');
    if (tick.status === 'failed') {
      expect(tick.isNonDeterministic).toBe(true);
      expect(tick.error).toBeInstanceOf(NonDeterminismError);
      expect(tick.error.message).toContain('step_seq=1');
      expect(tick.error.message).toContain('Step:step-1');
      expect(tick.error.message).toContain('Step:step-2');
    }
  });

  it('records ctx.now(), ctx.random(), and ctx.uuid() once and replays them deterministically', async () => {
    const wf = async (ctx: WorkflowContext) => {
      const ts = await ctx.now();
      const rand = await ctx.random();
      const id = await ctx.uuid();
      const stepRes = await ctx.step('final-step', () => `${ts}-${rand}-${id}`);
      return stepRes;
    };

    // First tick: records the 3 deterministic values, then suspends on 'final-step'
    const tick1 = await runWorkflowTick({
      workflowId,
      workflowFn: wf,
      input: {},
      history: [],
    });

    expect(tick1.status).toBe('suspended');
    expect(tick1.newEvents).toHaveLength(4); // 3 ValueRecorded + 1 StepScheduled

    const recordedNow = tick1.newEvents[0].payload.value;
    const recordedRand = tick1.newEvents[1].payload.value;
    const recordedUuid = tick1.newEvents[2].payload.value;

    // Convert newEvents into persisted WorkflowEvents + complete step 4
    const history: WorkflowEvent[] = [
      ...tick1.newEvents.map((e, idx) => ({
        workflowId,
        seq: idx + 1,
        ...e,
      })),
      {
        workflowId,
        seq: 5,
        stepSeq: 4,
        type: 'StepCompleted',
        name: 'final-step',
        payload: { result: `${recordedNow}-${recordedRand}-${recordedUuid}` },
      },
    ];

    // Second tick: replays ctx.now(), ctx.random(), ctx.uuid() and completes
    const tick2 = await runWorkflowTick({
      workflowId,
      workflowFn: wf,
      input: {},
      history,
    });

    expect(tick2.status).toBe('completed');
    if (tick2.status === 'completed') {
      expect(tick2.output).toBe(`${recordedNow}-${recordedRand}-${recordedUuid}`);
      expect(tick2.newEvents).toEqual([
        {
          stepSeq: null,
          type: 'WorkflowCompleted',
          name: null,
          payload: { output: `${recordedNow}-${recordedRand}-${recordedUuid}` },
        },
      ]);
    }
  });
});