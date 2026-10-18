import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { WorkflowContext, WorkflowRegistry } from '@ratchet/core';
import { Database, RatchetRepository, runMigrations } from '@ratchet/store';
import { RatchetWorker } from '../../apps/worker/src/worker';
import { RatchetTimerService } from '../../apps/timer/src/timer.service';
import { AppModule } from '../../apps/api/src/app.module';
import { WorkflowsController } from '../../apps/api/src/workflows.controller';
import { createDefaultRegistry } from '../../examples/workflows';

describe('Phase 3 E2E: Durable Timers, Signals, Early Buffering & Step Timeouts', () => {
  let app: INestApplication;
  let controller: WorkflowsController;
  let db: Database;
  let repo: RatchetRepository;

  beforeAll(async () => {
    db = new Database();
    await runMigrations(db);
    repo = new RatchetRepository(db);

    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.init();
    controller = app.get(WorkflowsController);
  });

  beforeEach(async () => {
    await db.query(`TRUNCATE TABLE workflows CASCADE`);
  });

  afterAll(async () => {
    await app?.close();
    await db?.close();
  });

  it('survives a full system shutdown during ctx.sleep() and wakes on time without repeating prior steps', async () => {
    let step1Calls = 0;
    let step2Calls = 0;

    const registry = new WorkflowRegistry();
    registry.register('sleep-restart-wf', async (ctx: WorkflowContext) => {
      const before = await ctx.step('before-sleep', () => {
        step1Calls += 1;
        return 'step1-done';
      });
      await ctx.sleep('300ms');
      const after = await ctx.step('after-sleep', () => {
        step2Calls += 1;
        return `${before}->woke-up`;
      });
      return after;
    });

    const wf = await controller.startWorkflow({
      type: 'sleep-restart-wf',
      input: {},
    });

    // Worker 1 executes 'before-sleep' and suspends at ctx.sleep('300ms')
    const worker1 = new RatchetWorker(repo, registry);
    await worker1.runNextTask(); // schedules before-sleep
    await worker1.runNextTask(); // runs before-sleep
    await worker1.runNextTask(); // replays, hits ctx.sleep('300ms'), enters status = 'waiting'

    const waitingWf = await controller.getWorkflow(wf.id);
    expect(waitingWf.status).toBe('waiting');
    expect(step1Calls).toBe(1);
    expect(step2Calls).toBe(0);

    // Simulate full system shutdown while workflow is sleeping in Postgres
    const sleepStart = Date.now();
    await new Promise((r) => setTimeout(r, 320));

    // Boot fresh Timer Service + Worker 2 after restart
    const timerService = new RatchetTimerService(repo);
    const firedCount = await timerService.tick();
    expect(firedCount).toBe(1);

    const worker2 = new RatchetWorker(repo, registry);
    while (await worker2.runNextTask()) {
      // Drain wakeup tick + after-sleep step + completion tick
    }
    const wakeElapsedMs = Date.now() - sleepStart;

    const completedWf = await controller.getWorkflow(wf.id);
    expect(completedWf.status).toBe('completed');
    expect(completedWf.output).toBe('step1-done->woke-up');
    expect(step1Calls).toBe(1); // Never re-ran before-sleep!
    expect(step2Calls).toBe(1);
    // Woke within documented tolerance
    expect(wakeElapsedMs).toBeLessThan(600);
  });

  it('delivers a signal sent BEFORE the workflow reaches waitForSignal (early signal race)', async () => {
    const stepCalls: string[] = [];
    const registry = createDefaultRegistry((stepName) => {
      stepCalls.push(stepName);
    });

    // 1. Start the onboard-customer workflow
    const wf = await controller.startWorkflow({
      type: 'onboard-customer',
      input: {
        email: 'early@ratchet.io',
        welcomeDelay: '50ms',
        kycTimeout: '5s',
      },
    });

    // 2. Immediately send the 'kyc-approved' signal BEFORE any worker has even run step 1!
    const sigRes = await controller.sendSignal(wf.id, {
      name: 'kyc-approved',
      payload: { approvedBy: 'compliance-officer-1' },
    });
    expect(sigRes.deliveredImmediately).toBe(false); // Buffered in mailbox

    // 3. Now run worker + timer service
    const worker = new RatchetWorker(repo, registry, { pollIntervalMs: 15 });
    const timer = new RatchetTimerService(repo, { pollIntervalMs: 15 });
    worker.start();
    timer.start();

    let status = (await controller.getWorkflow(wf.id)).status;
    while (status === 'running' || status === 'waiting') {
      await new Promise((r) => setTimeout(r, 25));
      status = (await controller.getWorkflow(wf.id)).status;
    }

    await worker.stop();
    await timer.stop();

    const finalWf = await controller.getWorkflow(wf.id);
    expect(finalWf.status).toBe('completed');
    expect(finalWf.output).toEqual({
      accountId: 'acc_early',
      status: 'active',
      approvedBy: 'compliance-officer-1',
    });
    expect(stepCalls).toEqual(['create-account', 'send-welcome', 'activate']);
  });

  it('takes the deterministic timeout branch when waitForSignal times out, and never fires a timer twice under concurrent pollers', async () => {
    const registry = createDefaultRegistry();

    const wf = await controller.startWorkflow({
      type: 'onboard-customer',
      input: {
        email: 'timeout@ratchet.io',
        welcomeDelay: '20ms',
        kycTimeout: '120ms',
      },
    });

    const worker = new RatchetWorker(repo, registry, { pollIntervalMs: 10 });
    // Run 4 concurrent timer services polling the same database to verify SKIP LOCKED deduplication
    const timers = Array.from(
      { length: 4 },
      () => new RatchetTimerService(repo, { pollIntervalMs: 10 }),
    );

    worker.start();
    timers.forEach((t) => t.start());

    let current = await controller.getWorkflow(wf.id);
    while (current.status === 'running' || current.status === 'waiting') {
      await new Promise((r) => setTimeout(r, 25));
      current = await controller.getWorkflow(wf.id);
    }

    await worker.stop();
    await Promise.all(timers.map((t) => t.stop()));

    expect(current.status).toBe('failed');
    expect(current.error).toContain('KYC timeout');

    // Verify each timer (step_seq=3 for sleep, step_seq=4 for waitForSignal timeout) fired exactly once
    const history = await controller.getWorkflowHistory(wf.id);
    const timerFiredEvents = history.events.filter((e) => e.type === 'TimerFired');
    expect(timerFiredEvents).toHaveLength(2);
    expect(timerFiredEvents.map((e) => e.stepSeq)).toEqual([3, 4]);
  });

  it('enforces per-step startToCloseTimeout when a step hangs', async () => {
    const registry = new WorkflowRegistry();
    let attempts = 0;

    registry.register('hanging-step-wf', async (ctx: WorkflowContext) => {
      return ctx.step(
        'slow-external-api',
        async () => {
          attempts += 1;
          await new Promise((r) => setTimeout(r, 300));
          return 'too-late';
        },
        { startToCloseTimeout: '60ms', maxAttempts: 2 },
      );
    });

    const wf = await controller.startWorkflow({
      type: 'hanging-step-wf',
      input: {},
    });

    const worker = new RatchetWorker(repo, registry, {
      pollIntervalMs: 10,
      retryBaseDelayMs: 10,
      retryMaxDelayMs: 20,
    });
    worker.start();

    let current = await controller.getWorkflow(wf.id);
    while (current.status === 'running') {
      await new Promise((r) => setTimeout(r, 25));
      current = await controller.getWorkflow(wf.id);
    }
    await worker.stop();

    expect(attempts).toBe(2);
    expect(current.status).toBe('failed');
    expect(current.error).toContain('timed out after 60ms (startToCloseTimeout)');
  });
});