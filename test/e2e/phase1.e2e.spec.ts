import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { WorkflowContext, WorkflowRegistry } from '@ratchet/core';
import { Database, RatchetRepository, runMigrations } from '@ratchet/store';
import { RatchetWorker } from '../../apps/worker/src/worker';
import { AppModule } from '../../apps/api/src/app.module';
import { WorkflowsController } from '../../apps/api/src/workflows.controller';
import { createDefaultRegistry } from '../../examples/workflows';

describe('Phase 1 E2E: Durable Core & Crash Recovery', () => {
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

  it('survives a worker crash after step 3 and finishes steps 4-5 without re-running steps 1-3', async () => {
    const executionCounts: Record<string, number> = {};
    const recordStepCall = (stepName: string) => {
      executionCounts[stepName] = (executionCounts[stepName] ?? 0) + 1;
    };

    // 1. Start workflow via NestJS controller (POST /workflows)
    const created = await controller.startWorkflow({
      type: 'five-step-pipeline',
      input: { orderId: 'ORD-9001' },
      idempotencyKey: 'order-9001-key',
    });
    expect(created.status).toBe('running');

    // 2. Boot Worker #1 and run until step 3 ('charge-payment') finishes, then kill Worker #1
    const registry1 = createDefaultRegistry(recordStepCall);
    let lastCompletedStep = 0;
    const worker1 = new RatchetWorker(repo, registry1, {
      onStepExecuted: ({ stepSeq }) => {
        lastCompletedStep = stepSeq;
      },
    });

    while (lastCompletedStep < 3) {
      const progressed = await worker1.runNextTask();
      expect(progressed).toBe(true);
    }

    // Verify Worker #1 only ran steps 1, 2, 3 and workflow is still 'running'
    expect(executionCounts).toEqual({
      'validate-order': 1,
      'reserve-inventory': 1,
      'charge-payment': 1,
    });
    const midStatus = await controller.getWorkflow(created.id);
    expect(midStatus.status).toBe('running');

    // 3. Simulate Worker #1 crash and boot a fresh Worker #2 with empty memory
    const registry2 = createDefaultRegistry(recordStepCall);
    const worker2 = new RatchetWorker(repo, registry2);

    while (await worker2.runNextTask()) {
      // Drain remaining workflow and step tasks to completion
    }

    // 4. Verify Steps 1-3 were NEVER re-executed, and Steps 4-5 ran exactly once!
    expect(executionCounts).toEqual({
      'validate-order': 1,
      'reserve-inventory': 1,
      'charge-payment': 1,
      'generate-invoice': 1,
      'dispatch-shipment': 1,
    });

    // 5. Verify final output via GET /workflows/:id
    const completed = await controller.getWorkflow(created.id);
    expect(completed.status).toBe('completed');
    expect(completed.output).toEqual({
      finalState: 'validated:ORD-9001->reserved->charged->invoiced->shipped',
    });

    // 6. Verify full event history via GET /workflows/:id/history
    const historyRes = await controller.getWorkflowHistory(created.id);
    expect(historyRes.events.map((e) => `${e.type}:${e.name ?? ''}`)).toEqual([
      'WorkflowStarted:five-step-pipeline',
      'StepScheduled:validate-order',
      'StepCompleted:validate-order',
      'StepScheduled:reserve-inventory',
      'StepCompleted:reserve-inventory',
      'StepScheduled:charge-payment',
      'StepCompleted:charge-payment',
      'StepScheduled:generate-invoice',
      'StepCompleted:generate-invoice',
      'StepScheduled:dispatch-shipment',
      'StepCompleted:dispatch-shipment',
      'WorkflowCompleted:',
    ]);
  });

  it('halts workflow with NonDeterminismError when code step order changes mid-flight', async () => {
    const created = await controller.startWorkflow({
      type: 'mutating-workflow',
      input: {},
    });

    // Version 1 of the workflow code runs step-A then step-B
    const v1Registry = new WorkflowRegistry();
    v1Registry.register('mutating-workflow', async (ctx: WorkflowContext) => {
      await ctx.step('step-A', () => 'A');
      await ctx.step('step-B', () => 'B');
    });

    const workerV1 = new RatchetWorker(repo, v1Registry);
    // Run 1 workflow tick (schedules step-A) + 1 step execution (completes step-A)
    await workerV1.runNextTask();
    await workerV1.runNextTask();

    // Now developer deploys Version 2 where step-X was inserted before step-A!
    const v2Registry = new WorkflowRegistry();
    v2Registry.register('mutating-workflow', async (ctx: WorkflowContext) => {
      await ctx.step('step-X-inserted-before-A', () => 'X');
      await ctx.step('step-A', () => 'A');
      await ctx.step('step-B', () => 'B');
    });

    const workerV2 = new RatchetWorker(repo, v2Registry);
    await workerV2.runNextTask();

    const failedWf = await controller.getWorkflow(created.id);
    expect(failedWf.status).toBe('failed');
    expect(failedWf.error).toContain('[NonDeterminismError]');
    expect(failedWf.error).toContain('step_seq=1');
  });
});