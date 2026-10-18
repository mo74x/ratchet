import { WorkflowContext, WorkflowRegistry } from '@ratchet/core';
import { Database, RatchetRepository, runMigrations } from '@ratchet/store';
import { RatchetWorker } from '../../apps/worker/src/worker';

jest.setTimeout(120_000);

describe('Phase 2 Chaos & Reliability Suite: Leases, Fencing, Retries & 1,000-Workflow Chaos', () => {
  let db: Database;
  let repo: RatchetRepository;

  beforeAll(async () => {
    db = new Database();
    await runMigrations(db);
    repo = new RatchetRepository(db);
  });

  beforeEach(async () => {
    await db.query(`TRUNCATE TABLE workflows CASCADE`);
  });

  afterAll(async () => {
    await db.close();
  });

  it('rejects late results from a zombie worker via lease_token fencing and records StepCompleted exactly once', async () => {
    const registry = new WorkflowRegistry();
    let callCount = 0;

    registry.register('fencing-test', async (ctx: WorkflowContext) => {
      const val = await ctx.step('contended-step', () => {
        callCount += 1;
        return `result-from-call-${callCount}`;
      });
      return val;
    });

    const wf = await repo.startWorkflow({ type: 'fencing-test', input: {} });

    // 1. Run the initial workflow tick to schedule 'contended-step'
    const setupWorker = new RatchetWorker(repo, registry);
    await setupWorker.runNextTask();

    const zombieCommitOutcomes: boolean[] = [];
    const fastWorkerCommitOutcomes: boolean[] = [];

    // 2. Worker A ("Zombie"): claims with a 0.2s (200ms) lease, heartbeats disabled (10s interval),
    // and stalls for 450ms BEFORE committing.
    const zombieWorker = new RatchetWorker(repo, registry, {
      workerId: 'zombie-worker-A',
      leaseDurationSeconds: 0.2,
      heartbeatIntervalMs: 10_000,
      beforeCommitStep: async () => {
        // Stall past the 200ms lease expiry!
        await new Promise((r) => setTimeout(r, 450));
      },
      onStepExecuted: ({ committed }) => {
        zombieCommitOutcomes.push(committed);
      },
    });

    // Start Zombie Worker A on the step task (do not await yet—it is stalling mid-step)
    const zombiePromise = zombieWorker.runNextTask();

    // Wait 260ms so Zombie Worker A's 200ms lease expires in Postgres
    await new Promise((r) => setTimeout(r, 260));

    // 3. Reaper runs and returns the expired step task to 'pending'
    const reaped = await repo.reapExpiredLeases();
    expect(reaped.requeued).toBe(1);

    // 4. Worker B ("Healthy") claims the reclaimed step task and completes it immediately
    const healthyWorker = new RatchetWorker(repo, registry, {
      workerId: 'healthy-worker-B',
      leaseDurationSeconds: 5,
      onStepExecuted: ({ committed }) => {
        fastWorkerCommitOutcomes.push(committed);
      },
    });
    await healthyWorker.runNextTask();

    // 5. Wait for Zombie Worker A to wake up from its 450ms stall and attempt its stale commit
    await zombiePromise;

    // Healthy Worker B succeeded; Zombie Worker A was fenced out!
    expect(fastWorkerCommitOutcomes).toEqual([true]);
    expect(zombieCommitOutcomes).toEqual([false]);

    // Finish the final workflow completion tick
    await healthyWorker.runNextTask();

    const finalWf = await repo.getWorkflow(wf.id);
    expect(finalWf?.status).toBe('completed');
    // The recorded output MUST be from Worker B (call #2), NOT overwritten by Zombie Worker A (call #1)
    expect(finalWf?.output).toBe('result-from-call-2');

    const history = await repo.getWorkflowHistory(wf.id);
    const stepCompletions = history.filter((e) => e.type === 'StepCompleted');
    expect(stepCompletions).toHaveLength(1);
  });

  it('keeps a long-running step alive past its initial lease duration via background heartbeats', async () => {
    const registry = new WorkflowRegistry();
    let executions = 0;

    registry.register('heartbeat-test', async (ctx: WorkflowContext) => {
      return ctx.step('slow-step', async () => {
        executions += 1;
        // Takes 450ms while lease duration is only 200ms
        await new Promise((r) => setTimeout(r, 450));
        return 'survived-via-heartbeat';
      });
    });

    const wf = await repo.startWorkflow({ type: 'heartbeat-test', input: {} });

    const worker = new RatchetWorker(repo, registry, {
      leaseDurationSeconds: 0.2, // 200ms lease
      heartbeatIntervalMs: 50,   // Heartbeat every 50ms
    });

    // Tick 1: schedule step
    await worker.runNextTask();

    // Run reaper concurrently every 80ms while the 450ms step executes
    const reaperInterval = setInterval(() => {
      repo.reapExpiredLeases().catch(() => {});
    }, 80);

    // Tick 2: execute slow 450ms step
    await worker.runNextTask();
    clearInterval(reaperInterval);

    // Tick 3: complete workflow
    await worker.runNextTask();

    const completed = await repo.getWorkflow(wf.id);
    expect(completed?.status).toBe('completed');
    expect(completed?.output).toBe('survived-via-heartbeat');
    // Because heartbeats extended the lease, the reaper never reclaimed it and it ran only once!
    expect(executions).toBe(1);
  });

  it('retries a failing step up to max_attempts with backoff and lands in dead status', async () => {
    const registry = new WorkflowRegistry();
    let attempts = 0;

    registry.register('poison-step-workflow', async (ctx: WorkflowContext) => {
      return ctx.step('always-fails', () => {
        attempts += 1;
        throw new Error(`Downstream 503 on attempt #${attempts}`);
      });
    });

    const wf = await repo.startWorkflow({
      type: 'poison-step-workflow',
      input: {},
    });

    const worker = new RatchetWorker(repo, registry, {
      retryBaseDelayMs: 10,
      retryMaxDelayMs: 40,
      pollIntervalMs: 10,
    });

    worker.start();

    // Poll until workflow transitions to 'failed'
    let current = await repo.getWorkflow(wf.id);
    while (current && current.status === 'running') {
      await new Promise((r) => setTimeout(r, 30));
      current = await repo.getWorkflow(wf.id);
    }
    await worker.stop();

    expect(attempts).toBe(5); // default max_attempts = 5
    expect(current?.status).toBe('failed');
    expect(current?.error).toContain('Downstream 503 on attempt #5');

    // Verify task row in Postgres landed in status = 'dead'
    const taskRes = await db.query(
      `SELECT status, attempt, max_attempts, last_error
       FROM tasks WHERE workflow_id = $1 AND kind = 'step'`,
      [wf.id],
    );
    expect(taskRes.rows[0]).toEqual({
      status: 'dead',
      attempt: 5,
      max_attempts: 5,
      last_error: 'Downstream 503 on attempt #5',
    });
  });

  it('CHAOS: runs 4 concurrent workers against 1,000 workflows with random worker kills and completes 100% with zero lost steps', async () => {
    const TOTAL_WORKFLOWS = 1000;
    const WORKER_COUNT = 4;

    // External side-effect ledger keyed by idempotencyKey (`${workflowId}:${stepSeq}`)
    const externalLedger = new Map<string, string>();
    let rawHandlerInvocations = 0;
    let workerKills = 0;

    const registry = new WorkflowRegistry();
    registry.register(
      'chaos-payment-pipeline',
      async (ctx: WorkflowContext, input: { index: number }) => {
        const s1 = await ctx.step('debit-account', async (idempotencyKey) => {
          rawHandlerInvocations += 1;
          if (!externalLedger.has(idempotencyKey)) {
            externalLedger.set(idempotencyKey, `debited:${input.index}`);
          }
          return externalLedger.get(idempotencyKey)!;
        });

        const s2 = await ctx.step('credit-merchant', async (idempotencyKey) => {
          rawHandlerInvocations += 1;
          if (!externalLedger.has(idempotencyKey)) {
            externalLedger.set(idempotencyKey, `${s1}->credited:${input.index}`);
          }
          return externalLedger.get(idempotencyKey)!;
        });

        return { settlement: s2 };
      },
    );

    // 1. Bulk-insert 1,000 workflows + WorkflowStarted events + initial workflow tasks
    await db.withTransaction(async (client) => {
      await client.query(
        `WITH inserted_wf AS (
           INSERT INTO workflows (type, status, input)
           SELECT 'chaos-payment-pipeline', 'running', jsonb_build_object('index', g)
           FROM generate_series(1, $1) AS g
           RETURNING id, input
         ),
         inserted_evt AS (
           INSERT INTO workflow_events (workflow_id, seq, step_seq, type, name, payload)
           SELECT id, 1, NULL, 'WorkflowStarted', 'chaos-payment-pipeline', jsonb_build_object('input', input)
           FROM inserted_wf
         )
         INSERT INTO tasks (workflow_id, kind, status)
         SELECT id, 'workflow', 'pending'
         FROM inserted_wf`,
        [TOTAL_WORKFLOWS],
      );
    });

    // 2. Spawn 4 concurrent workers with short 300ms leases
    const makeWorker = (slot: number) =>
      new RatchetWorker(repo, registry, {
        workerId: `chaos-worker-${slot}-${ Date.now()}`,
        pollIntervalMs: 5,
        leaseDurationSeconds: 0.3,
        heartbeatIntervalMs: 80,
        reapIntervalMs: 100,
      });

    const workers: RatchetWorker[] = Array.from({ length: WORKER_COUNT }, (_, i) =>
      makeWorker(i),
    );
    workers.forEach((w) => w.start());

    // 3. Chaos Monkey: every 120ms, randomly kill one worker immediately and replace it
    const chaosInterval = setInterval(() => {
      const victimIdx = Math.floor(Math.random() * WORKER_COUNT);
      workers[victimIdx].killImmediate();
      workerKills += 1;
      workers[victimIdx] = makeWorker(victimIdx);
      workers[victimIdx].start();
    }, 120);

    // 4. Wait until all 1,000 workflows reach 'completed'
    let completedCount = 0;
    while (completedCount < TOTAL_WORKFLOWS) {
      await new Promise((r) => setTimeout(r, 150));
      await repo.reapExpiredLeases(500);
      const res = await db.query<{ count: string }>(
        `SELECT COUNT(*) AS count FROM workflows WHERE status = 'completed'`,
      );
      completedCount = Number(res.rows[0].count);
    }

    clearInterval(chaosInterval);
    await Promise.all(workers.map((w) => w.stop()));

    // 5. Assertions
    expect(completedCount).toBe(TOTAL_WORKFLOWS);
    expect(workerKills).toBeGreaterThan(5);

    // Every single workflow had 2 steps -> exactly 2,000 unique idempotent side effects in the ledger
    expect(externalLedger.size).toBe(TOTAL_WORKFLOWS * 2);

    // Verify zero StepCompleted duplicates in workflow_events across all 1,000 workflows
    const eventCountRes = await db.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM workflow_events WHERE type = 'StepCompleted'`,
    );
    expect(Number(eventCountRes.rows[0].count)).toBe(TOTAL_WORKFLOWS * 2);

    console.log(
      `[Chaos Summary] Workflows: ${completedCount}/${TOTAL_WORKFLOWS} completed | ` +
        `Worker Kills: ${workerKills} | Unique Side Effects: ${externalLedger.size} | ` +
        `Total Handler Executions (incl. lease-expiry retries): ${rawHandlerInvocations}`,
    );
  });
});