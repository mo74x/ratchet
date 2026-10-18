import { Database, RatchetRepository } from '@ratchet/store';
import { createDefaultRegistry } from './workflows';
import { RatchetWorker } from './worker';

async function bootstrap() {
  const db = new Database();
  const repo = new RatchetRepository(db);
  const registry = createDefaultRegistry((stepName, stepSeq, key) => {
    console.log(`[Worker] Executing step #${stepSeq} "${stepName}" (idempotencyKey=${key})`);
  });

  const worker = new RatchetWorker(repo, registry, { pollIntervalMs: 100 });
  console.log('[Worker] Starting Ratchet worker loop...');
  worker.start();

  process.on('SIGINT', async () => {
    console.log('[Worker] Shutting down...');
    await worker.stop();
    await db.close();
    process.exit(0);
  });
}

bootstrap();
