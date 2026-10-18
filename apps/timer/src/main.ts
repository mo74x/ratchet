import { Database, RatchetRepository } from '@ratchet/store';
import { RatchetTimerService } from './timer.service';

async function bootstrap() {
  const db = new Database();
  const repo = new RatchetRepository(db);
  const timerService = new RatchetTimerService(repo, { pollIntervalMs: 100 });

  console.log('[Timer] Starting Ratchet timer service...');
  timerService.start();

  process.on('SIGINT', async () => {
    console.log('[Timer] Shutting down...');
    await timerService.stop();
    await db.close();
    process.exit(0);
  });
}

bootstrap();