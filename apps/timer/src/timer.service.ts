import { RatchetRepository } from '@ratchet/store';

export interface TimerServiceOptions {
  pollIntervalMs?: number;
  batchSize?: number;
}

export class RatchetTimerService {
  private running = false;
  private loopPromise: Promise<void> | null = null;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;

  constructor(
    private readonly repo: RatchetRepository,
    options: TimerServiceOptions = {},
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 100;
    this.batchSize = options.batchSize ?? 100;
  }

  async tick(): Promise<number> {
    return this.repo.fireDueTimers(this.batchSize);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.pollLoop();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.loopPromise) {
      await this.loopPromise;
      this.loopPromise = null;
    }
  }

  private async pollLoop(): Promise<void> {
    while (this.running) {
      try {
        const fired = await this.tick();
        if (fired === 0 && this.running) {
          await new Promise((r) => setTimeout(r, this.pollIntervalMs));
        }
      } catch {
        if (this.running) {
          await new Promise((r) => setTimeout(r, this.pollIntervalMs));
        }
      }
    }
  }
}