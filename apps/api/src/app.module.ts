// apps/api/src/app.module.ts
import { Module, OnModuleDestroy } from '@nestjs/common';
import { Database, RatchetRepository } from '@ratchet/store';
import { WorkflowsController } from './workflows.controller';

@Module({
  controllers: [WorkflowsController],
  providers: [
    {
      provide: Database,
      useFactory: () => new Database(),
    },
    {
      provide: RatchetRepository,
      useFactory: (db: Database) => new RatchetRepository(db),
      inject: [Database],
    },
  ],
  exports: [Database, RatchetRepository],
})
export class AppModule implements OnModuleDestroy {
  constructor(private readonly db: Database) {}

  async onModuleDestroy() {
    await this.db.close();
  }
}