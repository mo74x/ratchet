import * as fs from 'fs';
import * as path from 'path';
import { Database } from './db';

export async function runMigrations(db: Database): Promise<void> {
  const migrationsDir = path.resolve(__dirname, '../migrations');
  const files = fs
    .readdirSync(migrationsDir)
    .filter((f) => f.endsWith('.sql'))
    .sort();

  for (const file of files) {
    const sql = fs.readFileSync(path.join(migrationsDir, file), 'utf8');
    await db.query(sql);
  }
}

if (require.main === module) {
  const db = new Database();
  runMigrations(db)
    .then(async () => {
      console.log('Ratchet database migrations applied successfully.');
      await db.close();
    })
    .catch(async (err) => {
      console.error('Migration failed:', err);
      await db.close();
      process.exit(1);
    });
}