CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Workflows table
CREATE TABLE IF NOT EXISTS workflows (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  type            text NOT NULL,
  status          text NOT NULL CHECK (status IN ('running', 'waiting', 'completed', 'failed', 'cancelled')),
  input           jsonb NOT NULL,
  output          jsonb,
  error           text,
  idempotency_key text UNIQUE,
  parent_id       uuid REFERENCES workflows(id),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- 2. Append-only Workflow Events (Source of Truth for Replay)
CREATE TABLE IF NOT EXISTS workflow_events (
  workflow_id     uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  seq             bigint NOT NULL,
  step_seq        int,
  type            text NOT NULL,
  name            text,
  payload         jsonb,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, seq)
);

-- Guarantees exactly-once recording for any logical step event (e.g. StepCompleted for step_seq = 2)
CREATE UNIQUE INDEX IF NOT EXISTS workflow_events_step_once_idx
  ON workflow_events (workflow_id, step_seq, type)
  WHERE step_seq IS NOT NULL;

-- 3. Tasks Queue (SKIP LOCKED worker queue for both workflow ticks and step executions)
CREATE TABLE IF NOT EXISTS tasks (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_id     uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  kind            text NOT NULL CHECK (kind IN ('workflow', 'step')),
  step_seq        int,
  step_name       text,
  status          text NOT NULL CHECK (status IN ('pending', 'leased', 'done', 'dead')),
  run_at          timestamptz NOT NULL DEFAULT now(),
  attempt         int NOT NULL DEFAULT 0,
  max_attempts    int NOT NULL DEFAULT 5,
  lease_token     uuid,
  lease_expires   timestamptz,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Fast claim index for workers polling due tasks
CREATE INDEX IF NOT EXISTS tasks_claim_idx
  ON tasks (status, kind, run_at)
  WHERE status = 'pending';

-- ADR-003: Prevent two concurrent workflow replay ticks for the same workflow_id
CREATE UNIQUE INDEX IF NOT EXISTS tasks_one_active_workflow_tick_idx
  ON tasks (workflow_id)
  WHERE kind = 'workflow' AND status IN ('pending', 'leased');

-- Prevent duplicate active tasks for the exact same step_seq
CREATE UNIQUE INDEX IF NOT EXISTS tasks_one_active_step_idx
  ON tasks (workflow_id, step_seq)
  WHERE kind = 'step' AND status IN ('pending', 'leased');

-- 4. Durable Timers
CREATE TABLE IF NOT EXISTS timers (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_id     uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  step_seq        int NOT NULL,
  fire_at         timestamptz NOT NULL,
  fired           boolean NOT NULL DEFAULT false,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workflow_id, step_seq)
);

CREATE INDEX IF NOT EXISTS timers_due_idx
  ON timers (fire_at)
  WHERE NOT fired;

-- 5. External Signals
CREATE TABLE IF NOT EXISTS signals (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_id     uuid NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  name            text NOT NULL,
  payload         jsonb,
  consumed        boolean NOT NULL DEFAULT false,
  consumed_by_seq int,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS signals_unconsumed_idx
  ON signals (workflow_id, name, created_at)
  WHERE NOT consumed;