-- Enforce that any given step_seq can only have ONE terminal resolution event.
-- This makes it impossible for a waitForSignal(step_seq=k) to record BOTH SignalReceived and TimerFired.
CREATE UNIQUE INDEX IF NOT EXISTS workflow_events_single_terminal_outcome_idx
  ON workflow_events (workflow_id, step_seq)
  WHERE step_seq IS NOT NULL
    AND type IN ('StepCompleted', 'StepFailed', 'TimerFired', 'SignalReceived');

-- Support per-step startToClose timeout configuration on step tasks
ALTER TABLE tasks
  ADD COLUMN IF NOT EXISTS timeout_ms int;