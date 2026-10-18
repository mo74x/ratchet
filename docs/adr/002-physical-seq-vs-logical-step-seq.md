# ADR-002: Separate Physical Event Log Sequence (`seq`) from Deterministic Call Sequence (`step_seq`)

## Status
Accepted

## Context
An append-only event log records events in the chronological order they occur (`seq = 1, 2, 3...`). However, external signals (`SignalReceived`) can arrive at any time while a step is running, and parallel steps (`ctx.all([stepA, stepB])`) may complete in arbitrary wall-clock order across different workers.

If the replay engine matched the Nth `ctx.*` invocation in code against physical log index `seq = N`, any out-of-order signal or parallel completion would shift subsequent event indices and falsely raise `NonDeterminismError`.

## Decision
Separate ordering into two columns on `workflow_events`:
1. `seq` (`BIGINT`, monotonic per workflow): Represents physical arrival order in the append-only audit log.
2. `step_seq` (`INT`, deterministic counter): Incremented strictly by `WorkflowContext` in code execution order (`1, 2, 3...`) for every deterministic operation (`step`, `sleep`, `waitForSignal`, `now`, `random`, `uuid`).

A unique constraint `UNIQUE (workflow_id, step_seq, type)` guarantees at the database level that each logical step transition (`StepScheduled`, `StepCompleted`, `StepFailed`) is recorded at most once.

## Consequences
- **Positive:** Replay lookups (`history.get(stepSeq)`) are O(1) and completely immune to physical arrival timing of parallel steps or external signals.
- **Positive:** The database enforces exactly-once result recording per `(workflow_id, step_seq, type)` even under duplicate step execution.