# ADR-001: Suspend via Internal Control Exception and Split Task Kinds

## Status
Accepted

## Context
TypeScript async functions cannot have their live call stack serialized to PostgreSQL. When a workflow reaches an unrecorded `ctx.step()`, `ctx.sleep()`, or `ctx.waitForSignal()`, execution must pause without blocking a worker thread or holding a database connection open during long-running external I/O (such as LLM calls or 3-day timers).

## Decision
1. **Replay from the top:** Workflows are deterministic async functions re-executed from the beginning on each state transition.
2. **Fast-forward recorded calls:** Calls to `ctx.*` whose `step_seq` already has a terminal event (`StepCompleted`, `TimerFired`, `SignalReceived`) in `workflow_events` return their recorded payload synchronously from memory.
3. **Throw-to-suspend:** When `ctx.*` encounters a call that has not yet completed, it schedules the work (if not already scheduled) and throws a dedicated `WorkflowSuspendedSignal` control-flow error. The worker catches this specific signal, commits pending scheduling records, and frees the thread.
4. **Split task kinds (`workflow` vs `step`):**
   - A `workflow` task replays history and decides what to schedule next.
   - A `step` task executes the external handler `fn(idempotencyKey)` outside the workflow replay lock, records `StepCompleted` or `StepFailed`, and enqueues a new `workflow` task to advance the workflow.

## Consequences
- **Positive:** Long-running steps (e.g., 60s LLM calls) never block workflow state transitions such as incoming signals or cancellations.
- **Positive:** Parallel execution (`ctx.all([...])` in Phase 4) works naturally by scheduling multiple `step` tasks during a single replay tick.
- **Negative / Trade-off:** Every step requires two task claims (one `workflow` tick + one `step` execution). User code must not swallow `WorkflowSuspendedSignal` inside bare `try / catch` blocks (we provide a `isRatchetControlSignal(err)` helper and rethrow guard).