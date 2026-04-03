# Checkpoint 20260403 — COMPLETE

## Session
- Task: docs/superpowers/plans/2026-04-02-dag/2026-04-02-dag-p5-watchdog-sweeper.md
- Current Phase: Phase 7 — COMPLETE
- Completed Phases: 1, 2, 3, 4, 5, 6, 7

## Summary of Changes

### Production Code
- **DefaultPhaseGate.recoverStuckWorkflow()** — Replaced high-water-mark + successor BFS with iterate-all-sequences in ascending order. SCATTER re-dispatch, PARALLEL skip, LINEAR edge eval with PENDING/SKIPPED insertion. ABORT failure detection before completion check (excluding PARALLEL). CAS guard with retry.
- **JdbiWorkflowRepository.findStuck()** — Replaced max-sequence subquery with simpler global non-terminal check + EXISTS guard.
- **WorkflowWatchdog** — No change (already matched plan).

### Test Code
- **WorkflowWatchdogTest** — Restored ABORT test expectations. Added diamond DAG recovery test. Added findStuck EXISTS guard test. Added direct `recoverStuckWorkflow` tests (non-existent ID, COMPLETED workflow, FAILED workflow). Diamond DAG derives sequence numbers from `buildSequenceMap`.

## Test Results
- 32 tests, 0 failures, 0 errors — BUILD SUCCESS

## Decisions Log
| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Iterate-all-sequences over high-water-mark | Handles mid-DAG gaps in diamond/branching topologies |
| 2 | Keep EXISTS guard in findStuck | Prevents false positives on zero-task workflows |
| 3 | Global non-terminal check in findStuck | Simpler, conservative two-phase approach |
| 4 | SKIPPED insertion for untaken edges | Enables progress past conditional branches |
| 5 | Completion check after iterate loop | Marks workflow COMPLETED when all activities terminal |
| 6 | Skip JoinPolicy evaluation in recovery | Accepted limitation — onTaskCompleted is authoritative |
| 7 | log.warn for workflow-not-found | Observability improvement |
| 8 | Exclude PARALLEL from ABORT check | PARALLEL inherits policy; join eval is onTaskCompleted's job |
