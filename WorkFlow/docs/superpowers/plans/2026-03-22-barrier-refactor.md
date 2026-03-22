# Barrier Service Refactoring Plan

**Goal:** Simplify BarrierService by treating join as a separate activity (MapReduce model), fix payload propagation, and remove dead code.

## Problems

### P1: Join handler runs inside DB transaction (unbounded)
`BarrierService.kt:98-129` — inline join handler execution via `runBlocking` inside `inTransactionSuspend`. If the handler does network I/O, the transaction and connection are held open indefinitely. Also requires `HandlerRegistry` as a barrier dependency, coupling engine to worker layer.

### P2: Missing payload propagation
`BarrierService.kt:171` — LINEAR and SCATTER tasks are inserted with `payloadJson = null`. The previous sequence's completed task result should flow forward as the next task's payload (pipeline pattern).

### P3: Unused parameter
`BarrierService.kt:147` — `previousSeqInfo: SequenceInfo` is never referenced in `insertTasksForSequence`.

### P4: Join-as-inline-handler is wrong abstraction
The current DSL nests `join { transition("handler") }` inside `fanOut {}`, making the join handler a special case executed inline by the barrier. This is unlike every other handler in the system (which are regular tasks claimed by workers). A join/reduce step is conceptually a separate activity — just like MapReduce: map (parallel) → reduce (join).

## Design: Join as Separate Activity

### Before (current)
```kotlin
workflow {
    activity("email-blast") {
        transition("email.send")          // parallel handler
        fanOut {
            transition("email.scatter")   // scatter handler
            join {
                policy(JoinPolicy.Percentage(95))
                transition("email.aggregate")  // inline handler — the problem
            }
        }
    }
}
// Sequences: SCATTER(email.scatter) → PARALLEL(email.send) → [inline join]
```

### After (refactored)
```kotlin
workflow {
    activity("email-blast") {
        transition("email.send")
        fanOut {
            transition("email.scatter")
            joinPolicy(JoinPolicy.Percentage(95))  // just the wait policy
        }
    }
    activity("email-aggregate") {
        transition("email.aggregate")   // regular activity, regular task
    }
}
// Sequences: SCATTER(email.scatter) → PARALLEL(email.send) → LINEAR(email.aggregate)
```

The join/reduce handler is the next activity. No inline execution. No special barrier path. No `runBlocking`.

---

## Changes

### 1. DSL Layer

**Files:** `WorkflowDsl.kt`, `WorkflowDslBuilders.kt`

- [ ] **1a.** Remove `transition` field from `JoinDefinition`. It becomes:
  ```kotlin
  data class JoinDefinition(
      val policy: JoinPolicy = JoinPolicy.All,
  )
  ```
  Or simplify further: replace `join: JoinDefinition` in `FanOutDefinition` with `joinPolicy: JoinPolicy`:
  ```kotlin
  data class FanOutDefinition(
      val transition: String,
      val retries: Int = 0,
      val failurePolicy: FailurePolicy = FailurePolicy.ABORT,
      val deadline: Duration = Duration.ofMinutes(30),
      val joinPolicy: JoinPolicy = JoinPolicy.All,
  )
  ```
  **Decision:** Inline `JoinPolicy` directly. Remove `JoinDefinition` class entirely.

- [ ] **1b.** Update `JoinBuilder` / `FanOutBuilder`:
  - Remove `JoinBuilder.transition()` method
  - Or: remove `JoinBuilder` entirely if inlining to `JoinPolicy`, replace with `fun joinPolicy(p: JoinPolicy)` on `FanOutBuilder`

- [ ] **1c.** Update `WorkflowDslBuildersTest.kt`:
  - Remove test for join transition
  - Update fan-out builder tests to use `joinPolicy()` instead of `join { transition("...") }`

- [ ] **1d.** Update `WorkflowDslTest.kt` — serialization round-trip tests for new structure

### 2. BarrierService

**File:** `BarrierService.kt`

- [ ] **2a.** Remove inline join handler execution (lines 98-129). The entire `if (effectiveSuccess && currentSeqInfo.phaseType == PhaseType.PARALLEL)` block for join transition.

- [ ] **2b.** Remove `HandlerRegistry` from constructor dependencies. Barrier no longer resolves or executes handlers.
  ```kotlin
  class BarrierService(
      private val jdbi: Jdbi,
      private val workflowRepo: WorkflowRepository,
      private val taskRepo: TaskRepository,
      private val objectMapper: ObjectMapper,  // HandlerRegistry removed
  )
  ```

- [ ] **2c.** Remove `runBlocking` import (line 13) and `HandlerInput` import (line 10).

- [ ] **2d.** Fix payload propagation in `insertTasksForSequence`:
  - Add `payload: String?` parameter (the previous sequence's result)
  - LINEAR/SCATTER: use `payload` as `payloadJson` instead of `null`
  - PARALLEL: unchanged (reads scatter result directly)

- [ ] **2e.** Remove unused `previousSeqInfo` parameter from `insertTasksForSequence`.

- [ ] **2f.** Pass payload through the call chain:
  - `onTaskCompleted` already has `resultJson`
  - `advanceWorkflow` needs new `payload: String?` parameter
  - For single-task sequences (LINEAR, SCATTER): payload = `resultJson` from the completing task
  - For multi-task sequences (PARALLEL): payload = null (next task reads scatter result from DB)
  - Read the completing task's result within the transaction if needed for propagation

- [ ] **2g.** Update `evaluateOutcome` — change `activity.fanOut!!.join.policy` to `activity.fanOut!!.joinPolicy` (if JoinDefinition is inlined)

- [ ] **2h.** Update `buildSequenceMap` — no changes needed (SCATTER/PARALLEL expansion unchanged)

### 3. BarrierServiceTest

**File:** `BarrierServiceTest.kt`

- [ ] **3a.** Remove test #11 (join with inline transition) — no longer applicable. The join handler is now a regular task in the next activity, tested by WorkerLoop/IntegrationTest.

- [ ] **3b.** Update test #10 (pure barrier) — simplify. "Pure barrier" is now just "PARALLEL phase completes, workflow advances to next sequence." No join transition concept.

- [ ] **3c.** Add payload propagation tests:
  - LINEAR → LINEAR: completing task's `resultJson` becomes next task's `payloadJson`
  - LINEAR → SCATTER: completing task's `resultJson` becomes scatter task's `payloadJson`
  - PARALLEL → LINEAR: next task's payload is null (multiple parallel results, no single payload to propagate — the next activity queries DB if needed)
  - SCATTER → PARALLEL: already tested (test #12), verify payloads from scatter result

- [ ] **3d.** Update BarrierService constructor in all tests — remove `HandlerRegistry` parameter.

- [ ] **3e.** Update all test workflow definitions — remove `JoinDefinition.transition` references, use new `joinPolicy` field.

- [ ] **3f.** Update test #4 (CAS race) — already fixed for READ COMMITTED, verify it still passes.

### 4. Existing DSL Tests

**Files:** `WorkflowDslTest.kt`, `WorkflowDslBuildersTest.kt`

- [ ] **4a.** Update serialization tests for new `FanOutDefinition` structure (no `JoinDefinition.transition`)
- [ ] **4b.** Update builder tests — remove `join { transition("...") }` patterns
- [ ] **4c.** Remove "pure barrier (join with no transition)" builder test — all joins are now implicitly "pure barrier" (no transition concept)

### 5. Design Doc

**File:** `docs/superpowers/plans/2026-03-22-workflow-engine.md`

- [ ] **5a.** Update Task 1 (DSL Models) — reflect `JoinDefinition` simplification or removal
- [ ] **5b.** Update Task 8 (Barrier Service) — remove inline join handler references, add payload propagation
- [ ] **5c.** Update Task 7 (Handler Interface) — note that `HandlerRegistry` is no longer a barrier dependency
- [ ] **5d.** Update Tasks 10/13 — join handler is now a regular worker task, integration tests cover it

---

## Impact Summary

| Component | Change | Risk |
|-----------|--------|------|
| `WorkflowDsl.kt` | Remove `JoinDefinition.transition` or inline to `JoinPolicy` | Low — additive simplification |
| `WorkflowDslBuilders.kt` | Simplify/remove `JoinBuilder` | Low |
| `BarrierService.kt` | Remove 30 lines (join handler), add ~5 lines (payload propagation) | Medium — core algorithm change |
| `BarrierServiceTest.kt` | Remove 1 test, add 3-4 tests, update constructor + definitions | Medium |
| `WorkflowDslTest.kt` | Update serialization tests | Low |
| `WorkflowDslBuildersTest.kt` | Update builder tests | Low |

**Net effect on BarrierService:**
- Remove: `HandlerRegistry` dep, `runBlocking`, `HandlerInput` import, inline join execution (30 lines), unused param
- Add: payload propagation (5 lines)
- Constructor: 5 params → 4 params
- Transaction: always bounded (no unbounded handler execution)

---

## Execution Order

```
1a-1d (DSL) → 2a-2h (Barrier) → 3a-3f (Tests) → 4a-4c (DSL Tests) → 5a-5d (Docs)
```

DSL changes first (they change the data model), then barrier (consumes the model), then tests, then docs.
