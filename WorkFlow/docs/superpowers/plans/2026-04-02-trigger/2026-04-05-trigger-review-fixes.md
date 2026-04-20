# Trigger Feature — Post-Review Fixes

Three issues surfaced from code review. All are small, isolated changes.

---

## Fix 1: Test incorrectly asserts `DEFERRED -> PENDING` is illegal

**Problem:** `TaskStatus.kt:26` allows `DEFERRED to PENDING`, but:
- `WorkflowModelsTest.kt:221` lists it in the **illegal** transitions list
- `WorkflowModelsTest.kt:152-173` ("allows all legal transitions") omits it
- `WorkflowModelsTest.kt:202-215` ("DEFERRED allows transitions") also omits it

This test should be failing right now.

**Files to change:**
- `src/test/kotlin/workflow/model/WorkflowModelsTest.kt`

**Steps:**
- [ ] Remove `TaskStatus.DEFERRED to TaskStatus.PENDING` from the illegal list (line 221)
- [ ] Add `TaskStatus.DEFERRED to TaskStatus.PENDING` to `DEFERRED allows transitions` test (after line 208)
- [ ] Add `TaskStatus.DEFERRED to TaskStatus.PENDING` to `TaskStatus allows all legal transitions` test (after line 172)
- [ ] Run `WorkflowModelsTest` and confirm green

---

## Fix 2: Add exception handling to Defer path in WorkerLoop

**Problem:** `WorkerLoop.kt:268-280` — `taskRepo.defer()` can throw a DB exception (connection failure, constraint violation). Unlike the `Completed` path (lines 247-266) which wraps `taskSettler.settle()` in try/catch, the `Defer` path has no protection. An exception aborts the entire batch.

**File to change:**
- `src/main/kotlin/worker/usecase/service/execution/WorkerLoop.kt`

**Steps:**
- [ ] Wrap lines 269-279 in try/catch matching the `Completed` path pattern:
  ```kotlin
  is HandlerResult.Defer -> {
      try {
          val deferred = taskRepo.defer(
              taskId = task.id,
              triggerType = result.triggerType,
              triggerMeta = result.triggerMeta,
          )
          if (deferred) {
              log.info("Task {} deferred to trigger type={}", task.id, result.triggerType)
          } else {
              log.warn("Task {} defer failed (status was not PROCESSING), treating as failure", task.id)
              handleTaskFailure(task, IllegalStateException("Defer failed: task not in PROCESSING state"))
          }
      } catch (e: CancellationException) {
          throw e
      } catch (e: Exception) {
          log.error("Defer failed for task {}, falling through to failure path", task.id, e)
          handleTaskFailure(task, e)
      }
  }
  ```
- [ ] Add test case in `WorkerLoopTest`: mock `taskRepo.defer()` to throw, assert task goes to failure path
- [ ] Run `WorkerLoopTest` and confirm green

---

## Fix 3: Remove `objectMapper` param from `deferK8sJob` and `deferSqlExec`

**Problem:** `TriggerTypes.kt:11` — both helper functions require `ObjectMapper` as the first parameter, but the structures are fixed and trivial. This deviates from the spec (Section 8.2) which envisions clean handler ergonomics without injecting serialization machinery.

**File to change:**
- `src/main/kotlin/worker/usecase/port/inbound/trigger/TriggerTypes.kt`

**Steps:**
- [ ] Replace `deferK8sJob` implementation — drop `objectMapper` param, use string template:
  ```kotlin
  fun deferK8sJob(jobName: String, namespace: String): HandlerResult.Defer =
      HandlerResult.Defer(
          triggerType = TriggerTypes.K8S_JOB,
          triggerMeta = """{"jobName":"$jobName","namespace":"$namespace"}""",
      )
  ```
- [ ] Replace `deferSqlExec` implementation — drop `objectMapper` param. Since `params` map has variable shape, use a lightweight approach:
  ```kotlin
  fun deferSqlExec(
      objectMapper: ObjectMapper,
      datasource: String,
      sql: String,
      params: Map<String, Any?> = emptyMap(),
  ): HandlerResult.Defer
  ```
  **Decision needed:** `deferSqlExec` params map may have nested values. Options:
  1. Keep `objectMapper` for `deferSqlExec` only (variable shape justifies it)
  2. Restrict `params` to `Map<String, String>` and use manual serialization
  
  Defer this decision to SQL trigger driver implementation since `deferSqlExec` is not yet consumed.
- [ ] Update `K8sJobTriggerDriver` if it deserializes `triggerMeta` — verify `K8sJobMeta` deserialization still works (it reads the same JSON shape, so no change needed in the driver)
- [ ] Update any test that calls `deferK8sJob` with `objectMapper`
- [ ] Run affected tests and confirm green

---

## Verification

- [ ] Run full test suite: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
