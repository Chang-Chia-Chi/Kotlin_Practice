# Production Readiness Code Review

**Reviewer:** Staff SWE (10+ YoE Kotlin/Quarkus/Distributed Systems)
**Date:** 2026-03-14
**Branch:** `misc/ai_gen`
**Verdict:** NOT production-ready — 5 P0 blockers, 7 P1 high-severity issues

---

## P0 — Blockers (must fix before any real traffic)

### 1. DATA CORRUPTION: `reclaimStaleTask` can overwrite COMPLETED tasks back to PENDING

**`TaskRepository.kt:185-213`**

The second UPDATE has **no status guard**. If a task completes between `findStaleTasks()` and the transaction start, the first UPDATE (`WHERE status = 'CLAIMED'`) affects 0 rows, but the code doesn't check affected-row count. It proceeds to read `retry_count`, then unconditionally sets `status = 'PENDING'` — **overwriting a COMPLETED task**.

```kotlin
// Line 203-207: This UPDATE has no WHERE status = 'CLAIMED'
h.createUpdate("""
    UPDATE task SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL
    WHERE task_id = :taskId   // <-- MISSING: AND status = 'CLAIMED'
""")
```

**Impact:** A completed task gets re-executed. For map-reduce, this double-increments `completed_tasks`, potentially making `completed_tasks > totalTasks` and corrupting barrier detection.

**Fix:** Check affected rows from the first UPDATE; bail out if 0. Add `AND status = 'CLAIMED'` to subsequent UPDATEs.

---

### 2. DATA CORRUPTION: `completeMapTask` counter increment is not idempotent

**`JobRepository.kt:179-181`**

```kotlin
h.createUpdate(
    "UPDATE mr_job SET completed_tasks = completed_tasks + 1 ... WHERE job_id = :jobId"
).bind("jobId", jobId).execute()
```

The task status UPDATE on line 176 has no `AND status = 'CLAIMED'` guard. If a map task is reclaimed (bug #1, or legitimate stale reclaim) and re-executed, `completeMapTask` runs again. The counter increments twice, producing `completed_tasks > totalTasks`. The barrier fires early with incomplete data, or the reduce processes partial output.

**Fix:** Guard the task status UPDATE with `AND status = 'CLAIMED'`, check affected rows, and only increment the counter if the task was actually transitioned.

---

### 3. SPLIT-BRAIN: Leader election fails open

**`LeaderElection.kt:88-91`**

```kotlin
catch (e: Exception) {
    log.warnf("Leader election failed (%s) — assuming leader role", e.message)
    _isLeader.set(true)  // EVERY pod becomes leader on K8s API failure
}
```

During a K8s API outage or network partition, **every pod assumes leader**. Multiple orchestrators run concurrently, causing:

- Duplicate reduce tasks dispatched for the same job
- Concurrent `casJobStatus` calls (CAS protects against double-transition, but duplicate reduce tasks still get enqueued)
- `StaleTaskReaper` running on every pod simultaneously

**Impact:** Duplicate reduce tasks mean duplicate `onCompleted` side effects (double emails, double publishes).

**Fix:** Fail closed — `_isLeader.set(false)`. A leaderless cluster safely degrades (tasks still execute, barrier detection just pauses until a leader is elected). Log at ERROR level and emit a metric for alerting.

---

### 4. NO SCHEMA: Flyway enabled with no migration files

**`application.properties:10`**: `quarkus.flyway.migrate-at-start=true`

No `src/main/resources/db/migration/` directory exists. The application starts, Flyway runs, finds nothing, and the first DB call throws `ORA-00942: table or table view does not exist`.

**Fix:** Create `V1__init_schema.sql` with `task`, `mr_job`, `mr_output` tables and critical indexes (see P1 #6).

---

### 5. SIDE-EFFECT DUPLICATION: `onCompleted` called before persistence

**`ReduceTaskHandler.kt:35-38`**

```kotlin
definition.onCompleted(result)                    // side effect fires
val resultMetadata = definition.serializeResult(result)
jobRepository.completeReduceTask(...)             // persistence happens AFTER
```

If `completeReduceTask` fails (connection timeout, deadlock), the task retries. Reduce re-runs, `onCompleted` fires again. Emails sent twice, events published twice, etc.

**Fix:** Persist first, then call `onCompleted`. Or wrap `onCompleted` in an at-most-once guard keyed by `jobId`.

---

## P1 — High (will cause incidents under real traffic)

### 6. Missing database indexes

The `claim()` query (`TaskRepository.kt:58-67`) does:

```sql
WHERE status = 'PENDING' AND queue IN (...)
  AND (scheduled_at IS NULL OR scheduled_at <= ...)
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED
```

Without a composite index on `(status, queue, priority DESC, created_at ASC)`, this is a **full table scan** on every poll cycle (every 2s per pod). With millions of completed tasks accumulated, this becomes catastrophic.

Also needed:

- Index on `(group_id, status)` for `countByGroupAndStatus`
- Index on `(group_id, handler)` for `findByGroupAndHandler`
- Index on `(status, claimed_at)` for `findStaleTasks`
- Index on `(job_id)` on `mr_output` for `streamOutputs`

---

### 7. Connection pool too small for concurrency

**`application.properties:7`**: `quarkus.datasource.jdbc.max-size=10`

Per-pod concurrent DB consumers:

| Consumer | Connections |
|----------|-------------|
| Poll loop (claim attempt) | 1 |
| Bulkhead (4 concurrent tasks) | 4+ |
| `streamOutputs()` during reduce | 1 (held for entire reduce phase) |
| StaleTaskReaper | 1 |
| MapReduceOrchestrator | 1+ |
| **Total** | **8+** |

Under load, connection pool exhaustion causes `AgroalDataSourceException` (pool timeout).

**Fix:** `max-size` should be at least `bulkhead-size * 2 + 5` (13+ for default config). Consider separate pools for worker vs orchestrator paths. Document the relationship.

---

### 8. `findAllJobs()` and `findStaleTasks()` load unbounded results into memory

**`JobRepository.kt:100-105`:**

```kotlin
fun findAllJobs(): List<Job> =
    h.createQuery("SELECT * FROM mr_job ORDER BY created_at DESC")
        .mapTo(Job::class.java).list()
```

**`TaskRepository.kt:172-180`:**

```kotlin
fun findStaleTasks(threshold: Instant): List<Task> =
    h.createQuery("SELECT * FROM task WHERE status = 'CLAIMED' AND claimed_at < :threshold")
        .mapTo(Task::class.java).list()
```

After a node crash, thousands of CLAIMED tasks become stale. Loading all into memory causes OOM. Same for `findAllJobs` — after months of operation, millions of jobs.

**Fix:** Add `FETCH FIRST :limit ROWS ONLY` to both queries. Add pagination parameters to `listJobs` REST endpoint.

---

### 9. `split()` runs on the event loop thread

**`JobResource.kt:45`:**

```kotlin
val taskInputs = def.split(params)  // outside withContext(Dispatchers.IO)
```

`split()` is user-implemented and could do I/O (read files, query another DB) or heavy CPU work. Running it on the Quarkus I/O thread blocks all other requests.

**Fix:** Move inside the existing `withContext(Dispatchers.IO)` block.

---

### 10. Map output collected entirely in memory before batch insert

**`MapTaskHandler.kt:35-37`:**

```kotlin
val serialized = definition.map(input)
    .map { definition.serializeOutput(it) }
    .toList()  // ALL outputs materialized in memory
```

If a single map task produces millions of intermediate records, this OOMs. The Flow is collected greedily, defeating the purpose of streaming.

**Fix:** Chunk the flow (e.g., `flow.chunked(1000)`) and batch-insert incrementally within the transaction.

---

### 11. `@Timed` interceptor doesn't work with `suspend` functions

**`TimedInterceptor.kt:16`:**

```kotlin
@AroundInvoke
fun intercept(ctx: InvocationContext): Any? {
```

Jakarta `@AroundInvoke` interceptors don't understand Kotlin coroutines. When intercepting a `suspend fun`, `ctx.proceed()` returns immediately with a `COROUTINE_SUSPENDED` marker, and the timer records the time to the first suspension point — not the actual execution time. Success/failure counters fire at the wrong time.

**Fix:** Either detect the `Continuation` parameter and handle it, or use Micrometer's built-in Quarkus support (`@Timed` from Micrometer, not a custom interceptor). Alternatively, use a coroutine-aware AOP mechanism.

---

### 12. Startup ordering not guaranteed between `HandlerRegistry` and `MapReduceRegistrar`

Both observe `StartupEvent` with default priority. If `MapReduceRegistrar.onStart()` fires first, it calls `handlerRegistry.register()` while CDI handlers haven't been discovered yet. If a CDI handler has the same name as an auto-generated MR handler, the winner depends on observer invocation order (non-deterministic across JVMs).

**Fix:** Use `@Priority` on the observer methods, or have `MapReduceRegistrar` depend on `HandlerRegistry` initialization explicitly.

---

## P2 — Medium (correctness & operational concerns)

### 13. `fail()` method has no status guard on its first UPDATE

**`TaskRepository.kt:114-118`:**

```kotlin
h.createUpdate("""
    UPDATE task SET retry_count = retry_count + 1, error_message = :error
    WHERE task_id = :taskId  // <-- no AND status = 'CLAIMED'
""")
```

If called on a task that was already reclaimed and re-executed, this corrupts the retry counter of the re-execution.

---

### 14. `JobStatus.valueOf` throws 500 on invalid input

**`JobResource.kt:88`:**

```kotlin
jobRepository.findJobsByStatus(JobStatus.valueOf(status.uppercase()))
```

An invalid status string (`?status=BANANA`) throws `IllegalArgumentException`, which Quarkus maps to HTTP 500. Should catch and return 400.

---

### 15. `MapReduceRegistrar.definitionMap` is a non-thread-safe `mutableMapOf`

**`MapReduceRegistrar.kt:29`:**

```kotlin
private val definitionMap = mutableMapOf<String, MapReduceDefinition<*, *, *, *>>()
```

Written during startup, read concurrently at runtime. The lack of a happens-before guarantee between the startup thread and request threads means reads could see a partially-constructed map. Use `ConcurrentHashMap` or make it an immutable snapshot after init.

---

### 16. No health check for K8s probes

The app targets Kubernetes but doesn't include `quarkus-smallrye-health`. Without readiness/liveness probes:

- K8s can't detect a pod stuck in a DB connection deadlock
- Rolling deployments don't wait for readiness
- A pod that lost its DB connection keeps receiving traffic

**Fix:** Add `quarkus-smallrye-health` dependency. Implement a custom health check that verifies DB connectivity and leader election state.

---

### 17. `WorkerLoop.onStop` blocks the Quarkus shutdown thread

**`WorkerLoop.kt:98`:**

```kotlin
val drained = semaphore.tryAcquire(bulkheadSize, timeoutSeconds, TimeUnit.SECONDS)
```

This is a blocking call (up to 30s) on the Quarkus shutdown observer thread. If Quarkus has its own shutdown timeout < 30s, the process gets killed before draining completes.

**Fix:** Align `shutdown-timeout` with Quarkus's `quarkus.shutdown.timeout` and K8s `terminationGracePeriodSeconds`. Document the relationship.

---

### 18. Metadata field uses string interpolation

**`JobRepository.kt:76`:**

```kotlin
.bind("metadata", """{"task_index":$index,"phase":"MAP"}""")
```

While `index` is an `Int` (safe here), this pattern encourages copy-paste with string values, which would introduce JSON injection. Use Jackson serialization.

---

## P3 — Low (clean up before GA)

| # | File:Line | Issue |
|---|-----------|-------|
| 19 | `Task.kt:7` | `FAILED` in `TaskStatus` is never set anywhere — dead enum value |
| 20 | `Job.kt:7` | `CREATED` in `JobStatus` is never set — jobs go directly to `RUNNING` |
| 21 | `pom.xml:177` | `all-open` plugin missing `jakarta.interceptor.Interceptor` — `TimedInterceptor` may not be proxied correctly |
| 22 | `FrameworkConfig.kt` | No validation that `bulkhead-size > 0`, `poll-interval > 0`, etc. |
| 23 | `WorkerLoop.kt:37-38` | `CoroutineScope` created at field-init time — if config changes at runtime, scopes are stale |
| 24 | `application.properties:25` | DEBUG logging in production leaks task payloads to stdout |

---

## Architecture Assessment

### What's done well

- **Two-layer separation** (generic queue + MR pattern) is clean and extensible
- **`SELECT FOR UPDATE SKIP LOCKED`** is the right primitive for Oracle-based task claiming
- **CAS with version field** for job state transitions is correct
- **Fencing tokens** on leader election is a good practice
- **Atomic fan-out** (job + tasks in one txn) prevents orphaned state
- **Graceful shutdown** with two-scope design is thoughtful
- **SPI design** (`TaskHandler`, `MapReduceDefinition`) is well-factored

### What blocks production

The data corruption bugs (#1, #2) and split-brain (#3) are the most critical. Under real traffic, stale reclaim will eventually overwrite completed tasks, and a K8s API blip will cause duplicate side effects. These bugs are invisible in dev/staging (single pod, no failures) but surface immediately at scale.

The missing schema (#4) is a trivial fix but a hard startup blocker. The `onCompleted` ordering (#5) will cause duplicate side effects on the first reduce retry.

### Recommended fix order

```
Phase 1 (block release):  P0 #1-#5
Phase 2 (before load test): P1 #6-#9
Phase 3 (fast-follow):      P1 #10-#12, P2 #13-#18
Phase 4 (GA polish):         P3 #19-#24
```
