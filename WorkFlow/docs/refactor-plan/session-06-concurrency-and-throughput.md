# Session 6 — Concurrency & Throughput

**Tier:** 2 (performance and scalability blockers)
**Prerequisites:** Session 1 (drain window fix)
**Estimated scope:** 3 concurrency fixes + config changes + tests

---

## Items

### R2.4 — Make `batchSize` configurable

**Problem:** `WorkerLoop.start()` calls `pollAndProcess(workerId, pollInterval, 1)` — `batchSize` is hardcoded to 1. Each poll cycle claims exactly one task per DB round-trip. With `concurrency = 4`, throughput is capped at `4 / pollInterval` tasks/sec regardless of queue depth. The `claimNext` method supports a `limit` parameter, and `pollAndProcess` accepts `batchSize` — neither is used.

**Files to modify:**
- `src/main/kotlin/config/FrameworkConfig.kt` — add `batchSize` to `WorkerConfig`:
  ```kotlin
  interface WorkerConfig {
      // ... existing
      @WithDefault("1")
      fun batchSize(): Int
  }
  ```
- `src/main/kotlin/worker/WorkerLoop.kt` — use config value:
  ```kotlin
  // Change:
  pollAndProcess(workerId, pollInterval, 1)
  // To:
  pollAndProcess(workerId, pollInterval, config.worker().batchSize())
  ```
- `src/main/resources/application.properties` — add default:
  ```properties
  framework.worker.batch-size=1
  ```

**Behavior:** Each coroutine slot now claims up to `batchSize` tasks per DB round-trip. With `batchSize=4` and `concurrency=4`, peak throughput quadruples. The `pollAndProcess` method already iterates over claimed tasks in a loop, so no structural change is needed.

**Test:** Set `batchSize=3`, enqueue 6 tasks, start worker with `concurrency=2`. Assert all 6 tasks complete (2 slots * 3 tasks/batch = 6 tasks in one round).

---

### R2.5 — Add `SupervisorJob` to `unorderedMapAsync`

**Problem:** Inside `unorderedMapAsync`, the `channelFlow` scope uses a regular `Job`. If one `launch { send(transform(value)) }` throws an uncaught exception, it cancels the entire `channelFlow` — killing the consumer loop. Currently `transform` is `pollAndProcess` which catches exceptions internally, so this is safe in practice. But the design is fragile: any future change to `pollAndProcess` that lets an exception escape will silently kill the loop.

**Files to modify:**
- `src/main/kotlin/extension/FlowExtension.kt` — `unorderedMapAsync` function

**Fix:**
```kotlin
fun <T, R> Flow<T>.unorderedMapAsync(
    concurrency: Int,
    transform: suspend (T) -> R,
): Flow<R> = channelFlow {
    val semaphore = Semaphore(concurrency)
    collect { value ->
        semaphore.acquire()
        launch(SupervisorJob(coroutineContext[Job])) {
            try {
                send(transform(value))
            } finally {
                semaphore.release()
            }
        }
    }
}
```

Note: `SupervisorJob(coroutineContext[Job])` makes it a child of the channelFlow's job (so cancellation still propagates downward) but prevents a child failure from cancelling siblings.

**Test:** In `FlowExtensionTest`, create a flow where one element throws. Assert the other elements still complete. Assert the thrown exception does not kill the collecting coroutine.

---

### R2.6 — Use bounded dispatcher for handler execution

**Problem:** Handler coroutines run on `Dispatchers.Default` (shared with the rest of the app). On pods with 2-4 vCPUs, `Default` has 2-4 threads. CPU-bound handlers can starve Quarkus event loops, health probes, and metrics scraping.

**Files to modify:**
- `src/main/kotlin/worker/WorkerLoop.kt` — scope creation

**Fix:**
```kotlin
// Change:
val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

// To:
val handlerDispatcher = Dispatchers.IO.limitedParallelism(config.worker().concurrency())
val scope = CoroutineScope(SupervisorJob() + handlerDispatcher)
```

`Dispatchers.IO.limitedParallelism(N)` creates a bounded view of the IO pool. Handlers get at most N threads, leaving Default free for framework work. The IO pool is appropriate because handlers typically do DB-backed work (already blocking).

**Alternative:** Keep `Default` but document in `TransitionHandler` KDoc that handlers must not do CPU-bound work without `withContext(Dispatchers.IO)`. This is the lighter touch but pushes responsibility to handler authors.

**Recommendation:** Use the bounded dispatcher. It is a one-line change and eliminates the class of bugs entirely.

**Test:** No direct test needed — the existing `WorkerLoopTest` validates behavior. Optionally, add a stress test with `concurrency=2` and a CPU-spinning handler to verify the rest of the app remains responsive (health probe returns within 1s).

---

## Verification

1. `mvn test` passes
2. `FlowExtensionTest` extended with failure isolation test
3. `FrameworkConfigTest` extended to verify `batchSize` config binding
4. Manual: run with `batchSize=4`, observe claim throughput increase in logs
