# Peer Notification Resilience Design

**Date:** 2026-04-04
**Status:** Proposed
**Scope:** Make peer HTTP broadcast fire-and-forget with natural signal coalescing, add HTTP timeouts. Two changes to three files.

---

## 1. Problem Statement

The current `HttpWorkerNotifier.signal()` uses `supervisorScope` to fan out HTTP POSTs to peers. Two issues:

1. **Caller blocked on slowest peer.** `supervisorScope` suspends until all peer launches complete. If a peer is unreachable, the TCP connect timeout (Ktor default: unbounded) blocks the entire post-commit path in `PhaseGate.onTaskCompleted` or `WorkflowEngine.startWorkflow`. The local `tryEmit()` fires instantly, but the caller can't return.

2. **No sender-side coalescing.** 10 rapid `signal()` calls produce 10 * N_peers HTTP requests. The receiver's SharedFlow coalesces them into one wake-up, so correctness is fine, but the sender wastes network and coroutine resources on redundant broadcasts.

### What we do NOT need

- **Retry:** Notifications are performance hints. The 5s fallback poll covers missed signals. Retrying adds complexity for zero correctness gain.
- **Circuit breaker / rate-limit:** For this project's QPS, fire-and-forget + short timeout is sufficient. A dead peer costs one 2s-timeout coroutine per coalesced signal -- negligible.

---

## 2. Design Overview

Two changes:

### Change 1: Fire-and-forget broadcast with sender-side coalescing

Replace the `supervisorScope` fan-out with a background broadcast collector on the existing per-queue `SharedFlow`. `signal()` becomes a single `tryEmit()` and returns immediately.

```
signal("default")
  └─ flowFor("default").tryEmit()  → local awaitWork() collectors wake
                                   → broadcast collector also collects
                                   → 1 HTTP fan-out to all peers
                                   (N rapid signals coalesce to 1 fan-out)
```

The broadcast collector is just another subscriber on the same `SharedFlow` that already serves local `awaitWork()`. No new flows, no new data structures.

**SharedFlow config stays unchanged:**

```kotlin
MutableSharedFlow(
    replay = 0,
    extraBufferCapacity = 1,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
)
```

Why this works:
- SharedFlow delivers to all active collectors independently. The broadcast collector does not steal from `awaitWork()` collectors.
- While the collector is busy doing HTTP fan-out, rapid signals buffer (capacity 1) and drop older ones via `DROP_OLDEST` -- natural coalescing on the sender side.
- If no worker is subscribed when a signal fires, the broadcast collector still gets it and notifies peers. Workers catch up on fallback poll.

### Change 2: Short HTTP timeout

Configure Ktor `HttpClient` with 2s connect + 2s request timeout. A dead peer fails fast instead of hanging for 30s+.

---

## 3. Detailed Design

### 3.1 HttpWorkerNotifier changes

```kotlin
@ApplicationScoped
class HttpWorkerNotifier(
    private val peerDiscovery: PeerDiscovery,
    private val httpClient: HttpClient,
) : WorkerNotifier {
    private val log = LoggerFactory.getLogger(HttpWorkerNotifier::class.java)

    private val broadcastScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val flows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()

    private fun flowFor(queue: String) =
        flows.getOrPut(queue) {
            MutableSharedFlow<Unit>(
                replay = 0,
                extraBufferCapacity = 1,
                onBufferOverflow = BufferOverflow.DROP_OLDEST,
            ).also { flow -> launchBroadcastCollector(queue, flow) }
        }

    private fun launchBroadcastCollector(queue: String, flow: MutableSharedFlow<Unit>) {
        broadcastScope.launch {
            val encodedQueue = URLEncoder.encode(queue, Charsets.UTF_8)
            flow.collect {
                val peers = peerDiscovery.peers()
                for (peer in peers) {
                    launch {
                        try {
                            httpClient.post("http://$peer:8080/internal/dispatch-notify?queue=$encodedQueue")
                        } catch (e: Exception) {
                            log.debug("Peer notify failed for {}: {}", peer, e.message)
                        }
                    }
                }
            }
        }
    }

    override suspend fun signal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
    }

    override fun onRemoteSignal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
    }

    override suspend fun awaitWork(queueName: String, timeout: Duration): Boolean =
        withTimeoutOrNull(timeout.toMillis()) {
            flowFor(queueName).first()
        } != null

    @PreDestroy
    fun shutdown() {
        broadcastScope.cancel()
    }
}
```

Key properties:
- `signal()` does one `tryEmit()` and returns -- non-blocking to the caller.
- Broadcast collector runs in `broadcastScope` (SupervisorJob + Dispatchers.IO), independent of callers.
- Per-peer HTTP calls are launched in parallel within the collector via nested `launch` in `broadcastScope`.
- Individual peer failures logged at debug, don't affect siblings.
- `@PreDestroy` cancels `broadcastScope`, cleaning up all collectors.

### 3.2 HttpClientProducer changes

```kotlin
class HttpClientProducer {
    @Produces
    @ApplicationScoped
    fun httpClient(): HttpClient = HttpClient(Java) {
        install(HttpTimeout) {
            connectTimeoutMillis = 2_000
            requestTimeoutMillis = 2_000
        }
    }

    fun close(@Disposes client: HttpClient) = client.close()
}
```

2s is generous for intra-cluster HTTP POST (<5ms typical). Dead peers fail fast.

---

## 4. Failure Modes

The correctness invariant is unchanged: notifications are performance hints. Removing the entire notification layer degrades to 5s-poll mode.

| Failure | Before | After |
|---------|--------|-------|
| Peer unreachable | Caller blocked until TCP timeout (30s+) | Background coroutine fails in 2s, caller unaffected |
| 10 rapid signals, 3 peers | 30 HTTP requests | 1-2 HTTP fan-outs (coalesced) = 3-6 requests |
| All peers down | Caller blocked on all timeouts | Caller returns instantly, background collectors timeout in 2s |
| Broadcast scope cancelled (shutdown) | N/A | Collectors stop, no more HTTP. Fallback poll covers remaining work |

---

## 5. Testing Strategy

### Existing tests (still valid, minor adjustments)

All existing `WorkerNotifierTest` tests for signal/await/coalescing/multi-queue/onRemoteSignal remain valid. Tests that assert HTTP request count synchronously after `signal()` need adjustment: use `advanceUntilIdle()` to let the background collector execute.

### New tests

| Test | Assertion |
|------|-----------|
| `signal returns before HTTP completes` | Mock engine with 5s delay, assert `signal()` returns within 50ms |
| `rapid signals coalesce HTTP broadcasts` | 10 rapid `signal()` calls with 1 peer, assert < 10 HTTP requests (ideally 1-2) |
| `shutdown cancels broadcast collectors` | Call `shutdown()`, assert subsequent signals don't trigger HTTP |

---

## 6. Files Changed

| File | Change |
|------|--------|
| `src/main/kotlin/worker/adapter/http/HttpWorkerNotifier.kt` | Add `broadcastScope`, launch collector per queue in `flowFor()`, simplify `signal()` to single `tryEmit()`, add `@PreDestroy` |
| `src/main/kotlin/infrastructure/http/HttpClientProducer.kt` | Add `HttpTimeout` install (2s connect + 2s request) |
| `src/test/kotlin/worker/adapter/http/WorkerNotifierTest.kt` | Adjust async assertions, add fire-and-forget + coalescing + shutdown tests |

**No interface changes. No new classes. No config changes. No schema changes.**
