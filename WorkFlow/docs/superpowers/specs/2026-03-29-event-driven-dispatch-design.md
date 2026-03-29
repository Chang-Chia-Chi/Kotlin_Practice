# Event-Driven Task Dispatch Design

**Date:** 2026-03-29
**Status:** Proposed
**Scope:** Replace idle polling in WorkerLoop with event-driven dispatch via in-process signaling + cross-pod HTTP broadcast with K8s Endpoints Watch for peer discovery.

---

## 1. Problem Statement

The current WorkerLoop polls the database every 1 second (`delay(pollInterval)`) via `SELECT FOR UPDATE SKIP LOCKED`, regardless of whether new tasks exist. This creates three performance bottlenecks:

1. **Dispatch latency:** 0-1000ms average idle wait between a task becoming PENDING and a worker claiming it. A 5-phase linear workflow with instant handlers pays ~5s in pure dispatch latency.
2. **Burst drain inefficiency:** Fan-out of 50 tasks with `batchSize=1` and `concurrency=4` requires ~13 poll cycles (~13s) to drain.
3. **Idle waste:** 4 queries/second/pod even when no work exists. At 10 pods, 40 queries/second to Oracle for nothing.

### Comparison with Industry Frameworks

| Framework | Dispatch Model | Idle Cost | Dispatch Latency |
|-----------|---------------|-----------|-----------------|
| **This engine (current)** | Fixed-interval polling (1s) | 4 queries/sec/pod | 0-1000ms |
| **Temporal** | Long-polling / sticky task queues | ~0 | ~0ms |
| **Kestra** | Event-driven (Kafka/JDBC reactive) | ~0 | ~0ms |
| **Airflow** | Scheduler loop + Celery/K8s executor | Moderate | Seconds |

---

## 2. Design Overview

Replace `delay(pollInterval)` with a suspend-based notification mechanism. Workers suspend on a `SharedFlow` and wake instantly when signaled. Three signal sources notify workers when new PENDING tasks are inserted.

For multi-pod deployments, the signaling pod broadcasts an HTTP notification to all peer pods. Peer discovery uses a Kubernetes Endpoints Watch for real-time updates.

A lightweight fallback probe (every 5s) catches edge cases where signals are lost (network partition, pod startup race, missed HTTP).

### Architecture

```
Task inserted (any pod)
  -> DispatchNotifier.signal(queueName)
     -> SharedFlow.tryEmit()        -> local workers wake instantly
     -> HTTP POST to each peer      -> remote workers wake instantly
        -> PeerRegistry.peers()     -> always current via K8s Watch
           -> onRemoteSignal()      -> SharedFlow.tryEmit() on remote pod
```

### Design Constraints

- **Oracle stays as source of truth.** `SELECT FOR UPDATE SKIP LOCKED` remains the task claiming mechanism. Notifications are performance hints, never correctness requirements.
- **No new infrastructure.** Uses K8s API (already available for leader election) and HTTP (already served by Quarkus).
- **Portable persistence.** No Oracle-specific features (AQ, DBMS_ALERT). The notification layer is application-level.
- **No schema changes.**
- **Minor breaking changes OK.** Config property `poll-interval` deprecated in favor of `fallback-poll-interval`.

---

## 3. Detailed Design

### 3.1 DispatchNotifier

Single `@ApplicationScoped` bean that handles both local wake-up and remote broadcast.

```kotlin
@ApplicationScoped
class DispatchNotifier(
    private val peerRegistry: PeerRegistry,
    private val webClient: WebClient,        // Vert.x WebClient (non-blocking, ships with Quarkus)
) {
    private val flows = ConcurrentHashMap<String, MutableSharedFlow<Unit>>()

    private fun flowFor(queue: String) = flows.getOrPut(queue) {
        MutableSharedFlow(
            replay = 0,
            extraBufferCapacity = 1,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
        )
    }

    /**
     * Signal that new work is available. Wakes local workers and
     * broadcasts to all peer pods via HTTP (fire-and-forget).
     * HTTP calls are non-blocking (Vert.x WebClient) — this method
     * returns immediately without suspending.
     *
     * Called by: BarrierService, WorkflowEngine.
     */
    fun signal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
        val peers = peerRegistry.peers()
        for (peer in peers) {
            // Fire-and-forget: Vert.x WebClient is non-blocking.
            // Failures are silently ignored — correctness does not
            // depend on delivery. Fallback poll covers missed signals.
            webClient.post(8080, peer, "/internal/dispatch-notify?queue=$queueName")
                .send()
                .onFailure { log.debug("Peer notify failed for {}: {}", peer, it.message) }
        }
    }

    /**
     * Called by the HTTP endpoint when a remote pod signals us.
     * Wakes local workers only — does NOT re-broadcast (no loops).
     */
    fun onRemoteSignal(queueName: String) {
        flowFor(queueName).tryEmit(Unit)
    }

    /**
     * Suspend until work is signaled or timeout expires.
     * Called by WorkerLoop in the poll-and-process cycle.
     */
    suspend fun awaitWork(queueName: String, timeout: Duration): Boolean {
        return withTimeoutOrNull(timeout.toMillis()) {
            flowFor(queueName).first()
        } != null
    }
}
```

**SharedFlow configuration rationale:**

| Parameter | Value | Reason |
|-----------|-------|--------|
| `replay` | 0 | New waiters should not replay stale signals |
| `extraBufferCapacity` | 1 | If no collectors active when signal fires, buffer one signal for next waiter |
| `onBufferOverflow` | `DROP_OLDEST` | Rapid signals coalesce — 50 inserts = 1 wake-up |

### 3.2 PeerRegistry

Maintains a live list of peer pod IPs via Kubernetes Endpoints Watch.

```kotlin
@ApplicationScoped
class PeerRegistry(
    private val client: KubernetesClient,
    private val config: FrameworkConfig,
) {
    @Volatile
    private var _peers: List<String> = emptyList()

    fun peers(): List<String> = _peers

    fun start(@Observes ev: StartupEvent) {
        val myIp = config.worker().podIp()

        client.endpoints()
            .inNamespace(config.leaderElection().namespace())
            .withName(config.serviceName())
            .watch(object : Watcher<Endpoints> {
                override fun eventReceived(action: Watcher.Action, endpoints: Endpoints) {
                    _peers = endpoints.subsets
                        .flatMap { it.addresses.map { addr -> addr.ip } }
                        .filter { it != myIp }
                }

                override fun onClose(cause: WatcherException?) {
                    if (cause != null) log.warn("Endpoints watch closed, will reconnect", cause)
                }
            })
    }
}
```

**Behavior under dynamic scaling:**

| Event | Effect | Impact |
|-------|--------|--------|
| Pod scales up | Watch event fires, new IP added to `_peers` | New pod receives broadcast within seconds |
| Pod scales down | Watch event fires, IP removed from `_peers` | No stale broadcasts |
| Rolling deploy | Watch events for remove + add | Transient peer list churn, HTTP failures ignored |
| Pod not yet ready | Not in Endpoints `addresses` (only `notReadyAddresses`) | Excluded automatically |
| Watch disconnects | Fabric8 auto-reconnects, `onClose` logs warning | `_peers` stale briefly, fallback poll covers it |

### 3.3 Internal HTTP Endpoint

```kotlin
@Path("/internal/dispatch-notify")
class DispatchNotifyResource(
    private val notifier: DispatchNotifier,
) {
    @POST
    fun notify(@QueryParam("queue") queue: String = "default") {
        notifier.onRemoteSignal(queue)
    }
}
```

No authentication — relies on Kubernetes NetworkPolicy for pod-to-pod traffic isolation. The endpoint is idempotent and side-effect-free (worst case: a spurious wake-up that finds no work).

### 3.4 WorkerLoop Changes

Two changes inside `pollAndProcess`:

**Change 1 — Replace delay with awaitWork:**

```kotlin
// BEFORE
if (tasks.isEmpty()) {
    claimTotal("empty").increment()
    delay(pollInterval.toMillis())
    return@withContext
}

// AFTER
if (tasks.isEmpty()) {
    claimTotal("empty").increment()
    notifier.awaitWork(queueName, config.worker().fallbackPollInterval())
    return@withContext
}
```

**Change 2 — Configurable batch size:**

```kotlin
// BEFORE
val tasks = taskRepo.claimNext(workerId, batchSize)      // fixed 1

// AFTER
val tasks = taskRepo.claimNext(workerId, maxBatchSize)    // configurable, default 16
```

Each slot claims up to `maxBatchSize` tasks per poll and processes them sequentially. With `concurrency=4` slots, the system can have up to `4 * maxBatchSize` tasks claimed at once.

**Trade-off:** Larger batch = fewer claim round-trips, faster burst drain. But claimed tasks sit PROCESSING in memory — they extend the visibility window (time before sweeper would reclaim a stuck task). Default of 16 balances throughput against visibility risk. Tune based on handler duration: fast handlers (< 100ms) can use 32-64; slow handlers (minutes) should stay at 1-4.

The flow pipeline is unchanged:

```
indefinitelyRepeat(Unit)
    .takeUntilSignal(stopChannel)
    .unorderedMapAsync(concurrency) { pollAndProcess(...) }
    .collect {}
```

### 3.5 Signal Source Integration

Signals are placed **after the transaction commits** to avoid false wake-ups from rolled-back transactions.

**BarrierService.onTaskCompleted — after second transaction:**

```kotlin
suspend fun onTaskCompleted(...) {
    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        taskRepo.updateStatusWithHandle(handle, taskId, status, resultJson, claimedBy, claimedAt)
        // ...
    }

    var signalQueue: String? = null

    jdbi.inTransactionSuspend<Unit, Exception> { handle ->
        val nonTerminal = taskRepo.countNonTerminalWithHandle(handle, workflowId, sequenceNumber)
        if (nonTerminal > 0) return@inTransactionSuspend
        signalQueue = evaluateAndAdvance(handle, workflowId, sequenceNumber)
        // evaluateAndAdvance returns the queue name of the next phase's tasks (or null)
    }

    if (signalQueue != null) notifier.signal(signalQueue!!)
}
```

**WorkflowEngine.startWorkflow — after transaction:**

```kotlin
suspend fun startWorkflow(definition: WorkflowDefinition): String {
    val (wfId, queueName) = jdbi.inTransactionSuspend<Pair<String, String>, Exception> { handle ->
        // ... insert workflow + first task ...
        Pair(workflowId, firstActivity.queue)
    }
    notifier.signal(queueName)
    return wfId
}
```

**Sweeper — inherited:** Sweeper calls `barrierService.recoverStuckWorkflow()` which goes through the same `evaluateAndAdvance` path and signals automatically.

### 3.6 Configuration

New properties in `FrameworkConfig`:

```properties
# New
framework.worker.fallback-poll-interval=PT5S    # safety-net probe interval
framework.worker.max-batch-size=16              # per-slot batch ceiling (tune by handler speed)
framework.service-name=workflow-engine          # K8s Service name for peer discovery
framework.worker.pod-ip=${POD_IP:localhost}     # from K8s downward API

# Deprecated (replaced by fallback-poll-interval)
# framework.worker.poll-interval=PT1S
```

**K8s Pod spec for POD_IP injection:**

```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

---

## 4. RBAC Requirements

The existing `k8s/rbac.yaml` grants Lease access for leader election. Add Endpoints access for peer discovery:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: workflow-engine-leader
  namespace: default
rules:
  # Existing: leader election
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
  # New: peer discovery for dispatch notification
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get", "list", "watch"]
```

No ClusterRole needed — Endpoints are watched in the same namespace as the pods.

---

## 5. Performance Characteristics

### Dispatch Latency

| Scenario | Before (1s poll) | After |
|----------|------------------|-------|
| Same-pod phase transition | 0-1000ms | ~0ms (in-process signal) |
| Cross-pod phase transition | 0-1000ms | ~1-5ms (HTTP within cluster) |
| Fan-out burst (50 tasks) | ~13s (13 poll cycles) | ~50ms (1 adaptive batch claim) |
| New workflow (same pod) | 0-1000ms | ~0ms |
| New workflow (cross pod) | 0-1000ms | ~1-5ms |

### Resource Usage

| Metric | Before | After |
|--------|--------|-------|
| Idle DB queries (10 pods) | 40/sec | 2/sec (fallback probes) |
| Connections held during idle | 4/pod (polling) | 0 (suspended coroutines) |
| K8s API connections | 1/pod (leader lease) | 2/pod (lease + endpoints watch) |
| Network (notifications) | 0 | ~1 HTTP POST/peer per signal |

### End-to-End Workflow Latency

5-phase linear workflow with instant handlers:

| | Before | After |
|---|--------|-------|
| Dispatch latency per phase | ~500ms avg | ~0ms (same-pod) |
| Total dispatch overhead | ~2.5s | ~0ms |
| Barrier + CAS per phase | ~5ms | ~5ms (unchanged) |
| Total wall clock | ~2.5s | ~25ms |

---

## 6. Failure Modes and Safety

The notification layer is a **performance optimization, not a correctness mechanism**. Task claiming via `SELECT FOR UPDATE SKIP LOCKED` remains the single source of truth.

| Failure | Effect | Recovery |
|---------|--------|----------|
| HTTP broadcast fails (pod unreachable) | Remote workers don't wake | Fallback probe (5s) claims work |
| SharedFlow signal lost (no collectors) | Buffered (capacity=1), next waiter gets it | If buffer also missed: fallback probe |
| K8s Watch disconnects | Peer list stale | Fabric8 auto-reconnects; stale entries cause failed HTTP (ignored) |
| All notifications fail simultaneously | Workers fall back to 5s probe | Identical to polling at 5s interval |
| Spurious wake-up | Worker runs claimNext, finds nothing | Returns to awaitWork (no harm) |
| Duplicate signals | Multiple wake-ups, workers race on SKIP LOCKED | Disjoint claims, no contention |

**Invariant:** Removing the entire notification layer degrades performance to 5s-poll mode. It never affects correctness.

---

## 7. Testing Strategy

### Unit Tests

| Test | Assertion |
|------|-----------|
| `signal()` wakes single `awaitWork()` | Coroutine resumes within 50ms |
| `signal()` wakes multiple concurrent waiters | All suspended coroutines resume |
| `awaitWork()` returns false on timeout | No signal sent, verify timeout behavior |
| Multi-queue isolation | Signal on queue "a" does not wake waiter on queue "b" |
| `onRemoteSignal()` wakes local only | No HTTP broadcast triggered |
| Signal coalescing | 100 rapid signals = 1 wake-up (SharedFlow DROP_OLDEST) |
| `PeerRegistry` watch events | ADD/MODIFY/DELETE update peer list correctly |
| `PeerRegistry` self-exclusion | Pod's own IP not in peers() |

### Integration Tests

| Test | Assertion |
|------|-----------|
| End-to-end dispatch latency | Submit workflow, assert first task claimed within 100ms |
| Phase transition dispatch | Complete task, assert next-phase task claimed within 100ms |
| Fan-out burst drain | Scatter produces 50 tasks, all claimed within 500ms (adaptive batch) |
| Fallback correctness | Disable notifier signaling, tasks still claimed within fallbackPollInterval |
| Cross-instance notification | Two WorkerLoop instances (simulated), signal from one wakes the other via HTTP endpoint |

### Benchmark Updates

Update existing B1/B2/B3 benchmarks to verify latency improvement:
- B1: p50 should drop from ~500ms to <50ms
- B2: wall clock for 5 workflows x 52 tasks should improve significantly
- B3: 5-phase pipeline dispatch overhead should drop from ~2.5s to <100ms

---

## 8. Files Changed

| File | Type | Change |
|------|------|--------|
| `src/main/kotlin/worker/DispatchNotifier.kt` | New | SharedFlow-based notifier with local + HTTP broadcast |
| `src/main/kotlin/worker/PeerRegistry.kt` | New | K8s Endpoints Watch, maintains live peer list |
| `src/main/kotlin/worker/DispatchNotifyResource.kt` | New | Internal HTTP endpoint for remote signals |
| `src/main/kotlin/worker/WorkerLoop.kt` | Modified | `delay()` -> `awaitWork()`, adaptive batch sizing |
| `src/main/kotlin/engine/BarrierService.kt` | Modified | Inject notifier, call `signal()` after advance |
| `src/main/kotlin/engine/WorkflowEngine.kt` | Modified | Call `signal()` after startWorkflow |
| `src/main/kotlin/config/FrameworkConfig.kt` | Modified | New config: fallback-poll-interval, max-batch-size, service-name, pod-ip |
| `k8s/rbac.yaml` | Modified | Add Endpoints get/list/watch permission |
| `src/test/kotlin/worker/DispatchNotifierTest.kt` | New | Unit tests for signal/await/coalescing/multi-queue |
| `src/test/kotlin/worker/PeerRegistryTest.kt` | New | Unit tests for watch events, self-exclusion |
| `src/test/kotlin/worker/DispatchNotifyResourceTest.kt` | New | Endpoint test |
| `src/test/kotlin/worker/WorkerLoopTest.kt` | Modified | Update for notifier injection, test dispatch latency |
| `src/test/kotlin/stress/ThroughputBenchmarkTest.kt` | Modified | Verify latency improvements in B1/B2/B3 |

**No schema changes. No new database tables.**

---

## 9. Migration

1. Add new config properties with defaults (zero-config for existing deployments)
2. `poll-interval` remains functional but deprecated — if set and `fallback-poll-interval` is not, use `poll-interval` as fallback
3. Update `k8s/rbac.yaml` before deploying new version
4. Add `POD_IP` environment variable to pod spec
5. Rolling deploy — new pods with notification, old pods with polling. Both work correctly. SKIP LOCKED prevents conflicts.

---

## 10. Future Extensions

The `DispatchNotifier` interface allows swapping the notification mechanism without changing WorkerLoop or BarrierService:

- **Redis pub/sub:** For environments without K8s or needing sub-millisecond cross-pod notification
- **PostgreSQL LISTEN/NOTIFY:** When porting to PostgreSQL
- **gRPC streaming:** Persistent bidirectional connections between pods for high-frequency signaling
- **Rate limiting:** `signal()` can incorporate token-bucket throttling per queue before broadcasting

These are not in scope for this design.
