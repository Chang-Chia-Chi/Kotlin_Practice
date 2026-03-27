# Session 7 — Metrics & Health Probes

**Tier:** 3 (operational readiness)
**Prerequisites:** Session 1-2 (drain window, DEAD_LETTER status)
**Estimated scope:** New metrics registrations + health check class + tests

---

## Items

### R3.1 — Register `_inFlightTasks` as Micrometer gauge

**Problem:** `WorkerLoop._inFlightTasks` (AtomicInteger) tracks active handler count but is never exposed as a metric. Operators cannot monitor bulkhead utilization.

**Files to modify:**
- `src/main/kotlin/worker/WorkerLoop.kt` — inject `MeterRegistry`, register gauge in `onStart`

**Fix:**
```kotlin
meterRegistry.gauge(
    "taskqueue_worker_in_flight_tasks",
    Tags.of("pod", config.worker().id()),
    _inFlightTasks
) { it.get().toDouble() }

meterRegistry.gauge(
    "taskqueue_worker_concurrency_limit",
    Tags.of("pod", config.worker().id()),
    config.worker().concurrency().toDouble()
)
```

Utilization ratio: `taskqueue_worker_in_flight_tasks / taskqueue_worker_concurrency_limit`.

---

### R3.2 — Add claim rate counter + SKIP LOCKED contention metric

**Problem:** No metric exists for claim attempts vs. claim successes. Operators cannot detect SKIP LOCKED contention (many attempts, few successes = hot index contention).

**Files to modify:**
- `src/main/kotlin/worker/WorkerLoop.kt` — around `claimNext` call in `pollAndProcess`

**Fix:**
```kotlin
private lateinit var claimAttempts: Counter
private lateinit var claimSuccesses: Counter

// In onStart:
claimAttempts = meterRegistry.counter("taskqueue_claim_attempts_total", "pod", config.worker().id())
claimSuccesses = meterRegistry.counter("taskqueue_claim_successes_total", "pod", config.worker().id())

// In pollAndProcess:
claimAttempts.increment()
val tasks = taskRepo.claimNext(workerId, batchSize)
claimSuccesses.increment(tasks.size.toDouble())
```

Contention indicator: `rate(taskqueue_claim_attempts_total[5m]) - rate(taskqueue_claim_successes_total[5m])`.

---

### R3.3 — Add handler execution duration histogram by type

**Problem:** No per-handler timing exists. Operators cannot identify slow handlers or set latency SLOs.

**Files to modify:**
- `src/main/kotlin/worker/WorkerLoop.kt` — around `handler.execute(input)` in `processTask`

**Fix:**
```kotlin
val sample = Timer.start(meterRegistry)
val output = handler.execute(input)
sample.stop(
    Timer.builder("taskqueue_handler_duration_seconds")
        .tag("handler", task.handlerKey)
        .tag("status", "success")
        .publishPercentileHistogram()
        .register(meterRegistry)
)
```

On failure, record with `status=failure`. Use `Timer.builder` with `publishPercentileHistogram()` for Prometheus histogram buckets.

---

### R3.4 — Add liveness check for stuck consumer loop

**Problem:** `WorkerLoop._lastPollTimestamp` tracks the last poll time but is never exposed as a health indicator. A stuck loop (thread deadlock, infinite handler) will not surface as a liveness probe failure.

**Files to modify:**
- New file: `src/main/kotlin/worker/WorkerLoopHealthCheck.kt`

**Implementation:**
```kotlin
@Liveness
@Singleton
class WorkerLoopHealthCheck(
    private val workerLoop: WorkerLoop,
    private val config: FrameworkConfig,
) : HealthCheck {
    override fun call(): HealthCheckResponse {
        val lastPoll = workerLoop.lastPollTimestamp
        val threshold = config.worker().pollInterval().multipliedBy(5)
        val age = Duration.between(lastPoll, Instant.now())

        return if (age < threshold) {
            HealthCheckResponse.up("worker-loop")
        } else {
            HealthCheckResponse.named("worker-loop")
                .down()
                .withData("last_poll_age_seconds", age.seconds)
                .withData("threshold_seconds", threshold.seconds)
                .build()
        }
    }
}
```

Threshold: 5x poll interval. With default `PT1S`, the probe fails after 5 seconds of no polls.

Requires `quarkus-smallrye-health` extension in `pom.xml` (verify it is present).

---

### R3.5 — Add stale-leader health check + heartbeat age gauge

**Problem:** No mechanism detects "I think I'm leader but my lease is stale." If the K8s API becomes unreachable, `isActive` remains true until `onStoppedLeading` fires. The sweeper continues running with a stale lease.

**Files to modify:**
- `src/main/kotlin/leader/LeaderManager.kt` — add heartbeat age gauge in `registerMetrics`
- New file: `src/main/kotlin/leader/LeaderHealthCheck.kt`

**Gauge:**
```kotlin
meterRegistry.gauge(
    "leader_election_heartbeat_age_seconds",
    this
) { Duration.between(lastHeartbeat, Instant.now(clock)).toSeconds().toDouble() }
```

**Health check:**
```kotlin
@Liveness
@Singleton
class LeaderHealthCheck(
    private val leaderElection: LeaderElection,
    private val config: FrameworkConfig,
) : HealthCheck {
    override fun call(): HealthCheckResponse {
        if (!leaderElection.isActive) {
            return HealthCheckResponse.up("leader-election") // followers are always healthy
        }
        val age = Duration.between(leaderElection.lastHeartbeat, Instant.now())
        val threshold = config.leaderElection().leaseDuration()
        return if (age < threshold) {
            HealthCheckResponse.up("leader-election")
        } else {
            HealthCheckResponse.named("leader-election")
                .down()
                .withData("heartbeat_age_seconds", age.seconds)
                .build()
        }
    }
}
```

---

## Verification

1. `mvn test` passes
2. New tests for each health check (mock stuck loop, mock stale heartbeat)
3. Start `mvn quarkus:dev`, hit `/q/health/live` and `/q/health/ready`, verify new checks appear
4. Hit `/q/metrics`, verify new gauges/counters/histograms are present with correct names
