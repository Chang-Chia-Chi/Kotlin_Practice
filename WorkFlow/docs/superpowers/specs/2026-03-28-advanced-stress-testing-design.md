# Advanced Stress Testing: SQL Fault Injection, Throughput Benchmarks, History Checker

**Goal:** Extend the stress test suite with three capabilities inspired by MIT 6.824, hashicorp/raft, MicroRaft, and Jepsen testing patterns: (1) typed SQL-layer fault injection that goes beyond Toxiproxy's network-level faults, (2) throughput benchmarks establishing performance baselines, (3) a lightweight post-hoc history checker for correctness properties.

**Motivation:** The existing 41 stress tests validate liveness, correctness, idempotency, and resilience using Toxiproxy for network faults. But Toxiproxy operates at the TCP level -- it can't selectively fail a CAS update while allowing reads, simulate contention on `SKIP LOCKED`, or inject partial transaction failures. Throughput is untested. Subtle MVCC anomalies (duplicate execution, lost tasks) are checked per-test but not systematically.

---

## Phase 1: SQL Fault Injection Layer

### Architecture

A `FaultInjector` wrapping a JDBC `DataSource`. It returns proxy `Connection` and `PreparedStatement` objects that check registered fault rules before executing SQL.

```
StressTestBase
  +-- proxyDataSource (HikariCP -> Toxiproxy -> Oracle)
       +-- FaultInjectingDataSource(proxyDataSource, faultInjector)
            +-- proxyJdbi = Jdbi.create(faultInjectingDataSource)
```

### FaultInjector API

```kotlin
class FaultInjector {
    fun onSql(pattern: Regex): FaultRule
    fun reset()  // clear all rules -- call in @AfterEach
}

class FaultRule {
    fun failNext(times: Int = 1, exception: SQLException = SQLException("injected fault")): FaultRule
    fun delay(duration: Duration): FaultRule
    fun returnEmpty(times: Int = 1): FaultRule
    fun failNth(n: Int, exception: SQLException = SQLException("injected fault")): FaultRule
}
```

**Implementation approach:**
- `FaultInjectingDataSource` implements `DataSource`, delegates to the real one.
- `getConnection()` returns a `FaultInjectingConnection` (JDK dynamic proxy or manual delegation).
- `prepareStatement(sql)` checks `sql` against registered `FaultRule` patterns.
- Matching rules execute their fault behavior (throw, delay, wrap ResultSet with empty).
- Thread-safe: rules use `AtomicInteger` counters for `failNext`/`returnEmpty` decrements.

### Fault Types

| Type | Behavior | Use Case |
|---|---|---|
| `failNext(n)` | Throw `SQLException` on next N matching executions | CAS deadlock, connection error |
| `delay(duration)` | Sleep before executing matching statement | Slow disk, GC pause |
| `returnEmpty(n)` | Return empty `ResultSet` on next N matching SELECTs | Full contention, stale read |
| `failNth(n)` | Fail only the Nth matching execution (others pass) | Partial commit -- TX1 ok, TX2 fails |

### Test Scenarios (FaultInjectionStressTest)

| ID | Name | Fault Rule | Expected Outcome |
|---|---|---|---|
| F1 | CAS deadlock during phase advance | `failNext(1)` on `UPDATE workflow.*version` | Sweeper retries on next patrol, workflow completes |
| F2 | Full task contention | `returnEmpty(3)` on `SELECT.*FOR UPDATE SKIP LOCKED` | Workers back off, eventually claim after rules expire |
| F3 | Slow INSERT during fan-out | `delay(3s)` on `INSERT INTO task` | Fan-out completes slowly but correctly, no timeout |
| F4 | Partial commit -- task ok, CAS fails | `failNth(2)` within barrier transaction | Task COMPLETED but workflow not advanced, sweeper recovers |
| F5 | Intermittent barrier stale read | `returnEmpty(1)` on `SELECT COUNT.*task` | Barrier sees 0 non-terminal, fires prematurely or retries correctly |
| F6 | Deadlock storm then recovery | `failNext(20)` on `UPDATE.*version`, reset after 5s | System stalls during storm, converges after reset |

### File

- `src/test/kotlin/stress/FaultInjector.kt` -- `FaultInjector`, `FaultRule`, `FaultInjectingDataSource`, `FaultInjectingConnection`
- `src/test/kotlin/stress/FaultInjectionStressTest.kt` -- F1-F6
- Modify `src/test/kotlin/stress/StressTestBase.kt` -- wire `FaultInjector` around `proxyDataSource`

---

## Phase 2: Throughput Benchmark Harness

### Architecture

A `BenchmarkHarness` utility that wraps workflow submission + assertion into a timed measurement. No external framework -- `Instant.now()` deltas with percentile math.

```kotlin
data class BenchmarkResult(
    val label: String,
    val totalWorkflows: Int,
    val totalTasks: Int,
    val wallClockMs: Long,
    val workflowsPerSec: Double,
    val tasksPerSec: Double,
    val latencies: List<Long>,  // per-workflow completion time in ms
) {
    val p50ms: Long
    val p95ms: Long
    val p99ms: Long

    fun print()  // formatted stdout output
}
```

**Measurement approach:**
1. Record `Instant.now()` before submitting each workflow.
2. `assertWorkflowTerminates` records completion time per workflow.
3. Wall clock = max(completion) - min(submission).
4. Per-workflow latency = completion[i] - submission[i].
5. Percentiles computed from sorted latency list.

**Results are printed, not asserted.** Machine variance makes absolute thresholds brittle. Regressions are detected by comparing runs, not by hardcoded bounds.

### Benchmark Scenarios (ThroughputBenchmarkTest)

| ID | Name | Setup | Measures |
|---|---|---|---|
| B1 | Single-activity throughput | N workflows x 1 task, PassThroughHandler | Raw claim-execute-barrier rate |
| B2 | Fan-out/join throughput | N workflows, each scatter->50 parallel->join | Barrier contention under parallel completion |
| B3 | Multi-phase pipeline | N workflows x 5 sequential phases | Phase-advance CAS overhead |
| B4 | Throughput under fault | Same as B1 + Toxiproxy 500ms latency mid-run | Degradation ratio vs B1 baseline |
| B5 | Sweep overhead at scale | 100 stuck workflows, no workers, sweeper only | Patrol time and recovery throughput |

### File

- `src/test/kotlin/stress/BenchmarkHarness.kt` -- `BenchmarkResult`, timing utilities
- `src/test/kotlin/stress/ThroughputBenchmarkTest.kt` -- B1-B5, tagged `@Tag("benchmark")`

---

## Phase 3: Lightweight History Checker

### Architecture

A `HistoryRecorder` that wraps `TransitionHandler` and records operation events. A `HistoryChecker` scans the recorded history + final DB state for property violations.

```kotlin
data class HistoryEvent(
    val taskId: String,
    val workflowId: String,
    val thread: String,
    val timestamp: Instant,
    val type: EventType,  // CLAIM, EXECUTE_START, EXECUTE_END, COMPLETE, FAIL
)

class HistoryRecorder(private val delegate: TransitionHandler) : TransitionHandler {
    val events: ConcurrentLinkedQueue<HistoryEvent>
    // Records EXECUTE_START before delegate.execute(), EXECUTE_END after
}

object HistoryChecker {
    fun noDuplicateExecution(events: List<HistoryEvent>): List<Violation>
    fun monotonicPhase(events: List<HistoryEvent>, dbState: Map<String, Any?>): List<Violation>
    fun noLostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>): List<Violation>
    fun noGhostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>): List<Violation>
}
```

### Properties

| Property | What It Checks | Violation Example |
|---|---|---|
| NO_DUPLICATE_EXECUTION | No taskId has >1 EXECUTE_END event | Two workers both ran the same task |
| MONOTONIC_PHASE | Workflow current_sequence never decreases over time | CAS race caused phase regression |
| NO_LOST_TASKS | Every task that was PENDING eventually reached a terminal state | Task stuck in PROCESSING forever |
| NO_GHOST_TASKS | Every EXECUTE_END event corresponds to a DB row in COMPLETED/FAILED | Handler ran but DB has no record |

### Integration with Existing Tests

Not a new test class. Add `HistoryRecorder` to existing tests where the property check adds value:

| Test | Property |
|---|---|
| C1 (CAS race) | NO_DUPLICATE_EXECUTION |
| I1 (sweeper+worker race) | NO_DUPLICATE_EXECUTION |
| I7 (SKIP LOCKED) | NO_DUPLICATE_EXECUTION |
| F4 (partial commit) | NO_LOST_TASKS |
| F6 (deadlock storm) | MONOTONIC_PHASE + NO_LOST_TASKS |

### File

- `src/test/kotlin/stress/HistoryRecorder.kt` -- `HistoryEvent`, `HistoryRecorder`, `HistoryChecker`
- Modify existing tests to wrap handlers with `HistoryRecorder` and call `HistoryChecker.verify()` in assertions

---

## Complete File Map

| Action | File | Phase |
|---|---|---|
| Create | `src/test/kotlin/stress/FaultInjector.kt` | 1 |
| Create | `src/test/kotlin/stress/FaultInjectionStressTest.kt` | 1 |
| Modify | `src/test/kotlin/stress/StressTestBase.kt` | 1 |
| Create | `src/test/kotlin/stress/BenchmarkHarness.kt` | 2 |
| Create | `src/test/kotlin/stress/ThroughputBenchmarkTest.kt` | 2 |
| Create | `src/test/kotlin/stress/HistoryRecorder.kt` | 3 |
| Modify | `src/test/kotlin/stress/CorrectnessStressTest.kt` | 3 |
| Modify | `src/test/kotlin/stress/IdempotencyStressTest.kt` | 3 |
| Modify | `src/test/kotlin/stress/FaultInjectionStressTest.kt` | 3 |

---

## Design Inspiration Sources

| Source | Pattern Adopted |
|---|---|
| MIT 6.824 (Raft Labs) | Convergence-after-heal assertions, repeated runs for nondeterminism |
| MicroRaft (Java) | Typed fault injection (selective operation failure), `allTheTime` safety assertions |
| etcd/raft | Pure state machine testability -- fail specific operations, not the whole network |
| Jepsen/Maelstrom Kafka workload | Completeness checker: no lost tasks, no duplicates, monotonic progression |
| hashicorp/raft | Stability polling -- assert state is stable for a settling period |
| FoundationDB | Injectable I/O layer for deterministic fault exploration |
