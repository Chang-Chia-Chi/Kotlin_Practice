# Stress Test Design: Distributed Failure & Race Condition Coverage

**Date:** 2026-03-28
**Status:** Draft
**Goal:** Prove that the workflow engine does not hang under distributed system failures and race conditions. Every workflow must reach a terminal state (COMPLETED, FAILED, TIMED_OUT, CANCELLED) regardless of worker crashes, network partitions, CAS contention, timeout races, or infrastructure disruption.

---

## 1. Approach

**Layered testing:**
- **Integration stress tests** — Real Oracle (Testcontainers) + Toxiproxy for network faults, multiple concurrent coroutines simulating workers. Full stack: claim -> execute -> barrier -> CAS -> next-phase insertion.
- **Concurrency unit tests** — Real concurrency (multiple coroutines racing through BarrierService, WorkerLoop, Sweeper). Targets logic-level race conditions.

**Parameterized scale:**
- `MODERATE` (CI default): 10 workers, fan-out 50, 30s outer timeout.
- `HIGH` (pre-release): 50 workers, fan-out 500, 120s outer timeout.
- Driven by `-Dstress.scale=HIGH` system property.

**Dual assertion strategy:**
- **Outer guard:** Time-bounded completion via Awaitility. Catches any hang.
- **Inner assertions:** Invariant-based checks proving the *correct* recovery mechanism fired (e.g., sweeper detected stuck workflow within grace period, stale reclaim count incremented).

**Organization:** 4 test classes by system guarantee — Liveness, Correctness, Idempotency, Resilience.

---

## 2. Test Infrastructure

### 2.1 StressTestBase

Shared base class providing:

- **Oracle + Toxiproxy containers** — Reuses existing `OracleTestContainer` singleton. Adds a Toxiproxy container proxying the Oracle connection for per-test network fault control.
- **Parameterized scale** — `StressScale` enum resolved from `-Dstress.scale` system property:

```kotlin
enum class StressScale(
    val workers: Int,
    val fanOutSize: Int,
    val workflowBatchSize: Int,
    val outerTimeout: Duration,
    val innerMargin: Duration,
) {
    MODERATE(workers = 10, fanOutSize = 50, workflowBatchSize = 5,
             outerTimeout = 30.seconds, innerMargin = 5.seconds),
    HIGH(workers = 50, fanOutSize = 500, workflowBatchSize = 20,
         outerTimeout = 120.seconds, innerMargin = 15.seconds),
}
```

- **Worker pool lifecycle** — Helpers to spin up N `WorkerLoop` coroutines with controllable handlers. Started/stopped per test method via `@BeforeEach` / `@AfterEach`.
- **Sweeper control** — Shortened timers for test speed:

```properties
framework.sweeper.grace-period=2s
framework.sweeper.interval=1s
framework.worker.stale-task-threshold=3s
```

- **Diagnostic dump on failure** — JUnit `TestWatcher` extension that on failure queries and logs all workflow/task states (status, claimed_by, claimed_at, retry_count, updated_at).
- **Test isolation** — Each test method gets a fresh workflow (unique ID). No cross-test state.

### 2.2 Handler DSL

Inline test handler construction:

```kotlin
handler { input -> HandlerOutput(input.payload) }           // pass-through
handler { delay(500); throw RuntimeException("boom") }      // slow failure
failAtBarrier { /* cancel coroutine after TX1 */ }          // crash simulation
```

### 2.3 CrashableHandler

Wrapper that accepts a `CrashPoint` enum controlling where to simulate worker death:

- `BEFORE_HANDLER` — cancel coroutine before handler.execute()
- `MID_HANDLER` — cancel coroutine mid-execution
- `AFTER_HANDLER` — cancel coroutine after handler returns, before barrier call
- `AFTER_TX1` — cancel coroutine after task self-update commits, before CAS

### 2.4 Assertion Helpers

```kotlin
// Outer guard: workflow must reach terminal state
suspend fun assertWorkflowTerminates(workflowId: String, timeout: Duration) {
    Awaitility.await().atMost(timeout).untilAsserted {
        val wf = workflowRepo.findById(workflowId)
        assertThat(wf.status).isIn(COMPLETED, FAILED, TIMED_OUT, CANCELLED)
    }
}

// Inner assertion: sweeper advances stuck workflow within expected window
suspend fun assertSweeperRecovers(
    workflowId: String,
    previousSequence: Int,
    withinGracePeriod: Duration,
    sweepInterval: Duration,
    margin: Duration,
) {
    Awaitility.await()
        .atMost(withinGracePeriod + sweepInterval + margin)
        .untilAsserted {
            val wf = workflowRepo.findById(workflowId)
            assertThat(wf.currentSequence).isGreaterThan(previousSequence)
        }
}
```

### 2.5 JUnit Tags

```kotlin
@Tag("stress")          // all stress tests
@Tag("stress-network")  // Toxiproxy-dependent (ResilienceStressTest, L6a/L6b/L7)
```

Selective execution: `mvn test -Dgroups=stress` or `-DexcludedGroups=stress-network`.

---

## 3. Test Scenarios

### 3.1 Liveness Guarantee — `LivenessStressTest`

"Every workflow reaches a terminal state despite failures."

**Pattern:** Submit workflow -> inject failure -> ensure sweeper running -> assert terminal state (outer) + correct recovery path (inner).

#### Worker Crash Scenarios

| ID | Scenario | Failure Point | Recovery Mechanism |
|----|----------|---------------|-------------------|
| L1 | Worker dies after claiming task, before handler starts | Coroutine cancel post-claim | Stale task reclamation -> retry -> completion |
| L2 | Worker dies mid-handler execution | Coroutine cancel mid-handler | Stale task reclamation -> retry -> completion |
| L3 | Worker dies after handler success, before barrier call | Coroutine cancel post-handler | Task stays PROCESSING -> stale reclaim -> re-execute handler -> barrier |
| L4 | Worker dies after TX1 commit (task COMPLETED), before TX2 (CAS) | Crash between transactions | Stuck workflow detection -> sweeper CAS + advance |
| L5 | All workers die simultaneously, then restart | Kill all coroutines, start new pool | Stale reclaim batch -> retries -> workflows resume |

#### Network Fault Scenarios

| ID | Scenario | Failure Point | Recovery Mechanism |
|----|----------|---------------|-------------------|
| L6a | Network partition during TX1 (task update) | Toxiproxy cut mid-TX1 | TX1 rollback -> task stays PROCESSING -> stale reclaim -> retry |
| L6b | Network partition during TX2 (CAS + advance) | Toxiproxy cut mid-TX2 | TX2 rollback -> task already COMPLETED -> stuck workflow -> sweeper recovery |
| L7 | Network partition during task claim | Toxiproxy cut during SELECT FOR UPDATE | Claim fails -> task stays PENDING -> next poll claims it |

#### Timeout Scenarios

| ID | Scenario | Failure Point | Recovery Mechanism |
|----|----------|---------------|-------------------|
| L8 | Task deadline expires while handler runs slowly | Inject slow handler > deadline | Sweeper TIMED_OUT -> barrier fires with TIMED_OUT |
| L9 | Workflow deadline expires during execution | Short workflow deadline | Sweeper TIMED_OUT, cancels pending tasks |
| L10 | Stale task exhausts retries -> dead-letter -> barrier evaluates | max_retries=1, handler always fails | DEAD_LETTER terminal -> barrier fires -> failure policy decides |

#### Fan-Out & Leader Scenarios

| ID | Scenario | Failure Point | Recovery Mechanism |
|----|----------|---------------|-------------------|
| L11 | Fan-out: all sub-tasks fail under BEST_EFFORT | All handlers throw | All terminal -> JoinPolicy evaluated -> workflow advances or fails |
| L12 | Leader dies during sweep, new leader recovers | Stop leader mid-patrol | New leader elected -> next patrol picks up stuck workflows |

---

### 3.2 Correctness Guarantee — `CorrectnessStressTest`

"No duplicate tasks, no lost transitions, correct policy evaluation under concurrency."

**Pattern:** Submit workflow -> spin up N concurrent workers -> after termination, query DB and assert invariants.

| ID | Scenario | Assertion |
|----|----------|-----------|
| C1 | N workers complete final task of a phase simultaneously (CAS race) | Exactly 1 CAS win -> exactly 1 set of next-phase tasks. Zero duplicates. |
| C2 | Fan-out: scatter produces N payloads -> N sub-tasks atomically | Task count == payload list length. No partial insertion. |
| C3 | Fan-out JoinPolicy.ALL: 1 of N fails | Workflow FAILED (ABORT) or advances (BEST_EFFORT). Not stuck. |
| C4 | Fan-out JoinPolicy.Percentage(95): boundary precision | 95/100 succeed -> pass. 94/100 -> fail. Test at threshold-1, threshold, threshold+1. |
| C5 | Fan-out JoinPolicy.Threshold(N): boundary precision | N succeed -> pass. N-1 -> fail. Parameterized boundary. |
| C6 | FailurePolicy.ABORT mid-phase: task fails while others PROCESSING | Workflow eventually FAILED. No new phase started after failure. |
| C7 | FailurePolicy.BEST_EFFORT: all tasks fail | Workflow advances to next phase despite all failures. |
| C8 | Task result payload -> next phase receives it | Payload hash integrity across phase boundary. |
| C9 | Fan-out sub-task results -> join handler receives all | Join handler input contains complete result set. |
| C10 | Replay: DEAD_LETTER reset, resume from current_sequence | No re-execution of completed phases. Only current + future re-run. |
| C11 | Concurrent barrier probes under high write load | Probe returns 0 only when all tasks genuinely terminal. MVCC correctness. |

---

### 3.3 Idempotency Guarantee — `IdempotencyStressTest`

"Concurrent recovery and re-execution don't corrupt state."

**Pattern:** Arrange state where multiple actors could act -> trigger both concurrently (synchronized via CountDownLatch/CompletableDeferred) -> assert exactly-one semantics.

| ID | Scenario | Assertion |
|----|----------|-----------|
| I1 | Sweeper + worker race on same stuck workflow, both attempt CAS | Exactly 1 CAS wins. No duplicate next-phase tasks. |
| I2 | Two sweeper patrols overlap (brief dual-leader) | Both detect same stuck workflow. One wins. State consistent. |
| I3 | Sweeper expires task as TIMED_OUT at same moment worker completes it | One wins task row update. Barrier fires exactly once. |
| I4 | Sweeper reclaims stale task while worker about to report completion | Either worker TX1 wins (COMPLETED, reclaim no-ops) or reclaim wins (worker TX1 fails, retry). |
| I5 | Replay called while sweeper mid-recovery on same workflow | Replay changes version -> sweeper CAS fails. No conflict. |
| I6 | Sweeper detects same stuck workflow on consecutive patrols | First CAS wins + advances. Second re-probes, sees new tasks, skips. |
| I7 | Worker retries task (PENDING after reclaim) while another already claimed it | SKIP LOCKED: second claimer skips locked row. No double-claim. |
| I8 | Cancel workflow while barrier transaction in-flight | Cancel sets status -> CAS `AND status = 'RUNNING'` fails -> no post-cancel advance. |

---

### 3.4 Resilience Guarantee — `ResilienceStressTest`

"System self-heals after infrastructure failure without manual intervention."

**Pattern:** Submit multiple workflows -> let them progress partially -> inject infrastructure failure -> hold failure -> restore -> assert all workflows terminate + no corruption.

| ID | Scenario | Failure Injection | Assertion |
|----|----------|-------------------|-----------|
| R1 | Oracle unavailable then recovers | Toxiproxy: cut 10-30s | Workers resume, sweeper resumes, workflows complete. |
| R2 | Oracle latency spike | Toxiproxy: add 5s latency | No spurious timeouts (if deadline > latency). Backlog drains after. |
| R3 | Connection pool exhaustion | Toxiproxy: throttle + high concurrency | Workers back off. No permanent stall. Recovery when freed. |
| R4 | Full worker pool dies and restarts | Cancel all coroutines, wait, start new pool | PROCESSING -> stale reclaim -> retry. Workflows complete. |
| R5 | No leader for extended period, then elected | Stop leader > grace period, start new | Stuck workflows accumulate. First patrol recovers all. |
| R6 | Network partition heals after multiple stale reclaim cycles | Toxiproxy: cut > stale threshold x 2 | Tasks reclaimed during partition. After heal, system converges. |
| R7 | Rapid leader election flaps (3-4 changes) | Toggle leader on/off rapidly | No orphaned sweeper coroutines. State consistent after stabilization. |
| R8 | Oracle restarts (connections reset) | Toxiproxy: close all, reopen | Pool reconnects. Stale reclaim picks up. Workflows resume. |

---

## 4. Execution Strategy

### 4.1 Test Profiles

| Profile | Command | Scale | Tests | Expected Duration |
|---------|---------|-------|-------|-------------------|
| Default | `mvn test` | MODERATE | All stress tests | ~2-3 min |
| High | `mvn test -Dstress.scale=HIGH` | HIGH | All stress tests | ~10-15 min |
| Stress only | `mvn test -Dgroups=stress` | MODERATE | Stress tests only | ~2-3 min |
| No network | `mvn test -DexcludedGroups=stress-network` | MODERATE | Exclude Toxiproxy tests | ~1-2 min |

### 4.2 Toxiproxy Patterns

| Pattern | Toxiproxy API | Simulates |
|---------|---------------|-----------|
| Full cut | `proxy.toxics().bandwidth("cut", DOWN, 0)` | Network partition |
| Latency spike | `proxy.toxics().latency("slow", DOWN, 5000)` | Slow Oracle |
| Bandwidth throttle | `proxy.toxics().limitData("throttle", DOWN, 1024)` | Congestion |
| Connection reset | `proxy.disable()` / `proxy.enable()` | Oracle restart |

### 4.3 Test Isolation

- Each test method creates fresh workflow(s) with unique IDs.
- Workers and sweeper started/stopped per test method (`@BeforeEach` / `@AfterEach`).
- No shared mutable state across test methods.

---

## 5. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| 2-step TX reflected in crash scenarios (L4, L6b) | Production barrier splits self-update and CAS into separate transactions. Stuck workflow detection (not stale reclaim) is the recovery path for crashes between TX1 and TX2. |
| Shortened sweeper timers (2s grace, 1s interval) | Production defaults (2 min grace) would make stress tests impractically slow. Short timers still exercise the same code paths. |
| Parameterized scale via system property | Same test logic for fast CI feedback and thorough pre-release validation. No code duplication. |
| JUnit tags for selective execution | Toxiproxy tests need Docker. Tags let environments without Docker skip network fault tests while still running concurrency tests. |
| Diagnostic dump on failure | Stress test failures are notoriously hard to debug. Full state dump on failure makes root-cause analysis practical. |
| CountDownLatch for dual-actor races (idempotency) | Synchronizing two coroutines to fire at the same moment maximizes the chance of hitting the race window. |
| Fresh workflow per test method | Eliminates cross-test interference. Stress tests are stateful by nature; isolation prevents cascading failures. |

---

## 6. Scenario Count

| Guarantee | Test Class | Scenarios |
|-----------|------------|-----------|
| Liveness | `LivenessStressTest` | 12 |
| Correctness | `CorrectnessStressTest` | 11 |
| Idempotency | `IdempotencyStressTest` | 8 |
| Resilience | `ResilienceStressTest` | 8 |
| **Total** | | **39** |
