# Standalone Benchmark Suite Design

## Overview

A standalone CLI benchmark tool that boots Oracle via Testcontainers, runs parameterized workflow scenarios across a matrix of configurations, profiles per-phase timing breakdowns, and persists reproducible results to JSON files for regression tracking.

## Goals

1. **End-to-end with real Oracle** -- standalone runner (not inside `mvn test`) that boots Oracle, runs sustained load, and produces a report.
2. **Bottleneck profiling** -- identify where time is spent (DB round-trips, barrier CAS contention, connection pool exhaustion) with per-phase timing breakdowns and optional Micrometer metrics.
3. **Reproducible baselines** -- auto-persisted timestamped results with environment capture and git commit tracking for regression detection.
4. **Configurable scale** -- quick (2-3 min) for dev, thorough (10-15 min) for stable numbers, soak (30+ min) for saturation and leak detection.

## Architecture & Entry Point

Entry point: `src/test/kotlin/benchmark/BenchmarkMain.kt` -- a `fun main()` that:

1. Parses system properties for configuration
2. Boots Oracle via `OracleTestContainer` (shared singleton)
3. Creates HikariCP pool -> JDBI (direct path, no Toxiproxy)
4. Manually wires engine components with instrumented wrappers
5. Runs the scenario matrix
6. Persists results to `benchmarks/results/<timestamp>.json`
7. Prints summary table to stdout
8. Shuts down (pool, container)

### Run Command

```bash
mvn exec:java -Pbenchmark -Dbench.scale=thorough
mvn exec:java -Pbenchmark -Dbench.scale=soak -Dbench.scenarios=fanout
mvn exec:java -Pbenchmark -Dbench.metrics=true
```

Maven profile (`-Pbenchmark`) configures `exec-maven-plugin` with `classpathScope=test` and `mainClass=benchmark.BenchmarkMainKt`.

### Component Wiring (manual, no CDI)

```
OracleTestContainer -> HikariDataSource -> Jdbi
    -> InstrumentedTaskRepository(TaskRepositoryImpl, PhaseTimer)
    -> InstrumentedBarrierService(BarrierServiceImpl, PhaseTimer)
    -> WorkflowEngine
    -> WorkerLoop (configurable concurrency)
    -> Sweeper (manual trigger, no @Scheduled)
```

Instrumentation is at the boundary via decorators -- the engine itself is unmodified.

## Scenario Shapes

### 1. Single Activity -- Baseline engine overhead

```
[start] -> activity("process") -> [complete]
```

1 task per workflow. Measures: claim -> execute -> barrier -> complete cycle.

### 2. Fan-Out -- Scatter/parallel/join throughput

```
[start] -> scatter("scatter") -> parallel("parallel", F items) -> join("join") -> [complete]
```

1 + F + 1 tasks per workflow. Measures: scatter execution, INSERT...SELECT fan-out creation, join barrier with F tasks, input aggregation.

### 3. Multi-Step Pipeline -- Sequential phase transition overhead

```
[start] -> activity("step-1") -> activity("step-2") -> ... -> activity("step-S") -> [complete]
```

S tasks per workflow. Measures: repeated barrier evaluation + CAS + task creation per step.

## Parameter Matrix

| Dimension | Applies to | Description |
|---|---|---|
| `workflows` | all | Number of concurrent workflows submitted |
| `workers` | all | Worker pool concurrency |
| `fanOutFactor` | fan-out only | Items per scatter result |
| `stepCount` | multi-step only | Sequential activity count |
| `handlerLatencyMs` | all | Simulated handler delay (0 = passthrough) |
| `payloadSizeBytes` | all | Result JSON payload size |

The harness takes the Cartesian product of configured axis values per scenario and runs each combination as an independent benchmark point.

### Handlers

- `PassThroughHandler` -- zero-latency baseline (returns input or generates scatter array)
- `SimulatedLatencyHandler(delayMs)` -- adds `Thread.sleep` to simulate real work
- `PayloadHandler(sizeBytes)` -- returns a result of the specified size

## Scale Profiles & Execution Modes

### Execution Modes

**Batch mode** (quick, thorough): Submit N workflows upfront, wait for all to complete, measure. Same pattern as existing B1-B5.

**Sustained mode** (soak): Continuously submit workflows at a target rate for a fixed duration. A submitter coroutine launches workflows at the target rate (e.g., 50/s = 1 every 20ms). Every 10 seconds, a snapshot is taken: throughput, in-flight count, latency percentiles for workflows completed in that window. If the system can't keep up (in-flight count grows unbounded), the result captures the saturation point.

### Scale Matrix

| | Quick (~2-3 min) | Thorough (~10-15 min) | Soak (30+ min) |
|---|---|---|---|
| **Mode** | Batch | Batch | Sustained |
| **Single** | wf=[20,50], w=[5,10] | wf=[50,100,200], w=[10,20] | rate=50wf/s, dur=120s, w=[10,20,50] |
| **Fan-out** | wf=[5], fanOut=[10,50], w=[10] | wf=[5,10], fanOut=[50,100,500], w=[10,20] | rate=5wf/s, fanOut=[100,500,1000], dur=120s, w=[20,50] |
| **Multi-step** | wf=[10], steps=[3,5], w=[5] | wf=[10,20], steps=[3,5,10], w=[10,20] | rate=10wf/s, steps=[5,10,20], dur=120s, w=[10,20,50] |
| **Handler latency** | [0] | [0, 10] | [0, 10, 50] |
| **Payload size** | [100] | [100, 1000] | [100, 1000, 10000] |

### CLI Overrides

Any matrix value can be overridden:

```bash
mvn exec:java -Pbenchmark -Dbench.scale=thorough -Dbench.workers=50 -Dbench.fanout.factor=1000
```

## Instrumentation

### Phase Timing (always on)

A `PhaseTimer` collects nanosecond recordings per phase via instrumented decorators:

```kotlin
class PhaseTimer {
    fun <T> time(phase: String, block: () -> T): T
    fun summary(): Map<String, PhaseSummary>  // count, mean, p50, p95, p99
    fun reset()  // between matrix runs
}
```

Instrumented phases:

| Phase | Component | What it captures |
|---|---|---|
| `task.claim` | TaskRepository.claimNext | SELECT...FOR UPDATE SKIP LOCKED + UPDATE |
| `task.insert` | TaskRepository.insertBatchWithHandle | Single task creation for LINEAR phases |
| `task.fanout_insert` | TaskRepository.insertFanOutFromScatter | INSERT...SELECT with JSON_TABLE |
| `barrier.evaluate` | BarrierService.evaluateAndAdvance | Count queries + strategy resolution |
| `workflow.cas` | WorkflowRepository.compareAndSetSequence | CAS version update |
| `input.resolve` | InputResolver.resolve | Fetch + aggregate inputs for join |
| `handler.execute` | WorkerLoop (around handler call) | Handler execution time |
| `sweeper.cycle` | Sweeper.patrol | Full sweeper patrol |

### Micrometer Metrics (optional, `-Dbench.metrics=true`)

When enabled, creates a `PrometheusMeterRegistry`:

- **Timers:** Same phases as above with histogram buckets
- **Counters:** `barrier.cas.retries`, `sweeper.recovered`
- **Gauges:** `worker.inflight`, `hikari.active`, `hikari.pending` (HikariCP built-in Micrometer support)

Output:
1. Summary printed to console after each scenario
2. Prometheus scrape endpoint on `localhost:19090/metrics` for live Grafana dashboards during soak runs (port 19090 to avoid collision with Prometheus default 9090)

When disabled, no Micrometer dependency is exercised.

## Result Persistence

### Output Location

`benchmarks/results/` (gitignored). File naming: `<scale>-<timestamp>.json`.

### Batch Mode Schema

```json
{
  "timestamp": "2026-03-29T14:30:00",
  "scale": "thorough",
  "gitCommit": "007cf1c",
  "environment": {
    "os": "Windows 11",
    "cpuCores": 8,
    "jvmMaxMemoryMb": 4096,
    "oracleVersion": "23.4-free",
    "javaVersion": "21.0.2"
  },
  "scenarios": [
    {
      "name": "fan-out",
      "parameters": {
        "workflows": 10,
        "fanOutFactor": 500,
        "workerCount": 20,
        "handlerLatencyMs": 0,
        "payloadSizeBytes": 100
      },
      "results": {
        "wallClockMs": 8432,
        "workflowsPerSec": 1.19,
        "tasksPerSec": 595.0,
        "latency": { "p50Ms": 780, "p95Ms": 1200, "p99Ms": 1450 },
        "phaseBreakdown": {
          "task.claim":         { "count": 5020, "meanMs": 1.8, "p50Ms": 1.5, "p95Ms": 3.8, "p99Ms": 5.2 },
          "task.fanout_insert": { "count": 10,   "meanMs": 45.2, "p50Ms": 42.0, "p95Ms": 68.0, "p99Ms": 72.0 },
          "barrier.evaluate":   { "count": 5030, "meanMs": 2.1, "p50Ms": 1.9, "p95Ms": 4.5, "p99Ms": 6.8 },
          "handler.execute":    { "count": 5020, "meanMs": 0.3, "p50Ms": 0.2, "p95Ms": 0.5, "p99Ms": 0.8 }
        }
      }
    }
  ]
}
```

### Sustained Mode Additions

```json
"results": {
  "overall": { "workflowsPerSec": 42.1, "tasksPerSec": 210.5, "latency": { ... } },
  "windows": [
    { "offsetSec": 0,  "workflowsPerSec": 45.2, "inflightCount": 12, "latency": { ... } },
    { "offsetSec": 10, "workflowsPerSec": 43.8, "inflightCount": 15, "latency": { ... } }
  ],
  "phaseBreakdown": { ... }
}
```

### Console Output

Per-scenario one-liner:
```
[fan-out] wf=10 fanOut=500 w=20 -> 1.19 wf/s | 595 tasks/s | p50=780ms p95=1200ms p99=1450ms
```

End-of-run comparison table:
```
+-----------+----+--------+-----+---------+----------+--------+--------+--------+
| scenario  | wf | fanOut |  w  | wf/s    | tasks/s  | p50    | p95    | p99    |
+-----------+----+--------+-----+---------+----------+--------+--------+--------+
| fan-out   | 5  | 50     | 10  | 3.21    | 160.5    | 320ms  | 580ms  | 720ms  |
| fan-out   | 5  | 500    | 20  | 0.82    | 410.0    | 1.1s   | 1.8s   | 2.2s   |
+-----------+----+--------+-----+---------+----------+--------+--------+--------+
```

`gitCommit` captured via `git rev-parse --short HEAD` at startup.

## Error Handling & Diagnostics

### Scenario Isolation

Between matrix runs:
- All workflow and task rows truncated
- `PhaseTimer` reset
- Worker pool stopped and restarted with new concurrency
- Connection pool stats reset

### Timeouts

Per-scenario maximum wall-clock:
- Quick: 60s per point
- Thorough: 120s per point
- Soak: `duration + 60s` grace

Timeout is recorded as a failure with partial metrics. Matrix continues to next point.

### Diagnostic Dump on Failure

When a scenario fails or times out:
- Count of workflows by status (RUNNING, COMPLETED, FAILED)
- Count of tasks by status (PENDING, PROCESSING, COMPLETED, FAILED)
- Sample of stuck workflows (up to 5) with current sequence and task states

### Warmup

First matrix point per scenario shape is a discarded warmup run (small scale, not recorded). Lets JIT, connection pool, and Oracle query plan caches stabilize.

### Graceful Shutdown (SIGINT)

On Ctrl+C:
1. Stop current scenario (cancel worker pool)
2. Persist results collected so far (partial run)
3. Shut down connection pool and container
4. Exit code 0 (partial results are still useful)

## File Structure

```
src/test/kotlin/benchmark/
  BenchmarkMain.kt                  -- Entry point, Oracle boot, wiring, matrix orchestration
  BenchmarkConfig.kt                -- Scale profiles, matrix definitions, CLI arg parsing
  BenchmarkScenarios.kt             -- Workflow definitions + handler registration per shape
  BenchmarkHarness.kt               -- Enhanced harness (batch + sustained modes, window bucketing)
  BenchmarkReporter.kt              -- JSON persistence, console table formatting
  PhaseTimer.kt                     -- Per-phase nanosecond recording + percentile summary
  InstrumentedTaskRepository.kt     -- Decorator with PhaseTimer
  InstrumentedBarrierService.kt     -- Decorator with PhaseTimer
  MetricsSupport.kt                 -- Optional Micrometer wiring + Prometheus endpoint

benchmarks/
  results/                          -- Gitignored, timestamped JSON result files
  .gitignore

pom.xml                             -- New <profile id="benchmark"> with exec-maven-plugin
```
