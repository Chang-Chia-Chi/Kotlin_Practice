# Quarkus Query Exporter

A high-performance, configuration-driven SQL-to-Prometheus metrics exporter built with Kotlin + Quarkus.

## Architecture

```
application.yml → ConfigValidator → Scheduler → Coroutines → JDBI → MetricStateRegistry → /q/metrics
```

### Core Components

| Component | Class | Responsibility |
|-----------|-------|----------------|
| Config | `ExporterConfig` | `@ConfigMapping` binding YAML → Kotlin interfaces |
| Validation | `ConfigValidator` | Fail-fast startup validation, produces `ResolvedQuery` models |
| Metrics | `MetricStateRegistry` | Thread-safe bridge: query results → Micrometer gauges/counters/histograms |
| DB Access | `QueryExecutor` | JDBI-based, schema-agnostic `List<Map<String,Any?>>` results |
| Engine | `ExecutionEngine` | Startup orchestrator, programmatic scheduler registration |
| Row Processing | `RowProcessor` | Column extraction, type coercion, case-insensitive lookup |
| Job | `QueryJob` | Coroutine-based: fetch on `Dispatchers.IO`, process on `Dispatchers.Default` |

### Metric Types

| Type | Behavior |
|------|----------|
| **GAUGE** | Stores latest value via `AtomicReference<Double>` |
| **COUNTER** | Tracks monotonic increase with delta computation + reset detection |
| **HISTOGRAM** | Records every row value into `DistributionSummary` with SLO buckets |
| **SUMMARY** | Records values with p50/p90/p95/p99 percentiles |
| **ENUM** | Models states as N separate 0/1 gauges |

## Configuration

```yaml
exporter:
  queries:
    active_sessions:
      sql: "SELECT count(*) as value, 'webapp' as app FROM sessions"
      datasource: default          # references quarkus.datasource.<name>
      schedule:
        interval: "5s"             # OR cron: "0 0/5 * * * ?"
      metrics:
        - name: db_active_sessions
          type: GAUGE
          value-column: value
          tag-columns: [app]

    request_latency:
      sql: "SELECT endpoint, avg_ms FROM latency_stats"
      datasource: monitoring
      schedule:
        interval: "30s"
      metrics:
        - name: http_request_duration_ms
          type: HISTOGRAM
          value-column: avg_ms
          tag-columns: [endpoint]
          buckets: [10, 50, 100, 500, 1000]
```

### Validation Rules (Fail-Fast at Startup)

- Datasource must exist in Quarkus Agroal registry
- Schedule: exactly one of `interval` XOR `cron`
- Interval must be parseable and positive
- SQL must not be blank
- At least one metric per query
- HISTOGRAM requires non-empty `buckets`
- ENUM requires non-empty `states`
- `valueColumn` must not appear in `tagColumns`

## Build & Run

```bash
mvn quarkus:dev                    # Dev mode with live reload
mvn package -DskipTests            # Package
java -jar target/quarkus-app/quarkus-run.jar

# Metrics endpoint
curl http://localhost:8080/q/metrics
```

## Test

```bash
mvn test                           # All unit tests
mvn test -pl . -Dtest=ConfigValidatorTest  # Single test class
```

### Test Coverage

| Test Class | Coverage |
|-----------|----------|
| `ConfigValidatorTest` | All 6 validation rules, error accumulation, duration parsing |
| `RowProcessorTest` | Type coercion (Int/Long/Double/BigDecimal/String), case-insensitive lookup, null handling |
| `MetricStateRegistryTest` | All 5 metric types, tag separation, counter reset, enum transitions |
| `QueryJobTest` | Coroutine execution, multi-row/multi-metric, error resilience |
| `QueryExecutorTest` | Real H2 queries, aggregates, NULL handling, JDBI caching |
| `ExecutionEngineTest` | Startup orchestration, scheduler wiring, validation propagation |
| `ResolvedModelsTest` | Domain invariants (schedule XOR constraint) |

## Tech Stack

- **Quarkus 3.17** (JVM mode)
- **Kotlin 2.0** with coroutines
- **JDBI 3.45** for database access
- **Micrometer + Prometheus** for metrics
- **MockK + AssertJ** for testing
