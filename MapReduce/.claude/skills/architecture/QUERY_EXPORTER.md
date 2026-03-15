---
name: quarkus-query-exporter
description: >
  Build a production-grade, config-driven SQL-to-Prometheus metric exporter using Kotlin, Quarkus, JDBI, 
  Micrometer, and Kotlin Coroutines. Use this skill whenever implementing any part of the query-exporter 
  system: config mapping, validation, metric registry, JDBI query execution, coroutine-based scheduling, 
  or Prometheus exposition. Also trigger when the user asks about metric types (gauge, counter, histogram, 
  summary, enum), metric naming, label cardinality, fail-fast validation, or wiring Quarkus scheduled jobs 
  with coroutines. This skill contains zero code — only the mental model, invariants, and pitfalls needed 
  to write correct, production-ready code on the first pass.
---

# Quarkus Query Exporter — Implementation Skill

## Purpose

Turn `application.yml` declarations into live Prometheus metrics. The system is a **runtime interpretation engine**: YAML in, `/q/metrics` out. Every design decision below exists to keep the engine boring, predictable, and safe to run unsupervised in production.

---

## 1. Golden Rules

These are non-negotiable. Violating any one of them produces a system that silently lies to on-call engineers.

1. **Fail at startup, never at scrape time.** Every reachable error (missing datasource, bad SQL syntax, impossible metric config) must surface as a fatal `StartupException`. If the app boots, it must be correct.
2. **Metrics must never block the scrape.** The Prometheus pull model means `/q/metrics` is called on *their* schedule. If a query is slow or hanging, the scrape must still return stale-but-valid data. This means the query write path and the scrape read path share **lock-free** state only (`AtomicDouble`, `AtomicReference`, concurrent collections).
3. **Cardinality is the caller's problem, but guard the door.** The exporter does not invent labels — it reads column values from SQL results. However, it must never silently register unbounded label sets. Log and cap.
4. **A metric that stops updating is worse than a missing metric.** Silent query failures must be visible. Every query execution must update a companion `query_exporter_query_last_run_success` gauge (0/1) and a `query_exporter_query_last_run_timestamp_seconds` gauge.

---

## 2. Configuration Mental Model

### Shape

```
queries:
  <query-name>:            # unique key, used in logs and meta-metrics
    sql: "..."
    datasource: "<name>"   # must match a quarkus.datasource.<name>
    schedule:
      interval: "5s"       # Duration — XOR with cron
      cron: "0 */5 * * *"  # Quartz cron — XOR with interval
    metrics:
      - name: "..."
        type: GAUGE | COUNTER | HISTOGRAM | SUMMARY | ENUM
        valueColumn: "..."
        tagColumns: [...]
        buckets: [...]     # histogram only
        states: [...]      # enum only
```

### Key Invariants

- **`@ConfigMapping` with `@WithName`** — Quarkus maps YAML keys to interface methods. Use `Optional` for truly optional fields (buckets, states, cron, interval). Never use `@WithDefault` for things that must be validated contextually (e.g., buckets — the default is "empty" which is invalid *only* for histograms).
- **Named datasource lookup** — Quarkus registers datasources as CDI beans qualified by name. Use `Arc.container().select(DataSource::class.java, NamedLiteral.of(name))` for dynamic resolution. Do NOT inject them statically; the exporter doesn't know datasource names at compile time.
- **Schedule XOR** — Exactly one of `interval` or `cron`. Both present = ambiguous. Neither present = no execution. Validate as strict XOR at startup.

### Pitfalls

- Quarkus `@ConfigMapping` interfaces are **not** data classes. They are CDI proxies. Never rely on `equals()` / `hashCode()` / `toString()` from them. Extract values into plain Kotlin data classes immediately after loading if you need value semantics.
- YAML `Duration` parsing: Quarkus uses `java.time.Duration.parse` internally for `Duration` types on config interfaces. Short-form `5s`, `1m` work. Ensure the config interface declares `Duration` not `String`.
- `Optional<List<...>>` vs `List<...>` on config interfaces: An absent YAML key yields `Optional.empty()`. A present-but-empty key yields `Optional.of(emptyList())`. These are semantically different for validation — "you forgot buckets" vs. "you explicitly declared empty buckets".

---

## 3. Validation Strategy

### When

`@Observes StartupEvent` — runs after CDI is wired, before any scheduled job fires. This is the single synchronization point.

### What to Validate (Checklist)

| # | Rule | Why |
|---|------|-----|
| 1 | Every `datasource` reference resolves to a live CDI bean | Catch typos before first query |
| 2 | Schedule is XOR (exactly one of interval / cron) | Prevent ambiguous or missing execution |
| 3 | SQL string is non-blank | Prevent JDBI from throwing at runtime |
| 4 | `valueColumn` not in `tagColumns` | A column cannot be both a measurement and a dimension |
| 5 | HISTOGRAM → `buckets` non-empty | Micrometer requires explicit SLOs for histograms |
| 6 | ENUM → `states` non-empty | Enum metrics are meaningless without a state set |
| 7 | Metric `name` is valid Prometheus identifier | Prometheus silently drops illegal names; catch at startup |
| 8 | No duplicate metric names across entire config | Two queries writing the same metric name with different schemas = data corruption |

### How to Fail

Do NOT throw one error and bail. **Accumulate all violations**, then throw a single exception listing everything. Operators fixing config in production need to see *all* problems in one restart, not play whack-a-mole.

```
Pattern: List<String> errors = mutableListOf()
         ... validate everything, errors.add(...) on failure ...
         if (errors.isNotEmpty()) throw StartupException(errors.joinToString("\n"))
```

---

## 4. Metric Registry Design

This is the hardest part to get right. The registry is the **shared mutable state** between two unsynchronized worlds: the query coroutine (writer) and the Prometheus scrape handler (reader).

### Principle: Let Micrometer Own the State

Do NOT build a custom `ConcurrentHashMap<MetricID, AtomicDouble>` and then try to bridge it to Micrometer. Instead, register Micrometer meter objects directly and mutate them in place. Micrometer's registry is already thread-safe and designed for exactly this pattern.

### Per-Type Strategy

**GAUGE**
- Register: `Gauge.builder(name, atomicDouble, AtomicDouble::get).tags(...).register(registry)`
- Update: `atomicDouble.set(value)` — last-write-wins, which is correct for gauges.
- One `AtomicDouble` per unique tag combination.

**COUNTER (Monotonic from SQL)**
- The SQL returns a cumulative value (e.g., `SELECT total_bytes_sent FROM ...`). This is NOT a Micrometer `Counter.increment()` pattern.
- Register: `FunctionCounter.builder(name, atomicDouble, AtomicDouble::get).tags(...).register(registry)`
- Update: `atomicDouble.set(value)` — Micrometer exposes the raw value; Prometheus computes `rate()`.
- CRITICAL: If the SQL value *resets* (e.g., process restarts), the `FunctionCounter` handles this correctly — Prometheus `rate()` detects resets. Do NOT try to compute deltas yourself.

**HISTOGRAM**
- Register: `DistributionSummary.builder(name).serviceLevelObjectives(...buckets).tags(...).register(registry)`
- Update: `summary.record(value)` for EVERY row, EVERY execution.
- This means histograms are **cumulative and append-only**. You cannot "reset" a histogram. This is by design — Prometheus histograms are monotonic counters per bucket.
- PITFALL: If the SQL returns a snapshot (e.g., "current latency percentiles"), recording those into a `DistributionSummary` produces wrong results. Histograms only work when the SQL returns raw observations (e.g., one row per event). Document this constraint for users.

**SUMMARY**
- Same as HISTOGRAM but uses Micrometer's client-side quantile calculation.
- Register with `.publishPercentiles(...)` if desired.
- Same "raw observations only" constraint as histogram.

**ENUM (State Set)**
- Not natively supported by Micrometer. Implement as N gauges: `<name>{<name>="state_A"} 1`, `<name>{<name>="state_B"} 0`.
- For each declared state, register a `Gauge` backed by an `AtomicInteger` (0 or 1).
- Update: Set the matching state to 1, all others to 0. This must be atomic from the reader's perspective — update the "new active" to 1 FIRST, then set the "old active" to 0. A brief "both 1" is less dangerous than "both 0".

### Tag Combination Registration

Metrics are identified by (name + tag key-value set). A query returning 3 rows with different label values means 3 separate meter registrations. Use a `ConcurrentHashMap<Tags, MeterHandle>` to cache registrations. Register lazily on first observation.

**Cardinality guard:** Set a configurable ceiling (default: 1000) on the number of unique tag combinations per metric name. If exceeded, log an error, skip the row, and increment a `query_exporter_label_cardinality_overflow_total` counter. Do NOT register the meter — unbounded cardinality kills Prometheus TSDB.

---

## 5. Execution Engine

### Scheduler → Coroutine Bridge

Quarkus `Scheduler` runs jobs on the event loop (or a managed thread pool). The job body must be non-blocking. Use it purely as a trigger:

```
Pattern: scheduler.newJob("query-<name>")
           .setInterval/setCron(...)
           .setTask { _ -> scope.launch { executeQuery(queryConfig) } }
           .schedule()
```

The `scope` is a `CoroutineScope(SupervisorJob() + Dispatchers.Default)` tied to `@ApplicationScoped` lifecycle. `SupervisorJob` ensures one failing query doesn't cancel siblings.

### Inside the Coroutine

1. **I/O phase** — `withContext(Dispatchers.IO) { jdbi.withHandle { h -> h.createQuery(sql).mapToMap().list() } }`
2. **Transform phase** — Back on `Dispatchers.Default`. Extract `valueColumn` and `tagColumns` from each `Map<String, Any>`. Coerce value to `Double`. Build Micrometer `Tags`.
3. **Update phase** — Call the metric registry's type-specific update method.
4. **Meta-metrics** — Always update `last_run_timestamp_seconds` and `last_run_success` (1 on success, 0 on any exception in steps 1-3).

### Error Handling Per Query

- Catch at the coroutine level. NEVER let an exception propagate to the scheduler — it could descheduled the job permanently.
- On exception: log with query name and datasource, set `last_run_success=0`, return. The next scheduled tick retries automatically.
- On SQL returning 0 rows: This is NOT an error. Gauges keep their last value. Counters stay monotonic. Log at DEBUG.

### JDBI Usage

- Create `Jdbi` instances once per datasource at startup, cache in a `Map<String, Jdbi>`.
- Use `Jdbi.create(dataSource)` — not `Jdbi.open()`. Let JDBI manage the handle lifecycle via `withHandle`.
- `mapToMap()` returns `List<Map<String, Object>>`. Column names are case-sensitive (database-dependent). Document that `valueColumn`/`tagColumns` must match the SQL result column names exactly, including case.
- Install `KotlinPlugin` on the Jdbi instance for proper Kotlin type handling.

---

## 6. Meta-Metrics (Self-Observability)

The exporter itself must be observable. Register these automatically (not user-configured):

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `query_exporter_query_last_run_timestamp_seconds` | Gauge | `query` | Detect stale queries |
| `query_exporter_query_last_run_success` | Gauge | `query` | Alert on persistent failure |
| `query_exporter_query_duration_seconds` | Histogram | `query`, `datasource` | SLO on exporter itself |
| `query_exporter_query_rows_total` | Counter | `query` | Track result set sizes |
| `query_exporter_label_cardinality_overflow_total` | Counter | `query`, `metric` | Cardinality breach canary |

Alerting rule (document for users):
```
alert: QueryExporterQueryStale
expr: time() - query_exporter_query_last_run_timestamp_seconds > 300
```

---

## 7. Lifecycle & Shutdown

- On `@Observes ShutdownEvent`: Cancel the `CoroutineScope`. This cancels in-flight queries via cooperative cancellation.
- JDBI handles close automatically when the Agroal datasource pool shuts down (managed by Quarkus).
- Micrometer registry cleanup is handled by `quarkus-micrometer` extension automatically.
- The scheduler is stopped by Quarkus lifecycle — no manual cleanup needed.

---

## 8. Testing Strategy

### Unit Tests (No container)

- **Validator:** Feed it deliberately broken config (missing datasource, both cron+interval, histogram without buckets). Assert all expected errors are collected in one pass.
- **Metric Update Logic:** Create a `SimpleMeterRegistry`, exercise each type's update path, assert meter values. No SQL, no scheduling.
- **Tag extraction:** Given a `Map<String, Object>` row and a metric config, assert correct `Tags` construction. Test null column values, missing columns, type coercion edge cases.

### Integration Tests (Testcontainers)

- Spin up PostgreSQL (or target DB) with a known schema.
- Load a test `application.yml` with queries against the test schema.
- Let the scheduler tick once.
- Scrape `/q/metrics` and assert metric presence, values, and labels.
- Use `@QuarkusTest` + `@TestProfile` to isolate config per test class.

### What NOT to Test

- Don't test that Quarkus `@ConfigMapping` works — it's framework code.
- Don't test that Micrometer formats Prometheus output — it's library code.
- DO test the seams: your validator logic, your update logic, your error handling.

---

## 9. Common Pitfalls (Field Scars)

1. **Double-registration:** Calling `Gauge.builder(...).register(registry)` twice with the same name+tags silently returns the *existing* meter in Micrometer. This is fine for gauges, but for `DistributionSummary` it means you get the OLD config (old buckets). Always check if already registered when configs might change.

2. **Counter vs. FunctionCounter confusion:** `Counter.increment(delta)` is for events. `FunctionCounter` is for "I have a cumulative number, expose it." SQL returning cumulative values MUST use `FunctionCounter`. Using `Counter` with `increment(newValue - oldValue)` introduces race conditions and reset bugs.

3. **Cron + Interval ambiguity at YAML level:** YAML `null` vs. absent key vs. empty string are three different things. Use `Optional` on the config interface and validate explicitly. Don't rely on Quarkus default behavior — it has changed across versions.

4. **Dispatchers.IO starvation:** If you have 50 queries all on 5-second intervals against a slow database, the default IO dispatcher (64 threads) can saturate. Consider a dedicated `limitedParallelism()` dispatcher scoped to the exporter, so it doesn't starve other Quarkus coroutine users.

5. **Column name casing:** Oracle returns UPPER_CASE by default. PostgreSQL returns lower_case. MySQL preserves the query alias. If the user writes `SELECT count(*) as Total` and sets `valueColumn: "total"`, it works on Postgres but fails on Oracle. Document this. Consider adding a case-insensitive column lookup option.

6. **Metric name hygiene:** Prometheus metric names must match `[a-zA-Z_:][a-zA-Z0-9_:]*`. Label names must match `[a-zA-Z_][a-zA-Z0-9_]*`. Validate at startup. Don't sanitize silently — that produces metrics with names the user didn't expect.

7. **Startup ordering:** `@Observes StartupEvent` in Quarkus fires after CDI is ready but the exact ordering with other `StartupEvent` observers is undefined unless you use `@Priority`. Make the validator high priority, the scheduler registration low priority.

---

## 10. Operational Readiness Checklist

Before shipping, verify:

- [ ] App crashes on any config error (test with deliberately bad YAML)
- [ ] `/q/metrics` returns data within 10 seconds of first scheduled tick
- [ ] A hanging SQL query does not block `/q/metrics` response
- [ ] A failing SQL query sets `last_run_success=0` and does NOT descheduled the job
- [ ] Cardinality overflow is logged and metered, not silently dropped
- [ ] Graceful shutdown cancels in-flight coroutines (no leaked threads)
- [ ] Meta-metrics are present even if zero user queries are configured
- [ ] Memory is stable over 24 hours (no meter registration leak from tag churn)