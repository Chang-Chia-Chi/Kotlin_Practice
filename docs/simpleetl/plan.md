# YAML-Driven ETL Framework - Implementation Plan

Companion to the specification. Each phase is sized to be completed and reviewed on its
own: it names the public surface it adds, what must be true before it is considered done,
and what it deliberately does not do.

Ordering rule: Layer 1 is finished and usable before any of Layer 2 starts, so the
snapshot cache can adopt it without waiting for the task engine.

---

## P0 - Spikes

Runs before any framework code. Each spike is a throwaway test class, not a deliverable.

**S1 - appender flush cost.** Append one million rows with `flush()` once per 5000 rows
versus only at `close()`. Record wall time and peak RSS for both.

**S2 - scratch growth and RSS.** Five sequential writes of one million rows into one
DuckDB file, one of them failing and retrying twice under the attempt-suffix scheme of
spec 5.5. Repeat ten times in one JVM. Record RSS after each run against the baseline, and
the file size at the end of each run.

**S3 - implicit cast on append.** Create a table with BIGINT and DATE columns, append via
`appendBigDecimal` and `appendLocalDateTime`, read back and compare.

**Done when:** the three results are written down and the following are decided: whether
per-chunk flush is the default, whether retry stays at step level or moves to task level,
the volume `sizeLimit` number, and whether validation rule 15 can be relaxed.

**Blocking:** S2 can change spec 5.3 and 7.2. S1 and S3 change only implementation detail.

---

## P1 - Row and type mapping

The canonical type system and the read seam.

**Public surface**
- `Row`
- `RowMapper` - result set to Row, applying spec 4.3 and lower-casing keys
- `CanonicalType` - the enum plus its JDBC and DuckDB mappings

**Done when**
- Every mapping in spec 4.3 has a test against a real Oracle result set (Testcontainers)
  and a real DuckDB result set.
- An unsupported column type produces an error naming the step and column, not a class
  cast exception.
- Upper-case Oracle identifiers and lower-case DuckDB identifiers produce the same Row
  keys.
- A typed accessor called for the wrong type reports actual and requested type.

**Not in scope:** writing anything.

---

## P2 - Writers

The write seam, both targets, including the null handling of spec 4.6.

**Public surface**
- `RowWriter` - `open(columns)`, `write(chunk)`, `close()`
- `DuckDbTableWriter` - appender based, with AUTO DDL generation
- `JdbcTableWriter` - prepared batch, declarative table form
- `JdbcStatementWriter` - prepared batch, `target.sql` form with Row-key binding

**Done when**
- AUTO DDL creates nullable source columns as VARCHAR, DECIMAL, or TIMESTAMP, and NOT NULL
  columns with their natural mapping. Verified by reading back `information_schema`.
- A round trip with nulls in every nullable canonical type returns null, not empty string
  and not a placeholder.
- Column mapping is by name against catalog metadata; a test reorders the target table's
  columns and the data still lands correctly.
- A BLOB column targeted at DuckDB fails with the message from spec 4.6, at open time, not
  mid-chunk.
- `createTable: REQUIRED` with a nullable BIGINT column is rejected at open time (unless
  S3 says otherwise).
- `JdbcStatementWriter` reports missing bind names against the first chunk, listing them.
- Appenders and statements are closed on every path, verified by a leak-counting test
  double.

**Not in scope:** chunking, retry, transactions.

---

## P3 - RowPipe (Layer 1 complete)

**Public surface**
- `JdbcSource` - datasource, SQL, bound parameters
- `RowPipe` - source, target, chunk size, optional transform; `run(): PipeResult`
- `RowTransform`
- `PipeResult` - rowsRead, rowsWritten

**Done when**
- One million rows stream from Oracle to DuckDB without the heap growing with row count.
- `fetchSize` is set to the chunk size on the source statement; a test asserts it is not
  the Oracle default.
- Commit happens once per chunk, verified by counting rows visible from a second
  connection mid-run.
- A transform returning null drops the row; a transform adding a column lands that column.
- The source stream and every connection are closed when the target throws mid-chunk.
- Layer 1 satisfies the snapshot cache's `GenerationSource` contract, proven inside
  `SimpleEtl` with no dependency on the `snapshotcache` module:
  1. A `RowPipe` populates a caller-supplied, file-mode DuckDB write `Connection` it did not
     open, and leaves it open and usable after `run()` returns - exactly what
     `BuildContext.target` provides.
  2. Two `RowPipe`s writing two tables share one source read transaction (spec 9.5).
  `PipeGenerationSource` is caller-land wiring, owned by the cache's own plan, and is not
  built here. The original wording - "its existing test suite passes unchanged" - was struck
  as evidence: nothing in that module is modified, so its suite passes trivially.

**Not in scope:** retry, YAML, scratch lifecycle. A `RowPipe` failure propagates.

**Milestone:** at the end of P3 the snapshot cache can adopt the framework. Everything
after this point is the task engine.

---

## P4 - Scratch lifecycle

**Public surface**
- `ScratchDb` - `connection()`, `duplicate()`, `close()`
- `DatasetNamer` - attempt-suffixed physical names and the stable view

**Done when**
- The file is created lazily: a task shape that never references `scratch` leaves no file
  on disk.
- `memory_limit` and `temp_directory` are applied at open, matching the snapshot cache's
  existing configuration.
- The file is deleted on success, on failure, and on an exception thrown from inside the
  run block.
- After a failed attempt and a retry, `wip_stg` resolves to the second attempt's data and
  the first attempt's table still exists unreferenced.
- The stable view resolves identically whether the dataset is a table or a parquet file.
- No `CREATE TEMP TABLE` anywhere, enforced by an ArchUnit rule or an equivalent check.

**Not in scope:** deciding when a retry happens.

---

## P5 - Task model, variables, step executors

**Public surface**
- `TaskDefinition`, `Phase`, `Step` and its four subtypes - programmatically constructible
- `TaskEngine` - `run(definition, trigger): TaskOutcome`
- `VariableScope`

**Done when**
- All four step types execute against a definition built in code, with no YAML involved.
- Retry follows spec 5.3: transient classification, exponential backoff, scratch defaults,
  and the attempt-suffix cleanup from P4.
- A non-transient failure fails immediately with no retry.
- Variables resolve in step order; a variable used before its export is an error; an export
  returning two rows is an error.
- `chunkSize` resolves step, then task, then default.
- A failure in phase 2 leaves phase 1's external writes committed, and the test asserts
  this rather than pretending otherwise.

**Not in scope:** loading from files, scheduling.

---

## P6 - YAML loading and validation

**Public surface**
- `TaskFileLoader` - directory to `List<TaskDefinition>`
- `ValidationReport` - file, step, line, message

**Done when**
- Every rule in spec 10 has a test with a deliberately broken file, asserting the message
  identifies the file and the step.
- SQL containing `${...}` and multi-line SQL survive loading unchanged, proving the
  Quarkus-config path was correctly avoided.
- An unknown YAML field is rejected rather than ignored.
- One bad file out of ten prevents startup, and the report lists only that file's errors.

**Not in scope:** reload semantics.

---

## P7 - Scheduling, API, threading, reload

**Public surface**
- `TaskScheduler` - registers and unregisters programmatic jobs
- `TaskRunner` - the per-task dispatcher and the self-concurrency guard
- `AdminResource` - the four endpoints of spec 8.2

**Done when**
- `TaskScheduler.apply(definitions)` registers exactly the enabled tasks carrying a cron,
  unregisters removed ones, and re-registers only those whose cron changed - proved against
  a recording `CronScheduler`. A `CronScheduler` that throws on a bad cron leaves the
  registry unchanged and yields a `ValidationReport` (spec 8.5).
- Two runs of one task observe the same single worker thread, neither of which is the
  triggering thread, and the `CoroutineName` handed into the run body equals the task name.
  **Not asserted via the thread name** - measured, the `@name` tag exists only under `-ea`
  and is absent in production (spec 8.3).
- A second trigger while a run is in progress is rejected, not queued, from the
  `CronScheduler` callback and from `TaskAdmin.trigger` alike; after the first run
  completes, the rejected trigger has still not run.
- `TaskAdmin.trigger` returns `Accepted(runId)` while the run is parked, so a run outliving
  an HTTP timeout is never held open. No HTTP involved.
- `TaskAdmin` records the caller identity into the run and performs no authorisation of its
  own. **The `etl-admin` check itself is a host obligation (spec 8.6) and is not tested in
  this repository.**
- Reload with one invalid file changes nothing and returns the errors; reload while a task
  is running does not affect that run.

**Not in scope:** multi-replica coordination. The cron binding and the HTTP resource are the
host's (spec 8.6); `AdminResource` is therefore not built here.

---

## P8 - Listener, metrics, hooks

**Split into three sequential phases by the P8 contract round.** Two independent estimates put
the phase at 2.4x-3.3x the 200-600 budget, which CLAUDE.md makes a stop-and-report trigger. The
split also isolates the single most consequential and most reversible decision - whether the
framework takes a Micrometer dependency at all - into a phase of its own. P8a and P8b both edit
`TaskEngine.kt`, so they cannot be parallelised with each other.

### P8a - Run listener and task hooks (spec 9.2, 9.4)

JDK + kotlin-stdlib only; no new library on the compile classpath except the logging API
CLAUDE.md already fixes.

**Public surface**
- `TaskRunListener`, `TaskRunListener.NONE`, `TaskRunListener.of`
- `TaskContext`, `PhaseContext`, `StepContext`, `StepResult`
- `TaskHook`, `TaskHookRegistry`, `TaskHooks`
- `TaskEngine`: an injected `java.time.Clock`, a `listener`, a `hooks`, and `triggeredBy` on `run`

**Done when**
- Every call site in spec 9.2 fires in the right order for a successful run, a failed run,
  and a run with retries - asserted as a whole ordered trace, not as a subsequence.
- `logging: false` suppresses listener calls, asserted in a true/false pair so that an engine
  with no call sites at all cannot pass.
- `onSuccess` runs once after all phases succeed; a throwing `onSuccess` marks the task
  FAILED and then runs `onFailure`; a throwing `onFailure` is logged and swallowed, with the
  original failure surviving by identity.
- A hook name absent from the registry fails startup validation, proved through the real
  composition - one `TaskHooks` instance handed to both `TaskEngine` and `TaskFileLoader`.
  `TaskFileLoader(hooks = TaskHooks().names)` is an empty set that rejects everything and
  proves nothing.
- A throwing listener never changes a run's outcome, and `of` keeps a thrower from robbing the
  listeners behind it.
- `durationMs` is exact under an injected clock the test sleeper advances. Without the clock the
  claim that a duration spans all attempts is unfalsifiable, because the test sleeper does not
  sleep.
- An `Error` escaping the engine still propagates **and** still produces
  `TaskEvent.TaskEnd(FAILED)` - written `onTaskEnd(FAILED)` when P8a ran, before the 2026-08-29
  review closed the seven call sites into one sealed event (spec 9.2).

**Not in scope:** metrics of any kind.

### P8b - Metrics (spec 9.3)

**Public surface**
- `TaskMetrics` and its no-op default; `MicrometerTaskMetrics` in `infra.etl.micrometer`
- `ScratchDb.diskBytes()`

**Done when**
- Metric label sets are asserted by a contract test **from a real engine run into a real
  `SimpleMeterRegistry`**, so a later refactor cannot silently change a label and break
  dashboards. Asserting constants instead would assert nothing about what is emitted.
- Metrics fire regardless of `logging: false`.
- `micrometer-core` is `provided` scope, so Layer 1's consumers do not inherit it - spec 2.1's
  whole reason for existing is that Layer 1 ships without Layer 2, and Maven has no layer
  granularity.
- The `etl_scratch_file_bytes` gauge is backed by a strongly held `AtomicLong` per task.
  Measured: Micrometer holds a gauge's referent weakly and a locally-scoped one reads `NaN`
  after GC, silently.
- `diskBytes()` sums every regular file under the run directory. Measured: after 500,000
  appended rows the DuckDB file was 12,288 bytes and its WAL held 10,416,115, so summing only
  the database file under-reports by three orders of magnitude.
- ArchUnit confines `io.micrometer` to `infra.etl.micrometer` and stops anything in `infra.etl`
  depending on that package, with a positive canary rule so the confinement rules cannot pass
  over an empty package. The confinement rules pass vacuously when the adapter does not exist
  because their `that()` clause selects a non-empty set either way - not because of ArchUnit's
  `failOnEmptyShould`, which never fires here. The canary must therefore be a plain assertion
  over `JavaClasses`, not another `that()` rule.

**Not in scope, and moved out during the contract round:** injecting a `Clock` into `TaskRunner`.
That was drafted as a fix for `RunStatus.startedAt` and `TaskContext.startedAt` disagreeing, and
the fix does not work: one `Clock` gives one time *source*, not one time. `submit` reads it when
the trigger arrives and the engine reads it when the coroutine is dispatched, and a
`limitedParallelism(1)` view queues between them, so the two still differ in production by exactly
the queue delay. Equality holds only under a frozen test clock, which would have made the test
prove same-source while the stated defect stood. The two values measure different things - submit
time and run-start time - and the gap between them is the queue delay, which is information worth
having. The honest resolution is to name them distinctly, not to unify them.

### P8c - Coroutine-native event stream - **BUILT, THEN REVERTED**

Built to completion (900 lines, 331 tests green, commit `a78a49d`) and then **reverted on the
project owner's ruling**. Recorded here rather than deleted, because the next session will
otherwise rebuild it.

**Why it was built.** The owner asked that the framework be coroutine-friendly and that a
notification mechanism use `SharedFlow`, adding "just a recommendation, not a must follow". The
lead turned that into a mandated phase.

**Why it was reverted.** The owner's actual bar was *adopt it where it is really required, and
where it makes the code more scalable, simpler, more concise and easier to maintain.* Measured
against that bar the phase failed on every count:

| Test | Result |
|---|---|
| Really required? | **No consumer existed.** Zero references from production; only its own test and fixture touched it |
| Simpler? | No - a **second** observation surface parallel to `TaskRunListener`, so every future call-site change lands in two places |
| Concise? | 1,231 lines added for ~25 lines of mechanism |
| Easier to maintain? | No - the `events` KDoc needed **eleven** separate "Not promised" caveats to be used safely |

The eleven caveats are the tell, and none of them was padding: the stream is lossy, interleaves
concurrent runs, never completes, is silenced by `logging: false`, cannot report that nobody is
listening, and can **park an ETL run** if a host collects on an unconfined dispatcher. It was
strictly weaker than the `TaskRunListener` it duplicated.

The reviewer made this objection in the P8 contract round - *"a second, permanently-parallel public
surface for the same seven call sites"* - and the lead answered it by deferring the phase rather
than by dropping it. That was the error; deferring an objection is not answering it.

**When to revisit.** Only when a real consumer needs fan-out to multiple independent subscribers.
The engine is blocking JDBC by spec 8.3, and `TaskRunner` already uses coroutines where they earn
their place - one `limitedParallelism(1)` view per task. Until such a consumer exists, this is
speculative API.

**What was learned and is worth keeping** (all in progress.md, none of it requiring the code):
`BufferOverflow.SUSPEND` is the only policy under which a lost event is countable; `tryEmit` buys
"never suspends", not "never blocks"; `replayCache` is cumulative across runs; and with no
subscriber, `replay = 0` discards everything while `tryEmit` still returns true.

## P9 - Snapshot cache read step

**Public surface**
- One new step type for the file-to-file copy of spec 7.3

**Done when**
- The step copies a subset from a generation into scratch through the cache's `copyOut`,
  and no row passes through the JVM; asserted by instrumenting the JVM-side row counter and
  expecting zero.
- The lease is acquired and released within the step, not held for the task. **The second half of
  this criterion as originally written - "a test asserts the generation becomes reclaimable
  immediately after the step returns" - is not achievable in this module and was struck during the
  P9 contract round.** Reclamation lives in `DefaultSnapshotCache`, which is `internal` to the
  cache module, so SimpleEtl's tests use a double implementing the public interface and a double
  cannot reclaim anything. It is recorded as a host obligation in spec 8.6. What *is* provable
  here: the engine calls only `copyOut`, never `acquire` or `withSnapshot`, and retains nothing
  from the cache past the step - proved by a double that deletes its generation file inside
  `copyOut` while a later step still reads the copied dataset.
- A test covers the case where the cache has no current generation, **and asserts it is not
  retried** despite the step type carrying a `retries` field.
- **The step's `StepResult` is 0/0, like every non-pipe step.** `rowsCopied` is lineage, not
  throughput: `etl_step_rows_total{direction}` is one series across all step types, and making it
  mean "rows the cache's CTAS created" for this one would break the aggregation ruling already
  shipped in `TaskEngine`. The count goes in the lineage log with `generation` and `dataAsOf`.
- **The `Error` path keeps its coverage.** Removing `CacheCopyStep`'s `NotImplementedError` deletes
  the framework's only production `Error`, which two earlier tests use to pin behaviour unrelated
  to the cache. They migrate to an injected `Error`; they are not dropped.

---

## Sequencing summary

```
P0 spikes
  |
P1 Row and types
  |
P2 Writers
  |
P3 RowPipe .................. Layer 1 done, snapshot cache can adopt
  |
P4 Scratch lifecycle
  |
P5 Task model and executors
  |
P6 YAML loading
  |
P7 Scheduling, API, threading
  |
P8a Listener and hooks
  |
P8b Metrics
  |
P8c Event stream
  |
P9 Cache read step
```

P8a, P8b and P9 all edit `TaskEngine.kt` and were a chain. P8c did **not** - it added a listener
and no engine call site, which is the one thing that phase got right - and it was reverted anyway. The earlier claim that
"P8 and P9 are independent of each other and can be swapped or parallelised" was wrong and is
struck: P7's own handover note already recorded that P8 owns the listener call sites in the same
file P9 edits for the cache read step, and that two agents cannot build them in parallel.