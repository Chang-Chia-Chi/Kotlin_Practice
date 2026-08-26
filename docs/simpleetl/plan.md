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
- `quarkus.scheduler.start-mode=forced` is set and a test proves a task fires with no
  `@Scheduled` method present in the application.
- A run executes on the task's own `limitedParallelism(1)` dispatcher, not on the Vert.x
  worker thread; asserted by capturing the thread and coroutine name inside the run.
- A second trigger while a run is in progress is rejected, not queued, from both the
  scheduled path and the API path.
- The API returns 202 immediately for a task that then runs for longer than the HTTP
  timeout.
- All endpoints reject a caller without `etl-admin`.
- Reload with one invalid file changes nothing and returns the errors; reload while a task
  is running does not affect that run.

**Not in scope:** multi-replica coordination.

---

## P8 - Listener, metrics, hooks

**Public surface**
- `TaskRunListener` and the no-op default
- `TaskHook`, `TaskHookRegistry`
- The metric names of spec 9.3

**Done when**
- Every call site in spec 9.2 fires in the right order for a successful run, a failed run,
  and a run with retries.
- `logging: false` suppresses listener calls but not metrics.
- `onSuccess` runs once after all phases succeed; a throwing `onSuccess` marks the task
  FAILED and then runs `onFailure`; a throwing `onFailure` is logged and swallowed.
- A hook name absent from the registry fails startup validation.
- Metric label sets are asserted by a contract test, so a later refactor cannot silently
  change a label and break dashboards.

---

## P9 - Snapshot cache read step

**Public surface**
- One new step type for the file-to-file copy of spec 7.3

**Done when**
- The step copies a subset from a generation into scratch through the cache's `copyOut`,
  and no row passes through the JVM; asserted by instrumenting the JVM-side row counter and
  expecting zero.
- The lease is acquired and released within the step, not held for the task, and a test
  asserts the generation becomes reclaimable immediately after the step returns.
- A test covers the case where the cache has no current generation.

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
P8 Listener, metrics, hooks
  |
P9 Cache read step
```

P8 and P9 are independent of each other and can be swapped or parallelised. Everything
else is a chain.