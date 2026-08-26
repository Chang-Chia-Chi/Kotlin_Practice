# Implementation progress

One entry per completed phase, appended in order. Keep entries short - this
is a handover note for the next session, not a report.

Format:

    ## <PHASE ID> - <title>  (<YYYY-MM-DD>)

    ### Delivered
    - <classes added, tests added - one line each>

    ### Deviations from the documents
    - <anything done differently from spec.md or plan.md, and why>
    - <write "none" if there were none>

    ### Notes for later phases
    - <stubs left behind, assumptions not yet verified, awkward seams>

The deviations section matters most. Implementation always meets details
the documents did not anticipate. An unrecorded deviation leaves the next
session looking at code that disagrees with the documents, which it will
"correct" back - silently undoing a deliberate decision.

---

## P0 - Spikes  (2026-08-26)

Team: engineer only (composition table). No production code; P0 adds no public surface.

### Delivered

- `spike/SpikeSupport.kt` - OS-level RSS probe, latch-based `sampledPeak`, scratch opener
  applying spec 7.2 settings, LOW/HIGH entropy row generators, mixed-width column helpers.
- `spike/S1AppenderFlushCostSpike.kt` - flush per 5000 vs flush at close.
- `spike/S2ScratchGrowthSpike.kt` - 10 runs x 6.2M rows with attempt-suffix retry.
- `spike/S3ImplicitCastSpike.kt` - 20-case appender/column-type cast matrix.
- `spike/S4aWideRowDensitySpike.kt` - bytes per stored value at 4/15/30 columns.
- `spike/S4bSpillFactorSpike.kt` - sampled spill peak, two memory limits, three query
  shapes, plus the `temp_directory`-unset case.

Spikes are named `*Spike`, so surefire's default `*Test` include pattern skips them and they
cost CI nothing. Run one with `-Dtest=<name> -DfailIfNoSpecifiedTests=false`.
Every result below was re-run by the lead and reproduced within noise.

### The four P0 rulings

| Decision | Ruling |
|---|---|
| Per-chunk flush default | ON. Costs ~32% of append wall time, bounds no memory. Kept because it *is* the chunk boundary, not for memory. |
| Retry step vs task level | Step level, unchanged. S2's trigger did not fire. |
| Volume `sizeLimit` | 32 GiB, derived in 7.2 from measured `d` and `s` plus the user's R=2M / C=100 ceiling. |
| Relax validation rule 15 | BIGINT yes. DATE, DOUBLE, BOOLEAN no. |

### Deviations from the documents

All four are spec corrections forced by measurement, approved by the user, already applied.

1. **Spec 4.6 said `flush()` bounds memory. It does not.** S1 measured identical peak RSS
   with and without it at 1M and 10M rows. Sentence rewritten to say it marks the chunk
   boundary. The behaviour did not change; only the stated reason did.
2. **Spec 4.6's dispatch `when (col.type)` block had no DATE branch at all.** That omission
   is why the DATE hazard was never noticed. Branch added, rejecting at open time.
3. **Spec 7.2's `temp_directory` rationale was wrong.** It claimed a large join "fails
   outright instead of spilling" without it. S4b measured DuckDB 1.1.3 creating
   `<dbfile>.tmp/` and spilling there anyway, same peak within 0.2%. Replaced with the real
   reason: an unset value puts spill somewhere uncounted.
4. **Rule 15 now permits nullable BIGINT and rejects DATE regardless of nullability.**
   S3: `appendBigDecimal(42.7)` into BIGINT silently stores 43, and a `LocalDateTime` with a
   time component into DATE silently drops the time; overflow beyond Long throws loudly.
   BIGINT is safe *by construction* because the writer sources from `Row.long()` (scale 0,
   always fits INT64), so neither lossy case is reachable. DATE is not safe, because seam 1
   (4.3) maps JDBC DATE to `LocalDateTime`.

An engineer arithmetic error was also caught and corrected before it reached the spec: the
first `sizeLimit` proposal costed `1 + retries` copies of the failing dataset and ignored its
siblings in the same file. The correct count is `(N - 1) + 1 + retries`.

### Notes for later phases

- **P1 must resolve NOT NULL DATE.** Rule 15 governs nullable columns, but the DATE
  truncation does not depend on nullability. Rule 15 is currently written to reject DATE
  either way; this widening is recorded as OPEN in spec 12 and is not yet confirmed by the
  user. Decide before `CanonicalType` freezes.
- **P2's writer must dispatch nullable BIGINT through `appendBigDecimal`**, not
  `append(long)`, and the value must come from `Row.long()`. The safety argument depends on
  that accessor and breaks if the writer sources the value any other way.
- **P3's "commit once per chunk, visible from a second connection" is unverified.** S1
  measured flush cost, never cross-connection visibility, and `flush()` is not `commit()`.
  Do not assume the DuckDB appender makes rows visible to a duplicate connection at flush.
- What a failed attempt retains depends on where the failure came from. Measured three ways
  in P2 on the real driver: rows all completed with `endRow` survive `close()`; a row left
  PART-appended when an `append` throws discards the entire unflushed buffer including rows
  already completed in that chunk; an empty `beginRow` is harmless. So the framework's own
  validation errors (rejected before `beginRow`) retain the in-flight chunk's completed rows,
  while a driver error inside an append - a DECIMAL out of range, say - loses them. P0's S2
  saw 600,000 retained because it flushed per chunk and hit the first shape. P4 must budget
  for both; worst case `floor(rows_written / chunkSize) * chunkSize`.
- All numbers are Windows / Java 22 / NVMe. CI is Linux. Ratios (`d`, `s`) should travel;
  wall times will not. The Linux `/proc/self/status` RSS path in `SpikeSupport.kt` is
  written but never executed.
- `sizeLimit`'s dominant term is `retries`, not data volume. At `retries: 1` the file term
  falls from 12.6 GB to 9.0 GB.

---

## P1 - Row and type mapping  (2026-08-26)

Team: sdet + engineer + reviewer. One revision cycle. Final: 79 tests, 0 failures.

### Delivered

Production, `src/main/kotlin/infra/simpleetl/`:
- `CanonicalType.kt` - the enum, `duckDbType` natural mapping, `fromJdbc` (spec 4.3).
- `Row.kt` - immutable Row, typed accessors, `with`/`without`. The `internal` constructor
  carries a step label so 4.2's accessor error can name it; copies carry it forward.
- `RowMapper.kt` - `ColumnMeta` plus `RowMapper`. Metadata read once at construction;
  `map(rs)` reads the current row and never calls `next()`.

Tests, `src/test/kotlin/infra/simpleetl/`: `CanonicalTypeTest` (4.3 as a parameterised
table plus a `duckDbType` round-trip through real DuckDB), `RowTest`, `RowMapperDuckDbTest`
(real duckdb_jdbc 1.1.3), `RowMapperOracleTest` (Testcontainers `oracle-free` 23, one
container per class), `RowMapperErrorTest` (Mockito at the JDBC interface boundary only),
`DuckFixtures.kt`.

### Deviations from the documents

1. **Spec 11.1 gained `RowMapper` and `ColumnMeta`.** plan.md named `RowMapper` as P1's
   public surface; spec 11 never listed it. Since the sdet may only test through the public
   API and `RowPipe` does not exist until P3, P1's first done-when item was untestable as
   written. `RowMapper` takes `step: String` because 4.2 and 4.3 both require errors to name
   the step.
2. **Spec 4.3: `Types.DATE` (91) now maps to `LocalDate`, `Types.TIMESTAMP` (93) to
   `LocalDateTime`.** The sdet and engineer independently read the old single "DATE,
   TIMESTAMP" row in opposite ways and both documented it. An Oracle DATE reaches the driver
   as 93 (ojdbc `mapDateToTimestamp` defaults true) and keeps its time, so the row's intent
   is served by 93 alone; 91 only ever arrives from DuckDB, where a DATE has no time.
   Deciding the other way would have left `CanonicalType.DATE` unreachable from any result
   set and forced an `atStartOfDay()` workaround, since duckdb_jdbc 1.1.3 refuses to convert
   a DATE column to `LocalDateTime` or `Timestamp` at all.
3. **Spec 4.3 gained a BOOLEAN row.** It had none, because the table was written when Oracle
   had no SQL BOOLEAN. Without it a DuckDB BOOLEAN column could not be read at all, breaking
   task shapes B and C (2.4) and making the BOOLEAN branch of 4.6's writer dispatch
   unreachable by round trip. Verified on both drivers: DuckDB and ojdbc11 23.5 each report
   `Types.BOOLEAN` (16).
4. **Build:** `byte-buddy` pinned to 1.15.4 in `SimpleEtl/pom.xml`. Testcontainers 1.20.4
   pulls 1.14.18, Mockito 5.14.2 wants 1.15.4, and on Java 22 the skew fails at mock-creation
   time rather than at resolve time. Surefire also gets `-XX:+EnableDynamicAgentLoading`
   because Mockito 5 self-attaches its inline mock maker; marked `ponytail:` with `-javaagent`
   as the real fix when the flag stops working.

Deviations 2 and 3 are the same defect as P0's missing DATE branch in 4.6: **the type
contract was written Oracle-first, and the cases that arise only from DuckDB were never
filled in.** Expect more of these.

### Notes for later phases

- **P2, DECIMAL precision.** `DECIMAL.duckDbType` is the bare string `DECIMAL`, which DuckDB
  resolves to `DECIMAL(18,3)`. Correct as the *natural* mapping, but AUTO DDL must take
  precision and scale from `ResultSetMetaData` or an Oracle `NUMBER(38,10)` lands truncated.
- **P2/P3, Oracle folds INTEGER, SMALLINT and FLOAT into NUMBER.** All three arrive as
  `Types.NUMERIC` with typeName `NUMBER` and map to DECIMAL, never LONG or DOUBLE. So 4.3's
  LONG row and the FLOAT half of the Double row are reachable only from DuckDB. Anyone who
  assumes an Oracle `lot_id NUMBER(18)` yields `Row.long()` gets a wrong-type error at
  runtime. Combined with P0's ruling that nullable BIGINT is safe only because the value
  comes from `Row.long()`, that path is reachable only when the author CASTs in source SQL.
- **P2, DuckDB reports every column as nullable**, `NOT NULL` included - verified by the lead
  on the real driver. So 4.6's "NOT NULL columns keep their natural mapping and use the
  faster primitive path" is unreachable for any scratch-sourced pipe: AUTO DDL fed from
  scratch will emit VARCHAR/DECIMAL/TIMESTAMP for everything. Safe direction, wrong reason.
- **Oracle `TIMESTAMP WITH TIME ZONE` cannot be read as `Instant`** (ORA-17004);
  `getObject(i, OffsetDateTime::class.java)` works on both drivers.
- **`getObject`, not `getBytes`, for byte columns.** duckdb_jdbc 1.1.3 *does* implement
  `getBytes`; it is ojdbc that rejects it on a BLOB. The KDoc says so explicitly so a later
  phase does not "simplify" it back and break Oracle.
- **Always run `mvn clean test`.** Without `clean`, surefire runs stale compiled classes from
  `target/test-classes` whose sources were deleted; that produced three phantom passing tests.
- `Row` has no `equals`/`hashCode` - not in the frozen API, and `ByteArray` makes structural
  equality a trap. `toString` renders column names only, never values, so a listener at P8
  cannot export a production row into a log line.

---

## P2 - Writers  (2026-08-27)

Team: sdet + engineer + reviewer, plus one independent adjudicator. One review cycle.
Final: 115 tests, 0 failures. The phase the composition table calls highest-risk, and it
produced four contract changes and one real silent-corruption bug.

### Delivered

Production: `RowWriter.kt` (the interface plus `catalogColumns`, the shared target-catalog
read), `DuckDbTableWriter.kt` (`CreateTable`, AUTO DDL, catalog-ordered positional append,
4.6's null dispatch), `JdbcWriters.kt` (`JdbcTableWriter`, `JdbcStatementWriter`).
`RowMapper.kt` modified: `ColumnMeta` carries precision and scale.

Tests: `WriteFixtures.kt` (`Scratch`, `CountingConnections`), `DuckDbTableWriterAutoTest`,
`DuckDbTableWriterRequiredTest`, `WriterOracleTest`.

### Deviations from the documents

1. **Spec 11.1: `ColumnMeta` gained `precision` and `scale`.** Decided by an independent
   adjudicator that had not seen the engineer's work and was not told which answer was
   wanted. Bare `DECIMAL` resolves to `DECIMAL(18,3)`, which holds at most **15 integer
   digits** - so an ordinary Oracle `NUMBER(18)` key at or above 1e15 did not merely round,
   it failed the append mid-write after earlier chunks had committed. AUTO now emits
   `DECIMAL(p,s)`; a pair outside `1<=p<=38, 0<=s<=p` is a loud error at open. Measured:
   declared Oracle columns and explicit CASTs report usable pairs, while unconstrained
   `NUMBER`, `FLOAT` and every computed expression report `p=0` or `s=-127`.
2. **Spec 11.1: the three writers take `step: String`.** 4.6 requires a BLOB column to be
   rejected at open, before any Row exists, and 4.4 requires errors to name the step. Same
   gap and same resolution as `RowMapper` in P1.
3. **Spec 4.6 gained a rule for nullable columns with no write path.** BOOLEAN and DOUBLE
   have only primitive `append` overloads, DATE is rejected by rule 15 either way, and
   **INSTANT had no branch in the dispatch at all** - 1.1.3's appender has no `Instant` or
   `OffsetDateTime` method. All four are rejected at open under AUTO as well as REQUIRED.
   The engineer and sdet reached this independently, which is why it was ratified rather
   than adjudicated. Fourth instance of the type contract being written Oracle-first.
4. **`JdbcTableWriter` delegates part of spec 4.4 to the target database.** 4.4 specifies a
   framework runtime error for a NOT NULL target column with no matching Row key and no
   default. The writer instead omits the column from the INSERT and lets the database raise
   its own violation. The framework does not read `COLUMN_DEF`, so it cannot distinguish
   "no default" from "has a default" without another catalog round trip. Deliberate; the
   error is still loud, just not the framework's.
5. **P0's "a failed attempt keeps its partial rows" was wrong as written** and is corrected
   in spec 12 and in the P0 entry above. Three shapes measured on the real driver: rows all
   completed with `endRow` survive `close()`; a row left PART-appended discards the entire
   unflushed buffer including rows already completed in that chunk; an empty `beginRow` is
   harmless. Both shapes are reachable - framework validation errors give the first, a
   driver error inside an append (DECIMAL out of range) gives the second.
6. **Done-when item 7 is partially unmet, deliberately.** No leak-counting double is
   possible for the DuckDB appender: `DuckDbTableWriter` creates it via
   `DuckDBConnection.createAppender` with no injection seam, and `DuckDBConnection` is
   `public final`. (`DuckDBAppender` itself IS subclassable - the fixture KDoc originally
   claimed otherwise and was corrected.) The JDBC writers have a real counting double over
   connections, statements and result sets; the DuckDB exception path is proved by
   observable state. No better substitute exists: `close()` would make "was it closed"
   observable, except that on the part-appended path it discards everything, so closed and
   leaked are indistinguishable.

### Bug found in review, not by tests

`catalogColumns` filtered `getColumns` by exact TABLE_NAME but not by schema, so a
same-named table in another schema merged into the target column list. Measured:
`main.t1(a,b,q,...)` and `other.t1(zz,yy)` came back interleaved after sorting by
ORDINAL_POSITION into `[a, zz, b, yy, q, ...]`, shifting every value of a positional append.
The duplicate-column-name check caught it only when the two tables shared a column name.
Now guarded: the accepted rows must span exactly one `(TABLE_CAT, TABLE_SCHEM)` pair.

Two behaviours also had no test at all until review: the AUTO DECIMAL guard's rejection path
(this phase's headline change), and the exact TABLE_NAME filter - deleting that filter broke
zero tests, and it is the only thing between a JDBC wildcard match and a shifted append.

### Deliberate non-change

`CatalogColumn.precision`/`scale` are read from every catalog row and currently unused. The
reviewer asked for their deletion as dead code; the engineer declined and I agreed. Removing
them makes `toColumnMeta` fall back to the `= 0` defaults, so every target `ColumnMeta` would
report `precision=0` for a real `DECIMAL(38,10)` column - the "truth on one path, fabricated
zero on the other" the adjudicator had just ruled against. Dead-but-honest beats
deleted-and-lying for two `getInt` calls on an open ResultSet.

### Notes for later phases

- **P5: `JdbcStatementWriter` has no task-variable channel.** Spec 6.3 makes task variables
  available in `target.sql`, but the frozen constructor is `(jdbi, sql, step)` with no
  parameter map, so today every parsed `:name` must be a Row key. Needs a constructor
  amendment or pre-binding.
- **P4: budget scratch space from deviation 5**, not from a single number. A failed attempt
  costs between zero and one chunk of rows depending on where the failure landed.
- **P2 exercised rule 15's REQUIRED gate against `DatabaseMetaData.getColumns`**, which
  reports nullability truthfully on 1.1.3, unlike `ResultSetMetaData.isNullable`, which
  reports `columnNullable` for everything. The two must never be substituted for each other.
- `JdbcTableWriter` emits column identifiers unquoted, so Oracle folds them to upper case. A
  target column created as a quoted lower-case identifier fails with ORA-00904.

---

## P3 - RowPipe, Layer 1 complete  (2026-08-27)

Team: sdet + engineer + reviewer, plus one independent adjudicator. One review cycle.
Final: 137 tests, 0 failures. **Milestone: the snapshot cache can now adopt Layer 1.**

### Delivered

Production: `RowPipe.kt` - `JdbcSource` (two forms), `RowTransform`, `PipeResult`, `RowPipe`.
About 170 lines, half of it KDoc.

Tests: `PipeFixtures.kt` (`Pipe`, `ProbeWriter`, `RecordingConnections`), `RowPipeTest`,
`RowPipeCommitTest`, `RowPipeFailureTest`, `RowPipeOracleTest`.

### Deviations from the documents

1. **Spec 11.1: `JdbcSource` gained a borrowed-`Handle` form; spec 9.5's example was wrong.**
   Decided by an independent adjudicator that had not worked on either module. The frozen
   `JdbcSource(jdbi, sql)` opens a fresh `Handle` - fresh connection, fresh transaction - per
   pipe. `GenerationSource.refresh` requires all tables in a group to be read inside ONE
   source read transaction, so 9.5's own worked example published a torn snapshot: the union
   of tables showing duplicates or gaps, intermittently. The cache's own E2E cannot detect
   it, because its synthetic source generates rows in-process with no source transaction.
   9.5's example now wraps the pipes in `inTransaction` and passes the borrowed handle.
   Fifth instance of a contract written for one caller and never checked against the other.
2. **Spec 11.1: `RowPipe` gained `step: String`.** Third instance of the same gap after
   `RowMapper` (P1) and the three writers (P2). Settled by precedent, not adjudicated. Note
   that 9.5 was amended to pass it before 11.1 declared it, so the two documents contradicted
   each other until the reviewer caught it - the lead's omission.
3. **The `GenerationSource` acceptance criterion was rewritten** (plan.md P3). The original -
   "implemented in terms of `RowPipe` and its existing test suite passes unchanged" - cannot
   be satisfied from inside this module and proves nothing as evidence:
   - `GenerationSource` lives in the `snapshotcache` module, whose ArchUnit rule forbids it
     depending on the surrounding service; ETL spec 9.5 forbids Layer 1 knowing about the
     cache. `PipeGenerationSource` is caller-land, owned by the cache's own later phase.
   - Adding `snapshotcache -> SimpleEtl` would cycle against the `SimpleEtl -> snapshotcache`
     dependency that spec 7.3's cache-read step already requires at P9.
   - "its existing test suite passes unchanged" is vacuous: nothing in that module is
     modified, so it passes trivially. Worse, the cache's ArchUnit rule guards the literal
     patterns `etl..` and `source..`, which this framework's package `infra.simpleetl` does
     not match, and no rule names JDBI - so a cross-module dependency could have been added
     with all five boundary rules still green. If a later phase does add one, widen that rule
     in the snapshotcache project first, as its own change.
   Replaced by two properties provable here: a pipe populates a caller-supplied file-mode
   DuckDB connection and leaves it open and usable; two pipes share one source read
   transaction.
4. **Per-chunk commit needed no widening of `RowWriter`.** For a DuckDB target the per-chunk
   `flush()` is the commit - measured: unflushed rows are invisible even to the appending
   connection, and after flush they are immediately visible to a `duplicate()` connection.
   For a JDBC target each chunk is one prepared-batch execute on a handle whose `autoCommit`
   the framework never touches, so P1's ORA-17273 is unreachable. Spec 4.6's flush note now
   records the visibility measurement.
5. **`require(chunkSize > 0)` is not in the spec** and was kept deliberately. Recorded so it
   is not "simplified" away later.

### Bug found in review, not by tests

The engineer added a third `JdbcSource(Connection, ...)` form on its own initiative, and it
**closed the caller's connection** - the exact failure the borrowed form exists to prevent.
Its KDoc cited `SingleConnectionFactory.closeConnection` being `return;`, which is true but
about a method `Jdbi.open(Connection)` never calls: that goes through a lambda
`ConnectionFactory` inheriting the interface default, which calls `connection.close()`.
`SingleConnectionFactory` is reached only from `Jdbi.create(Connection)`. Measured on the
shipped classpath:

    Jdbi.open(conn)        -> caller connection closed = true
    Jdbi.create(conn).open -> caller connection closed = false

The form was deleted rather than fixed: absent from spec 11.1, unused, untested. **137 green
tests did not catch it because that form had no test** - coverage of what exists says nothing
about surface added on initiative.

This is the third phase running in which a KDoc driver claim was wrong and a reviewer caught
it, after P1's `getBytes` and P2's appender-subclassing note. All three sounded measured and
were refuted by measurement. The failure mode is reasoning from source or bytecode without
running the call path: here the engineer verified the method it expected to be called rather
than the one that is.

### Notes for later phases

- **P5/P6: a transform-added column is silently dropped under `createTable: AUTO`.** AUTO's
  DDL comes from source metadata, which cannot describe an added column, and P3's frozen
  signature has no `addColumns` channel. Under `REQUIRED` the same column lands. The
  behaviour differs by mode with no diagnostic either way. Validation rule 14 and spec 9.1
  make this Layer 2's to carry.
- **P5: a null in `JdbcSource.parameters` binds untyped** as `Types.OTHER`, which Oracle
  rejects on some columns. `Map<String, Any?>` carries no type, so it cannot be fixed here -
  note the asymmetry with `JdbcWriters.bindColumn`, which uses `bindByType` for exactly this
  reason. Reachable once P5's `export` step yields null for a zero-row export. Marked with a
  `ponytail:` comment.
- **A shared source read transaction needs SERIALIZABLE to mean anything.** Oracle's default
  READ COMMITTED gives statement-level consistency, so the shared-transaction test would pass
  against a pipe that opened a connection per run. The test sets it explicitly; a real caller
  must too.
- **`RowPipe` owns the target writer's lifecycle** (open/write/close), so a `RowWriter` is
  single-use. Spec 9.5 constructs the writer inline, so no one else could close it.
- `JdbcStatementWriter` is still unexercised by any pipe test.

---

## P4 - Scratch lifecycle  (2026-08-27)

Team: sdet + engineer + reviewer. One review cycle. Final: 160 tests, 0 failures.
First phase in which every driver claim the engineer made survived independent
re-measurement.

### Delivered

Production: `ScratchDb.kt` (lazy per-run DuckDB instance, settings at open, cleanup on every
path, runtime temporary-table guard), `DatasetNamer.kt` (attempt-suffixed names, stable view
over a table or a parquet file, shared `quoteIdentifier`/`sqlLiteral` internals).

Tests: `ScratchFixtures.kt`, `ScratchDbLifecycleTest`, `ScratchDbDeletionTest`,
`DatasetNamerTest`, `NoTempTableTest`.

### Deviations from the documents

1. **Spec 11.2 declares only `connection()`, `duplicate()`, `close()`.** The implementation
   needs a constructor: `ScratchDb(directory: Path, memoryLimitMb: Int, tempDirectory: Path
   = directory.resolve("spill"))`, because 7.2 requires file location, memory limit and temp
   directory to be decided at open. A public `val directory` was also added and then
   **removed in review** - nothing read it, and P5 builds the run directory itself. The
   public surface now matches 11.2 exactly.
2. **`DatasetNamer` has no signature anywhere in spec 11.** Built as
   `DatasetNamer(scratchDirectory)` with `physical`, `parquetPath`, `publishTable`,
   `publishParquet`. The publish pair executes rather than returning SQL, so the path
   escaping `read_parquet` needs lives in one place. Fourth phase running that the plan named
   a public type spec 11 never declared, after `RowMapper`, the writers' `step`, and
   `JdbcSource(handle)`.
3. **`close()` can throw `IllegalStateException`**, which spec 11.2's bare `override fun
   close()` does not sanction. Raised only for a leftover temporary table or a path that
   survived deletion, and only **after** cleanup completes - so on a failure path it arrives
   as suppressed and the run's own failure stays primary. Verified by probe, not by reading:
   the guard fired and the directory was already empty.
4. **`datasetIdentifier` validation was added on the engineer's initiative and kept.** Not
   required by 5.5 or 11.2. The reviewer probed the case that justifies it: without the
   check, `parquetPath("../../evil", 1)` resolves outside the scratch directory. A dataset
   name arrives from a YAML file and becomes both a SQL identifier no prepared statement can
   parameterise and a filesystem path. Validation at a trust boundary is not over-engineering.
5. **`require(memoryLimitMb > 0)` and `require(attempt >= 1)`** are not in the spec. Recorded
   rather than tested, following P3's precedent with `require(chunkSize > 0)`, so a later
   session does not "simplify" them away. `require(attempt >= 1)` did get a test alongside
   the identifier work.
6. **The temp-table ban is met by two checks, not one.** The plan says "an ArchUnit rule or
   an equivalent check", but ArchUnit reads bytecode and a temp table is a SQL string.
   ArchUnit is also not on the classpath. So: a source scan over `.kt` in both roots, and a
   runtime catalog guard in `close()` that asks **every issued connection** - measured, the
   temporary catalog is per connection, so a guard asking only the primary would be a false
   negative. Neither is complete: the scan cannot see SQL a YAML `sql` step assembles at run
   time, and the guard only covers paths a run actually exercises. Both are now proven able
   to fail.
7. **P2's `DuckDbTableWriter.kt` was modified** - its private `quote` lifted to the shared
   `internal quoteIdentifier`. Behaviour-identical (body verified byte-for-byte by the lead)
   and P2's 29 tests pass unchanged.

### The platform finding, which reaches beyond this phase

`fileIsDeletedEvenWhenTheRunLeavesADuplicateOpen` was **structurally unfalsifiable on the
Linux CI**. All its discriminating power came from a Windows file lock: on Windows
`Files.delete` throws while any connection is open, so the test detects a leaked duplicate.
On Linux the file unlinks successfully, the file-absence assertion passes, and the test goes
green against an implementation that never closes duplicates - exactly the behaviour it
exists to pin. Fixed with a platform-independent `leaked.isClosed()` assertion.

**Every measurement this project has is from Windows; CI is Linux.** P0 recorded that as a
precision caveat. It is not only precision: a test whose discriminating power comes from an
OS behaviour stops discriminating on CI and stays green. The sdet swept the rest of the P4
suite and found no other case - the deletion-path tests rest on delete *succeeding*, which is
the easy direction on Linux. Later phases should keep asking the question. Spec 7.2's 32 GiB
`sizeLimit` has the same exposure: it rests on bytes-per-value and spill-factor ratios
measured on Windows/NVMe, and ratios should travel better than wall times, but nobody has
confirmed that on Linux.

### Measured on duckdb_jdbc 1.1.3, verified twice

- The file is created at `getConnection`, before any statement runs.
- Windows blocks deletion while any connection is open; an outstanding duplicate keeps the
  lock and stays usable after the primary closes. **Windows-only.**
- The temporary catalog is per connection: a temp table on the write connection is invisible
  from a duplicate.
- `'512MB'` reads back as `488.2 MiB` - DuckDB reads MB as a power of ten. `'512MiB'` reads
  back as `512.0 MiB`.

### Notes for later phases

- **P5 owns when a retry happens and which attempt number to publish.** `DatasetNamer`
  deliberately does not decide when an attempt has succeeded.
- **P8 needs a way to reach the scratch file's size** for `etl_scratch_file_bytes` (9.3).
  Deliberately not built speculatively; it is one line when P8 needs it.
- `ScratchDb.close()`'s guard query is wrapped in `runCatching`, so a future driver change
  renaming `duckdb_tables().temporary` would make it a silent no-op while the KDoc still
  promises enforcement. The tripwire is the test that proves it fires, not runtime plumbing -
  unwrapping it would turn a genuinely broken connection into a second failure at close.
- `report()`'s undeleted-survivor branch is deliberately untested: it fires only where the OS
  locks open files, so a test would pass on Windows and fail on Linux CI.

---

## P5 - Task model, variables, step executors  (2026-08-27)

Team: sdet + engineer + reviewer, plus one independent adjudicator. One review cycle,
interrupted mid-revision by a session limit and resumed. Final: 209 tests, 0 failures.
Production is 642 lines across two files - over the 600 top of the size budget, recorded
rather than rounded down.

### Delivered

Production: `TaskDefinition.kt` (the definition model), `TaskEngine.kt` (`VariableScope`,
`TaskEngine`, four step executors, retry classification and backoff, the `addColumns` writer
decorator).

Tests: `TaskFixtures.kt`, `TaskEngineStepTypesTest`, `TaskEngineRetryTest`,
`TaskEngineVariableTest`, `TaskEngineChunkSizeTest`, `TaskEngineFailureTest`,
`TaskEngineGuardTest`, `VariableScopeTest`.

### Deviations from the documents

1. **Spec 6.3 no longer offers task variables in `target.sql`, and spec 11.1 was NOT
   amended.** Decided by an independent adjudicator. The frozen
   `JdbcStatementWriter(jdbi, sql, step)` has no parameter channel, and the obvious fix was
   refuted by measurement: `PreparedBatch.add()` clears the binding on JDBI 3.45.4, so
   "bind once per batch" writes the value into each chunk's **first row** and `Types.OTHER`
   NULL into the rest - `(F12,1,1) (null,2,2)` through a recording `PreparedStatement`.
   Binding per row instead costs what a Row key costs and buys a second namespace, a
   precedence rule, and a collision check startup cannot perform. The author projects the
   variable into the source select list instead: `select lot_id, :siteCode as site_code`.
   Zero code changed, zero tests changed, and validation rule 8 lost its Row-key-collision
   clause because the two can no longer meet. **First of the six contract gaps resolved by
   deleting an unpriced promise rather than widening a signature.**
2. **A null task variable carries its export column's type.** It travels as a JDBI
   `Argument` inside the existing `Map<String, Any?>`, which JDBI binds directly - measured
   `setNull(pos, 93)` versus `setNull(pos, 1111)` for a plain null. No signature change.
   `org.jdbi.v3.core.argument.NullArgument` already ships in jdbi3-core, so the fix is zero
   lines of new class. The engineer found a second null shape the ruling missed: `select
   max(ts)` over an empty table returns **one row holding SQL NULL**, equally untyped; both
   shapes are wrapped. `LiteralVar` with a null value is now an error - null carries no type
   and 1.3 makes an untyped value an error rather than a guess.
3. **Spec 5.5 gained a carve-out and spec 10 a rule 18: a scratch `createTable: REQUIRED`
   target with `retries > 0` is rejected.** Found in review. `retries` defaults to 3 for any
   scratch target including REQUIRED, but REQUIRED gets no attempt suffix - the framework
   does not own the physical name - so a transient mid-pipe failure left up to a chunk of
   flushed rows and the retry appended the whole source on top. Silent duplication, on a
   default the author never wrote. The departure had existed only in a KDoc.
4. **`VariableScope`, `LiteralVar`, `ExportVar`, `PipeSource`, `PipeTarget`/`TableTarget`/
   `StatementTarget`, `MaterializeFormat` and `SCRATCH` are not named in spec 11.** Sixth
   phase running that the plan named public surface spec 11 never declared. The reviewer
   judged each: `PipeTarget`'s sealed pair "earns it - two implementations, and it makes
   validation rule 10 unrepresentable rather than merely checked".
5. **Two silent scope holes closed in review**: two `ExportVar`s with the same name inside
   one step collided in a local map before `VariableScope.define` was reached, so 6.2's
   redefinition rule never fired; and a literal or exported variable named `attempt` was
   accepted by `define` and then silently discarded at bind time by the built-in. Both now
   `require`. `TaskDefinition` is public and programmatically constructible, so P6's
   validation is not the only gate.
6. **`DeclaredColumns` closes P3's `addColumns` silent drop inside Layer 2**, not P6. A
   private `RowWriter` decorator appends the declared columns to the list the target is
   opened with. `RowPipe` is frozen with no channel for it. Confirmed to fix
   `JdbcTableWriter` too: the column enters `open`'s `columns`, `bySource` picks it up, and
   a target lacking it now fires the existing `unknown` check loudly instead of dropping it.

### The mutation test, and why it matters

The reviewer did what no earlier reviewer did: it **introduced a regression and ran the
suite**. It replaced `it.sqlState?.startsWith("08") == true` with
`it.sqlState!!.startsWith("08")` and `TaskEngineRetryTest` stayed green at 13/13 - the test
whose KDoc claimed to reject exactly that implementation. The NPE is thrown inside
`execute`'s own catch, propagates out of `run`'s `catch (e: Exception)`, and lands as a
non-null `TaskOutcome.failure` with `attempts == 1` and no recorded delays, satisfying every
assertion.

The sdet had reported honestly that it could not mutation-test: editing `src/main/` is
outside its role and the permission system refused it. The reviewer, which writes neither
production nor test code, was the only agent positioned to falsify the tests.

After the fix the lead re-ran the same mutation: exactly one test fails, and it is the right
one. File restored, md5 verified against the pre-mutation checksum.

**Adopt this for later phases.** Reading cannot find this class of defect - the assertions
are individually reasonable and the failure arrives through a path nobody pictured.

### Measured on the shipped classpath

- Every JDBC failure reaches Layer 2 **wrapped**: `ResultSetException`,
  `UnableToExecuteStatementException`, `ConnectionException`, each with the real exception as
  cause. A classifier reading only the caught exception would retry **nothing**.
  `isTransient` walks the chain, guards a self-referential cause and caps at 32 hops.
- A DuckDB syntax error is a plain `java.sql.SQLException` with a **null** SQLState, so an
  unguarded `sqlState.startsWith("08")` throws inside its own error handling.
- `PreparedBatch.add()` clears bindings (deviation 1).
- Result set metadata survives an exhausted result set unchanged, so a zero-row export knows
  its column type.
- A KDoc claim that JDBI rejects superfluous bindings was wrong: it is rejected **only when
  the statement declares no parameters at all**. Fourth phase running with a confidently
  wrong driver claim in a KDoc, and the first where the engineer re-measured and agreed.

### Notes for later phases

- **P6 owns validation rule 18** (scratch REQUIRED + retries) at startup; the engine
  currently rejects it at step start.
- **Untested and deferred to an Oracle-backed test**: the `StatementTarget` happy path and
  the non-scratch `TableTarget` branch of `writer()`. Both need a non-DuckDB target, since a
  `pipe` into DuckDB must use the appender. The *guards* on both paths are tested.
- Non-scratch `materialize` retries will always fail the second attempt on "table already
  exists" - loud, so not a defect, but `MaterializeStep` has no `idempotent` channel for
  rule 12 to key off.
- `TaskDefinition.enabled`, `cron`, `logging`, `onSuccess`, `onFailure` and
  `PipeTarget.idempotent` are carried and unused - P6/P7/P8.

---

## P6 - YAML loading and validation  (2026-08-27)

Team: engineer + sdet (composition table: no reviewer), plus one independent adjudicator.
One review cycle. Final: 265 tests, 0 failures. P6 contributes 56.

Production is 501 lines of code across two files, over the 200-600 budget that counts tests
too. Eighteen rules plus the canaries that make them mean anything do not fit; recorded
rather than trimmed by dropping coverage.

### Delivered

Production: `TaskYaml.kt` (spec 3's schema as one `internal` DTO per document node, with a
`@JsonSubTypes` step hierarchy), `TaskFileLoader.kt` (`LoadResult`, `ValidationReport`,
`ValidationError`, `TaskFileLoader`, the per-file rule pass, the DuckDB syntax check).

Tests: `TaskFileFixtures.kt`, `TaskFileLoaderValidTest`, `TaskFileLoaderRulesTest`,
`TaskFileLoaderSqlFidelityTest`, `TaskFileLoaderDirectoryTest`.

### Deviations from the documents

1. **Spec 10 rule 15 was amended, and the amendment made startup check MORE than before.**
   Decided by an independent adjudicator. Both agents had independently concluded the rule
   was unenforceable at startup; both were right about the half they could see and both
   missed the other half. A *table's* declared types are genuinely unreachable - measured
   three ways: `json_serialize_sql` parses a `CREATE TABLE` but serializes SELECT only,
   `EXPLAIN create table` emits a single `CREATE_TABLE` box with no column list, and
   `PREPARE` rejects DDL outright. But `transform.addColumns` states its types **in the YAML
   text**, and the loader was accepting all of it: `DATE`, `BLOB`, `TIMESTAMP WITH TIME
   ZONE`, nullable `BOOLEAN`, nullable `DOUBLE` and `DECIMAL` with default precision all
   loaded clean and died at writer open. Rule 15 now splits where the information splits,
   using the wording rule 14 already carries for the same reason.
2. **`Result<List<TaskDefinition>, ValidationReport>` does not exist**, since Kotlin's
   stdlib `Result` takes one type parameter. Replaced by a sealed
   `LoadResult.Loaded` / `LoadResult.Invalid` pair. `kotlin.Result` plus a
   `ValidationException` was rejected because reading the report would need an unchecked cast
   at every call site and nothing constrains a `Result.failure` to carry that type. Follows
   P5's `PipeTarget` precedent: the invalid state is unrepresentable, not merely documented.
   Seventh phase running that the plan named public surface spec 11 never declared.
3. **`TaskFileLoader(datasources: Set<String>, transforms: Map<String, RowTransform>,
   hooks: Set<String>)`** is the answer to "rules 3, 4 and 5 need something the loader does
   not own": plain data in, no registry invented. `transforms` is a map rather than a name
   set because `PipeStep.transform` carries a resolved `RowTransform` while YAML carries a
   name, and the loader is the only place the two can meet.
4. **Rule 6 is enforced for scratch SQL and partial elsewhere.** DuckDB's own parser runs via
   `json_serialize_sql`, which - measured - parses **without binding**, unlike `PREPARE` and
   `EXPLAIN` which both bind and fail on a missing table. Non-scratch SQL gets blank,
   positional and named-parameter checks only: DuckDB is the only dialect on the classpath
   and it rejects a valid Oracle `MERGE`.
5. **Rule 16 is structural, not a cron parse** - field count and legal characters. No cron
   parser on the classpath; marked `ponytail:` with P7's `quarkus-scheduler` as the upgrade.
6. **Five checks beyond spec 10**, each one line and each converting a mid-run failure into a
   boot failure: duplicate YAML keys, non-scratch `format: PARQUET` (5.6),
   `datasetIdentifier`'s character check, negative `retries`, non-positive `chunkSize`.
7. **`unwritableToDuckDb` was lifted out of `DuckDbTableWriter`** to file level so the
   startup check and the writer-open check call the same predicate. Same shape as P4's
   `quote` -> `quoteIdentifier` lift. Verified by mutation: short-circuiting the predicate to
   null breaks 5 tests in `DuckDbTableWriterAutoTest`, 5 in `DuckDbTableWriterRequiredTest`
   and 6 in `TaskFileLoaderRulesTest` - direct evidence that one thing decides both checks,
   rather than a KDoc claiming it.

### The boot sandbox, refused on measurement rather than on the reason first given

The engineer rejected executing a task's scratch `sql` steps in a boot sandbox for two
reasons. The adjudicator measured the first one **false**: 1.1.3 *is* cancellable -
`Statement.cancel()` from a watchdog interrupts a runaway CTAS in ~200 ms - and
`set enable_external_access=false` refuses `read_parquet`, `COPY TO` and `ATTACH`. The
containment objection does not stand.

What defeats the sandbox is the engineer's **second** reason, which the adjudicator
confirmed and which is mainstream rather than a corner case: **spec 3.4's own canonical
example does not execute at boot.** `create index idx_wip_lot on wip_stg (lot_id)` fails
with `Catalog Error: Table with name wip_stg does not exist`, because `wip_stg` is created by
a `pipe` step and no pipe can run at boot. Spec 5.4's own PL/SQL `sql` step fails the same
way. So a sandbox would either ignore those failures - silently switching rule 15 off for
exactly the task files most likely to be complex - or honour them and refuse to boot a
correct application over a `create index`.

Fifth confidently-wrong driver claim in six phases, and the second refuted by an adjudicator
rather than a reviewer. The engineer's own process fix, adopted: nothing goes in a KDoc that
was not run in the scratchpad first. The two claims it *did* run this phase both survived.

### A bug found by running rather than reading

A file containing only `---`, or the literal `null`, deserialises to Java `null` rather than
throwing, so startup died with a `NullPointerException` instead of producing a report. An
empty file and a comments-only file do throw. Fixed.

### Measured Jackson behaviours

- `FAIL_ON_UNKNOWN_PROPERTIES` is already true by default on `YAMLMapper`; set explicitly
  anyway, because it is an acceptance criterion rather than something to rest on a default.
- `STRICT_DUPLICATE_DETECTION` is **off** by default: without it, `name:` twice parses
  silently and the second wins. Enabled.
- Every failure shape is a `JsonProcessingException` with a usable `location.lineNr`,
  including `MarkedYAMLException`, which is *not* a `JsonMappingException`.
- `As.PROPERTY` on a sealed interface consumes `type` and does not then report it as unknown.
- `${env.FOO}` survives folded and literal block scalars byte for byte.

### Notes for later phases

- **P7 owns reload semantics** and the real cron parse (rule 16 is structural today).
- **P7/P8 wire the loader's three constructor arguments**: datasource names, hook names, and
  the CDI-supplied transform map.
- **Rule 15's table half lives at writer open** (4.6, P2), before any row is written, with
  `retries` forced to 0 by rule 18. Reproducible in seconds through spec 8.2's manual
  trigger.
- `cacheCopy` has no YAML schema by design (P9's); it surfaces as an unknown type id listing
  the four known ones.
- **Rule order within a report is untested by choice** - pinning it would encode the loader's
  internal rule sequence, which nothing outside the class depends on. File-name ordering
  *is* tested.
- Non-scratch `materialize` output shares rule 9's uniqueness namespace with scratch
  datasets, so two materialize steps writing the same table name to two different Oracle
  datasources would be rejected. Matches 5.5's parenthetical literally.
