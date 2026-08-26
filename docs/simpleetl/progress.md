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
