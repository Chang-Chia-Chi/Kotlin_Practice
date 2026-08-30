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

---

## Interstitial - package restructure  (2026-08-27)

Not a phase. Done between P7 and P8, at the user's direction, after they asked why sixteen
production files sat in one flat package with no enforcement.

### What was wrong

The engineer charter this project runs under says: "Keep Layer 1 free of YAML, scheduling and
task concepts; keep the core free of DuckDB-specific types outside the writer. **ArchUnit
enforces this**." That was never set up. The lead mandated one flat package, `infra.simpleetl`,
in every spawn prompt so that the engineer and sdet - who work blind to each other - would
compile together. Real problem, cheap fix, and it left spec 2.1's Layer 1 / Layer 2 boundary
as prose only. Nothing stopped `RowPipe` importing `TaskEngine`, and the snapshot cache
adopting Layer 1 would have taken the YAML loader, the scheduler and Jackson with it.

### The convention now shared with the snapshotcache module

Agreed with the user as a decision procedure, not a fixed tree, because the two modules
differ in one structural fact: **snapshotcache ships one consumable unit; SimpleEtl ships
two** - Layer 1 is consumed without Layer 2, which is spec 2.1's whole reason for existing.

1. If a module ships more than one independently consumable unit, split by unit first.
   Otherwise the module is the unit.
2. Within a unit: `api` (callers call inward), `spi` (callers implement outward), `core`
   (internal) - the same three words meaning the same thing in both modules. Applied when a
   unit outgrows roughly a dozen files, so the names are decided in advance and only the
   timing is judgement.
3. Every technology adapter is its own package named for the technology, and adapters are
   leaves.
4. Rules are written as dependency sentences and enforced by ArchUnit. Packages are the
   mechanism, not the point.

**snapshotcache is unchanged** by this: `api`/`spi`/`core`/`duckdb` is what the procedure
generates for a one-unit module, and its `api` vs `spi` split is already a direction
distinction rather than a stack.

### The layout

| Package | Files |
|---|---|
| `infra.etl.pipe` | CanonicalType, Row, RowMapper, RowWriter, RowPipe |
| `infra.etl.duckdb` | DuckDbTableWriter, ScratchDb, DatasetNamer |
| `infra.etl.jdbc` | JdbcWriters |
| `infra.etl.task` | TaskDefinition, TaskEngine, TaskYaml, TaskFileLoader, TaskRunner, TaskScheduler, TaskAdmin |

Tests stay in one package, `infra.etl`, plus `infra.etl.spike`. The fixtures are shared
across seven phases and splitting them is a separate decision nobody has made.

### The rules, all six proven able to fail

Each was proven by introducing a real violation into a real source file, confirming that
rule failed, and reverting - not by asserting they pass on a clean tree.

| Rule | Because |
|---|---|
| `pipe` must not depend on `task` | Layer 1 ships to the cache without the task engine |
| `pipe` must not depend on `duckdb` or `jdbc` | Layer 1 defines the `RowWriter` seam; adapters implement it |
| only `duckdb` may depend on `org.duckdb` | one adapter, named for its technology |
| `duckdb` and `jdbc` must not depend on `task` | adapters are leaves |
| `duckdb` and `jdbc` must not depend on each other | a JDBC target must not drag DuckDB in behind it |
| no cycles across `infra.etl.(*)..` | a cycle makes the split unenforceable |

Probe 3 tripped only its own rule, confirming the rules are independent rather than one rule
wearing six hats.

### A trap worth recording

Three KDoc links in `pipe/RowPipe.kt` pointed at `DuckDbTableWriter` and the JDBC writers.
After the split they no longer resolve. **Making them resolve would require an import, and
that import is exactly the dependency rule 2 forbids - and ArchUnit reads bytecode, so a
KDoc-only import leaves no trace and the rule would have stayed green.** "Fix the broken doc
link" would have silently created the coupling the split exists to prevent. The links are
now plain backticks: the prose survives, the coupling does not.

### Cost

289 tests before, 295 after - the 6 new ArchitectureTest methods. No test added, deleted,
weakened or reordered; git recorded every file move as a rename. No `internal` symbol was
widened to make the move easier.

---

## P7 - Scheduling, triggering, threading, reload  (2026-08-27)

Team: engineer + sdet + reviewer, plus one independent adjudicator. One review cycle.
Final: 300 tests, 0 failures. Production 169 lines across three files; tests 696. Third phase
running over the 200-600 budget that counts tests - recorded, not trimmed.

### Delivered

`TaskRunner.kt` (TriggerResult, RunStatus, TaskRunner, the per-task slot and guard),
`TaskScheduler.kt` (CronScheduler, TaskScheduler), `TaskAdmin.kt` (TaskStatus, TaskAdmin),
plus a defaulted `runId` parameter on `TaskEngine.run`.

Tests: SchedulingFixtures.kt, TaskSchedulerApplyTest, TaskRunnerConcurrencyTest,
TaskAdminTriggerTest, TaskAdminReloadTest, TaskRunnerCoroutineNameTest, TaskAdminIdentityTest.

### Deviations from the documents

1. **Quarkus stays out of this module; spec 8.6 "Host Wiring Contract" is new.** Decided by an
   independent adjudicator. Spec 7.1's Quarkus datasources already arrived as
   `Map<String, Jdbi>` in P5, and 9.1's CDI transforms as `Map<String, RowTransform>` in P6;
   spec 8's `@Scheduled` and `@RolesAllowed` are the same sentence in a new place. The
   decisive measurement: **Quarkus does not read `application.properties` from a dependency
   jar**, so shipping `quarkus.scheduler.start-mode=forced` here would have put it in a file
   only this module's own tests read, while the real deployment fired nothing - a green test
   for a production failure. `TaskScheduler` takes a host-implemented `CronScheduler`;
   `TaskAdmin` returns sealed results a host's `AdminResource` maps to HTTP.
   **Two acceptance criteria are consequently untested anywhere in this repository** -
   `start-mode=forced` and the `etl-admin` role check - recorded in 8.6 with the symptom of
   missing each. A real gap, taken because the alternative was false evidence.
2. **One acceptance criterion was untestable as originally written, under any option.** The
   plan asked to assert the dispatcher by "capturing the thread and coroutine name inside the
   run". Measured: `coroutineContext` is unreachable from the blocking engine body (8.3 says
   coroutines buy nothing inside the engine), and the `@taskName#1` thread-name tag exists
   **only under `-ea`** - surefire's default, absent in production. Rewritten to assert the
   name the runner hands *into* the run body.
3. **`TaskScheduler(cron, runner)` is a constructor widening, not an addition.** Spec 11.2
   declares `TaskScheduler(cron)`; a second required parameter breaks that call. Justified - a
   firing must reach the runner and 11.2 declares no other route - but recorded as a
   deviation. Same for `TaskAdmin(runner, scheduler, loader, tasks)`, which 11.2 gives no
   constructor at all. Eighth phase running that the plan named public surface spec 11
   under-declares. **P7 is the first phase where the lead declared the surface in 11.2 before
   the phase started**, and these two are what that pass still missed.
4. **`TaskRunner.lastRun` / `outcome` / `context`, and `TaskEngine.run`'s defaulted `runId`,
   are additive.** The default is the identical expression P5 used internally, so P5's suite
   is unchanged - verified by diff and by 209 tests passing untouched. One id now names the
   scratch directory, the runId task variable, and the admin API.

### The mutation results, run by the lead

Six mutations. Two, nominated by the sdet, killed exactly what it predicted - the guard's
position in front of `launch`, and the per-task view. **Four others survived**, all on paths
the code's own KDoc called load-bearing:

| Mutation | Before | After the sdet's five changes |
|---|---|---|
| TaskScheduler rollback restore deleted | 24/24 green | killed by aRejectedApplyRestoresTheRegistrationItHadAlreadyCancelled |
| TaskAdmin.reload ignores the scheduler | 24/24 green | killed by aReloadTheSchedulerRejectsChangesNeitherTheScheduleNorTheDefinitions |
| scratch directory named from a fresh UUID | 67/67 green | killed by differentTasksRunConcurrentlyEachWithItsOwnScratchFile |
| outcome() drops the runId filter | 24/24 green | killed by triggerReturnsAcceptedWhileTheRunIsStillInProgress |

The third is the sharpest: the only justification for widening P5's shipped `TaskEngine.run`
signature was that one id must name all three places, and nothing pinned it.

### A process correction, recorded because it changed the rules

The reviewer executed builds, mutations and probes. The user ruled that a reviewer's job is to
review code, not to execute it, and the same now applies to adjudicators. **From P8: only the
engineer (production), the sdet (tests) and the lead (everything else) execute.** Reviewers
and adjudicators read, and end with a "For the lead to run" section naming the mutations and
measurements they want; the lead runs them and reports back. An adjudication may therefore
come back conditional on a measurement the lead then takes.

### Measured on the shipped classpath

- **A `limitedParallelism(1)` view queues.** With the first coroutine blocked the second did
  not run; after release, order `[1, 2]`. `dispatchInternal` does `queue.addLast(block)`. So
  the self-concurrency guard must sit **in front of** the submit - submitting first and
  rejecting after is spec 8.4's backlog with extra steps.
- **`Job.invokeOnCompletion` fires after the block and receives an Error as its cause.** On a
  job cancelled before it ever started it still fires, with no body event - which is the
  property the design rests on, since a `finally` inside the block would never have run.
- **`LimitedDispatcher.dispatch` hands itself to the underlying dispatcher, never the
  coroutine's context.** So a recording dispatcher wrapping `Dispatchers.IO` *below* the view
  reads `context[CoroutineName]` as null **against a correct implementation**. The sdet had
  written that test, read the coroutines source, and discarded it before shipping. Third
  distinct mechanism this project has found for a test that passes while proving nothing.

### A known CI exposure, recorded not fixed

`twoRunsOfOneTaskShareOneWorkerThreadAndNeitherIsTheTriggeringThread` asserts thread
*affinity*, but `limitedParallelism(1)` guarantees only *serialisation*. It holds because an
idle `Dispatchers.IO` pops the most recently parked worker. Measured: the real test shape held
100/100 at each of four configurations including 48 blocked IO coroutines and
`-XX:ActiveProcessorCount=2`; a synthetic shape with a trivial body degraded to 4/100. The
criterion is FIXED, the test implements it faithfully, and the property holds by scheduler
timing rather than by contract. Same register as P4's Windows-file-lock finding.

### Notes for later phases

- **P8 owns the listener call sites** in TaskEngine, which is the same file P9 edits for the
  cache read step. They cannot be built by two agents in parallel.
- `TaskAdmin.run(name, runId)` cannot say "still running" - TaskOutcome has only SUCCEEDED and
  FAILED and its signature is frozen. TaskStatus.running separates the cases; a host needs
  both calls to map a poll correctly.
- Only the current-or-last run per task is retained, marked `ponytail:`. A run displaced by a
  later firing 404s.
- TaskScheduler puts a **task name** into ValidationError.file, since a TaskDefinition does not
  know its file. A reload report mixes loader errors (file names) with scheduler errors (task
  names).
- A non-scratch Jdbi must be pool-backed now that two runs are concurrent; a
  `Jdbi.create(singleConnection)` would hand one Connection to two runs, which spec 7.2 calls
  a JVM crash rather than an error.

---

## P8a - Run listener and task hooks  (2026-08-27)

Team: engineer + sdet + reviewer, contract confirmed by all three before any code was written.
One review cycle. Final: **309 tests, 0 failures**. Production 490 lines added across five files;
tests 1,407. **1,897 lines against a 200-600 budget - the fourth consecutive overrun, and this
one after the phase had already been split three ways to avoid it.** Recorded, not trimmed. P8b
should be re-scoped before it is written.

### Delivered

`task/Observability.kt` (TaskContext, PhaseContext, StepContext, StepResult, TaskRunListener with
NONE and `of`), `task/TaskHooks.kt` (TaskHook, TaskHookRegistry, TaskHooks), TaskEngine's eleven
call sites plus an injected `java.time.Clock`, `TaskRunner` passing caller identity through, and
`jboss-logging` declared.

Tests: ObservabilityFixtures.kt, TaskListenerOrderTest, TaskHookTest, TaskListenerIsolationTest,
plus additive growth of TaskFixtures.kt.

### Deviations from the documents

1. **P8 was split into P8a / P8b / P8c.** Two independent estimates put the drafted phase at
   2.4x-3.3x budget, which CLAUDE.md makes a stop-and-report trigger. plan.md amended before the
   phase opened. The split isolated the Micrometer dependency decision into P8b, where it can be
   taken on its own evidence.
2. **`java.time.Clock` injected into `TaskEngine`** - additive, defaulted. Without it the
   contract's "durationMs spans all attempts" was **unfalsifiable**: the test sleeper does not
   sleep, so a retried step reports ~2 ms against correct code and no observation separates right
   from wrong. Root CLAUDE.md already mandates injected `Clock`; nothing observed engine time
   before (measured).
3. **`onTaskEnd` is emitted from a `finally`.** Without it an `Error` escaping the engine left
   `TaskAdmin` reporting FAILED while the listener never saw the run end - two subsystems
   disagreeing on the path P9 makes routine. The catch stays `Exception`, so P5's documented
   `NotImplementedError` propagation is untouched.
4. **Six types spec 11.3 never declared** - PhaseContext, StepContext, TaskHooks, TaskMetrics,
   `TaskRunListener.NONE`, `TaskRunListener.of`. Ninth consecutive phase in which spec 11's
   surface proved narrower than the phase needed. spec.md amended before the phase.
5. **`SimpleEtl/CLAUDE.md` created.** The root CLAUDE.md describes only the snapshot cache and
   says "modify only `infra/snapshotcache/`" - read literally it forbade eight phases of this
   module's work.

### Three contract justifications the confirmation round refuted

Written down because each was the lead's, stated confidently, and wrong:

1. "Hooks run after scratch close because a hook must not be able to hold the file." A `TaskHook`
   receives only a `TaskContext` and holds nothing. The real reason is that `ScratchDb.close()`
   **can throw**, and inside the `use` block that failure would arrive as a suppressed exception
   *after* `onSuccess` had already declared the run good.
2. "Rows are 0/0 for non-pipe steps because DuckDB answers -1." A non-scratch `sql`/`materialize`
   does have a real affected-row count and the engine discards it. The ruling stands on "only
   `pipe` moves rows through the JVM" alone.
3. "`ScratchDb.diskBytes()` prices the spill term of spec 7.2." Sampled once at run end it cannot:
   spill is reclaimed as queries finish, and spec 7.2's own arithmetic makes spill 17.6 of the
   30.2 GB. Carried into P8b's plan entry as a constraint, not a claim.

### Measured this phase (lead's scratchpad, not quoted from memory)

- `MutableSharedFlow(replay=0, extraBufferCapacity=N, onBufferOverflow=DROP_OLDEST)` -
  `tryEmit` returned **false 0 times out of 100** with a wedged subscriber and 0 with none. A
  `dropped` counter under that policy is **structurally always zero**. `SUSPEND` returned false
  91 times for 91 lost events. **P8c must use SUSPEND.** Also: with *no* subscriber, `replay = 0`
  discards everything and `tryEmit` returns true under **every** policy, so no counter of any
  design can report "nobody was listening".
- Micrometer 1.14.2 exports `etl_task_runs_total` to Prometheus **verbatim** - the feared
  `_total_total` does not occur. A `Timer` exports as `_count` + `_sum` + a separate `_max` gauge.
- A Micrometer gauge holds its referent **weakly**: a locally-scoped `AtomicLong` read `NaN`
  after GC. P8b must hold it in a field.
- After 500,000 appended rows the DuckDB file was **12,288 bytes** and its WAL held
  **10,416,115**. `diskBytes()` must sum the directory, not the file.

### Open review findings - **updated after P8b; read the P8b entry below for the current list**

Reviewer returned CHANGES REQUIRED. The blocking item and two overclaims were fixed in the P8a
commit. Status of the rest, as of P8b:

- **CLOSED by P8b** - `TaskHooks.names`' live view (M2), hook placement outside the `use` block
  (M3, closed for free by P8b's merged ordering trace: hooks run after `metrics.scratchBytes`,
  which is sampled inside the `use`, so the trace now falsifies "hooks moved inside"), non-scratch
  `materialize`'s 0/0 and `isolate`'s `Exception`-not-`Throwable` catch (M4).
- **STILL OPEN** - `TaskRunner`'s caller-identity pass-through has no test (M1): deleting the `by`
  argument leaves the suite green, and nothing in `SchedulingFixtures` builds an engine with a
  listener. Cut from P8b's scope rather than forgotten.
- **STRUCK, do not do this** - the earlier instruction here was *"inject the same `Clock` into
  `TaskRunner` in P8b"*. **That fix does not work and must not be attempted.** One `Clock` gives
  one time *source*, not one time: `submit` reads it when the trigger arrives and the engine reads
  it when the coroutine is dispatched, with a `limitedParallelism(1)` view queueing between them,
  so the two still differ in production by exactly the queue delay. Equality holds only under a
  frozen test clock, which would have made the test prove same-source while the defect stood.
  Measured alongside: `TaskRunner` has **three** `Instant.now()` reads and the draft moved one,
  which would have left a `RunStatus` able to report `finishedAt` before `startedAt` with no test
  to catch it. The right answer: `RunStatus.startedAt` and `TaskContext.startedAt` measure
  **different things** - submit time and run-start time - and the gap between them is the queue
  delay, which is information. Name them distinctly; do not unify them.

---

## Interstitial - AssertJ replaced by JUnit 5 assertions  (2026-08-27)

Not a phase. Done at the project owner's direction, after they asked why the tests used
`assertThat` rather than JUnit 5. The lead argued against it and was overruled; that is the
owner's call and it is recorded here rather than smuggled in.

### What was argued, and why it lost

Measured side by side on the shape P8a's ordering tests actually assert - a whole event trace.
JUnit 5: `iterable contents differ at index [4], expected: <X> but was: <Y>`. AssertJ prints both
traces plus "some elements were not found" / "others were not expected". For an ordering suite
that is the difference between a five-second and a five-minute diagnosis. The owner's decision
stands regardless; `assertAll` is the genuine win in the other direction and is now used in 168
places.

### Scope and cost

**SimpleEtl only.** 781 call sites, 40 files, +2,631/-1,361. `snapshotcache`'s 649 call sites are
untouched: it is a separate completed project and destabilising it as a side effect was not on the
table. `assertj-core` therefore stays declared in the pom.

**This deliberately violates CLAUDE.md's "never modify a test written by an earlier phase."** Every
phase P1-P8a had its assertions rewritten. The rule exists to stop a new phase weakening old
evidence, so the migration was verified against exactly that risk rather than against green.

### How it was verified - green was explicitly not the bar

309 tests before, 309 after; zero `@Test` lines added or removed; zero production changes. Then
**eleven mutations were re-applied to the migrated suite**: the four that were green before stayed
green, and **all seven that were red before stayed red** (M6-M12). A suite whose assertions had
been quietly blunted would have reported 309/0 just the same.

### Traps caught, each of which would have passed green

- `isEqualByComparingTo(BigDecimal)` -> `assertEquals` would have made **scale** significant, so
  `1.500` stops equalling `1.5`. Rendered as an explicit `compareTo(...) == 0`.
- `Row.columns` is a `Set` and `containsExactly` was asserting **iteration order**; a `setOf`
  comparison would have compiled, passed, and silently deleted that assertion. Rendered as
  `assertEquals(listOf(...), columns.toList())`.
- `ByteArray` needs `assertArrayEquals`; `assertEquals` compares identity and lies both ways.
- `isZero()` on a `Long` must be `assertEquals(0L, ...)`; an `Integer 0` passes the compiler and
  fails at runtime for the wrong reason.
- A bare `assertTrue(list.any { ... })` prints only `expected <true> but was <false>`, which is
  strictly weaker than the AssertJ form. **Every** introduced `assertTrue`/`assertFalse` carries a
  lazy message lambda printing the actual value.

### One change reverted by the lead

`RowPipeFailureTest` used a bare `catchThrowable { }` purely to swallow a failure - the test's
subject is the *second* pipe. The migration turned it into `assertThrows<TargetFailure>`, making
that failure a requirement. Defensible, but a migration may not change what an earlier phase's
test accepts. Reverted to `runCatching`.

### A mutation that was itself wrong

`willRetry = isTransient(failure)` deletes the attempts-exhausted bound, so the retry loop never
terminates and, with a non-sleeping test sleeper, spins forever. It hung the battery rather than
reporting. Replaced with `willRetry = attempt <= step.retries`, which breaks the same clause
without looping, and the harness now applies a per-mutation timeout. Worth recording: a mutation
that hangs looks like an infrastructure problem and is actually a finding about the code.

### A correction to earlier phases' run commands

`-Dtest='!*OracleTest'` **replaces** surefire's default `*Test` include pattern and so re-enables
the five P0 `*Spike` classes, one of which appends 6.2M rows ten times. Earlier phase reports that
used it counted five spike classes as tests: **P8a's real figures are 280 -> 309, not 285 -> 314**.
The correct exclusion is `-Dtest='!*OracleTest,!*Spike'`, now recorded in `SimpleEtl/CLAUDE.md`.

---

## P8b - Metrics, the Micrometer binding, and `ScratchDb.diskBytes()`  (2026-08-27)

Team: engineer + sdet + reviewer. Contract confirmed by all three, who returned **CHANGES REQUIRED
with four blocking items**; revision 2 struck three of the lead's own clauses rather than softening
them. One review cycle after landing. Final: **324 tests, 0 failures** on a clean build. Production
349 lines; tests 1,069. **~1,418 against a stated 1,000 ceiling - the fifth consecutive overrun**,
and the second where the lead set a number two roles had already said was unreachable.

### Delivered

`task/TaskMetrics.kt` (the technology-free seam plus `NONE`), `micrometer/MicrometerTaskMetrics.kt`
(the only class naming `io.micrometer`), `TaskEngine`'s four metric call sites and the `guard`
hoist, `ScratchDb.diskBytes()`, `micrometer-core` at `provided`, and one KDoc `@param` correction
in `Observability.kt`.

Tests: `micrometer/MetricLabelContractTest.kt`, `task/TaskMetricsTest.kt`,
`task/P8aCoverageTest.kt`, three ArchUnit rules including a canary, and additive fixture growth.

### Deviations from the documents

1. **`micrometer-core` is `provided`, not compile.** Spec 2.1 exists so Layer 1 ships to the
   snapshot cache without Layer 2, and Maven has no layer granularity. Verified two ways:
   `dependency:list -DincludeScope=runtime` shows no micrometer, and a throwaway two-module reactor
   resolved zero micrometer artefacts into a consumer while all five jars stayed on the lib's own
   test classpath. The host obligation this creates is in spec 8.6.
2. **`TaskRunner` was removed from the phase mid-contract** - see the struck finding above.
3. **Spec 11.2 amended**: `ScratchDb` gains a fourth method. **Spec 7.2 amended**: the gauge does
   not carry 7.2's spill term. **Spec 8.6 amended**: two new host obligations, and its lede still
   said "Two ... Neither" over a seven-row table.

### Four traps closed by measurement rather than by reasoning

- Micrometer holds a gauge's referent **weakly**: a locally-scoped `AtomicLong` read `NaN` after
  GC, and re-registering an id is **ignored with a WARNING** while the first object stays live. The
  gauge is a strongly held `AtomicLong` per task name, registered **inside** the `computeIfAbsent`
  mapping lambda so no external "did I create it?" branch reopens the window.
- **Timers take milliseconds in and report seconds out.** `record(durationMs, SECONDS)` is a 1000x
  error that passes every name and tag assertion.
- **`Meter.Id.getTags()` returns tags key-sorted** - `[direction, phase, step, task]`, measured.
  An assertion in spec 9.3's table order fails against correct code.
- After 500,000 appended rows the DuckDB file was **12,288 bytes** and its WAL held **10,416,115**,
  so `diskBytes()` sums the directory. No `CHECKPOINT`: it folded the same state fourfold.

### The review found two live mutations the phase shipped without

Both confirmed by the lead - each passed all 324 tests before being closed:

| Mutation | Why it survived |
|---|---|
| `taskEnded`'s timer records `SECONDS` | the unit was asserted on the *step* timer only; `taskEnded` uses a second, separate timer |
| `read` counter fed `rowsWritten` | every fixture had `rowsRead == rowsWritten` (6/6 or 0/0), so a swap was invisible. P8a's own test uses a row-dropping `RowTransform` for exactly this reason **and says so in its KDoc**; P8b did not reuse it |

The second is the one worth remembering: the hazard was documented, in this repo, by the same role,
one phase earlier, and reappeared one layer down anyway.

### The seventh confidently-wrong claim, and it was the lead's

Contract §4 and the shipped KDoc said *"Micrometer never removes a meter."* **False** -
`MeterRegistry.remove(Meter)` and `clear()` both exist and were measured to work. The operational
consequence still holds (*this binding* never removes one, so a renamed task leaves a stale
`etl_scratch_file_bytes{task=old}`), but as written it told a host that a cleanup path they
actually have does not exist. Corrected.

Three further KDoc claims were written as measured with no run on file - the checkpoint fold,
`increment(0.0)` registering, and key-sorted `getTags`. The latter two were measured in the review
round and now say so; the first is labelled as inherited from the P8a spike round.

### A build-hygiene trap worth recording

After reverting an ArchUnit falsification probe, an **incremental** `mvn test` still failed: Kotlin
left a stale top-level facade class in `target/classes` holding the deleted function, and **ArchUnit
reads bytecode, not source**. It reported a violation whose source no longer existed. Every
falsification exercise in this module must run `clean` after reverting, or it chases a phantom.

### Notes for later phases

- **`CacheCopyStep` throws `NotImplementedError`, an `Error`**, which the step loop's
  `catch (Exception)` misses. So under P8b a P9 cache-copy step produces `scratchBytes` and
  `taskEnded(FAILED)` but **no `stepEnded` and no `stepError`**. Consistent with the documented
  `Error` path, but P9 will meet it as "the metric is missing" unless it reads this first.
- **Still open from P8a:** M1, the caller-identity pass-through (above).
- **Not falsifiable by this suite, recorded rather than assumed covered:** §3.4's lambda-vs-value
  distinction on the scratch sample (`RecordingMetrics` throws from the recorder, which both shapes
  guard, and `diskBytes()` cannot throw); the `read`-then-`written` emission *order*; and
  `ScratchDb.close()`'s share of the task duration (the injected clock only advances in the
  sleeper, so `close()` contributes 0 either way).
- **ArchUnit rule 2's two halves and the canary are unproven.** The interstitial's standard is that
  every rule be demonstrated able to fail; only rule 1 has been. The canary in particular - moving
  `MicrometerTaskMetrics` to `infra.etl.task` should turn the canary red while rules 1 and 2 stay
  green - is the one exercise most worth running.

---

## P8c - Coroutine-native event stream: built, then reverted  (2026-08-27)

Team: engineer + sdet + reviewer. Contract took **three revisions**; shipped green at 331 tests and
900 lines; **reverted on the project owner's ruling** the same session. `plan.md`'s P8c entry
carries the full reasoning. Summarised here because this is the entry a later session reads.

### Why it was reverted

The owner's bar was *adopt `SharedFlow` where it is really required, and where it makes the code
more scalable, simpler, more concise and easier to maintain.* The phase failed all four: no
production consumer existed, it was a **second** observation surface parallel to `TaskRunListener`,
it cost 1,231 lines for ~25 lines of mechanism, and its `events` KDoc needed **eleven** "Not
promised" caveats to be usable safely.

**The lead's process error, which matters more than the code.** The reviewer raised exactly this
objection during the P8 contract round - "a second, permanently-parallel public surface for the same
seven call sites, and every future change to a call site made in two places forever" - and the lead
answered it by **deferring the phase into its own slot rather than dropping it**. Deferring an
objection is not answering it. Three contract revisions and four agent rounds then went into
hardening speculative API. The check that would have caught it is one line: *who consumes this?*

### What was measured, and is worth more than the code was

Kept because these are facts about kotlinx-coroutines 1.10.1 that cost real probe time, and the
next person to reach for a flow here will need them:

- **`BufferOverflow.SUSPEND` is the only policy under which a lost event is countable.** Under
  `DROP_OLDEST`, `tryEmit` returned false **0 times in 100** with a wedged collector, so a `dropped`
  counter is structurally always zero.
- **`tryEmit` buys "never suspends", NOT "never blocks".** A collector on `Dispatchers.Unconfined`
  or started `UNDISPATCHED` runs its body **inline on the producer's thread inside `tryEmit`** -
  measured, an emitter did not return for 300 ms while such a collector blocked.
- **`delivered + dropped == emitted` is false** by exactly `extraBufferCapacity`: events sit
  accepted by `tryEmit` but never handed to a wedged collector, counted in neither bucket.
- **`replayCache` is cumulative across runs**, and `resetReplayCache()` is on `MutableSharedFlow`,
  not `SharedFlow`.
- **With no subscriber at all**, `replay = 0` discards everything and `tryEmit` returns true under
  every policy - so no counter of any design can report "nobody was listening".

### Two review findings that died with the code, recorded so they are not rediscovered

- The shipped KDoc's "Not exception-isolated" bullet was **wrong**: a throwing collector on a real
  dispatcher does not reach the *ETL* thread's uncaught handler, and the consequence that actually
  matters - **the throw cancels the collection and the host silently stops receiving events** - was
  absent. Ninth confidently-wrong claim in this project.
- The engineer reported a correction to the contract ("five delegating subtypes should be six").
  The reviewer checked and it was **five**; applying the correction would have introduced a tenth
  wrong claim as the fix for the ninth. Recorded because it is the first time an agent's
  *correction* was the error.

### Still open, inherited by P9

- **P8a M1**: `TaskRunner`'s caller-identity pass-through has no test - deleting the `by` argument
  leaves the suite green. `SchedulingFixtures` builds no engine with a listener.
- `plan.md`'s "P8a, P8b, P8c and P9 all edit `TaskEngine.kt` and are a chain" is stale: P8c edited
  no engine code, and P9's merge surface is P8b's.
- `CacheCopyStep` throws `NotImplementedError`, an `Error`, which the step loop's `catch (Exception)`
  misses - so today it produces `scratchBytes` and `taskEnded(FAILED)` but no `stepEnded` and no
  `stepError`. P9 will meet this as "the metric is missing".


---

## P9 - The `cacheCopy` executor, its YAML form, and rules 19-21  (2026-08-27)

The last phase of the plan, and the reason this framework depends on `snapshotcache` at all. Spec
2.4's task shape D - `cacheCopy` into scratch, `materialize` over it, `pipe` out - is now
executable end to end. `CacheCopyStep` had existed in the model since P5 with a stub throwing
`NotImplementedError`; that stub is gone.

**Delivered**

- NEW `task/CacheBinding.kt` - `(SnapshotCache, GroupId)`. Two fields and not one: a cache serves
  many groups and `copyOut` takes the group, so a name alone conflates the task file's vocabulary
  with the cache's. It is the only file in `infra.etl` that names `infra.snapshotcache`.
- `TaskEngine.caches: Map<String, CacheBinding> = emptyMap()`, appended **last** after `clock`, and
  the `cacheCopy` executor: resolve the binding, reject bound variables, `copyOut(group,
  CopyOutSpec(sql, namer.physical(output, attempt), scratch.connection()))`, `publishTable` on
  success only, `NO_ROWS` returned, lineage logged at INFO.
- `TaskYaml`: `CacheCopyYaml` registered as type id `cacheCopy`.
- `TaskFileLoader.caches: Set<String> = emptySet()` as the **fourth and last** parameter, and rules
  21, 19, 20 plus rule 9 and spec 5.5's identifier check for `output`.
- Eight KDoc sites across four files that named the `NotImplementedError` as current behaviour.

**283 production lines** against a ceiling of 1,000 - the first phase in this project to come in
under its budget, and the reason is that the contract had already been through a confirmation round
that measured the answer instead of reasoning about it.

### Decisions worth the record

- **`StepResult` is 0 / 0, not `rowsCopied`.** `etl_step_rows_total{direction}` is one counter
  series across all five step types, and `TaskEngine.kt`'s own ruling says a field meaning "rows
  piped" for one type and "rows the database says it touched" for another is a number nobody can
  aggregate. `rowsCopied` is **lineage** and goes in the log line beside `generation` and
  `dataAsOf`, which the cache's spec 6.4 obliges a consumer to record.
- **The lease is never this framework's.** `copyOut` acquires and releases it; the engine never
  calls `acquire` or `withSnapshot` and never holds a `Snapshot` across steps (spec 7.3's
  30-minute stall). Each `cacheCopy` may therefore read its own generation - spec 3.6 records that
  and declines `withSnapshot` as the remedy.
- **`targetTable` goes over unquoted.** `DuckDbGenerationStore.copyOut` quotes it itself while
  `materialize` quotes at its own call site, so mirroring `materialize` literally would create a
  table whose name contains the quote characters, which every later step then fails to find.
- **The connection handed over is scratch's write connection, never a `duplicate()`.** The reason
  is resource lifetime, not catalog state: `ScratchDb` records every connection `duplicate()`
  issues in `issued` and releases none until the run ends, so a duplicate here would leak one
  connection per attempt, and there is no concurrent reader that would justify one. `copyOut` runs
  `USE` on whatever connection it is given and restores it in its own `finally`.

  **Not** because a `USE` on a duplicate would strand a later step. `USE` is per connection -
  `DuckDbGenerationStore.connection()` runs it on a *duplicate* of the serving connection precisely
  so that the serving connection keeps its own catalog - so a duplicate would have been safe on that
  count. The first version of the `cacheCopy` KDoc claimed the opposite, was self-refuting, and was
  the tenth confidently-wrong claim in this project. The residual hazard runs the other way: because
  the write connection is the one handed over, a throw from `copyOut`'s inner `USE <home>` restore
  would leave the run's only write connection pointed at a catalog that is then detached.
- **No `waitBudget` is passed.** The cache's `defaultWaitBudget` is the policy; it is also
  unobservable from a test, because Kotlin default arguments make it untestable - a code-review
  item rather than an acceptance criterion.
- **Rules 19 and 20 are startup rules, not runtime ones.** Both could have been a `require` in the
  executor and both would then boot green and kill a task thirty minutes in. The executor keeps its
  own guard for the definitions spec 2.1 lets a host build in code.
- **The YAML default for `cacheCopy.retries` is 0 while `CacheCopyStep.retries` stays 3.** Rule 20
  reads the *stated* value. Had the loader inherited the model's default, every file omitting
  `retries` would have failed rule 20 on a value nobody wrote - caught in the contract round,
  before a line was written.

### The stub was load-bearing, and the coverage moved rather than died

`CacheCopyStep`'s `NotImplementedError` was the **only `throw` of an `Error` anywhere in
production**, and three tests with nothing to do with the snapshot cache stood on it: `run` lets an
`Error` past while `onTaskEnd` still fires from the `finally`, no hook runs while one unwinds, and
`TaskRunner.release(cause)`'s non-null branch records the run FAILED instead of leaving it
`running` for the life of the process. The SDET caught this at contract time. The ruling was that
the permit is **to change the vehicle, not to drop the coverage**: `TaskHookTest` now injects
through `DuckFile.failFirst`, `TaskAdminTriggerTest` through a listener (`Events.isolate` catches
`Exception`, not `Throwable`), and every assertion survived with its meaning intact. Deleting them
would have taken the suite from 324 green to 336 green while covering strictly less, and no test
count would have shown it.

### The cache ArchUnit rule had never constrained anything - now falsified

`grep -rn "infra.snapshotcache" SimpleEtl/src/main` returned nothing before this phase, so
`only task may depend on the snapshot cache` had been green for nine phases with no class in its
scope. Falsified on the interstitial's standard: a real `infra.snapshotcache.api.GroupId` import was
introduced into `infra.etl.pipe.RowPipe`, the suite run, and **that rule and only that rule failed**
(10 architecture tests, 1 failure, naming the rule and its `because`). Reverted, and `clean` run -
ArchUnit reads bytecode, so a reverted probe leaves a phantom violation until the classes go.
The rule does **not** assert that `infra.etl.task` *does* depend on the cache, and needs no canary
for that direction: `CacheBinding` failing to compile is louder than any rule.

### Recorded, not fixed

- **`DuckDbGenerationStore.copyOut` only WARNs if its `DETACH` fails**, leaving the generation
  attached to the *scratch* instance until `ScratchDb.close()` - spec 7.3's reclamation-stall
  hazard arriving by a second route. Not this module's to fix; it belongs to the snapshotcache
  module.
- **The micrometer assertion in `CacheCopyStepTest` breaks a convention this project states.**
  `TaskMetricsTest`'s KDoc says "nothing here knows what Micrometer is", and criterion 8's series
  half imports `MicrometerTaskMetrics` and `SimpleMeterRegistry` into `infra.etl.task`. The lead
  ruled it should move to `micrometer/MetricLabelContractTest.kt`; it has not moved, because doing
  so means editing a test and the phase's rule is that the engineer never does. ArchUnit cannot
  catch it - the class graph is imported `DO_NOT_INCLUDE_TESTS` - which is why it has to be a
  decision rather than a green build.
- **P8a's M1 is still open**: `TaskRunner`'s caller-identity pass-through has no test. P9 did not
  close it, deliberately - it needs a fixture change outside this phase's boundary. `P7World` now
  carries a listener seam, so the change is cheaper than it was.

**Suite: 336 tests, 0 failures** (`mvn -pl SimpleEtl clean test -Dtest='!*OracleTest,!*Spike'`),
up from a 324 baseline; the SDET's twelve. Docker is unavailable, so the three `*OracleTest`
classes still cannot run. Nothing in the contract failed to survive contact with the code.

### P9 - documents amended (missing from the entry above)

Recorded because progress.md is the handover map and the commit message is not:

- **spec 3.6** - the `cacheCopy` YAML schema. Spec 3 defined four step types; the fifth had been
  in the model since P5 with a stub executor and no YAML form.
- **spec 10, rules 19, 20, 21** - no variables in cacheCopy SQL, no stated `retries > 0`, and every
  `cache` name present in the host-supplied set. Rule 20 as first written would have rejected every
  valid cacheCopy file, because the model defaults `retries` to 3; the loader resolves `?: 0` for
  this step type alone and rule 20 tests the *stated* value.
- **spec 11.2** - `CacheBinding`, and `ScratchDb.diskBytes()` from P8b.
- **spec 8.6** - three host obligations, including the honest one: plan P9's "a test asserts the
  generation becomes reclaimable" is **not achievable in this module**, because reclamation lives
  in `DefaultSnapshotCache`, which is `internal` to the cache module.
- **spec 7.3** - the cross-document contradiction with the cache's own spec 6.5 ("share a single
  consumer instance; don't open one per job") against SimpleEtl 7.2's one DuckDB per run, recorded
  as a deliberate deviation rather than left as two specifications that disagree.
- **plan.md P9** - the reclaimability criterion **struck**, with its reason, and replaced by what is
  provable here: the double deletes its generation file inside `copyOut` while a later step still
  reads the copied dataset.

A future session reading only this file would otherwise not learn that the plan's own acceptance
criterion was amended mid-phase.

### The micrometer-placement ruling, withdrawn by the lead

Recorded because it was a live instruction with no owner, and the reviewer was right to say so.

The lead ruled that `CacheCopyStepTest`'s `etl_step_rows_total` assertion should move to
`micrometer/MetricLabelContractTest.kt`, because importing `MicrometerTaskMetrics` into a test in
`infra.etl.task` breaks the convention `TaskMetricsTest`'s KDoc states. The engineer declined to
execute it - correctly, since it is a test edit and the engineer never makes one - so it survived
into the final review as an unexecuted ruling.

**Withdrawn on looking at the code.** Criterion 8 is one behavioural claim about one run: *the
listener and metric seams both see a cache-copy step*. The two assertions read the same
`StepResult` and the same registry from the same `runExpectingSuccess`. Splitting them across two
files would mean two runs each asserting half of one property, which is weaker evidence and more
brittle, in exchange for a naming convention that belongs to a different file. The convention's
purpose - that the engine's own tests do not depend on the metrics binding - is not served by
moving an assertion whose whole subject *is* the metrics binding.

The cost is real and stays: `infra.etl.task` now has one test that knows what Micrometer is, and
ArchUnit cannot see it (`DO_NOT_INCLUDE_TESTS`). A future phase adding a second such import should
treat this entry as the precedent to argue with, not as permission.

The general lesson, and the reason this is written down rather than quietly dropped: a ruling made
from a report is not the same as a ruling made from the code. This one was made from a summary,
and reading the file reversed it.


## Review fix pass 1 - the findings that needed no ruling  (2026-08-27)

Against `docs/simpleetl/code-review-2026-08-27.md`, both passes. This entry covers only the
findings whose fix was a code change inside an existing contract. Everything that needs a spec or
plan decision is listed as outstanding at the end and was deliberately **not** touched: deciding
one unilaterally is how the code and the documents diverge.

Suite: **349 tests green** (`-Dtest='!*OracleTest,!*Spike'`), 336 before, 13 added. No earlier test
was modified or deleted. Each new test was run with the fixes stashed: 10 of the 13 fail without
them, and the 3 that pass are the paired controls that stop a stricter implementation passing by
rejecting everything.

### Fixed

- **H1** `DuckDbTableWriter.validate` compared only the canonical type of a DECIMAL pair, so under
  `REQUIRED` a source wider in scale than the target was accepted at open and rounded away by
  `appendBigDecimal` on every row - the silent-rounding case `ddlType` refuses on the AUTO path.
  Now rejected at open. Only a source declaring a *usable* scale is judged: an unconstrained
  `NUMBER`, a `FLOAT` and every computed expression report scale -127 and state no scale to
  compare, so those stay exactly where they were.
- **M1** `JdbcTableWriter` fixes its INSERT at open from the source's columns, so a transform
  addition that `transform.addColumns` did not declare was bound nowhere and silently took the
  target's database default. Spec 4.4 promises a runtime error for a Row key with no matching
  column; it is now raised against the first chunk, the way `JdbcStatementWriter` already checked
  its bind names. A set difference per row would be work in the innermost loop.
- **M2** `catalogColumns` passed the schema to `getColumns` as a **pattern** and compared only
  `TABLE_NAME` exactly, so `etl_stg.wip` also matched `etl1stg.wip` - which either tripped the
  one-owner check, telling an already-qualified target to qualify itself, or silently supplied the
  wrong schema's column list. `TABLE_SCHEM` is now compared the same way `TABLE_NAME` is.
- **M4** `TaskScheduler.apply` registered cron callbacks before publishing `current`, so a task
  registered a moment before its cron boundary fired into an empty map at startup and was dropped
  in silence - indistinguishable from `fire`'s sanctioned removed-task skip. The definitions are
  published first; the error path restores the previous map along with the previous registrations.
- **M5** `TaskAdmin.trigger` was an unsynchronised check-then-act over a map `reload` replaces. An
  operator could disable a task, reload, be told the reload succeeded, and still watch a concurrent
  trigger launch the old enabled definition. `trigger` is now `@Synchronized` against `reload`; it
  only submits, and the run itself is launched on `TaskRunner`'s dispatcher outside the monitor.
- **M6** Each run resolves its own directory under the scratch root, and `ScratchDb.close` empties
  the directory but leaves it standing - right for a directory its caller owns. Nothing deleted the
  run's own: some 52,000 empty directories a year at a ten-minute cadence, on the volume spec 7.2
  sizes. `TaskEngine` now deletes it after the `use` (`deleteIfExists`, because anything that
  survived inside has already made `close()` throw). The comment claiming `close()` "deletes it on
  every path" meant *empties* all along and now says so.
- **M7** `phases` carries no `# optional` annotation in spec 3.1 but defaulted to the empty list,
  and no rule rejected one. A task with no step scheduled, ran and reported SUCCEEDED every ten
  minutes while its table stopped updating - spec 1.1's 03:00 failure. Zero phases, and a phase
  with zero steps, are now boot errors.
- **M8** Rule 15 is about what the DuckDB 1.1.3 appender can express, and it was applied to every
  `transform.addColumns` entry whatever the target. A nullable DOUBLE on a REQUIRED Oracle target -
  which `JdbcWriters.javaType` binds without complaint - was inexpressible: undeclared it was
  dropped silently (M1), declared it failed startup with a DuckDB-shaped message about a table
  DuckDB never sees. The rule now runs only when the pipe's target is scratch. The type-name check
  stays unconditional.
- **M9** `chunkSize` and `retries` are validated at boot precisely so a bad value is not a failure
  five minutes into a run; `scratch.memoryLimitMb` was not, and `ScratchDb`'s own `require` then
  fired at the first line of every run, forever. Checked at boot alongside the other two.
- **N1** A `cacheCopy`'s SQL is spliced into `CREATE TABLE <output> AS <sql>`, so it must be a
  SELECT. `json_serialize_sql` answers `not implemented` for a parsed non-SELECT and `errorIn`
  discarded that answer, so `copy (...) to ...` and a CTAS loaded clean and then failed on every
  firing, after the run had waited on the cache and taken a lease. `errorIn` gained a `selectOnly`
  flag: on for `cacheCopy`, off for a `sql` step, where DDL is the whole point.
- **N2** JDBI's lexer reads a colon followed by digits as a parameter name. Re-measured here on
  jdbi3-core 3.45.4: `select site_code[1:3] as prefix from wip` yields the name `3` and the rewrite
  `select site_code[1?]`, and `{'k':1}` yields `1`; both parse clean *as written* on duckdb_jdbc
  1.1.3. A cacheCopy's text reaches the cache verbatim through `CopyOutSpec.sql` and never passes
  through JDBI, so an all-digit "name" is punctuation there. Rule 19 and the executor's runtime
  guard now skip them, and rule 6 parses the **raw** text rather than JDBI's `?`-substituted
  rewrite, which was itself producing a syntax error the author never wrote.

  Note what is *not* fixed by this: the same mis-parse in an ordinary scratch `sql`, `source.sql`
  or `materialize` text is genuine. That text does go through JDBI at run time, so the rewrite
  breaks the statement whatever validation says, and rejecting it at boot is the right answer even
  though the message names a variable the author never wrote.
- **L1** The BLOB read released its locator from `.also`, which does not run when `getBytes` throws,
  and truncated silently through `length().toInt()` past 2^31. Now `try`/`finally`, with a range
  check that names step and column.
- **N3, N4** `SimpleEtl/CLAUDE.md`: the closing paragraph still told a future session to prefer a
  `SharedFlow` emitted with `tryEmit` "so telemetry can never back-pressure an ETL run (P8c)" - the
  guarantee P8c's own revert measured away. Inverted, with the measurement. The quick-test command
  said `-Dtest='!*OracleTest'`, which replaces surefire's default include and therefore *runs* the
  spikes, one of which appends 6.2M rows ten times; `!*Spike` is now written out, with the reason.
- **N5** The `copyOut` connection bullet in this file had the withdrawn claim spliced mid-sentence
  into its replacement, plus an orphaned fragment. Rewritten: the reason is connection lifetime
  (`issued` grows one per attempt), not catalog state, and the residual hazard is named.

### Not fixed - each needs a document decision first

- **H2** Rule 12 ("a **step** with a non-scratch target and retries > 0 must declare idempotent")
  is enforced only inside `pipe()`. A `sql` step with a non-scratch datasource and `retries: 3`
  loads clean and re-runs every already-committed statement on a retry. Fixing it means either
  adding `idempotent` to the `sql` and `materialize` YAML - a schema change - or rejecting
  non-scratch retries on those step types outright. Spec 3 and rule 12 have to say which.
- **H3** Validation rule 7 blesses variables in `materialize` SQL, and a non-scratch materialize
  runs `create table <output> as <sql>` through `update()`, which binds every parsed `:name`.
  Oracle rejects bind variables in DDL outright (ORA-01027), and the KDoc's "CTAS accepts bound
  parameters" was measured on duckdb_jdbc only. Either rule 7 loses that blessing for an external
  materialize, or the engine interpolates - and interpolation is the injection surface this engine
  otherwise avoids by binding.
- **H4** A same-datasource pipe holds two connections from one pool: the source handle for the whole
  step, and the writer's from inside `RowPipe.pump`. Two concurrent runs against a pool of two
  deadlock, with no acquisition timeout, and both tasks then skip every later firing as
  AlreadyRunning. Ordered acquisition does **not** fix it - both connections come from the same
  pool, so ordering cannot break the cycle. The remedies are a documented minimum pool size per
  datasource (spec 7.1 states none) or one connection for a same-datasource pipe; the first is a
  document change, the second changes how `RowWriter` is constructed.
- **M3** `RowPipe.pump` transforms before accumulating; spec 5.2 and RowPipe's own KDoc fix the
  order the other way. A selective transform makes one commit span far more source rows than
  `chunkSize`, which lengthens the span a retry must re-read. Either restore the spec order or
  record the deviation with that consequence stated.
- **M10-M12, L2-L10** The duplication and drift findings: datasource-dependent defaults re-derived
  at ten sites, the built-in variable set encoded twice, scratch-ness string-compared at seven,
  named-parameter parsing now in five copies. All still open; none is a behaviour change, and any
  M10 fix must preserve P9's recorded `cacheCopy` retries asymmetry.

## Review fix pass 2 - the four behaviour findings  (2026-08-27)

The findings held back from pass 1 because each changes something a document states.
The document moved first in every case; the commits are one per finding.

Suite: **362 tests green** (`-Dtest='!*OracleTest,!*Spike'`), 349 before, 13 added. No earlier
test was modified. One earlier test in this session's own pass-1 file had to be *adjusted* rather
than modified in spirit - see H2 below - and that is called out rather than buried.

### M3 - the chunk loop order. No ruling needed, and that was the finding.

`RowPipe.pump` applied the transform to each row as it was read, so a chunk filled with
`chunkSize` *surviving* rows. Spec 5.2 and the class's own KDoc both fix the order the other way.
No progress entry sanctioned the deviation, so the rule "documents win unless progress.md records
a deliberate deviation" already decided it - it needed a test, not a decision, and holding it back
in pass 1 was over-caution.

The test was written first and observed `[4, 1]` where the spec order gives `[2, 2, 1]`: ten
source rows, chunk size four, every second row dropped. Both orders read ten and write five, so
the chunk sizes are the only thing that tells them apart.

What the deviation cost was the span of source rows one commit covered. A transform keeping one
row in a thousand turned a chunk size of 5000 into a single commit across five million source
rows: a transient failure four million rows in committed nothing and the retry re-read the whole
span. It also deferred `JdbcStatementWriter`'s first-chunk bind check by the same factor.

New, and pinned by its own test: a chunk the transform empties is **not** written. That could not
arise under the old order - the buffer simply stayed short - and must not arise now, because
`DuckDbTableWriter.write` flushes its appender on every call and an empty write would add a commit
boundary for a chunk with nothing to commit.

### H3 - rule 7 narrowed for a non-scratch materialize.

Spec 10 rule 7 explicitly blessed variables in materialize SQL. A non-scratch materialize runs
`CREATE TABLE <output> AS <sql>` through `Handle.createUpdate`, which binds every `:name` it
parses, and Oracle rejects a bind variable in DDL outright with ORA-01027. The rule was blessing a
step shape that could never run: the file passed every startup check and then failed on every
firing, permanently, since ORA-01027 is not transient.

**Where the wrong belief came from is worth keeping.** The engine's KDoc recorded "DuckDB accepts
bound parameters in CREATE TABLE ... AS SELECT" - true, measured, and about DuckDB. It was then
read as a fact about CTAS. The bullet now carries the correction at the point of the claim, and
the scratch path still binds, which is why the identical step works on scratch and this could only
surface in production.

Interpolating the value textually was the alternative and is rejected in the amendment itself: it
is the injection surface every other statement in this framework avoids by binding, and no quoting
rule for an arbitrary task variable is worth defending at the trust boundary a task file already
is. The author materializes the wider set and filters in a following step.

The loader reports every name JDBI's parser finds, with nothing filtered - unlike a `cacheCopy`,
this text really does go through JDBI, so even the all-digit "name" JDBI reads out of a DuckDB
array slice is a parameter here, and would be rewritten to `?` and broken anyway. That is the
opposite of the N2 ruling one file away, and the two comments each say why.

### H2 - rule 12 enforced as the step-level rule it was always worded as.

Rule 12 reads "a **step** with a non-scratch target and retries > 0", and was checked only inside
`pipe()`. Both other step types that write to an external datasource escaped it.

- **`sql`** gains an `idempotent` field (spec 3.4, and `SqlStep` in spec 11.2), defaulting to
  false. Each statement is its own transaction (spec 5.2), so a retry re-runs the whole list: a
  transient drop between two committed statements re-executed the first, duplicated rows in an
  external table, and the run then reported SUCCEEDED.
- **`materialize`** gets no such field. A non-scratch materialize with `retries > 0` is a startup
  error outright, because a CTAS retry fails deterministically on table-already-exists and
  `idempotent: true` would be a promise no author could keep. Rules 18 and 20 already set the
  precedent: a knob that cannot work is refused, not accepted and ignored.

**`CREATE TABLE IF NOT EXISTS` was raised and rejected, and the reasoning is in the spec** so it
is not re-proposed. It cannot distinguish the table a failed attempt left behind from the table
*last run* produced, and `output` is a fixed name - so the second run of any external materialize
would silently no-op and freeze the downstream table while reporting SUCCEEDED, which trades a
loud failure for spec 1.1's silent staleness. It barely helps within a run either: a CTAS is one
statement, so a part-way failure leaves no table at all and the plain CREATE already succeeds on
retry; the only case where a table survives a failed attempt is one where the CTAS committed and
the failure came after it, and there the data is already correct. Drop-and-recreate would work
mechanically and is refused for authority: spec 5.4 gives this framework no licence to destroy
data outside scratch. The retryable external build is a `sql` step with MERGE or build-and-rename,
which is exactly what this finding made expressible.

Neither new check needs the datasource-dependent retries default: both return early on scratch,
and off scratch spec 5.3 defaults `retries` to 0, so only a stated value can trip them. That
keeps M10's site list from growing while M10 is still open.

**One pass-1 test was adjusted.** `anExternalMaterializeThatBindsNothingStillLoads` moved VALID's
materialize to `report_oracle`, and VALID states `retries: 3` - which rule 12 now refuses. The
file states `retries: 0` instead, and a sibling test asserts the refusal. A second test that used
`errors.single { it.step == "build-summary" }` now selects on the message too, because two rules
legitimately report against that step. Both are the new rule being correct, not a test being bent
to fit; the tests are this session's own, not an earlier phase's.

### H4 - spec 7.1 states the pool contract; the boot check is half of one, on purpose.

A pipe whose source and target name the same datasource holds two connections from that pool at
once. Two runs that each hold one and wait for the second are in a circular wait, and **no
acquisition order can break it** - both connections come from one pool, so ordering, which is the
usual remedy, does not apply. Undersized, both runs hang indefinitely with `busy = true` and every
later firing of either task is skipped as `AlreadyRunning`: the schedule stalls in silence.

Spec 7.1 now states the minimum - 2 x the number of tasks that may concurrently run a
same-datasource pipe step on that datasource - and `TaskAdmin.reload`, which is also the startup
path, computes and logs it per datasource with the tasks responsible.

**It logs rather than validates, and the spec records why.** The left-hand side of the inequality
is a property of the definitions just loaded and is knowable here. The right-hand side is not:
`javap` on jdbi3-core 3.45.4 confirms `Jdbi` exposes neither its `ConnectionFactory` nor its
`DataSource`, so reading the configured pool size means reflecting into a third party's private
fields. That is a worse thing to own than the problem it would detect, and it would rot the first
time JDBI renames a field. Emitting the number an operator must compare against is the honest
half. The work order asked for Agroal/Hikari introspection where available; this is the deviation
from it, and the reason is that there is no supported path from a `Jdbi` to its pool at all.

The arithmetic is an internal function rather than inline in the log call, so a test asserts it
without reading a log appender. Counted per *task*, not per step, because `TaskRunner` admits one
run per task at a time (spec 8.4). Scratch is excluded: its reads take a `ScratchDb.duplicate()`
and its writes the single write connection, so it is not a pool anyone can size.

### Still open after this pass

Only the dedup and drift findings: **M10-M12, L2-L10**. None is a behaviour change. M12 is the one
with teeth - a mispaired connection discipline is spec 7.2's JVM crash rather than a red test - and
M10 must preserve P9's recorded `cacheCopy` retries asymmetry.

## Review fix pass 3 - the dedup refactors  (2026-08-27)

Behaviour preserving throughout, and held to it: **no test was edited to make a refactor pass.**
Suite 362 green after every one of the seven commits, unchanged from the number pass 2 finished
with, which is the point - a dedup that moves a test count has changed behaviour.

The one test file that *is* touched is L8, and it deletes private duplicates without touching an
assertion. That is argued below rather than assumed.

### Landed

- **L3** `precision in 1..38 && scale in 0..precision` appeared verbatim in
  `DuckDbTableWriter.ddlType` and `TaskFileLoader.addColumn`. P6 lifted the rest of rule 15 into
  `unwritableToDuckDb` precisely so startup and writer open decide with the same code; this clause
  was left behind by that lift and now sits beside it as `isDuckDbDecimalPair`. A free function
  taking the pair rather than a method, because the two callers reach it from opposite directions -
  a pair a task file *states*, and a pair result set metadata *reports*. Both messages stay put:
  they explain different mistakes.
- **L2** Both table writers opened with the same set difference and the same sentence about it.
  `requireSourceSubset` now sits beside `catalogColumns`, which both already shared.
- **M10** The retries and createTable defaults were re-derived at ten sites. `defaultRetries` and
  `defaultCreateTable` sit next to `SCRATCH`; a grep for the old expressions finds only the
  helpers' own bodies. **P9's asymmetry is preserved and the helper's KDoc says so**: `CacheCopyStep`
  declares 3 while its YAML default is 0, because rule 20 rejects a stated non-zero value. Neither
  cacheCopy site was touched, and neither now looks like an oversight beside nine that share a
  helper. H2's two new checks also stay inline on purpose - both return early on scratch, and off
  scratch the default is 0, so neither needs the datasource-dependent answer.
- **M11** `BUILT_IN_VARIABLES` and `ATTEMPT_VARIABLE` now live in the task model. The part that
  makes it one source rather than two is `defineRunBuiltIns`, which holds the *values* next to the
  names and checks the two agree: adding a name to the set without giving it a value fails every
  run immediately and by name, instead of at whichever later step first writes it.
- **L4** Five copies of the named-parameter parse, two of them building a throwaway
  `handle.createUpdate(sql)` for a `StatementContext` a colon-prefixed parse never touches - and
  which, on scratch, opens a connection and so creates the scratch file. `parseNamedParameters`
  lives in `infra.etl.pipe`, not beside either caller: ArchUnit forbids `infra.etl.jdbc` from
  depending on `infra.etl.task` and both need it. It takes the parser as an optional argument, so
  the two sites holding a `Handle` still parse with that handle's own configured parser and nothing
  changes about which rules run where. **The messages stayed at the call sites**: each cites a
  different rule and offers a different remedy, so the sentence was never the duplication.
- **L5** One `openConfigured` for the handle open-and-release protocol, `addSuppressed` included.
  It returns the handle rather than assigning it, so a writer whose `open` throws is left with a
  null handle and a closed connection - the same end state by a shorter route. A shared base class
  was the alternative and is not worth a type hierarchy for the three remaining shared lines.
- **L7, the tractable half** `JdbcStatementWriter` pairs each bind name with its declared type once
  at open instead of lowercasing and hashing in the innermost per-row loop. The rest of L7 stays:
  `Row` lowercases on every lookup and this is the caller that makes that load-bearing.
- **L9** The parsed/unparsed map stitch is gone. Two `LinkedHashMap`s plus a third walk joining
  them by name with `unparsed[name] ?: parsed.getValue(name)` was correct only while every name
  landed in exactly one map - an invariant held by the shape of one if/else and by nothing the
  compiler could see. A sealed `ReadFile` with `Parsed` and `Failed` removes the convention rather
  than documenting it.
- **L8** `RowPipeOracleTest` declared private `exec` and `count` byte for byte identical to
  `Pipe.exec` and `Pipe.rowCount` - from its own phase's fixture file, which the class already
  imports for its DuckDB connections. **This edits a P3 test, which the standing rule forbids**, and
  the exemption is narrow and stated: it deletes private helpers and repoints their call sites at
  an identical implementation, touching no assertion, no scenario and no expected value. The rule
  exists to stop a later phase weakening an earlier phase's guarantees, and nothing here does.
  Verified by compilation when the change was made, because the class needs Docker, and by
  execution afterwards - see the closing note.

### Declined, with reasons

- **L6 (prepare once per step rather than per chunk).** Not the small change it looks like. A JDBI
  `PreparedBatch` is executed once by design; per-chunk commit comes from `autoCommit` on
  `executeBatch` rather than from statement lifetime; and ojdbc defers the parse to execution
  anyway, which is why the review measured it as minor against batch-insert I/O. Implicit statement
  caching on the datasource is the better lever and belongs to whoever configures the pool.
- **L10 (one authoritative definitions map behind a lookup).** A ruling, not a refactor. Spec 11.2
  writes `class TaskScheduler(cron: CronScheduler)`, and P7 already recorded widening that to
  `(cron, runner)` as a deviation - noting in terms that a second required parameter breaks the
  declared call. Changing the constructor again to take a `(String) -> TaskDefinition?` is a second
  deviation from a signature the spec does declare, for a finding the review itself rated PLAUSIBLE
  with no reachable failure. It needs the lead, not a refactor pass.

### M12 - stopped and reported, and the premise has weakened

The finding asks for a sealed `Scratch` / `External` type resolved once per step, owning the
connection discipline, to replace seven string comparisons in `TaskEngine`. Reading all seven
before starting, they are not seven of the same thing.

**Four are connection discipline**, and two of those are already the single place:

- `readFrom` - a scratch read takes `duplicate()`, because one DuckDB connection must never carry a
  streaming read and an appender at once.
- `onDatasource` - a scratch statement runs on the single write connection.
- `materialize` - branches to choose between those two plus the parquet path.
- `writer` - and this is the one that breaks the proposed shape. It needs a raw `Connection` for
  the DuckDB appender and a `Jdbi` for `JdbcTableWriter`. **Neither is a `Handle`**, so a sealed
  type exposing `readHandle()` / `statementHandle()` cannot serve it, and adding a third accessor
  re-opens exactly the choice the type was meant to close.

**Three are policy predicates**, not connection discipline, and a sealed type would rename them
rather than remove them: rule 18's runtime half ("is this an un-suffixed scratch table"),
`physicalDataset` ("does this dataset get an attempt suffix"), and rule 11's runtime half ("a
statement target on scratch"). The `init` guard on the reserved name is a fourth of that kind.

So the honest reckoning is that P8 and P9 already did most of M12 when they centralised `readFrom`
and `onDatasource`, and what remains would add a type and an indirection while leaving `writer`
where it is. Net reduction in mispairing risk is close to zero.

That matters because the risk of doing it is real and unchanged: no test pins the pairing, and
spec 7.2 makes a mispaired connection a JVM crash rather than a red test. A refactor whose upside
is a rename is not worth running that on. **Recorded as open pending a ruling** rather than done or
closed: if the lead still wants the type, the design question to settle first is what `writer`
gets from it.

### Closing note: the Oracle suite was run, and the inherited claims are now measured

Every number quoted in passes 1 to 3 above is 362, which is the suite **without Docker** - the
three Testcontainers Oracle classes could not run while the work was being done, so two claims in
pass 3 were made by reading the code rather than by executing it, and both were labelled as such
at the time.

Docker was started afterwards and the full suite run: **382 tests, 0 failures, 0 errors, 0 skipped,
BUILD SUCCESS** (`-Dtest='!*Spike'`, the image `gvenzl/oracle-free:slim-faststart` already cached).
`WriterOracleTest` 7, `RowMapperOracleTest` 9, `RowPipeOracleTest` 4, each about 45 seconds
including its container.

What that converts from inherited to measured:

- **L5.** `WriterOracleTest` holds the leak counters for the *failure* path - `open()` throws and
  the connection must still be closed, with a failing release recorded through `addSuppressed`
  rather than replacing the original failure. That is exactly the protocol L5 moved into
  `openConfigured`, and it is the only place that asserts it. A leaked connection on that path is
  invisible until the pool runs dry, which is the kind of defect a green non-Docker suite would
  have kept quiet about indefinitely.
- **L8.** The repointed call sites now pass as well as compile, so `Pipe.exec` and `Pipe.rowCount`
  really are equivalent to the private helpers they replaced and not merely equivalent-looking.
- **M3, incidentally.** `RowPipeOracleTest`'s chunk-boundary test watches the target row count from
  a *second* Oracle session, so the restored accumulate-transform-write order is confirmed against
  a real commit timeline and not only against `ProbeWriter`'s recorded chunk sizes.

Nothing needed amending: no commit's claims turned out to be wrong. The caveat sentences in the L5
commit message and in `SimpleEtl/CLAUDE.md`'s note about inherited Oracle claims are left as
written - they were true when written, and rewriting a pushed message to look better afterwards is
how a history stops being evidence.

## Design review of `infra.etl.task` - the two shallow seams  (2026-08-29)

Not a phase. A depth review of the whole `task` package - eleven files, 3,262 lines - asking of
each module how much behaviour a caller gets per unit of interface it has to learn. Suite 362
green before the work and 362 green after, without Docker; no test was edited to make a refactor
pass, and the two test files that are touched are repointed at a renamed call, not at a changed
assertion.

Most of the cluster is deep and was left alone. `TaskEngine` puts 893 lines behind `run(...)`,
`TaskFileLoader` 991 behind `load(directory)`, and both fail the deletion test in the right
direction - remove either and retry classification, spec 5.5's attempt-suffix publishing, variable
binding and the twenty-one validation rules reappear in every caller. `Events`, `LoadResult`,
`PipeTarget` and `TriggerResult` are all the shapes they should be. Two places were not.

### Landed

- **`TaskRunner`'s public surface is now the one method spec 11.2 declares.** It shipped four.
  `lastRun` and `outcome` have exactly one caller, `TaskAdmin`, in the same module; `context` has
  no production caller at all and existed so `TaskRunnerCoroutineNameTest` could read the
  `CoroutineName` - an internal seam exposed through the public interface because a test uses it.
  All three are `internal`, which the same-module tests and `TaskAdmin` still reach. This one moves
  *toward* the document rather than away from it: spec 11.2 line 1440 declares `TaskRunner` with
  `submit` and nothing else, and until now the code was wider than the contract.
- **`TaskRunListener` is one method over a sealed `TaskEvent`.** Seven abstract methods with no
  implementation behind any of them meant every implementation paid for the whole set: the no-op,
  the fan-out, the engine's dispatch and two test doubles carried **39 bodies** of pure mechanism
  between them, and an eighth event would have broken all of them. Now 4. `NoOpTaskRunListener`
  becomes `TaskRunListener {}`, `ForwardingListener` becomes one line, and
  `CompositeTaskRunListener` and `TaskEngine.Events` each collapse to a single method.

  Two properties were kept rather than traded. `TaskEvent.site()` renders `"on"` plus the case's
  own name, so an isolation warning still reads `threw from onStepError` and an operator's saved
  search survives. And the compile-time notice `TaskMetrics`' KDoc credits to having no default
  bodies still holds: `RecordingListener`'s `when` is exhaustive with no `else`, so an eighth event
  breaks the build - it is now the implementation's choice rather than the interface's obligation.

  **The measurement, because the first estimate was wrong.** This was proposed as deleting "~21
  bodies"; recounted against the real fixtures before writing, it deletes 35 method bodies and adds
  **16 lines net**, since the sealed hierarchy costs more declaration than seven abstract methods
  did. The win is structural - one call-site pattern, one file to touch when an event is added -
  not size. `TaskListenerOrderTest` is the evidence nothing moved: eleven tests asserting literal
  trace strings, untouched and green.

### The deviation this creates, and the document debt it leaves

**Spec 9.2 (line 1035) and spec 11.2 (line 1470) declare `TaskRunListener` with seven methods, and
the code no longer matches.** That is a deliberate deviation, recorded here, and it is recorded the
wrong way round: the house rule is to update the document first. It was not, because the review was
run with the documents explicitly set aside for it. **Spec 9.2 and 11.2 are now stale and should be
rewritten to the sealed form before the next phase reads them.** Until they are, a session that
follows the rule "documents win" will reintroduce seven methods.

### Declined - and already declined once

The third finding was that `TaskAdmin` and `TaskScheduler` each hold a copy of the current
definition map, with `TaskScheduler` carrying publish-before-register ordering and a rollback purely
to protect its copy. Traced before proposing a fix: `TaskAdmin` *receives* the scheduler, so handing
the scheduler a `(String) -> TaskDefinition?` is a construction cycle, and breaking it means either
inverting ownership - at which point `TaskAdmin`'s documented "the constructor registers no cron"
stops being true - or adding a module. The duplication has no observable defect: `reload` and
`trigger` share one monitor, so no trigger can see the intermediate state.

This is **L10 of review fix pass 3**, re-derived independently and reaching the same answer by a
different route. Pass 3 declined it as needing the lead rather than a refactor pass, on the grounds
that it is a second deviation from a signature the spec does declare. Both reasons still stand.
Left as is, now twice.

---

## Architecture review of `SimpleEtl`, and the M2 plan it produced  (2026-08-29)

A deepening review of the whole module - not a phase. It produced four plan entries (E10 to E13)
and the spec amendments they need, and **no code**. Scope was taken from the commit history:
`TaskEngine.kt` and `TaskFileLoader.kt` are the two most-touched files of the last sixty commits
and carry three of the four findings.

### What it found

- **E10, spec 10's rules are implemented twice.** Seven rules - 7, 8, 11, 12, 13's scratch half,
  19, and rule 6's positional-`?` half - are enforced over `TaskYaml` in the loader and again over
  `TaskDefinition` in the engine, worded independently. M10 and M11 already fixed two instances of
  the drift this causes and wrote down why it is dangerous; the mechanism was still there, seven
  rules wide. `Step.retries: Int?` folds in, because the one thing E10 creates is a single point at
  which a definition becomes runnable.
- **E11, two owners for the live definition set.** See the correction below.
- **E12, spec 5.5's write-then-publish protocol is copied into four executors.** `DatasetNamer`'s
  own KDoc hands the protocol to the caller, so each of `pipe`, `materialize` (twice) and
  `cacheCopy` sequences it by hand and each grew a comment restating the same rule.
- **E13, the retry loop's test surface is JDBC.** Reaching thirty lines of ordering rules needs
  `java.lang.reflect.Proxy` over `Connection`, `Statement` and `ResultSet`. Recorded as droppable.

### Three earlier tests decided the design before any code was written

Each of these was found while checking a proposal, and each killed or reshaped it:

- `TaskListenerOrderTest.aGuardRejectedStepReportsAStepStartAndThenATerminalStepError` pins the
  trace for a guard-rejected step, and its KDoc names the discriminating property outright. It
  **kills** the obvious form of E10 - "validate once at the top of `run`" - which would leave a
  trace with no `onStepStart` in it. `TaskRules` is therefore called *per step, at the guard
  position each `require` occupies today*.
- `DatasetNamerTest` pins `physical`, `parquetPath`, the `../../evil` rejection and attempt 0. So
  E12 **wraps** `DatasetNamer` rather than replacing it.
- `TaskSchedulerApplyTest`'s restore branch was written to survive deleting the restore line, so
  E11 keeps the rollback in the scheduler, where that test does not have to move.

### The correction, because the review's second finding was oversold

The review's E11 card claimed a **correctness gap**: that during a reload the scheduler fires the
new definition while `list()` reports the old. Checked against this file afterwards - too late,
which is the process failure worth recording - the finding had already been declined twice, as L10
of review fix pass 3 and again in the 2026-08-29 design review, on the grounds that "`reload` and
`trigger` share one monitor, so no trigger can see the intermediate state".

**That rebuttal is correct** - both are `@Synchronized` on the same `TaskAdmin`. The window the
review found is real but narrower than claimed: `list()` is not synchronized and `TaskScheduler`
`.fire` reads `current` under no lock, so the skew is `list` against `fire` and it costs an operator
a stale task list for the duration of a cron swap. `TaskRunner.submit` captures the definition by
value, so nothing is corrupted. It is a reporting skew, not a defect.

What E11's entry does answer is the *other* half of both declines - the construction cycle. Making
the registry drive registration leaves the scheduler holding no back-reference, so there is no
cycle rather than a cycle broken cleverly. The entry now states the concession, states the answer,
and says explicitly that declining it a third time is a legitimate outcome.

**Read this before starting E11.** Two sessions have now reached "leave it"; the third proposal is
only worth taking if single-ownership is worth a phase on its own merits.

### Document debt this settles, and what it leaves

Settled: spec 5.3 (`retries` nullable, one resolution point), spec 10 (which rules live where, and
why `TaskRules` being called twice with different SQL parsers is one module and not two), spec 10
rule 20 (the `cacheCopy` retries asymmetry retires with the `Int?` change), spec 5.5 (the publish
protocol as a module), spec 11.2 (`Step.retries`, `TaskScheduler.apply`, and a note on the four
internal modules M2 adds and why none of them is public).

**Nothing left open.** The seven-method `TaskRunListener` debt the 2026-08-29 entry above records
was closed by commit `ab685aa` before this session began: spec 9.2 and 11.2 now show
`fun on(event: TaskEvent)` and the sealed `TaskEvent`. This paragraph first asserted that debt was
still outstanding, copied from the older entry without opening the sections it named. Corrected
rather than deleted, because a stale "the documents are stale" note is precisely the claim a later
session inherits and acts on twice.


---

## E10 - One implementation of the task-shaped rules  (2026-08-29)

`internal class TaskRules` in `infra.etl.task` now holds every rule of spec 10 that is a statement
about a *task* rather than about a *file*, and both `TaskFileLoader` and `TaskEngine` call it.

Eight were genuinely written twice and are now written once: rule 6's positional-`?` half, rule 7,
rule 8, rule 11, rule 13's scratch-only half, rule 18, rule 19, and spec 5.3's "`retries` may not be
negative".

Two more moved *into* the module from the loader alone, and those change what the engine does rather
than only how it words it - **rule 12** in all three of its step-type variants, and **rule 7 as H3
amends it** for a non-scratch `materialize`. Neither had ever been on the run path, so a definition
built in code escaped both: that is precisely the spec 2.1 gap this phase exists to close, and it
is what made one earlier engine test fail until it stated the `idempotent: true` rule 12 has
demanded since H2.

`TaskRules.check(step, defined)` returns `List<RuleViolation>`; `TaskFileLoader` stamps the file
name onto each and files it as a `ValidationError`, and `TaskEngine` throws the first as
`IllegalArgumentException("step '<name>': <message>")`. The rule sentence itself never names the
step, which is what lets the loader keep it in the structured `step` field a report groups by.

`Step.retries` is `Int?`. `TaskRules.retries(step)` resolves an unstated one on the run path.

`TaskRulesParityTest` is the phase's own test: ten cases over eight rules, each breaking one twice - once in
YAML through `TaskFileLoader.load`, once in Kotlin through `TaskEngine.run` - and comparing the two
diagnostics **verbatim** rather than by a shared fragment. Fragment matching would have passed
against the drift M10 and M11 were raised about, because both old wordings named the rule and named
a remedy. Only equality fails when one copy is edited and the other is not.

**375 tests pass** (`mvn -pl SimpleEtl test -Dtest='!*OracleTest,!*Spike'`), 362 of them
pre-existing and unchanged in count, plus this phase's thirteen: ten parity cases, the defaults
test beside them, and the two regression tests the review pass below added.

**The three Testcontainers Oracle classes were run and pass** - 20 tests, `WriterOracleTest`,
`RowMapperOracleTest`, `RowPipeOracleTest`, about 2m20s. Docker was available on the machine this
session ran on, which several earlier entries had assumed it would not be, so their claims about
those classes were inherited from P5 rather than measured. These are measured.

### Four deviations from the plan entry, all recorded in spec and plan

**1. Rule 18 moved as well, making it eight rules and not seven.** The entry listed seven and rule
18 was not among them. It is duplicated on exactly the same terms as the other eight - a statement
about a step, worded twice, with two different remedy sentences - and it reads the resolved
`retries` this phase hands to `TaskRules`, so leaving it out would have had the engine ask the
module for a number and then apply its own copy of a rule to it. Ten lines and one parity case.

**2. The constructor is `TaskRules(parserFor)` and not
`TaskRules(datasources, transforms, hooks, caches)`.** The entry said the wiring should mirror
`TaskFileLoader`'s, and none of the rules that moved consults any of those four collections: the
rules that would are 3, 4, 5 and 21, and all four stay in the loader. `transforms` and `hooks` could
not be used by a per-step interface at all - rule 4 needs the YAML bean *name*, which the model has
already resolved to an object, and rule 5 is task-level. What the module genuinely needs bound at
construction is the `:name` parser, which varies by datasource, so the wiring is
`(String?) -> SqlParser`: `COLON_PREFIX` for the loader, the datasource's own
`SqlStatements.sqlParser` for the engine. Four unused constructor parameters would have been the
literal reading of the entry and a lie about what the module depends on.

**3. `TaskFileLoader` still resolves `retries` as it builds the model.** Spec 5.3 as written said
the default is resolved at exactly one point, which would leave a loaded `TaskDefinition` carrying
null wherever the file omitted `retries`. Three assertions written by P6 and P9 read the resolved
value off a loaded definition and would have failed: `TaskFileLoaderValidTest.theExportStepSurvives`
(`assertEquals(0, step.retries)`), `...theMinimalFileLoadsAndOmittedFieldsTakeTheirDeclaredDefaults`
(`assertEquals(3, step.retries)`), and `CacheCopyLoaderTest`'s cacheCopy default
(`assertEquals(0, step.retries)`). By this phase-group's own ruling the test wins, so the loader
keeps resolving through the same `defaultRetries` the module uses. The two paths therefore cannot
disagree about the value - which is what M10 was about - but a `TaskDefinition` from YAML and one
built in code still represent "unstated" differently. That is the residue; spec 5.3 now says so
instead of promising otherwise.

**4. Rule 20 stayed in the loader.** Spec 10 rule 20's E10 paragraph reads "this rule tests the
stated value on both", which can be read as "on both paths". It is not enforced on the engine path,
deliberately: `CacheCopyStepTest`'s no-retry criterion needs a `cacheCopy` step with `retries = 3`
in play, and an engine that enforced rule 20 would reject the definition that criterion is built
from. The rule is a startup rule about what an author *wrote* in a file, and a definition built in
code has no author's file to read.

### Two earlier-phase tests changed, and why neither is a weakening

Both edits add what a rule asks for and leave every assertion untouched.

- **`CacheCopyStepTest.aCacheWithNoGenerationFailsTheStepImmediatelyAndIsNeverRetried`** now builds
  its step with `retries = 3` instead of leaning on `CacheCopyStep`'s declared default, which this
  phase removes. `assertEquals(3, step.retries)` is unchanged and still guards the same thing: that
  "nothing was retried" is not being asserted about a step that was never allowed a second attempt.
- **`TaskEngineRetryTest.oneStatementTask`** now states `idempotent = true`. Rule 12 has demanded
  that of any step retried off `scratch` since review finding H2, and until this phase only the
  loader enforced it, so a definition built in code could ask for retries on an external datasource
  without ever saying a rerun converges. `Etl.sql` gained an `idempotent` parameter to make it
  sayable. The statement is `create or replace table touched as select 1 as ok`, which does
  converge, so the assertion is true and not merely convenient.

**The second edit is the visible half of what the phase is for.** Every engine test in the suite was
written against an engine that did not enforce rule 12, and exactly one of them turned out to be
running a definition no task file could have contained - a retried `sql` step on `report_oracle`
with no `idempotent` flag. The loader would have refused that file at boot since P6; the engine ran
it happily for four phases. Finding it took running the suite, which is what the phase changes.

### What was measured rather than assumed

- **The guard position.** `TaskRules.check` is called from `TaskEngine.Run.runOnce`, inside the
  attempt loop and below `onStepStart`, which is where the `require(step.retries >= 0)` it replaces
  sat. `TaskListenerOrderTest.aGuardRejectedStepReportsAStepStartAndThenATerminalStepError` passes
  unmodified, so the six-entry trace is byte-identical, and `TaskEngineGuardTest`'s
  `assertEquals(0, mes.attempts.get())` still holds: no rejected step opens a connection.
- **The loader's DuckDB parser is still unreachable from `run`.** Rule 6's syntax check stays in
  `TaskFileLoader`, and `TaskRules` imports nothing that boots one.
- **Error ordering.** Task-shaped errors for a step now arrive after that step's file-shaped ones
  rather than interleaved with them. No test pins an index into the report;
  `TaskFileLoaderDirectoryTest` pins file-name order, which is untouched. The one place order
  mattered semantically is the `cacheCopy` SELECT-only parse, which used to sit behind rule 19's
  early return: it is now gated on the step having no rule violation at all, so a `:name` DuckDB
  cannot parse still reports the binding error that explains it and not a syntax error on top.

### Review pass, and the regression it caught

A `/code-review` over the commit raised six findings. One was a real regression this phase
introduced, one a real narrowing of an existing check, and four were doc accuracy. All are fixed in
the follow-up commit; the two behavioural ones have a test each, and each test was confirmed to fail
against the defect it describes.

**The regression: one broken file-shaped rule hid every task-shaped rule for the same step.** The
loader converts each step to the model so `TaskRules` can judge it, and `toStep` threw at three
points a file-shaped rule discharges *on the loading path* - an unresolved `transform.bean` (rule
4), a target with neither `table` nor `sql` (rule 10), and an `addColumns` type naming no canonical
type (rule 15). The `runCatching { ... }.getOrNull()` that absorbed the throw also dropped every
task-shaped violation for that step, so a `pipe` with a mistyped bean *and* an undefined `:name`
reported only the bean. The author fixes it, reboots, meets the next one. It also regressed the
negative-`retries` check, which before this phase ran over the YAML unconditionally.

`toStep` is total now, and each of the three takes the reading that adds no error of its own: no
target becomes an empty `TableTarget` (a `StatementTarget` would trip rule 11 on scratch, reporting
a `target.sql` the author never wrote), an unresolved bean becomes no transform, an unwritable
column type is dropped. On the loading path none of the three is reachable, because `load` builds
definitions only once the whole directory has validated.

**The narrowing:** `cacheSelectOnly` was gated on the step having *no* violation, where the intent
is "rule 19 did not already reject this same text". So a `cacheCopy` with `retries: -1` and a
non-SELECT `sql` reported only the retries, deferring to the next boot exactly the failure that
check exists to catch early - one that passes every other rule and then dies on every firing, after
the run has waited on the cache and taken a lease. `RuleViolation` gained a `rule: Int?` and the
gate is now `violations.none { it.rule == 19 }`. Matching on the sentence would have made rewording
a diagnostic silently change which parse runs.

### Rule 14 is task-shaped, loader-only, and knowingly left that way

The review found that `TaskRules`'s "the split is not an omission" enumeration was not exhaustive.
Rule 20 is absent on purpose and now says so - it reads what a file *stated*, and a definition built
in code has no file. **Rule 14 is a genuine gap**: `createTable: AUTO` off scratch is a statement
about a step, it is enforced at load only, and a code-built `TableTarget("report_oracle", ...,
CreateTable.AUTO)` boots clean and then gets REQUIRED semantics, because `TaskEngine.writer` hands a
non-scratch table target to `JdbcTableWriter` and never reads `createTable`. That is the
boots-clean-dies-quietly shape `TaskRules` exists to prevent.

It **predates E10** and moving it changes engine behaviour beyond this phase's brief, which named
seven rules. Recorded here and in `TaskRules`'s KDoc rather than fixed in passing - the same
stop-and-report the module CLAUDE.md asks for. It is a two-line addition to `TaskRules.pipe` plus a
parity case whenever a later phase takes it.

### Left for E11 to E13

Unchanged from the M2 entry above, plus rule 14 above. E11 remains the phase whose case is
maintenance rather than correctness, and declining it a third time is still a legitimate outcome.

**Superseded the same day:** E11 was ruled on and declined - see "E11 - the third decline" below.
E12 and E13 stand.


---

## E11 - the third decline  (2026-08-29)

Asked to rule on E11 before building it. **Declined.** No code was written. Spec 11.2, spec 8.6 and
the plan entry are reconciled to what ships.

The finding is that `TaskAdmin.definitions` and `TaskScheduler.current` are two copies of one map,
written in sequence during a reload. It has now been raised three times and declined three times -
as L10 of review fix pass 3, in the 2026-08-29 design review, and here.

### What is actually true about the duplication

Re-derived from the code rather than from the entry. Both fields are `@Volatile`. `trigger` and
`reload` are `@Synchronized` on the same `TaskAdmin`, so they exclude each other. `list()` is not
synchronized and `TaskScheduler.fire` reads `current` from the host's cron thread under no lock.

So the window is real and is wider than "a few instructions": `apply` publishes `current` at its
top and `TaskAdmin` assigns `definitions` only after `apply` returns, which spans every cancel and
every registration - N calls into the host's scheduler. During it, a firing runs the **new**
definition while `list()` reports the **old** one.

What that costs is an operator reading a stale row for the length of a reload. Nothing is
corrupted: `TaskRunner.submit` captures the definition by value, and the rejection path restores
`current` before `TaskAdmin` has touched `definitions`. The entry concedes this, and the concession
is correct. It is a reporting skew.

### Why the entry does not carry the phase

The entry knew it needed a new argument and offered one - that an internal module makes the
construction cycle vanish. Three problems, the first of which is disqualifying:

**1. The contract does not close.** Spec 11.2 declared `class TaskScheduler(cron: CronScheduler)`
with `apply(wanted: Map<String, String>): List<ValidationError>`. That shape cannot fire a task:
`fire(name)` needs a runner to submit to and a definition to submit, and it is handed neither. The
ways to close it are the ways the entry rules out - give the scheduler a `(String) ->
TaskDefinition?` and the registry a scheduler, which is the construction cycle both earlier
declines named, or add constructor parameters spec 11.2 does not declare. The entry's own prose
contradicts itself here: "keeps **no back-reference at all**" three paragraphs above "`fire(name)`
asks the registry". Asking the registry *is* the back-reference.

By the root CLAUDE.md's stop-and-report rule, a fixed contract that does not survive contact with
reality goes back to the document before any code is written. That alone settles this session.

**2. It does not deliver its headline.** "Under one lock, with one rollback" is contradicted by the
same entry's next bullet, which keeps the best-effort restore in `TaskScheduler` deliberately, so
`TaskSchedulerApplyTest` does not move. After E11 there are still two rollbacks in two classes;
what changes is which class holds which.

**3. Its lead "done when" pins the wrong kind of property.** It asks for a test that observes the
swap from both sides and "must fail on the pre-E11 code" - for the reporting skew the entry itself
says should not be sold as a defect. A test that pins a cosmetic property in the shape of a safety
one is how a phase gets justified by a window nobody will ever see, which is the outcome the entry
explicitly warns against.

### What the FOR case really had, and where it went instead

Steelmanned before ruling, because two of the three points above are about the entry's wording
rather than about the finding.

The strongest argument for E11 is one the entry does not make. `TaskAdmin`'s KDoc says "a caller
that supplies `tasks` here calls it itself" - so a host using spec 2.1's programmatic path must
call `TaskAdmin(..., tasks)` **and** `TaskScheduler.apply`, and a host that forgets the second gets
a `list()` full of tasks that never fire, silently. That is not a race lasting a reload; it is a
permanent disagreement, and it is the genuine cost of two copies. One owner would make it
unrepresentable.

It was missing from spec 8.6's host wiring table, which is where every other obligation a library
cannot enforce is written down with its symptom. **It is now a row there.** Making the wrong thing
unrepresentable would be better than documenting it, and that is the honest residual case for a
future E11 - but it is one host obligation among twelve, and it does not on its own buy a public
signature change for zero behaviour change in the module that is most conservative about its
declared surface.

### The document debt this closes

The 2026-08-29 architecture review wrote spec 11.2's `TaskScheduler` block for the post-E11 world
and shipped no code, so from that day until this ruling **the spec described a class that did not
exist** - and, per finding 1, could not have. Declining without touching it would have left the
worst of both: a phase nobody will build and a document a later session would build from.

Reconciled:

- **Spec 11.2** now declares `TaskScheduler(cron: CronScheduler, runner: TaskRunner)` and
  `apply(definitions: List<TaskDefinition>): ValidationReport?`, which is what has shipped since
  P7. That also settles the older `Unit` vs `ValidationReport?` disagreement P7 left unrecorded -
  by writing down what exists, not by changing it.
- **Spec 11.2's "three modules E10 to E13 add"** note is now two. `TaskRegistry` is recorded as not
  being built.
- **Spec 8.6** gains the host row above.
- **The plan's E11 entry** is marked DECLINED with the two contract errors named, and the original
  text kept below the marker as the record of what was proposed.

### If a fourth session revisits this

Do not re-derive the finding; it is correct and it is not the question. The question is whether one
owner for the definition map is worth a public signature change and a new module, given no
correctness gap. Two things would change the answer: a host actually hitting the missing-`apply`
obligation now written into 8.6, or a second reader of the definition map appearing, at which point
"two copies" becomes "three". Neither has happened. A fourth proposal should also close the `fire`
hole in its contract before it is read, since that is what stopped this one.


---

## E12 - The scratch write-then-publish protocol becomes a module  (2026-08-29)

`internal class ScratchDatasets` in `infra.etl.duckdb` owns spec 5.5's sequence - name this
attempt, write into that name, publish the stable view only if the write returned. All four call
sites go through it and none of them names `publishTable` or `publishParquet` any more.

```kotlin
fun <T> attemptTable(dataset: String, attempt: Int, write: (String) -> T): T
fun <T> attemptParquet(dataset: String, attempt: Int, write: (Path) -> T): T
```

**379 tests pass.** `DatasetNamer` is untouched and `DatasetNamerTest` passes unmodified, as does
the end-to-end proof in `TaskEngineRetryTest` that a retry's view resolves to attempt 2. No earlier
phase's test was edited.

### What the module bought, concretely

**"A failed attempt does not publish" is now a test against four lines, not a whole engine run.**
`ScratchDatasetsTest` passes a block that throws; before E12 the only way to reach that rule was to
drive an engine, make real JDBC fail on schedule, and inspect a probe file. The new test also
asserts the failed attempt's *table survives with its rows*, because "did not publish" must not be
satisfied by cleaning up - DuckDB 1.1.3 reclaims nothing and the run directory is deleted whole.

**The generic return earns its place.** Two callers need a value out of the block: `pipe` carries
out the rows it moved and `cacheCopy` the generation it read, both reported after publishing. A
`Unit` block would have forced a `var` beside each call and put the write and its publish back in
two statements.

**No `try`/`catch`.** "Publishes on normal return, does nothing on a throw" is the statement after
the block being unreachable, not a caught-and-rethrown exception. Written down in the KDoc because
the absence is the design.

### The one judgement call

The plan said "`physicalDataset()` and the duplicate decision inside `writer()` both go: a step
that produces no scratch dataset never reaches the module." Half of that is exactly right and half
needed a ruling.

`physicalDataset` is gone. What replaces it, `scratchDataset(target)`, answers the **dataset** name
rather than the physical one - naming an attempt is now the module's half - and returns null for
the steps that produce none. That decision could not move into `ScratchDatasets`: it reads
`TableTarget` and `PipeTarget`, which live in `infra.etl.task`, and an adapter in `infra.etl.duckdb`
never depends on `task`. So the engine keeps "does this step produce a scratch dataset", the module
keeps "what is it called this attempt, and when is it live". That is the seam the ArchUnit rule
already draws, and it is why the module has two entry points instead of one and a `format` flag.

`writer()` keeps its `physical: String?` parameter, and the `?: target.table` fallback with it. The
plan expected both to go. They cannot: a non-scratch `TableTarget` and a `REQUIRED` scratch target
both write under a name the module never sees, so `writer` still has to be told which name to use.
What did go is the *duplication* - the physical name is computed once, by the module, and threaded
in, rather than computed in `physicalDataset` and again defaulted inside `writer`.

`pipe` split into `pipe` (which decides) and `pipeRows` (which moves rows), because the block form
needs the rows half to be callable both inside and outside `attemptTable`.

### Review pass

`/code-review` over the working tree found **no correctness bug in the extraction** - it verified
independently that all four call sites keep write-then-publish order, that a throw still skips the
publish, that `datasetIdentifier`'s name check still fires before any connection work (because
`physical`/`parquetPath` are evaluated before the block, as the old pre-computation did), and that
`scratchDataset` selects the same steps the old `physicalDataset` did.

One finding was mine and is fixed: `aFailedParquetAttemptPublishesNothing` promised "no view, and
no half-published file name" in its KDoc but asserted only the view, so the parquet path had no
equivalent of the table test's "the failed attempt's rows stay where they are". It now asserts
`summary__a1.parquet` still exists, and is renamed for what it checks. Without it the test passed
against an implementation that cleaned up on failure - which spec 5.5 forbids.

The review's other two findings are in `snapshotarchive` test fixtures belonging to a parallel
session, which were uncommitted in the working tree when the review ran. Not this phase's, and not
touched.

### Documents

**None changed, and that is worth recording.** Spec 5.5's E12 paragraph and spec 11.2's
`ScratchDatasets` note were both written by the 2026-08-29 architecture review ahead of the code,
and the code now matches them - `attemptTable`/`attemptParquet`, publish on normal return only, two
entry points because `MaterializeFormat` lives in `task`. That is the opposite outcome to E11,
where the same review's anticipatory spec text described a shape that could not work; here it
described one that did.

---

## E13 - dropped  (2026-08-29)

Asked to decide, before starting, whether the attempt loop's ordering rules are assertable through
the existing listener seam. **They are, and two of the three are already asserted verbatim.** E13 is
dropped. No code was written.

The entry marked this phase droppable itself: "If the ordering turns out to be assertable through
the existing listener seam - which already sees every event in order - then E13 is not worth its own
phase, and saying so is a better outcome than building it."

### The seam is not the listener alone, and that is why it works

The entry's own framing understates what exists. Ordering rule 1 is "metric before the listener
describing the same moment", and **no listener can see that** - it sees listener calls. What can see
it is `EventTrace`, which the P8b fixture already feeds from the listener, the metrics recorder and
the hooks into **one ordered list**. Its KDoc says why it was built that way, and it is the same
reason E13 exists: "spec 9.3's ordering clauses are all *relative* ones ... Two separate recordings
could express neither."

Against E13's three rules:

- **Metric before the listener for the same moment.** `TaskMetricsTest.everyMetricIsOrderedBefore-`
  `TheListenerCallForTheSameMoment` asserts one exact 14-entry list, interleaving
  `metric.stepRetried` / `onStepError(willRetry=true)` twice, then `metric.stepEnded` /
  `onStepEnd`, then `metric.scratchBytes` / hook / `metric.taskEnded` / `onTaskEnd`. Any reordering
  fails it.
- **A terminal failure metered as a step that *ended*, rows 0/0.**
  `aTerminalStepFailureIsMeteredWithNoRowsAndKeepsTheSameOrder` asserts a 12-entry list carrying
  `metric.stepEnded(..., attempt=2, read=0, written=0)` *before* `onStepError(willRetry=false)`,
  and separately `assertEquals(StepResult(0, 0, 2_000, 2), metrics.result("load-wip"))`. That is the
  rule, exactly, including the whole-step duration.
- **`willRetry` decided before the backoff.** The only one not in a single interleaved list, because
  the harness sleeper records into `delaysMillis` rather than into the trace. It is still pinned
  from two sides - `TaskEngineRetryTest` asserts the backoff schedule
  (`[2000, 4000]`, and `[2000, 4000, 8000, 16000, 30000, 30000]`), and the injected clock the
  sleeper advances is what makes the terminal test's `durationMs = 2_000` exact. Closing the last
  gap costs **one fixture line** - have the sleeper write `sleep(n)` into the trace - and **no
  production change at all**. A seam is not the cheapest way to buy a line.

### What dropping it costs, stated plainly

The entry's second argument was reach, not coverage: five of the seven failure injections use
`afterRows = 0`, so they fail at execution rather than mid-stream and "should stop needing a driver
at all". That is true, and those five tests stay slower and more indirect than they need to be.

It is not worth a seam, for three reasons:

1. **The `Proxy` machinery does not go away.** E13's own contract keeps it for the two mid-stream
   cases and for classification, so the phase deletes nothing - it reduces how often the existing
   fixture is used.
2. **It adds a test layer rather than replacing one.** Testing the policy through two recorders and
   a lambda proves the policy orders its calls correctly. It does not prove `TaskEngine` drives the
   policy correctly, so the engine-level trace tests above still have to exist - E13's own done-when
   says every existing `TaskEngineRetryTest` assertion must still pass. Net: one more place the same
   guarantee is asserted.
3. **It would put the loop's wiring behind a fake.** E13 correctly refuses to move `isTransient`
   behind the seam, because "putting the one part that needs a real exception behind a fake is how a
   green test covers a production failure". The ordering rules sit one line from `isTransient` in
   the same loop; the argument does not stop being true for its neighbours.

### If a later session revisits this

The trigger would be the loop growing a rule the trace cannot express - something decided but never
reported, or an ordering between two things neither seam observes. Nothing in spec 5.3 or 9.3 asks
for that today. If someone wants the five `afterRows = 0` tests cheaper, the small move is a fake
step executor at the `dispatch` boundary, not a policy object owning the call order.

**M2 is now complete: E10 built, E11 declined, E12 built, E13 dropped.**


---

## E14 - Adoption hardening  (2026-08-30)

A fresh agent built a host module against the public documents alone and reported twelve findings.
An adjudication pass rejected six of them against rulings already recorded here - an `etlHost(...)`
factory (P7 deviation 1), a step timeout (spec 3.6 states its absence as the design), two new
metrics (9.3's six are closed), a driver row count for `materialize`, and anything moving ownership
of the definition map (E11, declined three times). **The rejections are the more useful half of this
entry**: every one of them was a plausible-sounding fix that a reader of the code alone would have
built, and each is already argued against above. Four findings survived, and this phase is those
four and nothing else.

Two are documentation, two are code. Together they are the smallest change that makes a host's
first day match what the documents promise.

### 1. Spec 11.2 gains the three missing constructors (documentation)

11.2 calls itself "the frozen contract ... everything not listed is internal and free to change",
and declared **no constructor** for `TaskEngine`, `TaskAdmin` or `TaskRunner` - while a host has to
construct all three. P7 recorded this (deviation 3, above: "`TaskAdmin(runner, scheduler, loader,
tasks)`, which 11.2 gives no constructor at all") and it stayed open through six phases. The E11
ruling closed the `TaskScheduler` and `TaskFileLoader` half of it by writing down what ships; this
closes the rest the same way.

The three are now declared exactly as they compile today - `TaskEngine`'s nine parameters with
their defaults and the reason `caches` is last, `TaskRunner(engine)`, and
`TaskAdmin(runner, scheduler, loader, tasks = emptyList())`. **Writing down what exists, not
changing it**: no signature moved, and no test needed to.

**The P7 deviation is closed.** What remains open from P7 is deviation 4's defaulted `runId` on
`TaskEngine.run`, deliberately left: 11.2 declares `run` as the two-parameter call a host makes,
the third is internal to the runner, and declaring it would widen the frozen surface rather than
record it.

### 2. `reportPoolMinimums` now fires on the programmatic path too (code, 2 lines)

Spec 7.1 promises the pool minimum is logged "at startup **and** on every reload". It was called
only from `reload`. For the file-driven host that conforms exactly, because `reload` *is* the
startup path (8.5, and `TaskAdmin`'s own KDoc). For a host using the `tasks =` constructor
parameter - 2.1's programmatically built definitions, which have no directory to read - `reload` is
never called and the number never appeared. A code-vs-document conformance gap, not a design
change: `init` now calls `reportPoolMinimums(tasks)` when `tasks` is non-empty.

**Left at INFO, deliberately.** Raising it to WARN was considered and rejected on the reasoning
already in `TaskAdmin.reportPoolMinimums`: the framework knows the required minimum and cannot know
the configured pool size, so a severity implying a fault would fire identically on a correctly
sized configuration. A warning that is right half the time and unfixable the other half is how an
operator learns to filter the channel.

### 3. `TaskStatus.scheduled` (code, ~10 lines + `TaskAdminScheduledTest`)

Spec 8.6 already carries the host obligation "call `TaskScheduler.apply` yourself when you build
definitions in code", with the symptom "`list()` reports every task and not one of them ever fires,
**with no error raised**". It was the only obligation in that table whose evidence is already inside
this process and was still being stated as prose.

- `TaskScheduler.registeredNames(): Set<String>`, `internal` and `@Synchronized`. The
  synchronisation is not decorative: `registrations` is a plain `LinkedHashMap` published under
  `apply`'s monitor alone, so an unsynchronised read from an HTTP worker thread is a data race and
  not merely a stale answer.
- `TaskStatus` gains `scheduled: Boolean`, populated in `list()`.
- `list()` logs one WARN naming registrations no definition backs - the mirror direction, which no
  `TaskStatus` field can carry because such a task cannot appear in the listing at all.
- `list()` now reads the `@Volatile definitions` once into a local. Re-reading it per row let a
  concurrent `reload` serve a listing assembled from two different definition sets.

**This is not E11, and the difference is exact.** `registeredNames` reads `registrations` and never
`current`. E11's fourth-session tripwire, written above, is "a second reader of the definition map
appearing, at which point two copies becomes three" - and this reads the *registration* map, which
answers a question the definition map cannot: whether a name is actually wired to the host's
scheduler. Nothing here moves ownership, adds a copy, or changes a constructor. The obligation is
unchanged; only its silence is.

`TaskStatus` is **not named in 11.2's frozen list** - it never has been, and `TaskAdminReloadTest`'s
KDoc says so in as many words ("`TaskStatus`'s shape is the engineer's"). Adding a field therefore
changes no frozen contract. It is a `data class` with exactly one construction site in the module,
so `scheduled` sits beside `cron`, where it reads, rather than being appended to protect positional
callers that do not exist.

The test's oracle is `cron` versus `scheduled` **on one row**, not `scheduled` against a constant:
`aDefinitionWithACronThatWasNeverRegisteredIsListedAsNotScheduled` and
`scheduledIsTrueOnceTheHostCallsApply` build the identical definition and differ only in whether
`apply` was called, so neither passes for a hard-wired field. A third pins the API-only task
(`cron == null`) as the normal false, which is why 8.6's symptom is stated as a *non-null* cron with
`scheduled = false`. The fourth asserts the input the WARN is computed from and not the log line -
installing an appender would be asserting on JBoss Logging, which is the same choice
`sameDatasourcePipeUsers` was split out of `reportPoolMinimums` to avoid.

### 4. Spec 8.6 gains four host rows (documentation)

Each is an obligation a host can miss with no failure and no log line, found by actually wiring one:

- **Pass a `TaskRunListener`.** 9.2 specifies the no-op default, and the table had a row for the
  metrics binding and none for the listener. Unattached, a 30 minute run emits nothing at all -
  every call site fires and every one of them calls the no-op.
- **Pass `TaskFileLoader` all four name sets.** All four parameters default to empty, so bare
  `TaskFileLoader()` compiles - it is what the fixtures use - and rules 3, 4, 5 and 21 then pass
  vacuously for every name in every file. The hooks and caches rows already stated two instances of
  this; the general form was missing.
- **Configure a JDBC statement timeout per datasource.** The framework has none and that is the
  design (3.6). A wedged driver call parks the task's confined dispatcher permanently, leaving it
  `busy` forever with every later firing skipped as `AlreadyRunning` - the same silent stall 7.1
  describes for the pool deadlock. `SQLTimeoutException` is already in 5.3's transient set, so a
  host-side timeout retries rather than merely failing.
- **Alert on the absence of a metric series, not on a zero counter.** 9.3's meters are registered
  when a run first touches them, so a task that has never run in this process has no series at all
  and `runs_total{outcome="succeeded"} == 0` never fires for the task that stopped being scheduled.
  Normal after every deploy, not an edge case.

The `TaskScheduler.apply` row is also amended to record that E14 made its disagreement observable.

### Deviations from the documents

None. All four items either write down what already ships (1, 4), make code match a sentence the
spec already carried (2), or add to a type 11.2 does not freeze (3). Item 2 is the only behaviour
change, and it is one INFO log line on a path that previously produced none.

### Where a contract met reality

**One, and it did not need changing.** Spec 7.1's "at startup and on every reload" reads as two call
sites and there was one. The document turned out to be right and the code wrong - the rarer
direction - so the code moved. Had the sentence been the wrong one, the fix would have been to
narrow it to "on every reload" and record that the programmatic path is unserved; that alternative
was rejected because the number is exactly as knowable on one path as the other, and 2.1 makes the
programmatic path a first-class consumer rather than a convenience.

### If a later session revisits this

The remaining rows of 8.6 are still untested anywhere in this repository, which P7 recorded as a
real gap and E14 does not close - it only shortens the list of obligations whose breach is
invisible. The move that would close it is a host module in this repository, and P7 deviation 1
explains why that has not been built here. `scheduled` is the pattern worth reusing if another
obligation turns out to have in-process evidence: report the disagreement where the operator
already looks, rather than adding a check the framework cannot honestly make.


---

## E15a - Build hygiene  (2026-08-30)

Four fixes to how the module builds and tests, from the maintainer review of adoption findings.
The patch was authored by a build agent and reviewed at maintainer depth; two defects were found
in review and fixed before merge - both recorded below, because a review that finds nothing is
indistinguishable from a review that looked at nothing.

### 1. The JDK 17 test-JVM trap

`pom.xml` targets Java 17 as a consumer-compatibility promise, but surefire injected
`-XX:+EnableDynamicAgentLoading` unconditionally - a flag that exists only from JDK 21, so a
developer on the advertised JDK 17 got "Unrecognized VM option" the moment tests started. The
parent pom targets 21, which is why no CI run ever saw it. The argLine now lives in a
`dynamic-agent-loading` profile keyed on `<jdk>[21,)</jdk>`; `help:active-profiles` confirms it
activates here. Maven merges profile plugin config with the base, so `excludedGroups` (base) and
`argLine` (profile) both apply - verified, not assumed.

### 2. `provided` -> `optional` for micrometer-core

`provided` claimed a container supplies the jar at runtime; nothing does. `<optional>true</optional>`
buys the identical non-transitivity - P8b's two-module-reactor measurement still stands and the
comment still cites it - with honest semantics. Spec 8.6's micrometer row updated in the same
commit, because documents and code must agree.

### 3. Tags replace the naming-convention exclusion

Spikes dodged execution only because `*Spike` misses surefire's default include pattern, and any
`-Dtest=` destroyed that - CLAUDE.md documented a 6.2M-rows-times-ten spike as a hazard to step
around rather than defusing it. Now: `@Tag("spike")` on the five spikes, `@Tag("oracle")` on the
three Testcontainers classes, `<excludedGroups>spike,oracle</excludedGroups>` as the default.
Plain `mvn test` runs 385 with zero spikes and zero containers; `-Dgroups=oracle` /
`-Dgroups=spike` opt back in deliberately and survive any `-Dtest=` filter. The annotations add
no assertion and change no body - the only edits to earlier phases' test files are one import and
one annotation line each, recorded here per the never-touch-earlier-tests rule. CLAUDE.md's
Commands section rewritten: a warning about a defused hazard is future confusion.

### 4. The non-suspend invariant is now enforced, not narrated

CLAUDE.md's concurrency idiom records that no frame in `run -> execute -> pipe ->
ScratchDb.connection()` is `suspend` - the fact that makes a single DuckDB connection safe under
`synchronized` (spec 7.2: two threads on one connection crash the JVM) and makes `Mutex`
unreachable. That constraint lived in prose and was enforced by nothing. Two ArchUnit rules now
ban any dependency on `kotlin.coroutines.Continuation` - how `suspend fun` compiles - in
`infra.etl.duckdb..` and in `TaskEngine` and its nested classes.

**The review caught a real hole in the submitted rule.** As authored it matched
`haveSimpleName("TaskEngine")` - but the crash path runs through the *inner* class
(`TaskEngine$Run.execute -> pipe`), whose simple name is `Run`. Mutation-tested: a
`suspend fun` planted on `Run` sailed through the submitted rule and fails the merged one
(`haveNameMatching` on the FQN with a `$`-suffix alternative). Both rules were then proven by
mutation - a suspend fun planted in `ScratchDb` and in `Run` each fails exactly its rule, and
both probes were removed. A guard nothing has ever seen fire is indistinguishable from a guard
that does not work, which is this module's own P3 lesson applied to its own tests.

### Suite

385 green (383 + the two ArchUnit rules) via plain `mvn test` - the exclusion incantation is
gone. The Oracle tag path was exercised by the authoring agent against live Docker; the spike tag
path is excluded by the same mechanism and was verified by selection, not by running the 62M-row
spike.
