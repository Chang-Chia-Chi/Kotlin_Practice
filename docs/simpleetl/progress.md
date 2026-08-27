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

