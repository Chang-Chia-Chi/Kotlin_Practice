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
