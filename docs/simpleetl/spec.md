# YAML-Driven ETL Framework - Specification

Stack: Kotlin, Quarkus, JDBI, Oracle, DuckDB (duckdb_jdbc 1.1.3, file mode)
Build: Maven

---

## 1. Goals and Non-Goals

### 1.1 Goals

- Define an ETL task entirely in one YAML file. All task files share one identical schema.
- Move data between JDBC datasources, optionally using DuckDB as a working area.
- Express transformation as SQL executed inside a database engine, not as application code.
- Make failure behaviour explicit and safe by default.
- Fail at application startup, not at 03:00, when a task file is wrong.
- Share the row-moving engine with the snapshot cache instead of duplicating it.

### 1.2 Non-Goals

- No DAG. Phases and steps are strictly sequential.
- No distributed execution. Single pod, single JVM.
- No transaction spanning chunks, steps, or phases. See 5.4.
- No CDC or watermark semantics built in. A task needing a watermark uses an `export`
  step plus its own bookkeeping table.
- No UI for authoring.

### 1.3 Design Rules Applied Throughout

- The framework never guesses a type conversion. Ambiguity is a startup or runtime error.
- The framework never reuses or deletes a DuckDB table. See 5.5.
- Defaults are the safe choice. Unsafe behaviour must be written explicitly in YAML.

### 1.4 Version Constraint

`duckdb_jdbc` is pinned to 1.1.3. The CI environment provides a glibc older than 2.23,
which the native library in 1.4.x and 1.5.x requires; both fail to load with
`/lib64/libm.so.6: version 'GLIBC_2.23' not found`. This pin is a known constraint, not a
preference: 1.1.3 is from November 2024 and receives no updates. Upgrading the CI and
runtime base image is tracked separately; section 13 lists what becomes available when it
happens.

---

## 2. Structure

### 2.1 Two Layers

**Layer 1 - RowPipe.** Moves rows from a JDBC source into a target, chunked and typed.
Knows nothing about YAML, phases, scheduling, retry policy, or generations. Its only
inputs are a source query, a target, and a chunk size.

**Layer 2 - Task engine.** YAML loading and validation, phase and step sequencing, retry,
variables, scratch lifecycle, scheduling, the admin API, hooks, and listeners. The `pipe`
step is implemented by constructing a RowPipe and running it.

The split exists because the snapshot cache needs Layer 1 and not Layer 2. Its
`GenerationSource` seam hands the caller a write `Connection` to a candidate generation
file and asks it to populate the file; a RowPipe is exactly that, so the cache reuses the
row-moving, type-mapping, and appender code without inheriting the task model. See 9.5.

`TaskDefinition` is a public, programmatically constructible type. YAML is one source of
`TaskDefinition`, not the only one, so a caller that wants Layer 2 without YAML files can
build a definition in code.

Scheduling and the admin API are Layer 2 logic and live here. Their two *adapters* do not: a
cron registration binding and an HTTP resource. This follows 7.1 and 9.1, where Quarkus
datasources and CDI transforms are also named by this document and arrive as plain data.

### 2.2 Concept Model

```
Task            one YAML file, one schedule, one run at a time
  Phase         ordered group of steps, for grouping and observability
    Step        the unit of work, retry, and logging
```

A phase is an ordered, named group of steps. It has no transactional meaning and no
concurrency meaning. Its purpose is grouping in logs and metrics so a 10-step task reads
as "extract / build / publish" rather than as a flat list.

### 2.3 Step Types

Four step types. Each has a fixed field set, which is what keeps task files structurally
identical.

| Type | Purpose | Reads | Writes |
|---|---|---|---|
| `pipe` | move rows between datasources | one datasource via SQL | one datasource |
| `materialize` | compute a derived dataset inside one datasource | one datasource via SQL | a table or parquet file in that datasource |
| `sql` | run statements with no dataset output | one datasource | side effects only |
| `export` | produce task variables | one datasource, or literals | task variable scope |

`pipe` is the only step where rows pass through the JVM. Everything else executes inside
the database engine.

### 2.4 Supported Task Shapes

DuckDB is optional. All of these are first class:

```
A. Oracle to Oracle, no DuckDB
   pipe  oracle_mes -> report_oracle.wip_summary

B. Oracle staged through DuckDB
   pipe         oracle_mes -> scratch.wip_stg
   pipe         oracle_mes -> scratch.lot_stg
   materialize  scratch    -> scratch.summary
   pipe         scratch    -> report_oracle.wip_summary

C. Several external outputs
   ... build in scratch ...
   pipe  scratch -> report_oracle.wip_summary
   pipe  scratch -> other_oracle.wip_daily

D. Reading the snapshot cache
   cacheCopy    wip_cache  -> scratch.wip_cache   (file-to-file, see 7.3)
   materialize  scratch    -> scratch.summary
   pipe         scratch    -> report_oracle.wip_summary
```

In shape A no scratch file is created. The scratch DuckDB instance is created lazily on
first reference to the `scratch` datasource, so a task that never mentions it pays nothing.

---

## 3. YAML Schema

### 3.1 Task

```yaml
name: wip-summary                 # required, unique, [a-z0-9-]{1,64}
description: "..."                # optional
enabled: true                     # optional, default true
schedule:
  cron: "0 */10 * * * ?"          # optional; omit for API-triggered-only tasks
logging: true                     # optional, default true, see 9.2
chunkSize: 5000                   # optional, task-level default, default 5000
scratch:
  memoryLimitMb: 4096             # optional, default from application config
onSuccess: notify-downstream      # optional, hook name, see 9.4
onFailure: null                   # optional, hook name
vars:                             # optional, literal task variables
  - name: siteCode
    value: "F12"
phases:
  - name: extract
    steps: [ ... ]
```

### 3.2 Step: pipe

```yaml
- name: load-wip
  type: pipe
  chunkSize: 20000                # optional, overrides the task-level default
  source:
    datasource: oracle_mes
    sql: >
      select lot_id, cast(qty as number(18,3)) as qty, upd_ts
      from wip
      where upd_ts > :lastTs
  transform:                      # optional, see 9.1
    bean: wipEnricher
    addColumns:                   # required when the transform adds columns and
      - name: row_hash            # the target uses createTable AUTO
        type: VARCHAR
  target:
    datasource: scratch
    # --- form 1: declarative table ---
    table: wip_stg
    createTable: AUTO             # AUTO | REQUIRED
    # --- form 2: statement, non-DuckDB targets only ---
    # sql: >
    #   merge into ...
    # idempotent: true
  retries: 3                      # default 3 for a scratch target, 0 otherwise
```

Exactly one of `target.table` or `target.sql` must be present.

### 3.3 Step: materialize

```yaml
- name: build-summary
  type: materialize
  datasource: scratch
  output: summary                 # dataset name referenced by later steps
  format: TABLE                   # TABLE | PARQUET
  sql: >
    select w.lot_id, sum(w.qty) as qty, l.product
    from wip_stg w join lot_stg l using (lot_id)
    group by 1, 3
  retries: 3
```

### 3.4 Step: sql

```yaml
- name: index-staging
  type: sql
  datasource: scratch
  statements:
    - "create index idx_wip_lot on wip_stg (lot_id)"
  retries: 3
```

### 3.5 Step: export

```yaml
- name: read-watermark
  type: export
  datasource: oracle_mes
  vars:
    - name: lastTs
      sql: "select max(processed_ts) from etl_watermark where task_name = :taskName"
```

### 3.6 Step: cacheCopy

Added by the P9 amendment. Spec 3 defined four step types; the fifth existed in the model from P5
with a stub executor and no YAML form.

```yaml
- name: copy-wip
  type: cacheCopy
  cache: wip_cache          # a name the host binds to (SnapshotCache, GroupId) - see 8.6
  sql: select lot_id, qty from wip where site = 'F12'
  output: wip_cache         # a scratch dataset; gets 5.5's attempt suffix and stable view
  retries: 0                # see below - a non-zero value is rejected
```

`sql` runs **inside the cache's own DuckDB instance**, against the attached generation, and its
result is materialised into scratch by `CREATE TABLE ... AS SELECT`. No row passes through the
JVM (7.3).

**`sql` may not bind a variable.** `CopyOutSpec.sql` is a plain `String` with no binding channel,
and interpolating a task variable into it would be the injection path 6.3 refuses everywhere else.
A task needing a variable copies the wider subset and filters in the following `materialize` step.
Enforced at startup by rule 19, so it cannot die thirty minutes into a run.

**`retries` is dead on this step type and a non-zero value is rejected** (rule 20). 5.3's retry
classification is JDBC-shaped - `SQLTransientException`, `SQLRecoverableException`,
`SQLTimeoutException`, SQLState `08` - and the cache's `copyOut` reaches a local DuckDB file
through raw JDBC, so none of those ever arrives. `NotReadyException` and `ShuttingDownException`
are plain `RuntimeException`s and are deliberately not added to the classification: the waiting
mechanism is `copyOut`'s own `waitBudget`, which the framework does not override. Accepting a
`retries` that can never fire would be a knob that lies; rules 12 and 18 already set the precedent
of rejecting such a combination loudly.

**Each `cacheCopy` step takes its own lease and may therefore read its own generation.** Two such
steps against one group are not guaranteed to agree - the snapshot cache's own spec 6.4 calls that
a torn read and prescribes `withSnapshot()` to pin one generation across a round. **This framework
deliberately declines that remedy**: 7.3's operational constraint says a task holding a lease for
30 minutes stalls cache refreshing entirely, with the cause in a different system from the symptom,
and a run-scoped lease has nowhere to live across a failed step. A task needing two mutually
consistent cache reads expresses them as one `sql` in one step. Recorded rather than left for a
consumer to discover.

**The step blocks for the cache's `defaultWaitBudget`** while no generation is available. That
happens on the task's own confined dispatcher thread (8.3), and this framework has no step timeout
anywhere - so the budget is the cache's policy and the task's whole latency floor.

---

## 4. Type Contract

### 4.1 Canonical Types

A `Row` value is always one of:

```
String, Boolean, Long, BigDecimal, Double,
LocalDate, LocalDateTime, Instant, ByteArray, null
```

### 4.2 Row

```kotlin
class Row internal constructor(private val values: LinkedHashMap<String, Any?>) {

    val columns: Set<String>                    // lower case, source order

    operator fun get(name: String): Any?        // null if absent or SQL NULL
    fun contains(name: String): Boolean         // distinguishes absent from NULL

    fun string(name: String): String?
    fun long(name: String): Long?
    fun decimal(name: String): BigDecimal?
    fun double(name: String): Double?
    fun bool(name: String): Boolean?
    fun date(name: String): LocalDate?
    fun dateTime(name: String): LocalDateTime?
    fun instant(name: String): Instant?
    fun bytes(name: String): ByteArray?

    fun with(name: String, value: Any?): Row    // add or replace, returns a new Row
    fun without(name: String): Row
}
```

A typed accessor throws a diagnostic error naming step, column, actual type and requested
type, rather than silently coercing. `Row` is immutable; `with` and `without` return
copies. A transform reads with the accessors and returns `row.with("row_hash", h)`.

### 4.3 Seam 1: JDBC read into a Row

| JDBC / Oracle type | Canonical type |
|---|---|
| NUMBER, NUMERIC, DECIMAL | BigDecimal |
| INTEGER, BIGINT, SMALLINT | Long |
| FLOAT, DOUBLE, BINARY_DOUBLE | Double |
| VARCHAR2, CHAR, NVARCHAR2, CLOB | String |
| Oracle DATE, TIMESTAMP (`Types.TIMESTAMP`) | LocalDateTime |
| DuckDB DATE (`Types.DATE`) | LocalDate |
| TIMESTAMP WITH TIME ZONE | Instant |
| BOOLEAN | Boolean |
| RAW, BLOB | ByteArray |
| anything else | error |

An unsupported type is a runtime error naming step and column. The fix is a CAST in the
source SQL. The framework does not guess.

Two rows in this table were corrected in P1, both because the original was written
Oracle-first and neither case arises from Oracle.

**DATE splits by JDBC type code, not by name.** An Oracle `DATE` column carries a time
component and reaches the driver as `Types.TIMESTAMP` (93), because ojdbc's
`mapDateToTimestamp` defaults to true; it maps to `LocalDateTime` and keeps its time, which
is what the original row was protecting. `Types.DATE` (91) only ever arrives from DuckDB,
where a DATE genuinely has no time, so it maps to `LocalDate`. Mapping 91 to `LocalDateTime`
instead would make `CanonicalType.DATE` unreachable from any result set, and duckdb_jdbc
1.1.3 refuses to convert a DATE column to `LocalDateTime` or `Timestamp` at all - both throw
- so it would also require an `atStartOfDay` workaround to produce a value the source never
had.

**BOOLEAN was missing entirely.** Oracle had no SQL BOOLEAN when this table was written, but
DuckDB has always had one and Oracle 23 now does too; both emit `Types.BOOLEAN` (16). Without
this row a DuckDB BOOLEAN column cannot be read at all, which breaks task shapes B and C
(2.4) as soon as a scratch table has a boolean, and makes the `BOOLEAN` branch of 4.6's
writer dispatch unreachable by round trip.

### 4.4 Seam 2: Row written to a target

**Declarative target (`target.table`)**

- `createTable: AUTO` (scratch only): the framework generates DuckDB DDL from the source
  result set metadata using the mapping of 4.3 in reverse, subject to the nullable-column
  rule in 4.6. One mapping table both creates the table and drives the appender, so the
  two cannot disagree.
  When the step has a `transform`, source metadata does not describe the columns the
  transform adds, so `transform.addColumns` must declare them.

  **DECIMAL takes its precision and scale from the source.** `CanonicalType.DECIMAL.duckDbType`
  is the bare keyword, which DuckDB resolves to `DECIMAL(18,3)` - at most three decimal places
  and fifteen integer digits. So bare DECIMAL silently rounds a `NUMBER(38,10)` value, and an
  ordinary `NUMBER(18)` key at or above 1e15 fails the append outright, mid-write, after earlier
  chunks have committed. AUTO therefore emits `DECIMAL(p,s)` from `ColumnMeta.precision` and
  `ColumnMeta.scale`. A pair is usable when `1 <= p <= 38` and `0 <= s <= p`; DuckDB rejects
  anything else at parse time. An unusable pair is a runtime error at writer open, before any
  row is written, naming step and column: the fix is a CAST in the source SQL, and the
  framework does not guess (1.3, and the rule 4.3 already applies to an unsupported type).

  Measured on ojdbc 23.6: a declared `NUMBER(38,10)`, `NUMBER(18)`, `NUMBER(*,2)`, `INTEGER`
  or `SMALLINT` reports a usable pair, and so does `cast(x as number(18,3))`. An unconstrained
  `NUMBER` reports `p=0, s=-127`, a `FLOAT` reports `p=126, s=-127`, and every computed
  expression - `sum`, `avg`, `count`, arithmetic, `nvl`, `round`, a numeric literal - reports
  `p=0`. A DuckDB source always reports a usable pair, so a scratch-to-scratch pipe never hits
  this error.
- `createTable: REQUIRED` (default outside scratch, also available inside scratch): the
  table must already exist. Before writing, the framework reads the target column list and
  declared types from catalog metadata, then fills the positional appender or prepared
  statement **by column name**. YAML never carries a column order, so a DDL change cannot
  silently misalign data.
- A Row key with no matching column, or a NOT NULL column with no matching Row key and no
  default, is a runtime error naming step, column, and row ordinal.

A middle table inside scratch needs no hand-written DDL in the common cases: a
`materialize` step creates its output with CREATE TABLE AS SELECT, and a `pipe` step into
scratch creates it from source metadata. An explicit `sql` step with CREATE TABLE is only
needed when the author wants control the framework will not infer, such as a column wider
than the source declares, a constraint, or a computed default, and is then paired with
`createTable: REQUIRED`.

**Statement target (`target.sql`)**

The statement runs as a JDBI prepared batch, once per chunk, with Row values bound by
name: `:lot_id` binds the Row key `lot_id`. This is how MERGE and conditional INSERT are
expressed, and it is what makes a step idempotent:

```sql
merge into wip_summary t
using (select :lot_id as lot_id, :qty as qty from dual) s
on (t.lot_id = s.lot_id)
when matched then update set t.qty = s.qty
when not matched then insert (lot_id, qty) values (s.lot_id, s.qty)
```

Not available for DuckDB targets, because DuckDB writes go through the appender, which
takes a table and not a statement. Rejected at startup.

Binding names cannot be validated at startup, because the Row key set is only known once
the source query runs. They are checked against the first chunk and reported as a runtime
error listing the missing keys.

### 4.5 Column Name Case

Oracle returns upper case identifiers, DuckDB lower case. All Row keys are normalised to
lower case on read. Transforms and target mapping always see lower case. Not configurable.

### 4.6 DuckDB Appender and Null

DuckDB inserts always use `org.duckdb.DuckDBAppender`, never INSERT statements. Row-by-row
and multi-row INSERT are both too slow at this row count.

- The appender binds to a schema and table, not to SQL. This is why a DuckDB target is
  always a declarative table reference.
- Append is positional. Column order comes from catalog metadata, never from YAML.
- A PRIMARY KEY or UNIQUE violation fails the whole append batch and inserts nothing.
  Framework-created scratch tables therefore carry no constraints and no indexes. An index,
  if needed, is added by a later `sql` step.
- `flush()` marks the chunk boundary, and it is what makes the chunk visible. Measured in
  P3 on 1.1.3: rows appended but not flushed are invisible even to the appending connection;
  after `flush()` they are immediately visible to a `duplicate()` connection as well
  (`autoCommit` is true by default). **For a DuckDB target, flush is the per-chunk commit of
  5.2 step 4.** It does *not* bound memory: S1 measured the same peak RSS with and without
  it at both 1M and 10M rows, at a cost of roughly 32% of append wall time. So it is called
  once per chunk for visibility, not to cap memory.

**Null.** 1.1.3 has no public `appendNull()`, but it reaches the native
`duckdb_jdbc_appender_append_null` through three object-typed methods, each of which null
checks its argument:

| Method | Accepts null |
|---|---|
| `append(String)` | yes |
| `appendBigDecimal(BigDecimal)` | yes |
| `appendLocalDateTime(LocalDateTime)` | yes |
| `append(boolean/byte/short/int/long/float/double)` | no, primitive |
| `byte[]` | no such overload exists |

Because the framework owns the DDL for `createTable: AUTO`, the constraint is satisfied by
choosing types rather than by encoding tricks: **a source column marked nullable is created
as VARCHAR, DECIMAL, or TIMESTAMP**, all of which are reachable by a null-accepting method.
NOT NULL columns keep their natural mapping and use the faster primitive path.

The writer dispatches on the target column type read from catalog metadata, not on the
value:

```kotlin
when (col.type) {
    VARCHAR   -> appender.append(row.string(col.name))
    DECIMAL   -> appender.appendBigDecimal(row.decimal(col.name))
    TIMESTAMP -> appender.appendLocalDateTime(row.dateTime(col.name))
    BIGINT    -> appender.append(row.long(col.name) ?: nullNotAllowed(step, col, ordinal))
    DOUBLE    -> appender.append(row.double(col.name) ?: nullNotAllowed(step, col, ordinal))
    BOOLEAN   -> appender.append(row.bool(col.name) ?: nullNotAllowed(step, col, ordinal))
    DATE      -> rejectedAtOpen(step, col)   // S3: silently drops the time component
}
```

The Java parameters carry no nullability annotations, so Kotlin sees platform types and
passes a nullable value without complaint. The `?:` branches on the primitive paths are
defensive: under `AUTO` they are unreachable by construction.

Two consequences, both enforced at validation time where possible:

- **BLOB and RAW cannot be written to DuckDB.** There is no `byte[]` overload at all, null
  or not. Such a column must be converted in the source SQL, for example to base64 text.
- **Under `createTable: REQUIRED`, a nullable column whose declared type is not VARCHAR,
  DECIMAL, TIMESTAMP, or BIGINT is rejected.** S3 answered the cast question that this rule
  was standing in for:

  | Appender call | Column | Result |
  |---|---|---|
  | `appendBigDecimal(42)` | BIGINT | exact |
  | `appendBigDecimal(42.7)` | BIGINT | stores 43, silent round |
  | `appendBigDecimal(2^63)` | BIGINT | throws, loud |
  | `appendLocalDateTime(...T13:45:30)` | DATE | stores the date, silent truncation |

  BIGINT is therefore safe **by construction, not by luck**: the writer sources the value
  from `Row.long()`, so the BigDecimal it builds has scale 0 and the rounding case is
  unreachable, and a `Long` always fits INT64 so the overflow case is unreachable too.
  Nullable BIGINT is written as `appender.appendBigDecimal(row.long(col.name)?.toBigDecimal())`.

  DATE is **not** safe. Seam 1 (4.3) maps JDBC DATE and TIMESTAMP to `LocalDateTime`, so a
  value reaching a DATE column carries a time component that DuckDB drops without error.
  DOUBLE and BOOLEAN stay rejected: DOUBLE's BigDecimal boundary behaviour is untested, and
  BOOLEAN is reachable only through a text round trip via `append(String)`.

- **A nullable column with no null-accepting write path is rejected at open, under AUTO as
  well as REQUIRED.** The dispatch above reads a value with the accessor matching the *target*
  column type, so a nullable column must be created as a type whose accessor matches the
  *source* canonical type. STRING/VARCHAR, DECIMAL/DECIMAL, DATETIME/TIMESTAMP and LONG/BIGINT
  pair up. BOOLEAN and DOUBLE have only primitive `append` overloads; DATE is rejected by rule
  15 either way; and **INSTANT has no branch in the dispatch at all** - 1.1.3's appender offers
  no `Instant` or `OffsetDateTime` method, so `TIMESTAMP WITH TIME ZONE` cannot be appended.
  Routing any of them through VARCHAR/DECIMAL/TIMESTAMP would make `Row.string`/`Row.decimal`/
  `Row.dateTime` throw on the real value type, and is the encoding trick this section refuses.
  Because duckdb_jdbc reports `columnNullable` for every column, all four are reachable from
  any scratch-to-scratch pipe; the author's fix is a CAST in the source SQL. A column
  declared in `transform.addColumns` states its type in the task file, so that case is
  additionally rejected at startup (rule 15).

Never write a bare `appender.append(null)`. It compiles, because the primitive overloads do
not apply and it resolves to the String overload, but it is misleading to read and would
become ambiguous if a future version adds an overload. Always call the specifically named
method.

---

## 5. Execution Semantics

### 5.1 Order

Phases run in file order. Steps run in file order within a phase. No parallelism.

### 5.2 Chunking and Transaction Boundary

For a `pipe` step:

1. Open the source result set as a stream with `fetchSize = chunkSize`. Oracle defaults to
   a fetch size of 10, unusable at this row count.
2. Accumulate up to `chunkSize` Rows.
3. Apply the transform, if any, to each Row.
4. Write the chunk to the target and commit.
5. Repeat until the source is exhausted.

`chunkSize` resolves as step value, else task value, else 5000. A step moving wide rows can
lower it and a step moving narrow rows can raise it without affecting the rest of the task.

For `materialize`, `sql`, and `export`, each statement is its own transaction.

### 5.3 Retry

- Retry is per step. `retries` counts additional attempts after the first.
- Defaults: 3 for a scratch target, 0 for any other target.
- Retry applies only to transient failures: `SQLTransientException`,
  `SQLRecoverableException`, `SQLTimeoutException`, and SQLState class `08`. Any other
  failure, including a type or constraint error, fails immediately. Retrying a
  deterministic failure three times only turns a 10 minute failure into a 30 minute one.
- Backoff: exponential from 2s, doubling, capped at 30s.
- Scratch cleanup before a retry: see 5.5.
- A step with a non-scratch target and `retries > 0` must declare `idempotent: true`. This
  is an assertion by the author, not something the framework can verify. Its purpose is to
  force the intent to be stated, because the framework cannot make a partially written
  external target safe on its own. In practice it is justified by a MERGE statement target,
  or by a declarative target whose contents the step fully replaces.

### 5.4 Failure and Partial State

There is no rollback across chunks, steps, or phases. On failure, anything committed to
`scratch` is irrelevant because the scratch file is deleted at run end, and anything
committed to an external datasource stays committed.

A task may write to several external targets. Those writes are never mutually atomic; the
framework does not attempt it and cannot, since the targets may live in different
instances. Two mitigations are chosen per target:

- `idempotent: true` with a MERGE statement target. A rerun converges. This is the normal
  answer and is sufficient for most cases.
- Write to a work table and swap in a final `sql` step, for a target with live readers.
  Chunked commits otherwise leave the table visibly half-updated for the duration of the
  step, and permanently so if the step fails partway:

  ```yaml
  - name: load-work-table
    type: pipe
    target:
      datasource: report_oracle
      table: wip_summary_work
      createTable: REQUIRED

  - name: swap
    type: sql
    datasource: report_oracle
    statements:
      - "begin pkg_table_publish.swap('wip_summary'); end;"
  ```

  The atomic switch belongs to whatever publish mechanism already owns that table. The
  framework needs no concept of staging or promotion to support this: a `pipe` step and a
  `sql` step already express it.

### 5.5 Never Reuse or Delete a DuckDB Table

DuckDB 1.1.3 does not reliably reclaim space in a live database. `TRUNCATE` is an alias for
unqualified `DELETE`; rows are only marked deleted and space returns at `CHECKPOINT`;
`VACUUM` does not trigger deletion vacuuming; `VACUUM FULL` is not implemented; `DROP TABLE`
is reported not to reduce database size, and in in-memory mode not to release memory until
the connection closes.

The framework therefore never cleans up by deleting:

- Every dataset produced inside scratch is written under an attempt-suffixed name:
  `wip_stg__a1`, `wip_stg__a2`, and so on.
- After a successful write the framework creates the stable alias:
  `create or replace view wip_stg as select * from wip_stg__a2`
- Later steps always reference the stable name `wip_stg`.
- A failed attempt leaves `wip_stg__a1` in place, unreferenced. Nothing is deleted.

The same indirection covers parquet:
`create or replace view summary as select * from read_parquet('<dir>/summary__a2.parquet')`
so downstream SQL is identical regardless of the dataset's physical format, and `format`
can change without touching any other step.

Dataset names (`target.table` for scratch, `output` for materialize) are unique within a
task and validated at startup, so a parquet file name is unambiguous across phases and no
variable is involved.

The only reliable reclamation point is closing the instance and deleting the file, once per
run. Cost: with `retries: 3` a repeatedly failing dataset can occupy up to four copies, and
only the failing dataset is duplicated.

**The attempt suffix applies to datasets the framework creates, which is why a retried
scratch target must be `createTable: AUTO`.** Under `AUTO` the framework owns the physical
name and can write `wip_stg__a2` beside `wip_stg__a1`. Under `REQUIRED` the author owns a
stable-named table the framework did not create, so there is no suffixed name to write and
no view to repoint - a retry would append onto whatever the failed attempt already flushed,
which is between zero and one chunk of rows (see 12). That is silent duplication, so the
combination is rejected rather than performed: a scratch `createTable: REQUIRED` target with
`retries > 0` is an error naming the step, telling the author to use `AUTO` or to state
`retries: 0`. Rejected at step start, before the source query runs.

### 5.6 Parquet Materialisation

`format: PARQUET` runs
`COPY (<sql>) TO '<scratchDir>/<output>__a<n>.parquet' (FORMAT PARQUET)`
and creates the stable view over `read_parquet`.

What it buys: the dataset never becomes a DuckDB table, so it does not grow the scratch
database file, and a retry overwrites a file instead of adding a table.

What it does not buy: it does not release memory mid-run. The DuckDB instance stays open
for the whole run (7.2), so whatever the buffer manager holds is bounded by `memory_limit`
and released when the instance closes, not at a phase boundary.

What it cannot do: the initial landing from an external datasource must go into a DuckDB
table, because the appender can only append to a table. `format: PARQUET` is available on
`materialize` only, never on a `pipe` target.

---

## 6. Variables and Parameter Binding

### 6.1 Sources

- Built-in, always available: `runId`, `taskName`, `triggerTime`, `attempt`.
- Literal, from the task-level `vars` block.
- Exported, from an `export` step.

A literal var's value may not be null. Null carries no type, and 1.3 makes an untyped value
an error rather than a guess. An author who wants SQL NULL writes `null` in the query.

### 6.2 Scope and Evaluation

Task scope, evaluated in step order, so a variable exported in phase 1 is available in
phase 2. A variable may not be redefined once set.

### 6.3 Binding Rules

- Variables bind as JDBI named parameters: `where ts > :lastTs`.
- An `export` query returns exactly one row and one column. More than one row is an error;
  zero rows yields null.

  That null carries the export column's canonical type, taken from the query's result set
  metadata, which exists whether or not a row came back (measured: a zero-row
  `select max(ts)` reports `Types.TIMESTAMP`). It binds through `setNull(pos, <type>)`, not
  as `Types.OTHER`, which Oracle rejects on some typed columns - the same reason 4.4's writer
  uses `bindByType`. Mechanically it travels as a `java.sql.Types`-carrying
  `org.jdbi.v3.core.argument.Argument` inside the existing `Map<String, Any?>` of 11.1, which
  JDBI binds directly; no signature changes. Note the SQL consequence: `ts > :lastTs` with a
  null watermark matches nothing. A task whose first run must read everything writes
  `:lastTs is null or ts > :lastTs`.
- In `target.sql`, every `:name` binds from a Row key. Task variables are **not** available
  there. A statement target runs once per row, so a task variable a statement needs is
  projected into the source query's select list -
  `select lot_id, qty, :siteCode as site_code from wip` - where `JdbcSource.parameters` binds
  it (11.1) and it arrives as an ordinary lower-cased Row key (4.5). One namespace, so a
  `:name` in `target.sql` has exactly one meaning, and a name the source does not produce is
  the runtime error 4.4 already raises.

  Rejected: a `parameters` channel on `JdbcStatementWriter`. Measured on JDBI 3.45.4,
  `PreparedBatch.add()` clears the binding, so "bind the variable once per batch" writes the
  value into the chunk's first row and NULL into the rest - `(F12,1,1) (null,2,2)`, verified
  through a recording `PreparedStatement`. Binding per row instead costs the same as a Row
  key and buys a second namespace, a precedence rule, and a collision check startup cannot
  perform, since Row keys are unknown until the source query runs (rule 7 already concedes
  this).
- No identifier interpolation. `select * from :tableName` is not valid SQL and no substitute
  is provided. It would make startup SQL validation impossible and open an injection path
  whenever the value came from a query. The snapshot cache case that motivated the request
  is solved by 7.3 instead.

---

## 7. Resource Lifecycle

### 7.1 Datasources

Named Quarkus datasources with a Jdbi bean each. YAML refers to them by name. One name is
reserved: `scratch`, the per-run DuckDB working file.

### 7.2 Scratch DuckDB

**Scope.** One DuckDB instance and one file per task run, created lazily on first reference
and closed and deleted in a `finally` at run end, on success and failure alike. "Per run"
means one execution of one task: one scheduled firing or one API trigger. Two different
tasks running concurrently have separate files.

**Connections.** Writes are sequential and use a single connection. Additional connections,
if needed for concurrent reads, come from `DuckDBConnection.duplicate()`, which shares the
instance. A single `Connection` must never be used from two threads at once; that crashes
the JVM rather than raising an error. `memory_limit` is a database-level setting, so it is
not multiplied by the number of connections.

**Consequence of run-scoped lifetime.** There is no memory release point between phases.
Within a run, memory is bounded by `memory_limit` and disk by whatever the run accumulates.
This is accepted on the basis that a run lasts 5 to 30 minutes and the file is then deleted.
If S2 shows otherwise, the mitigation is in section 13.

**Settings applied at open**, matching what the snapshot cache already does:
`SET memory_limit`, `SET temp_directory`, and optionally `SET threads`. `temp_directory`
must point at disk-backed scratch space.

S4b refuted the original reason given here ("without it a large join fails outright instead
of spilling"). With `temp_directory` unset, DuckDB 1.1.3 creates `<dbfile>.tmp/` beside the
database file and spills into it: same peak within 0.2%, same outcomes, same wall times.
Nothing fails that would otherwise have succeeded. The real reason to set it is the inverse
- an unset value silently places a gigabyte or more of spill wherever the database file
happens to live, uncounted by anyone reading the YAML.

Two further S4b results bear on sizing. **Spilling is not a safety net**: a hash aggregate
ran out of memory at both a 256 MB and a 512 MB `memory_limit` after writing ~1 GB of spill,
so a query can consume peak spill and still fail. And **spill peak does not scale with
`memory_limit`**: doubling the limit moved the peak by under 10% and not consistently in one
direction, because the peak is set by the query's working set. `memory_limit` is therefore
not a lever on `sizeLimit`.

**Rules.** File mode, never in-memory. Never `CREATE TEMP TABLE`: `CHECKPOINT` has no effect
on temporary tables, which removes even the theoretical reclamation path.

**Deployment.**

- The scratch directory must be a disk-backed volume. An `emptyDir` with `medium: Memory`
  is tmpfs and charges the scratch file against the pod memory limit, converting file mode
  back into memory mode and causing OOMKill.
- Set an explicit `sizeLimit`: **32 GiB**. Derived, not guessed:

  ```
  peak volume = (N + retries) x R x C x d     file at run end, all attempts retained (5.5)
              + s x (bytes the heaviest query reads)      spill, concurrent with the file

  d = 9.0 bytes per stored value, high entropy    S4a, flat from 4 to 30 columns
      1.0 - 1.4 low entropy
  s = 3.3x to 4.9x the input read                 S4b, flat in memory_limit
  N = datasets in the task, R = rows, C = columns
  ```

  At the stated ceiling of R = 2M, C = 100, N = 4, `retries: 3`, high entropy, and a join
  reading two datasets: 12.6 GB of file plus 17.6 GB of spill = 30.2 GB, rounded to 32 GiB.
  The dominant term is `retries`, not the data: at `retries: 1` the file term falls from
  12.6 GB to 9.0 GB. Watch `etl_scratch_file_bytes` (9.3) and re-cut from production.

  **`etl_scratch_file_bytes` does not carry the spill term of this formula.** It is sampled once
  at run end (9.3), and DuckDB reclaims spill files as queries finish, so by then the spill is
  almost always gone - while at this ceiling spill is 17.6 of the 30.2 GB, the majority of it.
  What the metric prices is the file, its WAL and any retained attempt copies and parquet
  outputs, which is the `(N + retries) x R x C x d` half. Re-cutting the volume from the metric
  alone therefore under-sizes it. The spill term has to come from S4b's measured factor, not from
  the gauge. Recorded in P8b after the metric's rationale was written the other way round and
  refuted.
  Without an explicit limit a runaway scratch file consumes node disk and affects unrelated
  pods.
- Indicative budget at 8 GB pod memory: JVM heap 2 GB, DuckDB `memory_limit` 4 GB,
  remainder for JVM off-heap and DuckDB native allocation beyond the limit.

### 7.3 Reading the Snapshot Cache

The snapshot cache owns generation numbering, promotion, leases, the verify gate, and
reclamation. The ETL framework implements none of that and does not attach generation files
itself.

The cache serves reads from its own in-memory DuckDB instance with the generation file
attached read-only; reader connections are duplicates of that serving connection. Those
connections therefore belong to the cache's instance, not to the scratch instance, and
cannot join scratch tables directly.

The integration point is the cache's own `copyOut`, which attaches the generation file onto
the caller's instance and runs `CREATE TABLE ... AS SELECT`, so no row passes through the
application:

```kotlin
store.copyOut(opened, CopyOutSpec(
    targetConnection = scratchConnection,
    targetTable = "wip_cache",
    sql = "select lot_id, qty from wip where site = 'F12'",
))
```

This is exposed as a distinct step type rather than as a `pipe`, because it is a file-to-file
copy and not a row pipeline.

**A contradiction with the cache's own specification, recorded rather than shrugged at.** The
snapshot cache's spec 6.5 says: *"Share a single consumer instance. Don't open one per job - several
unbounded instances will add up and eat the pod's memory budget."* SimpleEtl's `ScratchDb` is one
DuckDB instance **per run**, at a default `memory_limit` of 4096 MB (7.2). That is one per job,
which is what 6.5 forbids.

SimpleEtl is right on its own terms and the deviation is deliberate: 7.2's per-run file exists
because DuckDB 1.1.3 has no vacuum and `DROP TABLE` does not shrink a file, so a shared instance
would carry a high-water mark forever, and 5.5's attempt-suffixed retries make that worse. The two
instances are also different things - the cache's *consumer* instance serves reads from generation
files, while scratch is a per-run working file that is deleted at run end. What P9 must not do is
create a **second consumer** instance: `copyOut` writes into the scratch connection the framework
already owns, so no new cache-side instance is opened, and 6.5's arithmetic is untouched. Under
CLAUDE.md the documents win, so this is written down rather than left as two specifications that
disagree.

**Operational constraint.** The cache refuses to detach a generation while an issued
connection into it is still open, and defers reclamation to the next pass. Generations that
cannot be reclaimed accumulate, and once the configured limit is reached the cache pauses
refreshing entirely. A task holding a lease for 30 minutes can therefore stall cache
refreshes, with the cause sitting in a different system from the symptom. The rule is to
copy the needed subset into scratch and release the lease immediately, rather than holding
it for the duration of the task.

### 7.4 Closing

Every appender is opened in a `use` block. Every result set stream is closed explicitly.
Every connection is returned in a `finally`. Release is never left to GC. The appender's
`finalize` is a backstop for bugs, not a cleanup mechanism.

---

## 8. Triggering and Concurrency

### 8.1 Schedule

The cron expression lives in the task file, so scheduling is programmatic:
`scheduler.newJob(taskName).setCron(cron).setTask { ... }.schedule()` at startup. The
`@Scheduled` annotation is compile-time and cannot express one schedule per file.

Two requirements follow, and both fall on the **host application** (8.6), not on this
framework. `TaskScheduler` takes a `CronScheduler` and registers each enabled task's cron
with it; the host implements `CronScheduler` over Quarkus's programmatic `Scheduler`.

- `quarkus.scheduler.start-mode=forced`. By default the scheduler does not start unless a
  `@Scheduled` business method exists. This application has none, so without this property
  no task would ever fire. **This module cannot ship the property even if it wanted to:
  Quarkus does not read an `application.properties` out of a dependency jar**, so a copy
  here would be read by this module's own tests and by nothing else.
- The scheduled callback hands off to the task's own dispatcher rather than running inline.
  Quarkus runs scheduled work on a Vert.x worker thread and the blocked-thread checker warns
  past 60 seconds; tasks here run 5 to 30 minutes.

`schedule` may be omitted, producing a task that runs only when triggered through the API.

### 8.2 API Trigger

An admin-only endpoint triggers any task on demand: reruns after a failure, backfills, and
testing a new task file before attaching a schedule.

```
POST   /admin/etl/tasks/{name}/runs      -> 202 { runId }
GET    /admin/etl/tasks                  -> tasks, schedules, last run outcome
GET    /admin/etl/tasks/{name}/runs/{id} -> run status
POST   /admin/etl/reload                 -> re-read task files, see 8.5
```

- All endpoints require the `etl-admin` role, enforced with `@RolesAllowed`.
- Trigger is asynchronous: validate, allocate a runId, submit to the task's dispatcher,
  return 202. A 30 minute request is never held open.
- 409 if the task is already running, 404 if unknown, 400 if disabled.
- An API-triggered run is identical to a scheduled run in every other respect. It appears in
  logs and metrics with trigger source `API` and the caller identity.

The endpoint table above is the contract the **host** exposes. The framework surface is
`TaskAdmin`, returning a sealed result per operation - `Accepted(runId)`, `AlreadyRunning`,
`Unknown`, `Disabled`, and a `ValidationReport` for reload. The host's `AdminResource` maps
those to 202 / 409 / 404 / 400 and carries `@RolesAllowed("etl-admin")`. `TaskAdmin.trigger`
takes the caller identity as a parameter, for `TaskContext.triggeredBy`, and performs no
authorisation of its own.

### 8.3 Threading Model

Each task owns a `Dispatchers.IO.limitedParallelism(1)` view, tagged with
`CoroutineName(taskName)`. Both the scheduled callback and the API trigger submit to it. The
engine itself is ordinary blocking code: sequential steps, blocking JDBC.

- A bare `Dispatchers.IO` is not used: it does not serialise work per task, so two firings of
  the same task could overlap.
- `newFixedThreadPoolContext` is not used: it is marked delicate, and a per-task pool would
  keep an idle thread alive per task. A `limitedParallelism` view shares the underlying IO
  threads and is not bounded by the IO parallelism limit.
- Coroutines buy nothing inside the engine. A run is one thread blocked for 5 to 30 minutes
  with nothing to yield to. The dispatcher is used for confinement, not for concurrency.

`CoroutineName` is passed into the run body by `TaskRunner`. It is **not** readable from the
run itself, precisely because the engine is blocking code, and the `@name` suffix on the
thread name exists only under `-ea` - surefire's default, absent in production. Measured:
with assertions enabled the thread reads `DefaultDispatcher-worker-1 @wip-summary#1`, and
without them `DefaultDispatcher-worker-2`. A test must never assert the coroutine name via
the thread name.

### 8.4 Concurrency

- A task never runs concurrently with itself, whether triggered by schedule or API. The
  serialised dispatcher provides this. The framework rejects rather than queues, returning
  409 for an API trigger and skipping a scheduled firing, so a slow run cannot accumulate a
  backlog.
- Different tasks may run concurrently, each with its own dispatcher and scratch file.
- The guard is in-process only. The deployment is a single pod, an explicit assumption.
  Multiple replicas would need leader election, out of scope, no hook reserved.

### 8.5 Reload

`POST /admin/etl/reload` re-reads the task directory and applies the result atomically:

- All files are parsed and validated first. If any file fails, nothing changes and the
  endpoint returns the errors. A bad edit cannot take the scheduler down.
- A task currently running keeps the definition it started with. The definition is captured
  at run start and never swapped mid-run.
- Schedules are re-registered for tasks whose cron changed, unscheduled for removed tasks,
  and scheduled for new ones.

Rule 16 stays structural at load. `CronScheduler.schedule` is contractually required to
throw on an unparseable expression; `TaskScheduler.apply` registers into a staging set and
converts such a throw into a `ValidationError` before committing the swap, so a bad cron is
rejected atomically rather than taking the scheduler down.

A filesystem watcher is deliberately not used. ConfigMap propagation to the volume is
asynchronous and partial updates are visible mid-write, so an explicit reload gives a
deterministic point at which a change takes effect. Startup runs the same load path, so
there is one code path and one set of validation rules.

### 8.6 Host Wiring Contract

Several of this section's requirements cannot be met by a library and are the host application's,
enumerated here with the symptom of missing each. **None of them is tested in this repository**,
because no host module exists in it; that is a real gap, recorded rather than papered over. The
list has grown with each phase that discovered another one - it was two when P7 wrote it.

| The host must | Symptom if missed |
|---|---|
| set `quarkus.scheduler.start-mode=forced` in the **application's** `application.properties` | no task ever fires, and no error is raised |
| implement `CronScheduler` over the programmatic `Scheduler`, handing off to `TaskRunner` rather than running inline | Vert.x blocked-thread warnings past 60s; a 5-30 minute run pinned to a worker thread |
| expose `AdminResource`, mapping `TaskAdmin`'s sealed results to 202 / 409 / 404 / 400 | - |
| put `@RolesAllowed("etl-admin")` on every endpoint | an unauthenticated caller can trigger any task |
| make `CronScheduler.schedule` throw on an unparseable cron | 8.5's atomic reload silently accepts a bad cron |
| construct `TaskFileLoader` with the name set of the **same** `TaskHookRegistry` it hands `TaskEngine` | validation rule 5 passes for every hook name, and a typo dies at the end of a 30 minute run - precisely the failure 9.4 exists to prevent |
| register `MicrometerTaskMetrics` against the application's `MeterRegistry` | every metric in 9.3 is silently absent; nothing fails and no dashboard populates |
| construct the `SnapshotCache`, and own the `cache` name -> `CacheBinding(cache, group)` map handed to `TaskEngine` | a `cacheCopy` step fails at run time naming the unknown cache |
| construct `TaskFileLoader` with the **same** cache-name set it hands `TaskEngine` | rule 21 passes for every name, and a typo dies at the end of a 30 minute run - the same failure the hooks row above records |
| **assert that a generation becomes reclaimable after a `cacheCopy` step.** Not testable in this repository: reclamation lives in `DefaultSnapshotCache`, which is `internal` to the cache module, so SimpleEtl's tests use a double implementing the public interface. Plan P9's "a test asserts the generation becomes reclaimable" is achievable only in a host that owns a real cache | a step that holds or references a generation stalls refreshing, and this repository's suite cannot see it |
| put `io.micrometer:micrometer-core` (>= 1.14.x) on the application's **runtime** classpath - the framework declares it `provided` and does not ship it | `NoClassDefFoundError: io/micrometer/core/instrument/MeterRegistry` when the binding is constructed. Loud, at wiring time, which is the good failure mode |

Two notes on the metric binding, measured on micrometer 1.14.2 rather than assumed:

- Prometheus's naming convention appends `_total` to a counter and `_seconds` to a timer **only
  when not already suffixed**, so every name in 9.3 exports byte-identical. The feared
  `etl_task_runs_total_total` does not occur.
- A Micrometer `Timer` exports as `_count` + `_sum` plus a separate `_max` gauge, so
  `etl_task_duration_seconds` is three Prometheus series, not one. That is standard Timer
  behaviour and is the host's to know when writing a dashboard.

---

## 9. Extension Points

### 9.1 Transform

```kotlin
fun interface RowTransform {
    fun apply(row: Row): Row?     // null drops the row
}
```

Resolved from YAML by CDI bean name. Three contractual rules:

- Stateless. No accumulation across rows, no caching.
- No database access of any kind.
- No side effects.

A stateful or effectful handler makes retry non-deterministic and hard to diagnose, and a
transform is an escape hatch through which business logic migrates out of SQL and out of the
YAML file, defeating a declarative framework. Intended uses are limited to computing a value
the database cannot: a hash, a run identifier, a constant from the runtime context.

Columns a transform adds must be declared in `transform.addColumns` when the target uses
`createTable: AUTO`, because source metadata cannot describe them.

### 9.2 Run Listener

The existing in-house logging mechanism plugs in here. The framework supplies the call sites
and ships a no-op default.

```kotlin
interface TaskRunListener {
    fun onTaskStart(ctx: TaskContext)
    fun onTaskEnd(ctx: TaskContext, outcome: Outcome)
    fun onPhaseStart(ctx: PhaseContext)
    fun onPhaseEnd(ctx: PhaseContext, outcome: Outcome)
    fun onStepStart(ctx: StepContext)
    fun onStepEnd(ctx: StepContext, result: StepResult)
    fun onStepError(ctx: StepContext, attempt: Int, error: Throwable, willRetry: Boolean)
}
```

`TaskContext` carries runId, taskName, triggerSource, triggeredBy, startedAt.
`StepResult` carries rowsRead, rowsWritten, durationMs, attempt.
`logging: false` suppresses listener invocation for that task.

### 9.3 Metrics

Micrometer, independent of the `logging` flag:

```
etl_task_runs_total{task, trigger, outcome}
etl_task_duration_seconds{task}
etl_step_duration_seconds{task, phase, step}
etl_step_rows_total{task, phase, step, direction}   # direction = read | written
etl_step_retries_total{task, phase, step}
etl_scratch_file_bytes{task}                        # sampled at run end
```

### 9.4 Task Hooks

```kotlin
fun interface TaskHook {
    fun run(ctx: TaskContext)
}

interface TaskHookRegistry {
    fun register(name: String, hook: TaskHook)
}
```

`onSuccess` runs once after every phase has succeeded. `onFailure` runs once on any failure.
If `onSuccess` throws, the task becomes FAILED and `onFailure` then runs. If `onFailure`
throws, the error is logged and not propagated.

Hooks are named, and names are registered by the application at startup, which is what lets
one implementation serve many instances without a per-instance CDI bean and without an
argument map in YAML:

```kotlin
@Startup
class EtlHookRegistration(registry: TaskHookRegistry, caches: List<SnapshotCache>) {
    init {
        caches.forEach { c -> registry.register("invalidate-${c.name}") { c.invalidate() } }
    }
}
```

A name not present in the registry fails startup validation, so a typo is caught at boot
rather than at the end of a 30 minute run.

`TaskContext` fields are for log correlation. A hook implementation should not pass `runId`
or `taskName` to an external system as a business key; they are framework identifiers.

Per-step hooks are deliberately not offered. Applied globally they make a YAML file an
incomplete description of what happens; declared per step they duplicate what a `sql` step
already does, while adding ambiguity about ordering with respect to retry.

### 9.5 GenerationSource for the Snapshot Cache

Layer 1 is used directly by the cache. `GenerationSource.refresh(ctx)` receives a write
`Connection` to the candidate generation file; a RowPipe writes into it:

```kotlin
class PipeGenerationSource(private val specs: List<TableSpec>) : GenerationSource {
    override fun refresh(ctx: BuildContext) {
        oracleMes.inTransaction<Unit, Exception> { handle ->     // ONE read transaction
            specs.forEach { spec ->
                RowPipe(
                    source = JdbcSource(handle, spec.sql),       // borrowed, not closed
                    target = DuckDbTableWriter(ctx.target, spec.table, AUTO, spec.step),
                    step = spec.step,
                    chunkSize = 5000,
                ).run()
            }
        }
    }
}
```

**The single transaction is load-bearing.** An earlier draft of this example passed the
`Jdbi` to each `JdbcSource`, which opens a fresh `Handle` - and so a fresh connection and
transaction - per pipe. `GenerationSource.refresh` requires all tables in the group to be
read inside one source read transaction; reading them separately publishes a torn snapshot,
where the union of tables shows duplicates or gaps intermittently. That is why `JdbcSource`
takes a borrowed `Handle` (11.1). The cache's own end-to-end test cannot catch the mistake,
because its synthetic source generates rows in-process with no source transaction at all.

`PipeGenerationSource` itself is caller-land wiring and belongs to no library module: the
cache's plan places `GenerationSource` implementations outside its framework packages and
confines JDBI to caller-land implementations, and the cache's own later phase owns the real
one. Adding a `snapshotcache -> SimpleEtl` dependency would also cycle against the
`SimpleEtl -> snapshotcache` dependency that 7.3's cache-read step already requires.

The direction of control matters: the cache calls the ETL, not the other way round.
Promotion, verification, leases, and reclamation stay with the cache, and the framework
needs no concept of a generation. Layer 1 knows nothing about the cache in return; its only
contract is a source query, a target connection, and a table name.

---

## 10. Startup and Reload Validation

Task files are mounted from a Kubernetes volume, read by scanning the directory and
deserialising with Jackson YAML plus Bean Validation. They are deliberately not read through
Quarkus configuration: the config model is flat properties rather than a set of structurally
identical documents, config performs property expansion which would corrupt SQL containing
`${...}`, and binding failures report a property key rather than a file and line.

Any failure below prevents startup, or causes a reload to be rejected with no change.

1. YAML parses and deserialises; unknown fields rejected.
2. `name` unique across files and matching the allowed pattern.
3. Every referenced datasource name exists as a configured Jdbi bean.
4. Every `transform.bean` resolves to a `RowTransform` CDI bean.
5. Every `onSuccess` / `onFailure` name exists in the `TaskHookRegistry`.
6. Every SQL text parses.
7. Every `:name` in source, export, materialize, and statement SQL is a built-in, a literal
   var, or an export appearing earlier in step order, except Row-bound names in `target.sql`,
   which are checked at runtime (4.4).
8. No variable defined twice. No literal var with a null value (6.1). The former
    "no Row key colliding with a task variable name" clause is gone: with 6.3 amended, a Row
    key and a task variable can no longer meet in the same statement.
9. Dataset names unique within the task.
10. Exactly one of `target.table` or `target.sql` present.
11. `target.sql` not used on a DuckDB datasource.
12. `retries > 0` on a non-scratch target requires `idempotent: true`.
13. `format: PARQUET` only on `materialize`.
14. `createTable: AUTO` only on scratch targets, and not combined with a transform that
    lacks `addColumns`. AUTO's DECIMAL precision cannot be validated at startup, because
    result set metadata exists only once the source query runs; that check is at writer open
    (4.4) and is named here for completeness.
15. **DuckDB target column types (4.6).** No nullable column whose declared type is outside
    VARCHAR, DECIMAL, TIMESTAMP, BIGINT, and no DATE, BLOB or TIMESTAMP WITH TIME ZONE column
    whether nullable or not. BIGINT was added once S3 showed the cast is exact when the value
    comes from `Row.long()`. DATE is rejected regardless of nullability, because the
    truncation in 4.6 is silent and does not depend on it.

    Startup enforces this over every column type a task file *states*, which is every
    `transform.addColumns` entry (3.2, 9.1). It cannot enforce it over a *table's* declared
    types: under `REQUIRED` those live in a catalog the run creates, and under `AUTO` they
    come from result set metadata that exists only once the source query runs. duckdb_jdbc
    1.1.3 offers no parse-to-AST path for DDL - `json_serialize_sql` parses a `CREATE TABLE`
    but serializes SELECT only, `EXPLAIN` binds it without emitting a column list, and
    `PREPARE` takes no DDL - so that half is enforced at writer open (4.6), before any row is
    written, and is named here for completeness, as in rule 14.
16. Cron expression valid, when present.
17. Each step's field set matches its declared type exactly.
18. A scratch target with `createTable: REQUIRED` and `retries > 0` is rejected (5.5). The
    attempt suffix needs a framework-owned physical name, so a retry onto an author-owned
    stable table would append onto the failed attempt's flushed rows.
19. **`cacheCopy` SQL binds no variable.** `CopyOutSpec.sql` is a plain string with no binding
    channel (3.6, 7.3), so a `:name` in a `cacheCopy` step is rejected at startup naming the step.
    Checked at load rather than at run time because the alternative is a file that boots green and
    fails at the end of a 30 minute run - the failure this whole section exists to prevent.
20. **`cacheCopy` with a *stated* `retries > 0` is rejected.** No failure a cache copy can produce
    is transient under 5.3, so the knob can never fire (3.6). Same treatment as rules 12 and 18 -
    reject the combination rather than accept and ignore it.

    **The YAML default for this step type is 0, not 3.** `CacheCopyStep.retries` defaults to 3 in
    the programmatic model, frozen since P5, because every other scratch-targeted step does. If the
    loader inherited that default, **every task file that omits `retries` would fail rule 20** -
    caught while checking rule 18's precedent, which deliberately rejects the omitted-and-defaulted
    case. So the loader resolves `retries ?: 0` for `cacheCopy` alone, and rule 20 tests the stated
    value. The asymmetry between the YAML default and the model default is deliberate and is
    recorded here because nothing else would explain it.
21. **Every `cache` name exists in the host-supplied binding set**, the exact analogue of rule 3
    for datasources.

Errors report file name, step name, and where available the YAML line.

---

## 11. Public API

The frozen contract. Everything below is what callers and later phases depend on;
everything not listed is internal and free to change. The plan names the subset each phase
introduces. Types described elsewhere in this document are cross-referenced rather than
repeated in full.

### 11.1 Layer 1

```kotlin
// Canonical values and the read seam (spec 4.1 to 4.3)
enum class CanonicalType {
    STRING, BOOLEAN, LONG, DECIMAL, DOUBLE, DATE, DATETIME, INSTANT, BYTES;

    val duckDbType: String                    // natural mapping; 4.6's nullable rule overrides

    companion object {
        fun fromJdbc(sqlType: Int, typeName: String): CanonicalType   // 4.3, throws if unsupported
    }
}

class Row {                                  // full signature in spec 4.2
    val columns: Set<String>
    operator fun get(name: String): Any?
    fun contains(name: String): Boolean
    // typed accessors: string, long, decimal, double, bool, date, dateTime, instant, bytes
    fun with(name: String, value: Any?): Row
    fun without(name: String): Row
}

// precision and scale are 0 when the source does not state them - an Oracle unconstrained
// NUMBER, or any computed expression, reports p=0. Read for every column, consulted only
// for DECIMAL, by AUTO DDL generation (4.4).
class ColumnMeta(
    val name: String,
    val type: CanonicalType,
    val nullable: Boolean,
    val precision: Int = 0,
    val scale: Int = 0,
)

// Result set to Row: applies 4.3, lower-cases keys per 4.5, reads metadata once.
// `step` is carried so an unsupported type or a wrong typed accessor names the step (4.2, 4.3).
class RowMapper(metaData: ResultSetMetaData, step: String) {
    val columns: List<ColumnMeta>
    fun map(rs: ResultSet): Row
}

// Source. Two forms, because the Jdbi form opens a fresh Handle - and so a fresh
// transaction - per pipe, which cannot satisfy 9.5's "one source read transaction".
class JdbcSource {
    // Borrows the caller's Handle and never closes it, so N pipes share one read
    // transaction. Required by the GenerationSource seam (9.5).
    constructor(handle: Handle, sql: String, parameters: Map<String, Any?> = emptyMap())

    // Convenience: opens one Handle for the run and closes it. Single-pipe use.
    constructor(jdbi: Jdbi, sql: String, parameters: Map<String, Any?> = emptyMap())
}

// Targets
interface RowWriter : AutoCloseable {
    fun open(columns: List<ColumnMeta>)
    fun write(chunk: List<Row>): Int          // rows written
    override fun close()
}

// `step` is carried on every writer because 4.6 rejects a BLOB column at OPEN time, before
// any Row exists, and 4.4 requires the error to name the step. Same reason as RowMapper.
class DuckDbTableWriter(
    connection: Connection,
    table: String,
    createTable: CreateTable,                 // AUTO | REQUIRED
    step: String,
) : RowWriter

class JdbcTableWriter(jdbi: Jdbi, table: String, step: String) : RowWriter

class JdbcStatementWriter(jdbi: Jdbi, sql: String, step: String) : RowWriter

enum class CreateTable { AUTO, REQUIRED }

// Transform (spec 9.1)
fun interface RowTransform {
    fun apply(row: Row): Row?
}

// The pipe
// `step` is carried for the same reason RowMapper and the writers carry it: 4.2 and 4.3
// require errors to name the step, and RowPipe is what constructs the RowMapper.
class RowPipe(
    source: JdbcSource,
    target: RowWriter,
    step: String,
    chunkSize: Int = 5000,
    transform: RowTransform? = null,
) {
    fun run(): PipeResult
}

data class PipeResult(val rowsRead: Long, val rowsWritten: Long)
```

### 11.2 Layer 2

```kotlin
// Definition model. YAML is one source of these, not the only one (spec 2.1).
data class TaskDefinition(
    val name: String,
    val enabled: Boolean = true,
    val cron: String? = null,
    val logging: Boolean = true,
    val chunkSize: Int = 5000,
    val scratchMemoryLimitMb: Int? = null,
    val onSuccess: String? = null,
    val onFailure: String? = null,
    val vars: List<LiteralVar> = emptyList(),
    val phases: List<Phase>,
)

data class Phase(val name: String, val steps: List<Step>)

sealed interface Step {
    val name: String
    val retries: Int
}
class PipeStep : Step         // source, transform?, target, chunkSize?
class MaterializeStep : Step  // datasource, output, format, sql
class SqlStep : Step          // datasource, statements
class ExportStep : Step       // datasource, vars
class CacheCopyStep : Step    // cache, sql, output (spec 7.3)

// P9. What a task file's `cache:` name resolves to. Two fields, because one SnapshotCache serves
// many groups - copyOut(group, ...) takes the group - so a name alone cannot identify both.
data class CacheBinding(val cache: SnapshotCache, val group: GroupId)

// Engine
class TaskEngine {
    fun run(definition: TaskDefinition, trigger: TriggerSource): TaskOutcome
}

enum class TriggerSource { SCHEDULE, API }
enum class Outcome { SUCCEEDED, FAILED }
data class TaskOutcome(val runId: String, val outcome: Outcome, val failure: Throwable?)

// Loading
class TaskFileLoader {                        // (datasources, transforms, hooks, caches)
    fun load(directory: Path): LoadResult
}

/**
 * P6 amendment, reconciled here in P9. This section declared
 * `Result<List<TaskDefinition>, ValidationReport>`, which does not exist: Kotlin's stdlib
 * `Result` takes one type parameter. `kotlin.Result` plus a `ValidationException` was rejected
 * because reading the report would need an unchecked cast at every call site and nothing
 * constrains a `Result.failure` to carry that type. The sealed pair makes the invalid state
 * unrepresentable rather than merely documented. Recorded as a deviation in progress.md at P6 and
 * left unreconciled here for three phases - a frozen document that disagreed with shipped code.
 */
sealed interface LoadResult {
    data class Loaded(val tasks: List<TaskDefinition>) : LoadResult
    data class Invalid(val report: ValidationReport) : LoadResult
}

data class ValidationReport(val errors: List<ValidationError>)
data class ValidationError(val file: String, val step: String?, val line: Int?, val message: String)

// Scheduling, triggering, concurrency (spec 8). The two adapters - a cron binding and an
// HTTP resource - are the host's (8.6) and are deliberately absent here.
fun interface CronScheduler {                 // host-implemented over Quarkus's Scheduler
    fun schedule(taskName: String, cron: String, run: () -> Unit): AutoCloseable
}

class TaskScheduler(cron: CronScheduler) {
    fun apply(definitions: List<TaskDefinition>)   // register / unregister / re-register
}

class TaskRunner {                            // one limitedParallelism(1) view per task
    fun submit(definition: TaskDefinition, trigger: TriggerSource, by: String?): TriggerResult
}

sealed interface TriggerResult {
    data class Accepted(val runId: String) : TriggerResult
    data object AlreadyRunning : TriggerResult
    data object Unknown : TriggerResult
    data object Disabled : TriggerResult
}

class TaskAdmin {                             // what AdminResource maps to HTTP
    fun trigger(name: String, by: String?): TriggerResult
    fun list(): List<TaskStatus>
    fun run(name: String, runId: String): TaskOutcome?
    fun reload(directory: Path): ValidationReport?    // null when the reload succeeded
}

// Scratch (spec 7.2)
class ScratchDb : AutoCloseable {
    fun connection(): Connection
    fun duplicate(): Connection
    fun diskBytes(): Long                     // total bytes under the run directory; feeds 9.3's gauge
    override fun close()                      // closes the instance and deletes the file
}
```

### 11.3 Extension Points

```kotlin
interface TaskRunListener {          // full signature in spec 9.2
    companion object {
        val NONE: TaskRunListener                                  // the no-op default 9.2 ships
        fun of(vararg listeners: TaskRunListener): TaskRunListener // fan-out, isolated per listener
    }
}
fun interface TaskHook { fun run(ctx: TaskContext) }
interface TaskHookRegistry { fun register(name: String, hook: TaskHook) }

/** The concrete registry. 9.4 declares registration only; the engine and `TaskFileLoader`
 *  also need lookup and the name set, so the shipped class adds them. */
class TaskHooks : TaskHookRegistry {
    val names: Set<String>
    operator fun get(name: String): TaskHook?
}

data class TaskContext(
    val runId: String,
    val taskName: String,
    val triggerSource: TriggerSource,
    val triggeredBy: String?,
    val startedAt: Instant,
)
data class PhaseContext(val task: TaskContext, val phase: String)
data class StepContext(val task: TaskContext, val phase: String, val step: String)
data class StepResult(
    val rowsRead: Long,
    val rowsWritten: Long,
    val durationMs: Long,
    val attempt: Int,
)

/** The metric seam of 9.3, kept technology-free so `core` names no Micrometer type.
 *  `MicrometerTaskMetrics` is the shipped binding and is the only class that may. */
interface TaskMetrics {
    fun taskEnded(ctx: TaskContext, outcome: Outcome, durationMs: Long)
    fun stepEnded(ctx: StepContext, result: StepResult)
    fun stepRetried(ctx: StepContext)
    fun scratchBytes(ctx: TaskContext, bytes: Long)
    companion object { val NONE: TaskMetrics }
}
```

`PhaseContext`, `StepContext`, `TaskHooks`, `TaskMetrics`, `TaskRunListener.NONE` and
`TaskRunListener.of` were added by the P8 amendment. 9.2 names `PhaseContext` and `StepContext`
inside a FIXED signature and never defined them, and `(task, phase)` / `(task, phase, step)` is
the only shape that produces 9.3's `{task, phase, step}` label sets. The rest are forced the same
way: 9.4 declares hook registration but nothing that can look a hook up, and 9.3 declares meter
names but nothing that emits them. This is the ninth consecutive phase in which 11's declared
surface proved narrower than the phase needed - recorded in progress.md rather than left as a
pattern nobody named.

---

## 12. Open Items

- **S1. Appender flush cost. ANSWERED (P0).** Flush costs ~32% of append wall time and
  bounds no memory. Kept on regardless: it is the chunk boundary, and 0.5s per million rows
  is noise against the Oracle read. 4.6 corrected.
- **S2. Scratch growth and process RSS.** Simulate five steps of one million rows, force one
  step to fail and retry twice, repeat ten runs. Measure process RSS against baseline and
  scratch file size at run end. The target of measurement is RSS, not `database_size`. If
  RSS does not return to baseline across runs, retry moves from step level to task level,
  discarding the scratch file on retry. This spike also produces the volume `sizeLimit`.
  **ANSWERED (P0):** RSS returns from a 392 MB in-run peak to ~104 MB against a 70 MB
  baseline, every run; the file is flat at ~178 MB with no cross-run high-water mark. The
  trigger did not fire, so **retry stays at step level** and 13's `scratch.scope: PHASE` is
  not needed. `sizeLimit` is NOT answered: S2 measured 7.2 bytes per column-value at high
  entropy and 0.54 at low, but the workload shape and the spill factor were never measured.
  See S4.
- **S3. Implicit cast on append. ANSWERED (P0).** BIGINT casts exactly and is now allowed
  by rule 15; DATE truncates silently and stays rejected. See the table in 4.6.
- **Rule 15 at startup. ANSWERED (P6), by splitting the rule where the information splits.**
  The declared types of a target *table* are not reachable at startup on this classpath:
  `json_serialize_sql` parses DDL but serializes SELECT only, `EXPLAIN` binds a
  `CREATE TABLE` without emitting columns, and `PREPARE` takes no DDL. Executing a task's
  scratch `sql` steps in a boot sandbox was measured and rejected - 1.1.3 *is* cancellable
  (`Statement.cancel()` interrupts a runaway CTAS in ~200 ms) and `enable_external_access=false`
  blocks read, write and attach, so the containment objection does not stand - but 3.4's own
  `create index idx_wip_lot on wip_stg (lot_id)` and 5.4's own PL/SQL `sql` step both fail in
  a boot sandbox, so the rule would either silently switch off or refuse to boot a correct
  file. Rule 15 now enforces at startup the columns the file declares, and at writer open the
  columns a catalog or a result set declares.
- **Null task variable binding. ANSWERED (P5).** A null from a zero-row export binds with
  its export column's type. Untyped `Types.OTHER` was reachable the moment `export` shipped.
  No signature changed: measured, JDBI 3.45.4 binds an `Argument` value handed to `bindMap`
  directly, so `Map<String, Any?>` already carries a typed null.
- **Task variables in `target.sql`. ANSWERED (P5), by deleting the promise.** 6.3 offered
  them; the frozen `JdbcStatementWriter` could not express it, and the obvious amendment was
  refuted by measurement. The author projects the value into the source select list instead.
- **AUTO DECIMAL precision. ANSWERED (P2).** `ColumnMeta` widened with precision and scale;
  AUTO emits `DECIMAL(p,s)`; an unstated or unusable pair is a loud error at writer open. Bare
  `DECIMAL` resolves to `DECIMAL(18,3)`, which silently rounds past three decimals and cannot
  hold a 16-digit key at all, so the previous wording made AUTO unusable for ordinary Oracle
  `NUMBER(18)` keys.
- **S4. Spill factor and wide-row density. ANSWERED (P0).** Storage density is
  width-independent: 8.96 and 9.07 bytes per stored value at 15 and 30 columns, high
  entropy, holding total values constant; 1.0 to 1.4 at low entropy. Spill peaks at 3.3x to
  4.9x the bytes a query reads, flat in `memory_limit`, and is fully reclaimed on close even
  after a failure. `sizeLimit` set to 32 GiB in 7.2 from these constants. S4b also refuted
  7.2's `temp_directory` rationale; corrected there. The copy count is
  `(N - 1) + 1 + retries`, not `1 + retries`: a failing dataset's siblings occupy the same
  file. **What a failed attempt retains depends on where the failure came from** - measured
  on 1.1.3 during P2, three cases:

  | Shape before `close()` | Retained |
  |---|---|
  | every begun row completed with `endRow` | all of them |
  | a row left part-appended when an `append` threw | **nothing unflushed**, including rows already completed in that chunk |
  | `beginRow` with no values appended, then close | the completed rows |

  So a failed attempt keeps every chunk already flushed, plus the completed rows of the
  chunk in flight **only if no row was left part-appended**. A framework-detected error
  (wrong type, missing key, unwritable column) is rejected before `beginRow` and retains
  them; a driver-detected error inside an append - a DECIMAL value out of range, say -
  discards the whole in-flight chunk. P4's accounting must allow for both; the worst case
  is `floor(rows_written / chunkSize) * chunkSize`.
- **NOT NULL DATE on a DuckDB target. ANSWERED (P0).** Confirmed: rule 15 rejects DATE
  whether nullable or not. The 4.6 truncation does not depend on nullability, so a rule that
  reached only nullable columns would have left the same hazard open. DATE is not a
  supported DuckDB target column type; the author casts in source SQL, or uses TIMESTAMP.
  `LocalDate` remains a canonical type (4.1) on the read side; it simply never appears as a
  DuckDB write target.

---

## 13. Deferred

- **`scratch.scope: PHASE`.** One scratch file per phase instead of per run, closing and
  deleting the file at each phase boundary. This is the only way to get a real memory and
  disk release point mid-run. It requires declaring which datasets survive the boundary:

  ```yaml
  - name: extract
    carryOver: [wip_stg, lot_stg]   # copied into the next phase's file
  ```

  Everything not listed is discarded with the file. The cost is copying the carried
  datasets, which is why it is not the default: it trades run time for a release point that
  may not be needed. Decided by S2.
- **Producing snapshot cache generations from a YAML task.** Requires a dynamic write target
  (a target chosen at run time rather than a statically named datasource) and a matching
  start-of-run hook. Not needed while the cache calls Layer 1 directly (9.5).
- **After the base image is upgraded past glibc 2.23**: a newer `duckdb_jdbc` brings MERGE
  INTO for DuckDB targets, partial space reclamation at CHECKPOINT, and concurrent reads
  during checkpoint. Validation rule 15 and parts of 4.6 could then be relaxed.
- Non-scalar export variables.
- Parallel steps within a phase.
- Leader election for a multi-replica deployment.