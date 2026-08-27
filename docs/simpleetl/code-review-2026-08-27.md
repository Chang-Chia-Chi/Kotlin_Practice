# SimpleEtl Code Review — 2026-08-27

Scope: all main and test sources under `SimpleEtl/`, reviewed against
`docs/simpleetl/spec.md`, `docs/simpleetl/plan.md`, and the deviations recorded in
`docs/simpleetl/progress.md`. Method: 8 independent finder passes (line-by-line,
spec-contract, cross-file tracing, conventions, efficiency, simplification, reuse,
altitude), 44 raw candidates deduplicated to 37, each then adversarially verified
against the code and documents. 27 findings survived; 9 were refuted (kept in the
last section so they are not re-flagged).

Severity: **HIGH** = data corruption, silent data loss/duplication, deadlock, or a
spec-legal feature unreachable in production. **MEDIUM** = races with
operator-visible effects, validation gaps that defer boot failures to run time,
spec deviations, and drift traps likely to cause a future bug. **LOW** = cleanups
and micro-efficiency.

Each finding notes its verification verdict: CONFIRMED (defect reproduced by
reading the code; failure scenario reachable) or PLAUSIBLE (facts verified but the
defect is latent or judgment-dependent).

---

## HIGH

### H1. REQUIRED path never checks DECIMAL precision/scale — silent rounding
`SimpleEtl/src/main/kotlin/infra/etl/duckdb/DuckDbTableWriter.kt:164` — CONFIRMED

Under `createTable: REQUIRED`, `validate()` compares only the canonical type
(`sourceColumn.type == column.type`) for DECIMAL columns and never precision or
scale (the target's precision/scale are noted as "currently unused" in
progress.md). A source DECIMAL wider in scale than the author-created target
column is accepted at open and silently rounded by `appendBigDecimal` at write
time — the exact silent-rounding class the AUTO path's `ddlType` carefully
rejects, and documented as measured behavior in spec.md:310-312 (42.7 → 43 into
BIGINT) and the class KDoc.

Failure scenario: author creates a scratch table via a sql step
(`create table wip (qty DECIMAL(18,3))`, explicitly supported for REQUIRED by
spec.md:333-338) and pipes an Oracle NUMBER(38,10) column into it. `open()`
passes (both `CanonicalType.DECIMAL`); at write time 1.2345678901 is stored as
1.235 with no error, for every row.

Suggested fix: in the REQUIRED validate path, compare precision/scale for DECIMAL
pairs (reject a source wider in scale than the target), mirroring the AUTO
path's guard. No recorded deviation sanctions the current behavior.

### H2. Rule 12 (retries requires idempotent) unenforced for sql/materialize steps
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:464` — CONFIRMED

Spec 5.3 says "A **step** with a non-scratch target and retries > 0 must declare
idempotent: true" — a step, not a pipe — but the check exists only inside
`pipe()`. The `SqlYaml` branch (~line 365) and `materialize()` check nothing
about retries/idempotency, and `SqlYaml`/`MaterializeYaml` (TaskYaml.kt:123-137)
have no `idempotent` field at all. Meanwhile `TaskEngine.sql`
(TaskEngine.kt:281-284) re-runs ALL statements on a retry.

Failure scenario: a non-scratch sql step
`{datasource: report_oracle, statements: ['insert into audit_log ...', 'update watermark ...'], retries: 3}`
loads clean. A transient connection drop between the two committed statements
triggers a retry that re-executes the already-committed insert — silently
duplicated rows in the external table, then SUCCEEDED. progress.md P5 records
only the materialize case ("loud on table-already-exists"); the silent sql-step
duplication is recorded nowhere.

Suggested fix: add the `idempotent` field to sql/materialize YAML (or reject
non-scratch retries on those step types), and enforce rule 12 in all step
branches. Spec/schema change — record in progress.md.

### H3. External materialize with task variables fails on Oracle (ORA-01027)
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskEngine.kt:262` — CONFIRMED

A non-scratch materialize runs `create table <output> as <sql>` through
`update()`, which binds every parsed `:name` (TaskEngine.kt:383-387). Oracle
rejects bind variables in DDL outright (ORA-01027). Spec validation rule 7
(spec.md:1039) explicitly blesses variables in materialize SQL; the KDoc's
measured claim that CTAS accepts bound parameters (TaskEngine.kt:103-105) was
measured on duckdb_jdbc only; and TaskFileLoader's rule 6 checks non-scratch SQL
only structurally — so nothing catches this before the run.

Failure scenario: `materialize {datasource: report_oracle, output: wip_snap,
sql: "select * from wip where updated > :lastTs"}` passes boot validation and
fails ORA-01027 on every firing — non-transient, so the task is FAILED forever.
The identical step on scratch works, so it surfaces only in production.

Suggested fix: either interpolate validated variables textually for external
CTAS (with strict typing/quoting), or run the CTAS unbound and reject
variable-referencing external materialize SQL at boot (loud beats broken).
Either way the spec's rule 7 or the engine must change — record the decision.

### H4. Same-datasource pipe step holds two pool connections — deadlock
`SimpleEtl/src/main/kotlin/infra/etl/jdbc/JdbcWriters.kt:53` — CONFIRMED

`TaskEngine.readFrom` (TaskEngine.kt:423-425) acquires the source handle from
the pool and holds it while `RowPipe.pump` (RowPipe.kt:163-168) calls
`writer.open`, which acquires a second connection from the same `Jdbi` when
source and target name the same datasource. Nothing validates or documents the
2-connections-per-step requirement (progress.md:896 only requires the Jdbi to be
pool-backed), and spec 8.4/spec.md:842 allows concurrent tasks.

Failure scenario: pool max = 2; two tasks each hit a same-datasource pipe step
concurrently. Each acquires its read connection (pool exhausted), then each
blocks in `jdbi.open()` waiting for a writer connection the other will never
release — a circular wait. With no acquisition timeout both runs hang
indefinitely, each holding `busy=true`, so every subsequent firing of both tasks
is skipped as AlreadyRunning and the schedule silently stalls.

Suggested fix: document and validate a minimum pool size per datasource at boot,
or acquire the writer connection before opening the source cursor (ordered
acquisition), or use a single connection for same-datasource pipes.

---

## MEDIUM

### M1. Undeclared transform-added Row keys silently dropped on JDBC REQUIRED targets
`SimpleEtl/src/main/kotlin/infra/etl/jdbc/JdbcWriters.kt:65` — CONFIRMED

Spec 4.4 promises "a Row key with no matching column is a runtime error naming
step, column, and row ordinal" (spec.md:330-331), but `JdbcTableWriter` computes
`binds` as the intersection of open()-time source columns and the target
catalog: a transform-added key not declared in `addColumns` is silently omitted.
Rule 14 forces `addColumns` only under AUTO (TaskFileLoader.kt:430-437,
spec.md:904), so a REQUIRED JDBC target with an undeclared transform column
passes validation. DuckDbTableWriter under REQUIRED *does* land the same column
(DuckDbTableWriter.kt:87,178, matching progress.md:360), so behavior also
diverges between target types. progress.md sanctions only the NOT
NULL/COLUMN_DEF half and the declared-addColumns path — not this.

Failure scenario: REQUIRED Oracle-target pipe, transform adds `row_hash`, no
`addColumns`: every row inserts with ROW_HASH = NULL, silently. If the target
lacks the column entirely, the promised runtime error never fires.

Suggested fix: at write time (or first chunk), compare `row.columns` against
`binds` and raise the spec-promised error for unmatched keys; or extend rule 14
to require `addColumns` under REQUIRED whenever a transform is present.

### M2. catalogColumns passes the schema as a JDBC wildcard pattern
`SimpleEtl/src/main/kotlin/infra/etl/pipe/RowWriter.kt:88` — CONFIRMED

The schema argument to `DatabaseMetaData.getColumns` is a pattern where `_` is a
single-character wildcard. `TABLE_NAME` is exact-compared (line 90) but
`TABLE_SCHEM` is never compared against the requested schema. The P2 one-owner
guard (line 105, progress.md:243-248) closes the unqualified-name case but not
this one.

Failure scenario: schemas ETL_STG and ETL1STG both hold table WIP; a step
targeting `etl_stg.wip` matches both, trips `byOwner.size == 1`, and fails with
the dead-end "Qualify the target with its schema" although it already is
qualified. If only the wrong wildcard-matched schema holds the table, its column
list is silently used.

Suggested fix: escape `_`/`%` in the schema (and table) pattern arguments, or
exact-compare `TABLE_SCHEM` the same way `TABLE_NAME` is compared.

### M3. Chunk loop order deviates from spec 5.2 (transform before accumulate)
`SimpleEtl/src/main/kotlin/infra/etl/pipe/RowPipe.kt:176` — CONFIRMED

Spec 5.2 (and RowPipe's own KDoc at lines 93-94) fixes the order: accumulate up
to chunkSize rows, then transform, then write/commit. `pump()` applies the
transform before accumulation, so chunks fill with chunkSize *surviving* rows.
No progress.md deviation records the reordering, and no test pins either order.

Failure scenario: a selective transform dropping 999/1000 rows with chunkSize
5000 means one commit spans 5,000,000 source rows: a transient failure 4M rows
in commits nothing and the retry re-reads the whole span; a bad `:name` in a
statement target (first-chunk bind check) surfaces only after millions of source
rows instead of after 5000.

Suggested fix: either restore the spec order (accumulate source rows, then
transform the chunk) or record the deviation deliberately in progress.md with
the retry-span consequence stated. Spec-numbered loop — documents win.

### M4. Scheduler registers callbacks before publishing definitions; firings lost/stale
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskScheduler.kt:106` — CONFIRMED

`apply()` registers cron callbacks with the host (line 111, loop at 79-95)
before assigning the volatile `current` map at line 106; `fire()` (line 119)
reads `current` unsynchronized on the host scheduler thread and silently returns
on null. The fire() KDoc sanctions the null only for a task *removed* by a
concurrent reload.

Failure scenario: at startup, a task registered just before its cron boundary
fires while `current` is still emptyMap — the run is silently lost,
indistinguishable from the intentional removed-task skip. After a reload, a
firing in the window can run the stale pre-reload definition, and its busy flag
then rejects the corrected definition's real firing as AlreadyRunning.

Suggested fix: publish `current` before registering callbacks (stale-window
shrinks to the map swap), or have fire() consult the definitions being applied.

### M5. trigger()/reload() admission race starts runs of disabled tasks
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskAdmin.kt:63` — CONFIRMED

`trigger()` (lines 62-66) is an unsynchronized check-then-act over the volatile
`definitions` map; `reload()` is `@Synchronized` only against itself. A trigger
thread paused between the read and `runner.submit()` starts a run of the
pre-reload definition after reload returned success. Spec 8.5's "a task
currently running keeps the definition it started with" (spec.md:852-853)
covers runs already started, not admission.

Failure scenario: operator disables a corrupting task, reloads, gets success; a
concurrent API trigger then launches a 30-minute run of the old enabled
definition — the corruption the disable was meant to stop — while list() shows
the task disabled.

Suggested fix: synchronize trigger()'s read-check-submit against reload() (or
re-check the current map inside submit under the runner's own admission).

### M6. One empty scratch directory leaks per run, forever
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskEngine.kt:164` (with `ScratchDb.kt:158-169`) — CONFIRMED

Each run resolves a unique per-runId scratch directory (TaskEngine.kt:158);
`ScratchDb.deleteContents` filters `it != root` and the KDoc (ScratchDb.kt:28-32)
says the directory itself is left. No caller anywhere deletes it. (Spec 7.2's
"closed and deleted" strictly refers to the DB file, which is deleted — so this
is a leak, not a literal spec violation.)

Failure scenario: a scratch-touching task at 10-minute cadence accumulates
~52,000 empty directories/year on the size-bounded volume; nothing reclaims
them, restarts don't help; inode/dirent growth eventually slows creation and can
exhaust inode quotas with near-zero byte usage.

Suggested fix: delete the run directory (and its `spill` subdir) in the same
finally that closes the ScratchDb — one `Files.delete` on the now-empty root.

### M7. Missing/empty `phases` loads clean and reports green no-op runs
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskYaml.kt:42` — CONFIRMED

`TaskYaml.phases` defaults to `emptyList()` (and `PhaseYaml.steps` at line 52
likewise), though spec 3.1 annotates every optional field `# optional` and
`phases` carries no such annotation (spec 11.2 also declares
`TaskDefinition.phases` with no default). None of the 18 validation rules
rejects empty phases or steps; `TaskEngine.run` (line 163) iterates nothing and
returns SUCCEEDED.

Failure scenario: an author forgets the phases block (or leaves `phases: []`
from a template): the task schedules and reports SUCCEEDED every 10 minutes
while the downstream table silently stops updating — the exact 03:00 failure
mode spec 1.1 exists to prevent.

Suggested fix: validation rule rejecting a task with zero phases or a phase with
zero steps (or make the DTO fields nullable and required).

### M8. Rule 15's DuckDB type check applied to non-DuckDB targets
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:482` — CONFIRMED

`addColumn` runs `unwritableToDuckDb` on every `transform.addColumns` entry with
no condition on `target.datasource`, rejecting at boot types that
`JdbcWriters.javaType` (JdbcWriters.kt:208-219) binds fine on Oracle: nullable
DOUBLE, DATE, INSTANT, BYTES. Rule 15's own spec scope is "DuckDB target column
types (4.6)", and the P6 amendment's premise (AddColumnYaml KDoc: "the only
target an added column can reach is DuckDB") is false — a REQUIRED Oracle target
with a transform is legal and P5 deviation 6 wired addColumns into
JdbcTableWriter.

Failure scenario: combined with M1, an Oracle-target transform-added DOUBLE
column is inexpressible — undeclared it is silently NULLed; declared it fails
startup with a DuckDB-specific error.

Suggested fix: apply rule 15 only when the pipe's target datasource is
scratch/DuckDB; validate JDBC-writable types separately if desired.

### M9. `scratch.memoryLimitMb` unvalidated at boot; fails on first run
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:352` — CONFIRMED

The loader validates negative retries (line 359) and non-positive chunkSize
(line 352) precisely to convert mid-run failures into boot failures (P6
deviation 6), but never checks `scratch.memoryLimitMb`; `ScratchDb.kt:65` has
`require(memoryLimitMb > 0)` which fires at run start (ScratchDb constructed in
TaskEngine.run:161).

Failure scenario: `scratch: {memoryLimitMb: 0}` (author meant "unlimited") loads
clean; every scheduled run fails with IllegalArgumentException at 03:00 —
exactly the deferred-failure class the P6 checks were added to close.

Suggested fix: add the boot check alongside the chunkSize/retries checks.

### M10. Datasource-dependent defaults re-derived at ~10 sites
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:414` — CONFIRMED

The retries default (scratch→3 else 0, spec 5.3) and createTable default
(scratch→AUTO else REQUIRED, spec 4.4) are independently re-derived in:
FileValidation.pipe (TaskFileLoader.kt:414, 420), toStep
(TaskFileLoader.kt:702-703, 714, 724, 731), and the constructor defaults in
TaskDefinition.kt (86-87, 126, 145, 156). No shared helper; no recorded
deviation.

Failure scenario: a default change applied to toStep but not FileValidation
makes rules 12/14/18 judge resolved values the engine never runs (file passes
boot, fails mid-run — or is rejected while safe); applied to toStep but not the
constructors, YAML-built and code-built definitions diverge for identical input.

Suggested fix: `internal fun defaultRetries(datasource: String)` and
`defaultCreateTable(datasource: String)` next to `SCRATCH` in TaskDefinition.kt,
referenced by all ten sites.

### M11. Built-in variable set encoded twice (loader vs engine)
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:24` — CONFIRMED

`BUILT_INS = setOf("runId", "taskName", "triggerTime", "attempt")` (rule 7's
seed) versus TaskEngine's imperative defines of runId/taskName/triggerTime
(TaskEngine.kt:179-183) plus the private `ATTEMPT` constant (line 33)
special-cased in `variables()` (line 407) and `VariableScope.define` (line 65).
Both are file-private; nothing shared.

Failure scenario: P8 adds a built-in (e.g. `triggerSource`) to the engine only —
rule 7 rejects valid files at boot; the reverse drift boots clean and dies
mid-run with "no built-in ... has defined", the mid-run failure spec 10 exists
to convert into boot failures.

Suggested fix: one `BUILT_IN_VARIABLES` declaration owned by the task model
(with `attempt` flagged per-attempt), seeded by the engine and imported by rule 7.

### M12. Scratch-ness string-compared at six sites, each re-pairing connection discipline
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskEngine.kt:424` — CONFIRMED

`datasource == SCRATCH` / `!= SCRATCH` at TaskEngine.kt:228, 254, 356, 363/372,
424, 429 — each site separately re-pairs the answer with the correct connection
discipline (single write connection for statements/appends vs `duplicate()` for
streaming reads vs Jdbi map lookup). The reserved-name require is also
duplicated (TaskEngine.kt:132-137 vs TaskFileLoader.kt:205-210). Spec 7.2
(spec.md:667-669) and readFrom's KDoc confirm a mispairing is a JVM crash, not a
catchable error. No current site is wrong; P9's CacheCopyStep (seam at
TaskEngine.kt:214) will re-derive the pairing at a seventh site.

Suggested fix: resolve the name once per step into a small sealed type —
`Scratch(scratchDb)` / `External(jdbi)` — exposing `readHandle()` /
`statementHandle()`, so the rule is decided in exactly one place.

---

## LOW

### L1. BLOB read: `length().toInt()` truncation + locator leak on throw
`SimpleEtl/src/main/kotlin/infra/etl/pipe/RowMapper.kt:109` — CONFIRMED

`value.getBytes(1, value.length().toInt()).also { value.free() }`: `toInt()`
silently truncates for lengths ≥ 2^31 (a >4GiB BLOB yields a positive wrapped
int and a truncated array), and `.also` only runs on success so `free()` is
skipped when getBytes throws. Edge cases (a ≥2GiB BLOB cannot fit a Java byte[]
anyway; an aborting run's connection reclaims the locator) — but cheap to
harden: try/finally around free(), range-check the length with a step/column-
naming error.

### L2. Unknown-source-columns check duplicated verbatim in both table writers
`SimpleEtl/src/main/kotlin/infra/etl/duckdb/DuckDbTableWriter.kt:148` and
`SimpleEtl/src/main/kotlin/infra/etl/jdbc/JdbcWriters.kt:60` — CONFIRMED

Same set-difference and byte-identical message ("the source produces columns
$unknown which table ... does not have. Drop them in the source SQL, or add them
to the table."). Both already share `catalogColumns` in RowWriter.kt — lift a
shared `requireSourceSubset(source, targetNames, table, step)` beside it.

### L3. DECIMAL(p,s) predicate left out of the P6 shared-code lift
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:513` and
`SimpleEtl/src/main/kotlin/infra/etl/duckdb/DuckDbTableWriter.kt:136` — CONFIRMED

`precision in 1..38 && scale in 0..precision` appears verbatim in both, though
P6 deviation 7 lifted the rest of rule 15 into shared `unwritableToDuckDb()`
(DuckDbTableWriter.kt:251) precisely so startup and writer-open decide with the
same code. Extend the predicate family (e.g. `invalidDecimalPair(p, s)`).

### L4. Named-parameter parsing triplicated
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskEngine.kt:398`,
`SimpleEtl/src/main/kotlin/infra/etl/jdbc/JdbcWriters.kt:141`,
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:629` — CONFIRMED

Three copies of ColonPrefixSqlParser-parse-plus-positional-'?'-rejection with
near-identical errors citing different spec sections. TaskEngine and
JdbcStatementWriter each build a throwaway `handle.createUpdate(sql)` solely for
a StatementContext that TaskFileLoader's SQL_PARSER KDoc (lines 39-46) proves
unnecessary. One shared `parseNamedParameters(sql)` next to SQL_PARSER.

### L5. Handle open/close lifecycle copy-pasted between the two JDBC writers
`SimpleEtl/src/main/kotlin/infra/etl/jdbc/JdbcWriters.kt:52` — CONFIRMED

Near byte-identical: open guard (52-54 vs 136-137), 8-line
close-with-addSuppressed catch (75-82 vs 150-157), checkNotNull in write (86 vs
161), idempotent close (98-102 vs 183-187). A protocol fix must land twice with
no pinning test; internal structure is FREE, so a shared base/helper is allowed.

### L6. Statement re-prepared every chunk
`SimpleEtl/src/main/kotlin/infra/etl/jdbc/JdbcWriters.kt:88` (and 173) — CONFIRMED

`prepareBatch(sql)` created and closed per 5000-row chunk; per-chunk commit
comes from autoCommit on executeBatch, not statement lifetime, so one prepared
batch could be reused. Verified minor: ojdbc defers the parse to execution and
spec's own wording says "a JDBI prepared batch, once per chunk" — fix
opportunistically or enable ojdbc implicit statement caching.

### L7. Per-row lowercase + map lookups in the innermost bind loop
`SimpleEtl/src/main/kotlin/infra/etl/jdbc/JdbcWriters.kt:175` — CONFIRMED

`it.lowercase()` plus a types-map lookup per bind name per row, and
`bindColumn`'s `row[name]` lowercases again (Row.kt:29). All precomputable at
open()/first chunk; verified micro against batch-insert I/O.

### L8. Oracle test duplicates its own phase's fixture helpers
`SimpleEtl/src/test/kotlin/infra/etl/pipe/RowPipeOracleTest.kt:66` — CONFIRMED

Private `exec()`/`count()` byte-identical to `Pipe.exec`/`Pipe.rowCount` in
`PipeFixtures.kt` (same phase, already imported by this very file for DuckDB
connections; both take plain `java.sql.Connection`). Unlike the sanctioned
cross-phase fixture copies this intra-phase copy has no recorded justification —
delete the private helpers.

### L9. Three-loop parsed/unparsed stitch in load() relies on convention
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskFileLoader.kt:250` — PLAUSIBLE

Two parallel LinkedHashMaps plus a third walk stitching via
`unparsed[name] ?: parsed.getValue(name)`. No failure reachable today (the
invariant holds within one if/else body); latent NoSuchElementException risk
under future edits. A single ordered list of a Parsed/Failed sum type removes
the convention.

### L10. Two parallel definition maps swapped non-atomically
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskAdmin.kt:101` (with TaskScheduler.kt:52) — PLAUSIBLE

`TaskAdmin.definitions` and `TaskScheduler.current` are populated from the same
loaded list in two separate assignments; the microsecond skew is behaviorally
equivalent to triggers straddling the reload boundary and the maps re-converge
after the KDoc-directed apply. Simplification: scheduler takes a lookup
`(String) -> TaskDefinition?` backed by the single authoritative map.

### L11. Wall-clock `Instant.now()`; triggerTime and run timestamps untestable
`SimpleEtl/src/main/kotlin/infra/etl/task/TaskEngine.kt:182` (and TaskRunner.kt:116, 192, 203) — PLAUSIBLE

Verified that the repo CLAUDE.md injected-Clock rule governs the snapshotcache
framework, not SimpleEtl, and docs/simpleetl states no time rule — no contract
violated. What remains is the testability gap: `triggerTime` binding and run
start/finish/duration cannot be asserted deterministically. The engine's
constructor (free per spec 11.2) is a zero-cost injection point for a
`java.time.Clock`.

---

## Refuted candidates (do not re-flag)

These were raised by finder passes and killed in verification. Recorded so a
follow-up session does not rediscover them.

1. **Row backed by LinkedHashMap / per-row map allocation / copy-on-edit cost**
   (Row.kt, RowMapper.kt) — spec 4.2 (spec.md:234) fixes
   `class Row internal constructor(private val values: LinkedHashMap<String, Any?>)`
   and "Row is immutable; with and without return copies" verbatim. The
   internals are NOT free to change to an array-backed form; documents win.
2. **Row lowercases on every lookup** (Row.kt:29) — the premise "all hot callers
   pass lowercase names" is false: JdbcStatementWriter binds raw SQL bind names,
   so the normalization is load-bearing; and `String.toLowerCase` returns the
   same instance for already-lowercase strings (no allocation).
3. **Nullable BIGINT routed through appendBigDecimal** (DuckDbTableWriter.kt:186)
   — spec.md:439 prescribes `appendBigDecimal(row.long(col.name)?.toBigDecimal())`
   verbatim and progress.md:85-87 explicitly mandates dispatching nullable
   BIGINT through appendBigDecimal, not append(long). Sanctioned design.
4. **rowsAffected counts EXECUTE_FAILED as written** (JdbcWriters.kt:205) — per
   the JDBC contract, executeBatch throws BatchUpdateException whenever a
   command fails; EXECUTE_FAILED appears only in the exception's
   getUpdateCounts(), never in a normally returned array. Unreachable on a
   conformant driver (Oracle stops and throws).
5. **TaskSlot leak for removed tasks** (TaskRunner.kt:156) — retention is
   design-sanctioned: TaskAdmin.run's KDoc ("the record belongs to the run, not
   to the definition") and progress.md:891-892 record the one-run ponytail;
   growth is bounded by distinct task names ever loaded.
6. **ClassCastException from triple target dispatch** (TaskEngine.kt:250) —
   PipeTarget is sealed and writer()'s `when` is an expression, so a new subtype
   is a compile error; the cast is guarded by `physical != null` which
   physicalDataset's `as? TableTarget` makes safe. Minor smell only.
7. **chunkSize DTO default 5000 as dead code** (TaskYaml.kt:37) — the KDoc's
   null-for-not-stated rule covers only defaults that depend on another field;
   the fixed 5000 is pinned identically by spec 3.1/11.1/11.2 and the
   constructor default serves the programmatic path.
8. **Duplicate-column detection duplicated** (RowWriter.kt:111 vs RowMapper.kt:65)
   — the copies live in different frozen phases (P2 vs P1) guarding different
   inputs; cross-phase copying instead of editing earlier-phase files is this
   project's sanctioned pattern.
9. **Injected-Clock convention violation** (as a conventions breach) — the rule
   belongs to the snapshotcache framework docs, not SimpleEtl; survives only as
   the L11 testability note.

---

## Conventions sweep (clean)

No `Thread.sleep` in tests (backoff sleeper injected and overridden), no Quarkus
imports anywhere, DuckDB pinned to 1.1.3 in `SimpleEtl/pom.xml`, no
`CREATE TEMP TABLE` (runtime guard + NoTempTableTest), no bare
`appender.append(null)`, no post-1.1.3 appender APIs, coroutine-name test avoids
the forbidden thread-name assertion.

## Suggested fix order for a follow-up session

1. H1–H4 (each is a small, well-scoped change; H2 and H3 need a spec/progress.md
   decision recorded first).
2. M4/M5 (both are small synchronization fixes in the reload path), M6 (one-line
   directory delete), M9 (one validation clause), M7 (one validation rule),
   M2 (pattern escaping).
3. M1/M8 together (they interlock: the addColumns/REQUIRED story), M3 (decide:
   restore spec order or record deviation).
4. M10–M12 refactors, then the LOW list opportunistically.
