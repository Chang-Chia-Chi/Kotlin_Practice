package infra.etl.task

import infra.etl.duckdb.CreateTable
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.RowTransform

/**
 * The one reserved datasource name (spec 7.1): the per-run DuckDB working file, not a configured
 * Jdbi bean. It is also what decides three defaults, because "is this step writing into scratch"
 * is the same question in all three cases: `createTable` (spec 4.4), `retries` (spec 5.3), and
 * whether a dataset gets the attempt-suffixed name and stable view of spec 5.5.
 */
const val SCRATCH = "scratch"

/**
 * One ETL task (spec 3.1). YAML is one source of these and not the only one, so every field is
 * constructible in code and P6's loader is just another caller (spec 2.1).
 *
 * @param chunkSize the task-level default. A step may lower or raise it; the resolution order is
 *   step, then task, then 5000 (spec 5.2).
 * @param scratchMemoryLimitMb null takes the application-wide default the engine was built with.
 * @param logging, onSuccess, onFailure, cron, enabled carried but not acted on here: listeners and
 *   hooks are P8, scheduling and the API are P7.
 */
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

/**
 * An ordered, named group of steps. It has no transactional and no concurrency meaning: a failure
 * in phase 2 leaves phase 1's external writes committed (spec 2.2, 5.4). Its purpose is grouping
 * in logs and metrics.
 */
data class Phase(val name: String, val steps: List<Step>)

/**
 * The unit of work, retry and logging (spec 2.2). Five executable types, [CacheCopyStep] included
 * since P9 built its executor.
 *
 * [retries] counts *additional* attempts after the first, and defaults to 3 when the step writes
 * into [SCRATCH] and 0 everywhere else (spec 5.3). Retrying anything else needs the author to say
 * `idempotent: true`, which is validation rule 12 and P6's to enforce; the field exists here so
 * that the assertion has somewhere to live.
 */
sealed interface Step {
    val name: String
    val retries: Int
}

/** Where a [PipeStep] reads from: one datasource, one query (spec 3.2). */
data class PipeSource(val datasource: String, val sql: String)

/**
 * Where a [PipeStep] writes. Exactly one of `target.table` and `target.sql` is present in YAML
 * (validation rule 10); expressing that as two types rather than two nullable fields is what
 * makes the rule unrepresentable rather than merely checked.
 *
 * @param idempotent the author's assertion that a rerun converges, required by validation rule 12
 *   whenever a non-scratch target is retried. The framework cannot verify it (spec 5.3).
 */
sealed interface PipeTarget {
    val datasource: String
    val idempotent: Boolean
}

/**
 * The declarative form: a table, filled by column name against the target's catalog metadata.
 *
 * `createTable` defaults to [CreateTable.AUTO] inside scratch and [CreateTable.REQUIRED] outside
 * it (spec 4.4). Only an AUTO scratch table gets the attempt-suffix treatment of spec 5.5: under
 * REQUIRED the author created the table under its stable name with a `sql` step, so there is no
 * suffixed name for the framework to write and no view for it to repoint.
 */
class TableTarget(
    override val datasource: String,
    val table: String,
    val createTable: CreateTable =
        if (datasource == SCRATCH) CreateTable.AUTO else CreateTable.REQUIRED,
    override val idempotent: Boolean = false,
) : PipeTarget

/**
 * The statement form: the author's own MERGE or conditional INSERT, run as a prepared batch once
 * per chunk with Row values bound by name (spec 4.4). Not available on a DuckDB datasource, which
 * validation rule 11 rejects at startup and [TaskEngine] rejects at run time.
 *
 * **Every `:name` here is a Row key. Task variables are not available** (spec 6.3): a statement
 * target runs once per row, so a task variable the statement needs is projected into the source
 * query's select list - `select lot_id, qty, :siteCode as site_code from wip` - where
 * [JdbcSource] binds it and it arrives as an ordinary lower-cased Row key. One namespace, so a
 * `:name` has exactly one meaning and a name the source does not produce is the runtime error
 * [JdbcStatementWriter] already raises.
 */
class StatementTarget(
    override val datasource: String,
    val sql: String,
    override val idempotent: Boolean = false,
) : PipeTarget

/**
 * The only step where rows pass through the JVM (spec 2.3). Implemented by constructing a
 * [RowPipe], so chunking, per-chunk commit and type mapping are Layer 1's.
 *
 * @param addColumns the columns [transform] adds, which source metadata cannot describe
 *   (spec 9.1, validation rule 14). They are appended to the column list the target is opened
 *   with, so `createTable: AUTO` generates DDL for them and a declarative target binds them.
 *   Without the declaration the value is silently dropped under AUTO and by [JdbcTableWriter].
 * @param chunkSize overrides the task-level default for this step alone (spec 5.2).
 */
class PipeStep(
    override val name: String,
    val source: PipeSource,
    val target: PipeTarget,
    val transform: RowTransform? = null,
    val addColumns: List<ColumnMeta> = emptyList(),
    val chunkSize: Int? = null,
    override val retries: Int = if (target.datasource == SCRATCH) 3 else 0,
) : Step

/** How a [MaterializeStep] stores its output (spec 5.6). PARQUET is scratch-only. */
enum class MaterializeFormat { TABLE, PARQUET }

/**
 * Computes a derived dataset inside one datasource, entirely in the engine: `CREATE TABLE ... AS
 * SELECT`, or `COPY (...) TO ... (FORMAT PARQUET)` (spec 3.3, 5.6). No row passes through the JVM.
 *
 * Inside scratch the output is written under an attempt-suffixed name and published as the stable
 * view [output], so `format` can change without touching any other step (spec 5.5).
 */
class MaterializeStep(
    override val name: String,
    val datasource: String,
    val output: String,
    val sql: String,
    val format: MaterializeFormat = MaterializeFormat.TABLE,
    override val retries: Int = if (datasource == SCRATCH) 3 else 0,
) : Step

/**
 * Statements with no dataset output: an index, a publish procedure, a bookkeeping update
 * (spec 3.4). Each statement is its own transaction (spec 5.2), so a retry re-runs all of them.
 */
class SqlStep(
    override val name: String,
    val datasource: String,
    val statements: List<String>,
    override val retries: Int = if (datasource == SCRATCH) 3 else 0,
) : Step

/** One exported task variable: a query returning exactly one row and one column (spec 6.3). */
data class ExportVar(val name: String, val sql: String)

/**
 * Produces task variables (spec 3.5). Every variable of one step is defined only once all of that
 * step's queries have succeeded, so a retry after a partial success does not trip spec 6.2's
 * "a variable may not be redefined once set" and mask the real failure.
 */
class ExportStep(
    override val name: String,
    val datasource: String,
    val vars: List<ExportVar>,
    override val retries: Int = 0,
) : Step

/**
 * The file-to-file copy out of a snapshot cache generation into scratch (spec 3.6, 7.3). [cache]
 * is a name the host binds to a `(SnapshotCache, GroupId)` pair - see [CacheBinding] - and
 * [output] is an ordinary scratch dataset, so it gets spec 5.5's attempt-suffixed table and stable
 * view like any other.
 *
 * [sql] runs inside the cache's own DuckDB instance and binds **no** variable: `CopyOutSpec.sql`
 * is a plain string with no binding channel, so a `:name` here is rejected - by validation rule 19
 * for a task file, and by the executor for a definition built in code (spec 2.1). A task needing a
 * variable copies the wider subset and filters in the following `materialize`.
 *
 * [retries] keeps the 3 every scratch-targeted step declares, frozen since P5. It can never fire:
 * spec 5.3's retry classification is JDBC-shaped and a local DuckDB copy raises none of it. The
 * **YAML** default is 0 and a stated non-zero value is rejected by rule 20 - the asymmetry is
 * deliberate, because inheriting the 3 here would make every task file that omits `retries` fail
 * that rule on a value nobody wrote.
 */
class CacheCopyStep(
    override val name: String,
    val cache: String,
    val sql: String,
    val output: String,
    override val retries: Int = 3,
) : Step

/**
 * A task-level literal variable (spec 6.1). The value is whatever the YAML scalar was, and it may
 * not be null: null carries no type, and spec 1.3 makes an untyped value an error rather than a
 * guess. An author who wants SQL NULL writes `null` in the query. Validation rule 8 says the same
 * thing at startup; this is the same rule where a definition built in code also meets it.
 */
data class LiteralVar(val name: String, val value: Any?) {
    init {
        require(value != null) {
            "literal var '$name' has a null value. Null carries no type, so it cannot be bound as one " +
                "(spec 6.1, validation rule 8). Write 'null' in the SQL that needs it."
        }
    }
}

/** What started a run. Carried into logs and metrics from P7 onwards (spec 8.2). */
enum class TriggerSource { SCHEDULE, API }

/** The result of a run (spec 9.2). */
enum class Outcome { SUCCEEDED, FAILED }

/** What one [TaskEngine.run] did. [failure] is the exception that failed the run, if any. */
data class TaskOutcome(val runId: String, val outcome: Outcome, val failure: Throwable?)
