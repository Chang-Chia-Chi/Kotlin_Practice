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
 * Spec 5.3's `retries` default: 3 for a step writing into scratch, 0 for anywhere else.
 *
 * Scratch may be retried freely because spec 5.5 gives each attempt its own suffixed dataset name,
 * so a failed attempt's rows are unreferenced rather than mixed into the next one's. An external
 * datasource has no such protection, so the default is not to retry and rule 12 makes an author who
 * wants one say that a rerun converges.
 *
 * Here, next to [SCRATCH], because both defaults were re-derived independently at ten sites - the
 * loader's validation, the loader's model building, and the model's own constructors - and a change
 * applied to some of them would make validation judge a value the engine never runs, or make a
 * YAML-built definition differ from a code-built one for identical input (review finding M10).
 *
 * Since E10 a step that states no `retries` carries null, and `TaskRules.retries` is what turns
 * that into a number on the run path (spec 5.3, 10). **`CacheCopyStep` still does not use this**:
 * it resolves to 0, because no failure a cache copy can produce is transient under spec 5.3 and
 * rule 20 rejects a stated non-zero value. Both paths now agree on that 0, which is what retired
 * the model-versus-YAML asymmetry rule 20 used to record.
 */
fun defaultRetries(datasource: String): Int = if (datasource == SCRATCH) 3 else 0

/**
 * Spec 4.4's `createTable` default: AUTO for a scratch target, REQUIRED for anywhere else.
 *
 * AUTO generates DuckDB DDL from source metadata, which is only meaningful where the target is
 * DuckDB; off scratch the author owns the table and the framework requires it to exist. Shared for
 * the reasons given on [defaultRetries].
 */
fun defaultCreateTable(datasource: String): CreateTable =
    if (datasource == SCRATCH) CreateTable.AUTO else CreateTable.REQUIRED

/**
 * The built-in that changes between attempts of the same step, so it never lives in a
 * [VariableScope]: the engine supplies its value per attempt (spec 6.1).
 */
const val ATTEMPT_VARIABLE = "attempt"

/**
 * The four built-in variables of spec 6.1. Always defined, never redefinable.
 *
 * One declaration, because there were two: validation rule 7 seeded its resolvable-name set from a
 * private copy in `TaskFileLoader` while the engine defined the same names imperatively and kept
 * `attempt` in a private constant of its own (review finding M11). Drift in either direction was
 * silent and asymmetric - a built-in added to the engine alone makes rule 7 reject valid files at
 * boot, and one added to the rule alone boots clean and then dies mid-run on "no built-in ... has
 * defined", which is exactly the mid-run failure spec 10 exists to convert into a boot failure.
 *
 * [defineRunBuiltIns] holds the values, next to the names, and checks the two agree.
 */
val BUILT_IN_VARIABLES: Set<String> = setOf("runId", "taskName", "triggerTime", ATTEMPT_VARIABLE)

/**
 * Seeds the three built-ins whose value is fixed for a whole run. [ATTEMPT_VARIABLE] is the fourth
 * and is deliberately absent: a scope defines each name exactly once, and this one changes between
 * attempts of one step.
 *
 * The `check` is what makes [BUILT_IN_VARIABLES] a single source rather than a second copy. Adding
 * a name to that set without giving it a value here fails every run immediately and by name,
 * instead of at whichever later step first writes `:theNewOne`.
 */
internal fun VariableScope.defineRunBuiltIns(runId: String, taskName: String, triggerTime: Any?) {
    define("runId", runId)
    define("taskName", taskName)
    define("triggerTime", triggerTime)
    check(names + ATTEMPT_VARIABLE == BUILT_IN_VARIABLES) {
        "the built-ins this run defined are ${(names + ATTEMPT_VARIABLE).sorted()} but spec 6.1 declares " +
            "${BUILT_IN_VARIABLES.sorted()}. Both come from BUILT_IN_VARIABLES, so one of them was " +
            "changed without the other."
    }
}

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
 * [retries] counts *additional* attempts after the first. **null means "not stated"** and takes
 * spec 5.3's datasource-dependent default - 3 when the step writes into [SCRATCH], 0 everywhere
 * else - resolved by `TaskRules` rather than by a constructor default, because the value depends on
 * another field and a Kotlin default cannot (spec 5.3, 11.2). Retrying anything off scratch needs
 * the author to say `idempotent: true`, which is validation rule 12.
 */
sealed interface Step {
    val name: String
    val retries: Int?
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
    val createTable: CreateTable = defaultCreateTable(datasource),
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
    override val retries: Int? = null,
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
    override val retries: Int? = null,
) : Step

/**
 * Statements with no dataset output: an index, a publish procedure, a bookkeeping update
 * (spec 3.4). Each statement is its own transaction (spec 5.2), so a retry re-runs all of them.
 *
 * @param idempotent the author's statement that re-running the whole list converges - spec 10's
 *   rule 12, which reads "a step with a non-scratch target and retries > 0" and until review
 *   finding H2 was enforced only on a pipe. Nothing here can check it; what it buys is that the
 *   duplicate rows a retry can leave in an external table are a consequence someone chose.
 */
class SqlStep(
    override val name: String,
    val datasource: String,
    val statements: List<String>,
    override val retries: Int? = null,
    val idempotent: Boolean = false,
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
    override val retries: Int? = null,
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
 * [retries] resolves to **0** and not to the 3 a scratch output would otherwise earn, because it
 * could never fire anyway: spec 5.3's retry classification is JDBC-shaped and a local DuckDB copy
 * raises none of it. From P5 to P9 this field declared 3 while the loader resolved 0 for the same
 * step type, an asymmetry rule 20 recorded and both sites commented; with null representable, both
 * paths resolve the same and the asymmetry is gone.
 *
 * **Rule 20 itself is a startup rule and does not run here.** It rejects a *stated* non-zero value
 * in a task file, and a definition built in code has no file and no author's word to read - so a
 * caller may still write `retries = 3` and this engine will honour it, harmlessly, since nothing a
 * cache copy raises is transient. `TaskRules` records the split.
 */
class CacheCopyStep(
    override val name: String,
    val cache: String,
    val sql: String,
    val output: String,
    override val retries: Int? = null,
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
