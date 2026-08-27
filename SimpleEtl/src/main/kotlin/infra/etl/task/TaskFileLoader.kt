package infra.etl.task

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.datasetIdentifier
import infra.etl.duckdb.isDuckDbDecimalPair
import infra.etl.duckdb.unwritableToDuckDb
import infra.etl.pipe.CanonicalType
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.RowTransform
import infra.etl.pipe.parseNamedParameters
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

/** Spec 3.1: `[a-z0-9-]{1,64}`. */
private val TASK_NAME = Regex("[a-z0-9-]{1,64}")

/**
 * One field of a Quartz cron expression, of which spec 8.1's schedule has six or seven.
 *
 * ponytail: structural only - it rejects an empty expression, a five-field Unix cron, and a field
 * holding characters no cron field can hold, but it does not range-check `0 99 * * * ?`. There is
 * no cron parser on this module's classpath and the phase brief forbids adding one. P7 brings
 * `quarkus-scheduler` and with it a real parser; swap this for it there.
 */
private val CRON_FIELD = Regex("[0-9*?/,\\-A-Za-z#]+")

/**
 * Measured on jackson-dataformat-yaml / jackson-databind 2.18.2 (P6 scratchpad `Probe.kt`,
 * `Probe2.kt`), because none of it was assumed:
 *
 * - `FAIL_ON_UNKNOWN_PROPERTIES` is already true by default. It is enabled explicitly anyway,
 *   because "an unknown YAML field is rejected rather than ignored" is a stated acceptance
 *   criterion and should not rest on a library default a future upgrade could flip.
 * - `STRICT_DUPLICATE_DETECTION` is **off** by default, and without it a file naming `name:` twice
 *   parses silently and the second one wins. Enabled: a duplicated key is a `JsonParseException`
 *   naming the field and the line.
 * - Every failure shape reaches the caller as a `JsonProcessingException` carrying a usable line
 *   number: `UnrecognizedPropertyException` for an unknown field, `InvalidTypeIdException` for an
 *   unknown or missing step `type`, `MissingKotlinParameterException` for a required field that is
 *   absent or null, `InvalidFormatException` for a scalar of the wrong type, and
 *   `MarkedYAMLException` - which is *not* a `JsonMappingException`, but is a
 *   `JsonProcessingException` - for malformed YAML. One catch covers all five.
 */
private val YAML: ObjectMapper = YAMLMapper.builder()
    .addModule(kotlinModule())
    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .build()

/** For [DuckDbSyntax]'s result only. `json_serialize_sql` answers JSON, not YAML. */
private val JSON = ObjectMapper()

/**
 * DuckDB's own parser, applied to `scratch` SQL for validation rule 6.
 *
 * `json_serialize_sql` **parses without binding**, which is what makes it usable at startup: no
 * table in a task file exists yet, and measured on duckdb_jdbc 1.1.3 (P6 scratchpad `Duck.kt`,
 * `Duck2.kt`) it accepts `select ... from wip_stg` and `read_parquet('/nowhere')` against an empty
 * in-memory database, while `PREPARE` and `EXPLAIN` both bind and fail on the missing table.
 *
 * Its three answers, all measured, are what make the rule complete rather than lumpy:
 *
 * | Result | Meaning |
 * |---|---|
 * | `error: false` | a SELECT, and it parses |
 * | `error_type: "not implemented"` | **parsed**, but is not a SELECT - `create table t (a int)`, `copy (...) to ...` |
 * | `error_type: "parser"` | a syntax error, with a message and a character offset |
 *
 * A malformed non-SELECT reports `parser`, not `not implemented` - `create tabl t (a int)` gives
 * "syntax error at or near \"tabl\"" - so DDL in a `sql` step is checked as well as a SELECT.
 *
 * **Only `scratch` SQL is checked.** DuckDB is the only dialect on this module's classpath and a
 * task file's SQL is written in whichever dialect its datasource speaks: measured, a perfectly
 * valid Oracle `MERGE INTO` is a DuckDB syntax error. Running Oracle SQL through this parser would
 * reject every correct task file that has one.
 *
 * The connection is opened on the first scratch SQL text and not before, so a directory of tasks
 * that never touch scratch does not load the native library to validate, and it is closed before
 * `load` returns.
 *
 * **In-memory here does not contradict spec 7.2's "file mode, never in-memory".** That rule is
 * about the scratch working file, which holds data and has to reclaim space; this database holds no
 * table, no row and no temporary object, and exists only to own a parser for the length of one
 * `load` call.
 */
private class DuckDbSyntax : AutoCloseable {

    private val session = lazy {
        val connection = DriverManager.getConnection("jdbc:duckdb:")
        try {
            connection to connection.prepareStatement("select json_serialize_sql(cast(? as varchar))")
        } catch (e: Throwable) {
            connection.close()
            throw e
        }
    }

    /**
     * The syntax error in [sql], or null when it parses.
     *
     * @param selectOnly also reports the `not implemented` answer of the table above - the
     *   statement parsed but is not a SELECT. Off for a `sql` step, where DDL is the whole point;
     *   on for `cacheCopy`, whose text is spliced into `CREATE TABLE <output> AS <sql>` and is
     *   therefore legal only as a SELECT.
     */
    fun errorIn(sql: String, selectOnly: Boolean = false): String? {
        val statement: PreparedStatement = session.value.second
        statement.setString(1, sql)
        val answer = statement.executeQuery().use { rows ->
            rows.next()
            rows.getString(1)
        }
        val result = JSON.readTree(answer)
        if (!result.path("error").asBoolean()) return null
        val message = result.path("error_message").asText()
        return when {
            result.path("error_type").asText() == "parser" ->
                "$message (at character ${result.path("position").asText()})"
            selectOnly -> "it parses, but is not a SELECT statement (DuckDB: $message)"
            else -> null
        }
    }

    override fun close() {
        if (!session.isInitialized()) return
        val (connection: Connection, statement: PreparedStatement) = session.value
        connection.use { statement.close() }
    }
}

/**
 * One task file after reading, in file-name order: either it deserialised or it did not.
 *
 * Replaces the two parallel `LinkedHashMap`s the load pass used to build - one of parsed documents,
 * one of parse errors - which a third walk then stitched back together by name with
 * `unparsed[name] ?: parsed.getValue(name)`. That worked only while every name landed in exactly
 * one of the two maps, an invariant held by the shape of a single if/else and by nothing else; a
 * later edit that added a third outcome, or forgot a map, would turn it into a
 * `NoSuchElementException` at startup (review finding L9). One ordered list of a two-case type
 * cannot fall out of step with itself.
 */
private sealed interface ReadFile {
    val name: String

    class Parsed(override val name: String, val task: TaskYaml) : ReadFile

    class Failed(override val name: String, val error: ValidationError) : ReadFile
}

/**
 * What [TaskFileLoader.load] returns.
 *
 * Spec 11.2 writes `Result<List<TaskDefinition>, ValidationReport>`, which does not exist: Kotlin's
 * `kotlin.Result` takes one type parameter. This sealed pair is the smallest replacement that keeps
 * both halves of the declared contract, and follows the precedent set for [PipeTarget] in P5 - two
 * cases, so "tasks and errors at the same time" is unrepresentable rather than merely documented.
 * Spec 10 makes any error fatal, so there is no third case to model.
 */
sealed interface LoadResult {

    /** Every file parsed and passed spec 10, in file-name order. */
    data class Loaded(val tasks: List<TaskDefinition>) : LoadResult

    /** At least one file failed. Nothing is loaded: one bad file out of ten prevents startup. */
    data class Invalid(val report: ValidationReport) : LoadResult
}

/** Every error found across the whole directory, in file-name order and then rule order. */
data class ValidationReport(val errors: List<ValidationError>)

/**
 * One validation failure.
 *
 * @param step null for a task-level rule, which has no step.
 * @param line the YAML line, when one is cheaply available. Jackson reports one for every
 *   deserialisation failure, so rule 1's errors carry it. The semantic rules - 2 to 18 - work on
 *   the deserialised object tree, which holds no source positions, so they report null. That is
 *   what the field being nullable is for. ponytail: recovering a line for them needs a second pass
 *   recording a position per node; add it when a report reader asks, since the file name and the
 *   step name already locate the error.
 */
data class ValidationError(val file: String, val step: String?, val line: Int?, val message: String)

/**
 * Reads a directory of task files and returns either every [TaskDefinition] in it or every error
 * in it (spec 10).
 *
 * **All files are read before anything is decided.** One bad file out of ten prevents startup, and
 * the report lists that file's errors and no phantom errors for the other nine. That is also what
 * makes P7's reload atomic: parse and validate everything, then swap or reject.
 *
 * **Task files are not read through Quarkus configuration**, deliberately (spec 10). Config
 * performs property expansion, which would corrupt SQL containing `${...}`; nothing here expands,
 * interpolates or substitutes anything, and a `|` block scalar reaches [TaskDefinition] with its
 * newlines intact.
 *
 * Three rules need something a file loader does not own, so the caller supplies them rather than
 * the loader discovering them. None of the three is a registry this class invents: P7 wires the
 * datasource names it configured, P8 wires the hook names it registered, and the CDI container
 * supplies the transforms.
 *
 * @param datasources the configured Jdbi bean names (validation rule 3). [SCRATCH] is reserved and
 *   always valid, so it must not appear here - the same requirement [TaskEngine] makes.
 * @param transforms the `RowTransform` CDI beans by bean name (validation rule 4). A map rather
 *   than a set of names because [PipeStep.transform] carries a resolved object and YAML carries a
 *   name, so the loader is the only place the two can meet.
 * @param hooks the names registered in the `TaskHookRegistry` (validation rule 5). Names only: the
 *   loader never runs a hook, and building the registry is P8's.
 * @param caches the `cache:` names the host bound to a `(SnapshotCache, GroupId)` pair (validation
 *   rule 21, spec 3.6). Names only, for the same reason as [hooks]: the loader never reads a
 *   cache, and the pairs themselves go to [TaskEngine]. **Fourth and last**, and defaulted: two
 *   fixture call sites pass the earlier three positionally, so a parameter inserted anywhere else
 *   would silently rebind `transforms`.
 */
class TaskFileLoader(
    private val datasources: Set<String> = emptySet(),
    private val transforms: Map<String, RowTransform> = emptyMap(),
    private val hooks: Set<String> = emptySet(),
    private val caches: Set<String> = emptySet(),
) {

    init {
        require(SCRATCH !in datasources) {
            "'$SCRATCH' is the reserved name of the per-run DuckDB working file and cannot also be a " +
                "configured datasource (spec 7.1)."
        }
    }

    /**
     * @param directory the mounted task-file directory. `*.yaml` and `*.yml` regular files are read
     *   in file-name order; a name starting with `.` is skipped, which keeps a Kubernetes
     *   ConfigMap's `..data` bookkeeping out of the scan.
     * @throws java.io.IOException if the directory cannot be read at all. That is a deployment
     *   fault rather than a task-file fault, and reporting it as a validation error would file it
     *   in the one place an operator looks for authoring mistakes.
     */
    fun load(directory: Path): LoadResult {
        val files = Files.newDirectoryStream(directory).use { it.toList() }
            .filter { Files.isRegularFile(it) && it.fileName.toString().isTaskFile() }
            .sortedBy { it.fileName.toString() }

        val read = files.map { file ->
            val name = file.fileName.toString()
            val text = Files.readString(file)
            try {
                // Measured: an empty file and a comments-only file both throw "No content to map",
                // but a document that is only `---`, or the literal `null`, deserialises to Java
                // null instead - so without this branch such a file leaves startup with a
                // NullPointerException rather than with a report.
                when (val task: TaskYaml? = YAML.readValue(text, TaskYaml::class.java)) {
                    null -> ReadFile.Failed(
                        name,
                        ValidationError(name, null, 1, "the file holds no task document (rule 1)."),
                    )
                    else -> ReadFile.Parsed(name, task)
                }
            } catch (e: JsonProcessingException) {
                ReadFile.Failed(name, parseError(name, text, e))
            }
        }
        val parsed = read.filterIsInstance<ReadFile.Parsed>()

        // Rule 2's cross-file half needs every file read before any file can be judged, which is
        // the same reason nothing is loaded until everything has validated.
        val filesByTask = parsed.groupBy({ it.task.name }, { it.name })
        val errors = DuckDbSyntax().use { syntax ->
            read.flatMap { file ->
                when (file) {
                    is ReadFile.Failed -> listOf(file.error)
                    is ReadFile.Parsed -> FileValidation(
                        file.name, file.task, datasources, transforms.keys, hooks, caches,
                        filesByTask, syntax,
                    ).validate()
                }
            }
        }

        return if (errors.isEmpty()) LoadResult.Loaded(parsed.map { it.task.toDefinition(transforms) })
        else LoadResult.Invalid(ValidationReport(errors))
    }
}

private fun String.isTaskFile(): Boolean = !startsWith(".") && (endsWith(".yaml") || endsWith(".yml"))

/**
 * Validation rule 1. The line comes from Jackson; the step name does not, because deserialisation
 * failed before any step object existed - so the document is re-read as a tree, on the error path
 * only, and the step name is looked up at the failing property's own path. Without that, a bad
 * field inside a step could name only `phases[0].steps[2]`, and every error is supposed to identify
 * the step.
 */
private fun parseError(file: String, text: String, e: JsonProcessingException): ValidationError {
    val path = (e as? JsonMappingException)?.path.orEmpty()
    val pointer = path.joinToString("") { "/" + (it.fieldName ?: it.index.toString()) }
    val where = if (pointer.isEmpty()) "" else " (at ${pointer.drop(1).replace('/', '.')})"
    return ValidationError(file, stepNameAt(text, pointer), e.location?.lineNr, e.originalMessage + where)
}

private fun stepNameAt(text: String, pointer: String): String? {
    if (!pointer.contains("/steps/")) return null
    val tree = runCatching { YAML.readTree(text) }.getOrNull() ?: return null
    var at = pointer
    while (at.isNotEmpty()) {
        if (at.substringBeforeLast('/').endsWith("/steps")) {
            return tree.at(at).path("name").takeIf { it.isTextual }?.asText()
        }
        at = at.substringBeforeLast('/', "")
    }
    return null
}

/**
 * Rules 2 to 21 of spec 10 for one file, accumulating rather than stopping - a report naming one
 * error per file makes an author fix a ten-error file ten times.
 *
 * Four of the twenty-one are not checked here because [TaskYaml]'s schema already makes them
 * unrepresentable, which is stronger than checking them: **rule 1** (unknown fields) and **rule
 * 17** (each step's field set matches its declared type) are Jackson's per-subtype binding, and
 * **rule 13** (`format` only on `materialize`) is the absence of that field from every other step
 * class. **Rule 15** reaches every column type a task file states and no other - see [addColumn].
 */
private class FileValidation(
    private val file: String,
    private val yaml: TaskYaml,
    private val datasources: Set<String>,
    private val transforms: Set<String>,
    private val hooks: Set<String>,
    private val caches: Set<String>,
    private val filesByTask: Map<String, List<String>>,
    private val syntax: DuckDbSyntax,
) {

    private val errors = mutableListOf<ValidationError>()
    private val defined = BUILT_IN_VARIABLES.toMutableSet()
    private val datasets = mutableSetOf<String>()

    fun validate(): List<ValidationError> {
        // Rule 2, both halves: the name pattern, and uniqueness across files.
        if (!TASK_NAME.matches(yaml.name)) {
            err(null, "task name '${yaml.name}' does not match ${TASK_NAME.pattern} (spec 3.1, rule 2).")
        }
        val alsoIn = filesByTask[yaml.name].orEmpty().filter { it != file }
        if (alsoIn.isNotEmpty()) {
            err(null, "task name '${yaml.name}' is also used by $alsoIn; names are unique across files (rule 2).")
        }
        // Rule 16.
        yaml.schedule?.cron?.let { cron ->
            val fields = cron.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }
            if (fields.size !in 6..7 || fields.any { !CRON_FIELD.matches(it) }) {
                err(
                    null,
                    "schedule.cron '$cron' is not a cron expression (rule 16). Expected six or seven " +
                        "whitespace-separated fields, seconds first, as in \"0 */10 * * * ?\" (spec 8.1).",
                )
            }
        }
        // Rule 5.
        hook("onSuccess", yaml.onSuccess)
        hook("onFailure", yaml.onFailure)
        // Rule 8, the literal half.
        yaml.vars.forEach { literal ->
            if (literal.value == null) {
                err(
                    null,
                    "literal var '${literal.name}' has no value (rule 8). Null carries no type, so it cannot " +
                        "be bound as one (spec 6.1); write 'null' in the SQL that needs it.",
                )
            }
            variable(null, literal.name)
        }
        // Not a rule of spec 10: RowPipe requires a positive chunk size and ScratchDb a positive
        // memory limit. Catching them here turns a failure five minutes into a run - or, for the
        // memory limit, at the first line of every run forever - into a failure at boot.
        if (yaml.chunkSize <= 0) err(null, "chunkSize must be positive, got ${yaml.chunkSize} (spec 5.2).")
        yaml.scratch?.memoryLimitMb?.let {
            if (it <= 0) {
                err(
                    null,
                    "scratch.memoryLimitMb must be positive, got $it (spec 7.2). There is no value that " +
                        "means unlimited; omit the field to take the engine default.",
                )
            }
        }
        // Also not a rule of spec 10, and here for the same reason: a task with no step at all runs,
        // reports SUCCEEDED and updates nothing, which is spec 1.1's 03:00 failure exactly. Spec 3.1
        // annotates every optional field '# optional' and `phases` carries no such annotation.
        if (yaml.phases.isEmpty()) {
            err(null, "the task declares no phases, so every run would succeed having done nothing (spec 3.1).")
        }
        yaml.phases.filter { it.steps.isEmpty() }.forEach {
            err(null, "phase '${it.name}' declares no steps, so it would do nothing (spec 3.1).")
        }

        yaml.phases.forEach { phase -> phase.steps.forEach(::step) }
        return errors
    }

    private fun step(step: StepYaml) {
        if ((step.retries ?: 0) < 0) {
            err(step.name, "retries must not be negative, got ${step.retries} (spec 5.3).")
        }
        when (step) {
            is PipeYaml -> pipe(step)
            is MaterializeYaml -> materialize(step)
            is SqlYaml -> {
                datasource(step.name, step.datasource)
                step.statements.forEach { sql(step.name, "statements", it, step.datasource, resolveNames = true) }
                sqlRetries(step)
            }

            is ExportYaml -> export(step)
            is CacheCopyYaml -> cacheCopy(step)
        }
    }

    /**
     * Spec 3.6's `cacheCopy`: rules 21, 19, 20 and 9, in the order an author is best served by.
     *
     * **Rules 19 and 20 are startup rules and not runtime ones**, deliberately. Both could have
     * been a `require` in the executor - and both would then let a file boot green and kill a task
     * thirty minutes in, which is the failure spec 10 exists to prevent. Neither is a condition
     * that might come good on the day: a `:name` in cache SQL cannot be bound at all, because
     * `CopyOutSpec.sql` is a plain string with no binding channel, and a `retries` above zero can
     * never fire, because spec 5.3's retry classification is JDBC-shaped and a local DuckDB
     * file-to-file copy raises none of it. The executor keeps its own guard for the definitions
     * spec 2.1 lets a host build in code, which have no loader in front of them.
     */
    private fun cacheCopy(step: CacheCopyYaml) {
        // Rule 21, the exact analogue of rule 3 for datasources. First, so that a file with a
        // mistyped name is told about the name rather than about its SQL.
        if (step.cache !in caches) {
            err(
                step.name,
                "cache '${step.cache}' is not bound (rule 21). Bound caches are ${caches.sorted()}. A " +
                    "cache name is not a datasource: the host binds it to a snapshot cache and a group " +
                    "(spec 3.6, 8.6).",
            )
        }
        cacheSql(step.name, step.sql)
        // Rule 20, over the value the file *states*. `retries ?: 0` below is this step type's
        // default and is not the author's word, so it is not what this rule reads.
        val stated = step.retries
        if (stated != null && stated > 0) {
            err(
                step.name,
                "retries $stated on a cacheCopy step is rejected (rule 20). No failure a cache copy can " +
                    "produce is transient under spec 5.3, whose classification is JDBC-shaped, so the knob " +
                    "can never fire; the waiting mechanism is the cache's own waitBudget (spec 3.6). Omit " +
                    "retries or state 0.",
            )
        }
        // Rule 9 and spec 5.5's character check: `output` is an ordinary scratch dataset and
        // shares one namespace with every other dataset the task produces.
        dataset(step.name, step.output)
    }

    /**
     * Rule 19, and rule 6 for the same text.
     *
     * Rule 6's DuckDB parse applies here without the dialect caveat that limits it elsewhere: a
     * `cacheCopy` runs **inside the cache's own DuckDB instance** (spec 7.3), so DuckDB is not a
     * guess about the datasource's dialect but the dialect itself.
     *
     * Rule 19 is not `!text.contains(":")`. That would reject `qty::varchar` and `'a:b'`, both
     * legal DuckDB and both correctly skipped by JDBI's own parser, and would make every task file
     * needing a cast unwritable. The parse is what tells a bound name from punctuation.
     *
     * **JDBI's parser is not the last word on what a bind name is here.** Its lexer reads a colon
     * followed by digits as a parameter - measured on jdbi3-core 3.45.4, `select site_code[1:3]`
     * yields the name `3` and the rewrite `select site_code[1?]`, and `{'k':1}` yields `1`. Both
     * are ordinary DuckDB syntax, an array slice and a struct literal, and both parse clean as
     * written (measured on duckdb_jdbc 1.1.3). A cacheCopy's text reaches the cache verbatim
     * through `CopyOutSpec.sql` and never passes through JDBI, so an all-digit "name" is
     * punctuation here rather than a binding - and it is the **raw text**, never JDBI's
     * `?`-substituted rewrite, that DuckDB is asked to parse.
     */
    private fun cacheSql(step: String, text: String) {
        if (text.isBlank()) {
            err(step, "sql is empty (rule 6).")
            return
        }
        val parsed = try {
            parseNamedParameters(text)
        } catch (e: RuntimeException) {
            err(step, "sql does not parse: ${e.message} (rule 6).")
            return
        }
        val parameters = parsed.parameters
        if (parameters.isPositional) {
            err(step, "sql uses positional '?' parameters, and a cacheCopy binds nothing at all (rule 19).")
            return
        }
        val bound = parameters.parameterNames.filterNot { name -> name.all(Char::isDigit) }.distinct().sorted()
        if (bound.isNotEmpty()) {
            err(
                step,
                "sql binds ${bound.map { ":$it" }}, and a cacheCopy takes no variables at all (rule 19). It " +
                    "runs inside the cache's own DuckDB instance through CopyOutSpec.sql, a plain string " +
                    "with no binding channel (spec 3.6, 7.3), so this cannot be bound even if the variable " +
                    "is defined. Copy the wider subset and filter it in the following materialize step.",
            )
            return
        }
        // selectOnly: the runtime splices this text into `CREATE TABLE <output> AS <sql>`, which is
        // legal only for a SELECT. Without it a parsed non-SELECT - `copy (...) to ...`, a CTAS -
        // comes back "not implemented", passes every rule, and then fails at run time on every
        // firing, after the run has waited on the cache and taken a lease.
        syntax.errorIn(text, selectOnly = true)?.let { err(step, "sql does not parse: $it (rule 6, 19).") }
    }

    /**
     * **Rule 15 splits in two, and only one half is reachable here** (spec 10 rule 15, 4.6).
     *
     * The half a task file *states* - every `transform.addColumns` entry - is enforced, in
     * [addColumn], through the same [unwritableToDuckDb] predicate the writer uses. The half a
     * *table* declares is not: under `REQUIRED` those types live in a catalog the run creates, and
     * under `AUTO` they come from result set metadata that exists only once the source query runs.
     *
     * [DuckDbSyntax] does not close that second half, and it was tried. `json_serialize_sql` parses
     * DDL - a malformed `create tabl` is reported - but **serializes SELECT only**, so a well-formed
     * `create table wip_req (id BIGINT, note DATE)` yields "Only SELECT statements can be serialized
     * to json" and no column list. `EXPLAIN` binds without emitting columns and `PREPARE` takes no
     * DDL at all, so 1.1.3 offers no parse-to-AST path for DDL.
     *
     * Executing the task's scratch `sql` steps into a sandboxed in-memory DuckDB at boot does not
     * work either, and the reason is the task files themselves rather than the sandbox. Measured
     * (P6 scratchpad `Sandbox.kt`, in-memory 1.1.3 with `set enable_external_access=false`): the
     * sandbox holds - `read_parquet`, `COPY TO` and `ATTACH` are all refused with a Permission or
     * IO error - but **spec 3.4's own example fails inside it**, because
     * `create index idx_wip_lot on wip_stg (lot_id)` needs a table a `pipe` step creates and no
     * pipe can run at boot: "Catalog Error: Table with name wip_stg does not exist". A CTAS
     * `materialize` fails the same way. So a boot sandbox either ignores those failures, which
     * silently switches the rule off for the task, or honours them, which refuses to boot a correct
     * file. That is the reason of record; an earlier draft of this comment blamed a runaway query
     * hanging startup, which is false - `Statement.cancel` interrupts one in about 200 ms.
     *
     * So the table half stays where P2 put it: `DuckDbTableWriter` rejects at writer **open**,
     * before any row is written.
     */
    private fun pipe(step: PipeYaml) {
        val name = step.name
        datasource(name, step.source.datasource)
        sql(name, "source.sql", step.source.sql, step.source.datasource, resolveNames = true)
        if (step.chunkSize != null && step.chunkSize <= 0) {
            err(name, "chunkSize must be positive, got ${step.chunkSize} (spec 5.2).")
        }

        val target = step.target
        datasource(name, target.datasource)
        val scratch = target.datasource == SCRATCH
        val retries = step.retries ?: defaultRetries(target.datasource)

        // Rule 10.
        if ((target.table == null) == (target.sql == null)) {
            err(name, "exactly one of target.table and target.sql must be present (spec 3.2, rule 10).")
        } else if (target.table != null) {
            val createTable = target.createTable ?: defaultCreateTable(target.datasource)
            if (createTable == CreateTable.AUTO) {
                // Rule 14.
                if (!scratch) {
                    err(
                        name,
                        "createTable AUTO generates DuckDB DDL from source metadata, so it is available only " +
                            "on the '$SCRATCH' datasource, not '${target.datasource}' (spec 4.4, rule 14).",
                    )
                }
                if (step.transform != null && step.transform.addColumns.isEmpty()) {
                    err(
                        name,
                        "createTable AUTO with a transform requires transform.addColumns (rule 14). Source " +
                            "metadata cannot describe a column the transform adds, so the value would be " +
                            "silently dropped (spec 9.1).",
                    )
                }
            } else if (scratch && retries > 0) {
                // Rule 18.
                err(
                    name,
                    "a scratch target with createTable REQUIRED cannot be retried (retries $retries, rule " +
                        "18). Its table has no attempt-suffixed name, so a retry appends onto the rows the " +
                        "failed attempt already flushed (spec 5.5). Use createTable AUTO, or retries: 0.",
                )
            }
            if (scratch) dataset(name, target.table)
        } else {
            // Rule 11.
            if (scratch) {
                err(
                    name,
                    "target.sql is not available on '$SCRATCH' (rule 11). DuckDB writes go through the " +
                        "appender, which takes a table and not a statement (spec 4.4).",
                )
            }
            // Rule 7 excepts target.sql: every ':name' there is a Row key, and Row keys are unknown
            // until the source query runs, so they are checked against the first chunk at run time
            // (spec 4.4, 6.3). The SQL is still parsed, which is rule 6.
            sql(name, "target.sql", target.sql!!, datasource = null, resolveNames = false)
        }

        // Rule 12.
        if (!scratch && retries > 0 && !target.idempotent) {
            err(
                name,
                "retries $retries on the non-scratch target '${target.datasource}' requires idempotent: true " +
                    "(rule 12). The framework cannot make a partially written external target safe on its " +
                    "own, so the author states that a rerun converges (spec 5.3).",
            )
        }

        // Rule 4.
        step.transform?.let { transform ->
            if (transform.bean !in transforms) {
                err(
                    name,
                    "transform.bean '${transform.bean}' is not a RowTransform bean (rule 4). Known beans are " +
                        "${transforms.sorted()}.",
                )
            }
            transform.addColumns.forEach { addColumn(name, it, duckDbTarget = scratch) }
        }
    }

    /**
     * One `transform.addColumns` entry: rule 15 over a column type the task file states outright,
     * which is the half of the rule startup can reach (spec 10 rule 15, 4.6). The same predicate
     * runs again at writer open over the *table's* types, which no file states.
     *
     * `nullable` defaults to true (spec 3.2), so `type: BOOLEAN` with nothing else written is a
     * rejection - hence the message naming `nullable: false` as one of the two fixes.
     *
     * @param duckDbTarget whether the pipe's target is the scratch DuckDB. Rule 15's own scope is
     *   "DuckDB target column types (4.6)" and the whole predicate is about what the 1.1.3 appender
     *   can express, so it is asked only of a step that reaches that appender. An added column on a
     *   REQUIRED Oracle target - legal, and wired into `JdbcTableWriter` in P5 - is bound by
     *   `JdbcWriters.javaType`, which takes a nullable DOUBLE, a DATE, an INSTANT and a BYTES
     *   without complaint. Applying rule 15 there made those columns inexpressible: undeclared they
     *   were dropped silently, declared they failed startup with a DuckDB-shaped message about a
     *   table DuckDB never sees.
     */
    private fun addColumn(step: String, column: AddColumnYaml, duckDbTarget: Boolean) {
        val type = canonicalOf(column.type)
        if (type == null) {
            err(
                step,
                "transform.addColumns column '${column.name}' has type '${column.type}', which is not a " +
                    "type this framework writes. One of ${CanonicalType.entries.map { it.duckDbType }} " +
                    "(spec 3.2, 4.6).",
            )
            return
        }
        if (!duckDbTarget) return
        val meta = ColumnMeta(column.name, type, column.nullable, column.precision, column.scale)
        unwritableToDuckDb(meta)?.let {
            err(step, "transform.addColumns column '${column.name}': $it (rule 15).")
        }
        // Rule 14's DECIMAL clause. AUTO cannot check a *source* column's precision at startup, but
        // an added column states its own, so the pair DuckDbTableWriter.ddlType demands is knowable
        // here. Default precision 0 is rejected: the bare keyword resolves to DECIMAL(18,3), which
        // rounds past three decimals and cannot hold a 16-digit key (spec 4.4).
        if (type == CanonicalType.DECIMAL && !isDuckDbDecimalPair(column.precision, column.scale)) {
            err(
                step,
                "transform.addColumns column '${column.name}' declares DECIMAL precision ${column.precision} " +
                    "and scale ${column.scale}, which is not a DuckDB DECIMAL(p,s) - p must be 1 to 38 and s " +
                    "0 to p. State them, because bare DECIMAL resolves to DECIMAL(18,3) (spec 4.4, rule 14).",
            )
        }
    }

    /**
     * **Rule 12 on a `sql` step**, which spec 10 amends to enforce as the step-level rule it has
     * always been worded as (review finding H2).
     *
     * Each statement is its own transaction (spec 5.2), so a retry re-runs the whole list: a
     * transient drop between two committed statements re-executes the first, duplicating rows in
     * an external table, and the run then reports SUCCEEDED. That is the same hazard rule 12
     * covers on a pipe target, so it takes the same answer - the author states that a rerun
     * converges, and the framework holds them to having said it.
     */
    private fun sqlRetries(step: SqlYaml) {
        if (step.datasource == SCRATCH) return
        // Off scratch spec 5.3 defaults retries to 0, so a stated value is the only way to have one
        // - which is why this does not need the datasource-dependent default at all.
        val retries = step.retries ?: 0
        if (retries <= 0 || step.idempotent) return
        err(
            step.name,
            "retries $retries on the non-scratch datasource '${step.datasource}' requires idempotent: true " +
                "(rule 12). Each statement is its own transaction, so a retry re-runs all of them - a failure " +
                "after the first statement committed would run it a second time (spec 5.2, 5.3).",
        )
    }

    /**
     * **Rule 12 on a `materialize` step.** A non-scratch materialize with retries is rejected
     * outright rather than made conditional on an `idempotent` flag, because it runs
     * `CREATE TABLE <output> AS <sql>`: a retry after the table was created fails deterministically
     * on table-already-exists, so `idempotent: true` would be a promise no author could keep.
     * Spec 10 rule 12 records the ruling, including why drop-and-recreate was refused.
     */
    private fun materializeRetries(step: MaterializeYaml) {
        if (step.datasource == SCRATCH) return
        val retries = step.retries ?: 0
        if (retries <= 0) return
        err(
            step.name,
            "retries $retries on the non-scratch datasource '${step.datasource}' is rejected for a " +
                "materialize (rule 12). It runs as CREATE TABLE ${step.output} AS <sql>, so a retry after " +
                "the table was created fails on table-already-exists every time - there is no idempotent: " +
                "true that could make it converge. State retries: 0, or build the table with a sql step " +
                "that can express its own recovery (spec 5.3, 5.4).",
        )
    }

    /**
     * Rule 13's scratch-only half, rule 12 as spec 10 amends it for a materialize, and **rule 7 as
     * spec 10 amends it for a non-scratch materialize**: such a step may bind no variable at all.
     *
     * A non-scratch materialize runs `CREATE TABLE <output> AS <sql>` through `Handle.createUpdate`,
     * which binds every `:name` it parses, and Oracle rejects a bind variable in DDL outright with
     * ORA-01027. Rule 7 blessed the shape - it names `materialize` explicitly - so a file like
     * `materialize {datasource: report_oracle, sql: "select ... where upd_ts > :lastTs"}` passed
     * every startup rule and then failed on every firing, permanently, because ORA-01027 is not
     * transient. The identical step on `scratch` works, which is why it could only surface in
     * production: the engine's own measurement of "CTAS accepts bound parameters" was taken on
     * duckdb_jdbc.
     */
    private fun materialize(step: MaterializeYaml) {
        datasource(step.name, step.datasource)
        sql(step.name, "sql", step.sql, step.datasource, resolveNames = true)
        if (step.datasource != SCRATCH) externalMaterializeBinds(step)
        materializeRetries(step)
        dataset(step.name, step.output)
        // Rule 13's other half: PARQUET is structurally impossible on any other step type, and
        // spec 5.6 puts the file in the scratch directory, so it is scratch-only here too.
        if (step.datasource != SCRATCH && step.format == MaterializeFormat.PARQUET) {
            err(
                step.name,
                "format PARQUET writes a file into the scratch directory, so it is available only on the " +
                    "'$SCRATCH' datasource, not '${step.datasource}' (spec 5.6, rule 13).",
            )
        }
    }

    /**
     * Every `:name` the SQL of a non-scratch materialize binds. Nothing is filtered: unlike a
     * `cacheCopy`, this text *does* go through JDBI at run time, so whatever JDBI's parser calls a
     * parameter is a parameter here - including the all-digit name it reads out of a DuckDB array
     * slice, which would be rewritten to `?` and broken anyway.
     *
     * A parse failure and a positional `?` are already reported by [sql] for the same text, so
     * both are passed over here rather than reported twice.
     */
    private fun externalMaterializeBinds(step: MaterializeYaml) {
        val parameters = runCatching { parseNamedParameters(step.sql).parameters }.getOrNull() ?: return
        if (parameters.isPositional) return
        val bound = parameters.parameterNames.distinct().sorted()
        if (bound.isEmpty()) return
        err(
            step.name,
            "sql binds ${bound.map { ":$it" }}, and a materialize on the non-scratch datasource " +
                "'${step.datasource}' can bind nothing (rule 7). It runs as CREATE TABLE ${step.output} AS " +
                "<sql>, and Oracle rejects a bind variable in DDL with ORA-01027 - so this step would fail " +
                "on every run, not just some. Materialize the wider set here and filter it in a following " +
                "step, where variables do bind (spec 6.3, 10 rule 7).",
        )
    }

    /**
     * A step's exports become available to *later* steps and never to its own queries: [TaskEngine]
     * defines them only once the whole step has succeeded (spec 6.2), so they are added here after
     * all of this step's SQL has been resolved.
     */
    private fun export(step: ExportYaml) {
        datasource(step.name, step.datasource)
        val declared = mutableSetOf<String>()
        step.vars.forEach { variable ->
            sql(step.name, "vars[${variable.name}].sql", variable.sql, step.datasource, resolveNames = true)
            if (!declared.add(variable.name)) {
                err(
                    step.name,
                    "variable '${variable.name}' is exported twice by this step. A variable may not be " +
                        "redefined once set (spec 6.2, rule 8).",
                )
            }
        }
        declared.forEach { variable(step.name, it) }
    }

    /** Rule 3. [SCRATCH] is reserved rather than configured, so it is always valid (spec 7.1). */
    private fun datasource(step: String, name: String) {
        if (name != SCRATCH && name !in datasources) {
            err(
                step,
                "datasource '$name' is not configured (rule 3). Known datasources are " +
                    "${datasources.sorted()}, plus the reserved '$SCRATCH'.",
            )
        }
    }

    /** Rule 5. */
    private fun hook(field: String, name: String?) {
        if (name != null && name !in hooks) {
            err(
                null,
                "$field hook '$name' is not registered (rule 5). Registered hooks are ${hooks.sorted()}. A " +
                    "typo is caught here rather than at the end of a 30 minute run (spec 9.4).",
            )
        }
    }

    /** Rule 8's other half, and the running scope that rule 7 resolves against. */
    private fun variable(step: String?, name: String) {
        if (!defined.add(name)) {
            err(
                step,
                "variable '$name' is defined more than once (rule 8). A variable may not be redefined once " +
                    "set (spec 6.2). The built-ins ${BUILT_IN_VARIABLES.sorted()} are always defined.",
            )
        }
    }

    /**
     * Rule 9, plus the character check `datasetIdentifier` exists for. That check is not a rule of
     * spec 10, but a dataset name arrives from a file and becomes both a SQL identifier no prepared
     * statement can parameterise and a parquet file name, so it belongs at the trust boundary
     * rather than mid-run.
     */
    private fun dataset(step: String, name: String) {
        runCatching { datasetIdentifier(name) }.onFailure { err(step, "${it.message} (spec 5.5).") }
        if (!datasets.add(name)) {
            err(
                step,
                "dataset name '$name' is produced by more than one step; dataset names are unique within a " +
                    "task (spec 5.5, rule 9).",
            )
        }
    }

    /**
     * Rule 6 for every SQL text, and rule 7 for all of them but `target.sql`.
     *
     * **Rule 6 is honoured only as far as the named-parameter parse reaches.** Nothing on this
     * module's classpath parses Oracle or DuckDB grammar, and a task file's SQL is written in
     * whichever dialect its datasource speaks, so "every SQL text parses" cannot mean a grammar
     * check at startup. What is reachable is an empty statement, a malformed `:name`, and a
     * positional `?`; a genuine syntax error still surfaces where it always did, at execution.
     */
    private fun sql(
        step: String,
        where: String,
        text: String,
        datasource: String?,
        resolveNames: Boolean,
    ) {
        if (text.isBlank()) {
            err(step, "$where is empty (rule 6).")
            return
        }
        val parsed = try {
            parseNamedParameters(text)
        } catch (e: RuntimeException) {
            err(step, "$where does not parse: ${e.message} (rule 6).")
            return
        }
        // JDBI's ?-substituted form, so a ':name' the author wrote is an ordinary placeholder to
        // DuckDB rather than a syntax error in a dialect that spells parameters differently.
        if (datasource == SCRATCH) {
            syntax.errorIn(parsed.sql)?.let { err(step, "$where does not parse: $it (rule 6).") }
        }
        val parameters = parsed.parameters
        if (parameters.isPositional) {
            err(
                step,
                "$where uses positional '?' parameters. Variables bind by name, so write ':name' (spec 6.3).",
            )
            return
        }
        if (!resolveNames) return
        val undefined = parameters.parameterNames.filterNot { it in defined }.distinct().sorted()
        if (undefined.isNotEmpty()) {
            err(
                step,
                "$where binds ${undefined.map { ":$it" }}, which no built-in, literal var or earlier export " +
                    "has defined (rule 7). Variables resolve in step order, so an export step must come " +
                    "before its use (spec 6.2). Defined at this point: ${defined.sorted()}.",
            )
        }
    }

    private fun err(step: String?, message: String) {
        errors += ValidationError(file, step, null, message)
    }
}

/**
 * Spec 3.2 writes an added column's type as the DuckDB keyword an author would put in DDL -
 * `VARCHAR`, not the canonical constant `STRING`. Matched case-insensitively; null means no match,
 * which the caller reports rather than throws.
 */
private fun canonicalOf(duckDbType: String): CanonicalType? =
    CanonicalType.entries.firstOrNull { it.duckDbType.equals(duckDbType.trim(), ignoreCase = true) }

/**
 * The YAML form to the definition model, run only once the whole directory has validated - so every
 * `!!` here is discharged by a rule above. The datasource-dependent defaults of spec 4.4 and 5.3
 * are applied explicitly rather than left to the constructor, because a Kotlin default cannot be
 * conditionally skipped.
 */
private fun TaskYaml.toDefinition(transforms: Map<String, RowTransform>) = TaskDefinition(
    name = name,
    enabled = enabled,
    cron = schedule?.cron,
    logging = logging,
    chunkSize = chunkSize,
    scratchMemoryLimitMb = scratch?.memoryLimitMb,
    onSuccess = onSuccess,
    onFailure = onFailure,
    vars = vars.map { LiteralVar(it.name, it.value) },
    phases = phases.map { phase -> Phase(phase.name, phase.steps.map { it.toStep(transforms) }) },
)

private fun StepYaml.toStep(transforms: Map<String, RowTransform>): Step = when (this) {
    is PipeYaml -> {
        PipeStep(
            name = name,
            source = PipeSource(source.datasource, source.sql),
            target = if (target.table != null) {
                TableTarget(
                    datasource = target.datasource,
                    table = target.table,
                    createTable = target.createTable ?: defaultCreateTable(target.datasource),
                    idempotent = target.idempotent,
                )
            } else {
                StatementTarget(target.datasource, target.sql!!, target.idempotent)
            },
            transform = transform?.let { transforms.getValue(it.bean) },
            addColumns = transform?.addColumns.orEmpty().map {
                ColumnMeta(it.name, canonicalOf(it.type)!!, it.nullable, it.precision, it.scale)
            },
            chunkSize = chunkSize,
            retries = retries ?: defaultRetries(target.datasource),
        )
    }

    is MaterializeYaml -> MaterializeStep(
        name = name,
        datasource = datasource,
        output = output,
        sql = sql,
        format = format,
        retries = retries ?: defaultRetries(datasource),
    )

    is SqlYaml -> SqlStep(
        name = name,
        datasource = datasource,
        statements = statements,
        retries = retries ?: defaultRetries(datasource),
        idempotent = idempotent,
    )

    is ExportYaml -> ExportStep(
        name = name,
        datasource = datasource,
        vars = vars.map { ExportVar(it.name, it.sql) },
        retries = retries ?: 0,
    )

    // `?: 0`, and not the 3 CacheCopyStep declares. Rule 20 rejects a stated non-zero value, so
    // inheriting the model's default would fail every file that omits the field on a value its
    // author never wrote. Spec 10 rule 20 records the asymmetry.
    is CacheCopyYaml -> CacheCopyStep(
        name = name,
        cache = cache,
        sql = sql,
        output = output,
        retries = retries ?: 0,
    )
}
