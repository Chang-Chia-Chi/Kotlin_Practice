package infra.etl.task

import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DatasetNamer
import infra.etl.duckdb.DuckDbTableWriter
import infra.etl.duckdb.ScratchDb
import infra.etl.duckdb.datasetIdentifier
import infra.etl.duckdb.quoteIdentifier
import infra.etl.duckdb.sqlLiteral
import infra.etl.jdbc.JdbcStatementWriter
import infra.etl.jdbc.JdbcTableWriter
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.JdbcSource
import infra.etl.pipe.PipeResult
import infra.etl.pipe.Row
import infra.etl.pipe.RowMapper
import infra.etl.pipe.RowPipe
import infra.etl.pipe.RowWriter
import java.nio.file.Path
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLRecoverableException
import java.sql.SQLTimeoutException
import java.sql.SQLTransientException
import java.time.Clock
import java.util.Collections
import java.util.UUID
import org.jboss.logging.Logger
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.argument.NullArgument
import org.jdbi.v3.core.statement.SqlStatements

/** The built-in that changes between attempts of the same step, so it never lives in the scope. */
private const val ATTEMPT = "attempt"

private val log = Logger.getLogger(TaskEngine::class.java)

/** What a step type that moves no row through the JVM reports to [StepResult] (spec 2.3). */
private val NO_ROWS = PipeResult(0, 0)

/**
 * The task variables of spec 6: built-ins, task literals, and whatever `export` steps have
 * produced *so far*. Task scope, evaluated in step order, so a variable exported in phase 1 is
 * available in phase 2 and one used before its export is simply not here yet (spec 6.2).
 *
 * Names are case sensitive and are **not** normalised the way Row keys are (spec 4.5). `:lastTs`
 * in a source query binds the variable written `lastTs` in the task file; lower-casing it would
 * make the YAML and the SQL disagree.
 *
 * `attempt` is deliberately absent: it is a built-in whose value changes between attempts of one
 * step, and this scope defines a name exactly once. [TaskEngine] supplies it per attempt.
 */
class VariableScope {

    private val values = LinkedHashMap<String, Any?>()

    /** Every variable defined so far, in definition order. */
    val names: Set<String> get() = Collections.unmodifiableSet(values.keys)

    /** The value, or null both for "defined as null" and for "not defined" - see [contains]. */
    operator fun get(name: String): Any? = values[name]

    fun contains(name: String): Boolean = values.containsKey(name)

    /**
     * @throws IllegalArgumentException if [name] is already defined. A variable may not be
     *   redefined once set (spec 6.2), which is also what stops an export colliding with a
     *   built-in or with a task literal.
     */
    fun define(name: String, value: Any?) {
        require(name != ATTEMPT) {
            "variable '$name' is a reserved built-in (spec 6.1). It is resolved per attempt by the " +
                "engine, so a literal var or an export of that name would be accepted and then " +
                "silently ignored."
        }
        require(!values.containsKey(name)) {
            "variable '$name' is already defined. A variable may not be redefined once set, so an " +
                "export cannot overwrite a built-in, a literal var, or an earlier export (spec 6.2)."
        }
        values[name] = value
    }
}

/**
 * Layer 2 (spec 2.1): runs the phases and steps of one [TaskDefinition] in order, on one thread,
 * with per-step retry, task variables, and the per-run scratch file. Scheduling, the admin API,
 * listeners, metrics and YAML loading are later phases; this class is the part that executes.
 *
 * **No transaction spans chunks, steps or phases** (spec 5.4). A failure in phase 2 leaves phase
 * 1's external writes committed, and nothing here attempts to undo them - the mitigations are the
 * author's: `idempotent: true` with a MERGE target, or a work table swapped in by a `sql` step.
 * Scratch is different only because the whole file is deleted at run end.
 *
 * **Scratch is lazy.** The [ScratchDb] is constructed for every run but touches no filesystem
 * until a step asks for a connection, so task shape A of spec 2.4 - Oracle straight to Oracle -
 * leaves no file behind. Each run gets its own directory under [scratchDirectory], named by
 * its runId.
 *
 * **Datasources are resolved by name** (spec 7.1). [SCRATCH] is reserved and must not appear in
 * [datasources]; every other name must, or the step fails naming the datasource.
 *
 * Measured on duckdb_jdbc 1.1.3 and JDBI 3.45.4 (Windows, Java 22, P5 scratchpad probe
 * `P5Probe`, `P5Probe2`, `P5Probe4`, `Drive2`), because the DuckDB paths here are all
 * statement-shaped rather than appender-shaped and none of them was exercised before:
 *
 * - `Jdbi.create(connection)` hands out handles over the caller's connection and closing the
 *   handle leaves that connection **open** - which is what lets scratch keep one write connection
 *   for the whole run. `Jdbi.open(connection)` closes it instead, so it is never used here (P3).
 * - DuckDB accepts bound parameters in `CREATE TABLE ... AS SELECT` and in `COPY (...) TO ...
 *   (FORMAT PARQUET)`; both report a row count of -1 and 3 respectively, so the return value is
 *   ignored. DDL through `Handle.createUpdate` works the same way.
 * - A `create or replace view` executed on the write connection is visible from a `duplicate()`
 *   opened before it, including after the view is repointed at a second attempt's table.
 * - Only the names a statement parses out are bound. JDBI's superfluous-binding check is **not**
 *   what forces that: measured, `Superfluous named parameters provided` is raised only when the
 *   statement declares no parameters at all, and a statement with at least one named parameter
 *   accepts extra bindings silently, on `createQuery` and `prepareBatch` alike. The parse is done
 *   because it is the only place "a variable used before its export" (spec 6.2) can be caught with
 *   a message, and because a task with no parameters in one statement would otherwise fail on the
 *   one check JDBI does make.
 *
 * @param datasources the configured Jdbi beans by name (spec 7.1).
 * @param scratchDirectory the directory runs create their scratch directories under. Must be a
 *   disk-backed volume with an explicit `sizeLimit` (spec 7.2).
 * @param scratchMemoryLimitMb the application-wide default DuckDB `memory_limit`, overridden per
 *   task by [TaskDefinition.scratchMemoryLimitMb]. 4096 is spec 7.2's indicative budget at 8 GB
 *   of pod memory.
 * @param sleeper the retry backoff, injected so a test does not spend spec 5.3's 2, 4, 8 seconds
 *   actually waiting.
 * @param listener the host's observation seam (spec 9.2). One listener; a host with several
 *   composes them with [TaskRunListener.of]. Never allowed to fail a run - see [Events].
 * @param hooks the registry `onSuccess` and `onFailure` names resolve in (spec 9.4). The same
 *   instance whose [TaskHooks.names] the host hands `TaskFileLoader`, or validation rule 5 checks
 *   names against a set the engine does not read.
 * @param clock the only source of time in this file. `TaskContext.startedAt`, the `triggerTime`
 *   task variable and every [StepResult.durationMs] come from it; there is no `Instant.now()` and
 *   no `System.nanoTime()` left here. A cost worth recording rather than hiding: a wall-clock
 *   `Clock` is not monotonic, so an NTP step mid-run can skew a `durationMs`. What it buys is that
 *   a duration is assertable exactly, by a test that never sleeps.
 */
class TaskEngine(
    private val datasources: Map<String, Jdbi>,
    private val scratchDirectory: Path,
    private val scratchMemoryLimitMb: Int = 4096,
    private val sleeper: (Long) -> Unit = { Thread.sleep(it) },
    private val listener: TaskRunListener = TaskRunListener.NONE,
    private val hooks: TaskHooks = TaskHooks(),
    private val clock: Clock = Clock.systemUTC(),
) {

    init {
        require(SCRATCH !in datasources) {
            "'$SCRATCH' is the reserved name of the per-run DuckDB working file and cannot also be a " +
                "configured datasource (spec 7.1)."
        }
    }

    /**
     * Runs every phase in order and returns the outcome rather than throwing: a failed task is a
     * result, not an exception, because P7's dispatcher and P8's listeners both need the run to
     * end normally.
     *
     * An `Error` is not caught. A [CacheCopyStep] therefore propagates its [NotImplementedError]
     * out of this method instead of being reported as an ordinary task failure, which is what a
     * not-yet-built step should look like.
     *
     * `onTaskStart` fires **first**, before [ScratchDb] is constructed: its `init` rejects a
     * non-positive `memory_limit`, and a run that dies there is still a run the listener has to
     * have seen start and end. `onTaskEnd` fires from a `finally`, so the `Error` above ends the
     * run for the listener on its way past - two subsystems reporting the same run must not
     * disagree about whether it ended. Hooks are not run on that path: executing host code while
     * an `OutOfMemoryError` unwinds is a worse failure than not reporting.
     *
     * @param runId defaulted so a direct caller need not invent one, and passed in by
     *   [TaskRunner], which has to answer `Accepted(runId)` the instant a run is submitted and
     *   long before this method returns (spec 8.2). One id, so the scratch directory, the `runId`
     *   task variable and the admin API all name the same run.
     * @param triggeredBy the caller identity of spec 8.2, null for a scheduled firing. Carried
     *   into [TaskContext] and nowhere else: this module records it and authorises nothing.
     */
    fun run(
        definition: TaskDefinition,
        trigger: TriggerSource,
        runId: String = UUID.randomUUID().toString(),
        triggeredBy: String? = null,
    ): TaskOutcome {
        val task = TaskContext(runId, definition.name, trigger, triggeredBy, clock.instant())
        val events = Events(task, definition.logging)
        var outcome = Outcome.FAILED
        try {
            events.taskStart()
            val directory = scratchDirectory.resolve(runId)
            val memoryLimit = definition.scratchMemoryLimitMb ?: scratchMemoryLimitMb
            var failure: Throwable? = try {
                ScratchDb(directory, memoryLimit).use { scratch ->
                    Run(definition, task, scratch, DatasetNamer(directory), events).execute()
                }
                null
            } catch (e: Exception) {
                e
            }
            // Hooks run here rather than inside the `use` because ScratchDb.close() can throw -
            // report() raises on a leftover temporary table or an unreclaimed path. Inside the
            // block that failure would arrive as a suppressed exception *after* onSuccess had
            // already declared the run good, flipping the outcome underneath a hook that had
            // already fired. Out here the close is decided first and onSuccess sees the truth.
            if (failure == null) failure = onSuccess(task, definition.onSuccess)
            if (failure != null) onFailure(task, definition.onFailure)
            outcome = if (failure == null) Outcome.SUCCEEDED else Outcome.FAILED
            return TaskOutcome(runId, outcome, failure)
        } finally {
            events.taskEnd(outcome)
        }
    }

    /**
     * Spec 9.4's success hook, or the failure that stands in for it.
     *
     * A name is resolved **at invocation**, so a name absent from the registry is reported exactly
     * as a hook that threw would be: the run fails, and `onFailure` then runs. Deferring the
     * lookup is what makes that possible, and the diagnostic names both the task and the hook so
     * an operator knows which registration is missing.
     */
    private fun onSuccess(task: TaskContext, name: String?): Throwable? {
        if (name == null) return null
        val hook = hooks[name] ?: return IllegalArgumentException(
            "task '${task.taskName}': the onSuccess hook '$name' is not registered (spec 9.4). Known " +
                "hooks are ${hooks.names.sorted()}. The run itself succeeded and is reported failed " +
                "because a hook that was asked for and never ran is not a success.",
        )
        return try {
            hook.run(task)
            null
        } catch (e: Exception) {
            e
        }
    }

    /**
     * Spec 9.4's failure hook. Both ways it can go wrong - unregistered, or throwing - are logged
     * and swallowed, and neither touches the outcome or replaces the failure being reported.
     *
     * That asymmetry with [onSuccess] is the point: the failure-reporting path may never change
     * the failure it reports, or an operator reading `TaskOutcome.failure` would be looking at the
     * reporting mechanism instead of at what broke.
     */
    private fun onFailure(task: TaskContext, name: String?) {
        if (name == null) return
        val hook = hooks[name]
        if (hook == null) {
            log.warn(
                "${task.describe()}: the onFailure hook '$name' is not registered (spec 9.4). Known " +
                    "hooks are ${hooks.names.sorted()}. The run's own failure is reported unchanged.",
            )
            return
        }
        try {
            hook.run(task)
        } catch (e: Exception) {
            log.warn(
                "${task.describe()}: the onFailure hook '$name' threw. It is swallowed: the run keeps " +
                    "the failure this hook was called to report.",
                e,
            )
        }
    }

    /**
     * The seven call sites of spec 9.2, each of which catches, logs at WARN and continues.
     *
     * A listener never changes a run's outcome - a logging plug-in that failed the ETL run it was
     * logging would invert the point of the seam. [TaskRunListener.of] applies the same isolation
     * per listener, so for a composite this catch never fires; it is not redundant, because a host
     * may attach a bare listener, and both guards log so nothing is lost either way.
     *
     * `logging: false` is implemented by binding [sink] to [TaskRunListener.NONE] for the whole
     * run rather than by an `if` at each site: one decision, taken once, that no later call site
     * can forget. Hooks and (from P8b) metrics are reached elsewhere and are unaffected.
     */
    private inner class Events(private val task: TaskContext, logging: Boolean) {

        private val sink: TaskRunListener = if (logging) listener else TaskRunListener.NONE

        fun taskStart() = isolate("onTaskStart") { sink.onTaskStart(task) }

        fun taskEnd(outcome: Outcome) = isolate("onTaskEnd") { sink.onTaskEnd(task, outcome) }

        fun phaseStart(phase: String) = isolate("onPhaseStart") { sink.onPhaseStart(PhaseContext(task, phase)) }

        fun phaseEnd(phase: String, outcome: Outcome) =
            isolate("onPhaseEnd") { sink.onPhaseEnd(PhaseContext(task, phase), outcome) }

        fun stepStart(step: StepContext) = isolate("onStepStart") { sink.onStepStart(step) }

        fun stepEnd(step: StepContext, result: StepResult) = isolate("onStepEnd") { sink.onStepEnd(step, result) }

        fun stepError(step: StepContext, attempt: Int, error: Throwable, willRetry: Boolean) =
            isolate("onStepError") { sink.onStepError(step, attempt, error, willRetry) }

        fun step(phase: String, step: String) = StepContext(task, phase, step)

        private fun isolate(site: String, call: () -> Unit) {
            try {
                call()
            } catch (e: Exception) {
                log.warn(
                    "${task.describe()}: the listener threw from $site and was ignored. A listener " +
                        "never fails the run it is observing (spec 9.2).",
                    e,
                )
            }
        }
    }

    private inner class Run(
        private val definition: TaskDefinition,
        private val task: TaskContext,
        private val scratch: ScratchDb,
        private val namer: DatasetNamer,
        private val events: Events,
    ) {

        private val scope = VariableScope().apply {
            define("runId", task.runId)
            define("taskName", definition.name)
            define("triggerTime", task.startedAt)
            definition.vars.forEach { define(it.name, it.value) }
        }

        /**
         * Every phase in order, each reported started and ended.
         *
         * A terminal step failure ends its phase FAILED and rethrows, so no later step and no
         * later phase starts (spec 2.2). The catch is on `Exception` and not `Throwable` for the
         * same reason [run]'s is: an `Error` is not a task failure and has no business being
         * dressed up as one on its way out.
         */
        fun execute() {
            definition.phases.forEach { phase ->
                events.phaseStart(phase.name)
                try {
                    phase.steps.forEach { execute(phase.name, it) }
                } catch (e: Exception) {
                    events.phaseEnd(phase.name, Outcome.FAILED)
                    throw e
                }
                events.phaseEnd(phase.name, Outcome.SUCCEEDED)
            }
        }

        /**
         * One step, with the retry policy of spec 5.3: `retries` additional attempts, only for a
         * transient failure, backing off 2s, 4s, 8s, 16s, 30s, 30s.
         *
         * Each attempt writes its scratch datasets under its own attempt-suffixed name, so a
         * failed attempt's rows - between nothing and one chunk short of everything it wrote
         * (spec 12) - stay where they are, unreferenced, and cost only space in a file that is
         * deleted at run end (spec 5.5).
         *
         * `onStepStart` fires once, before attempt 1 and **before the guard below**, so a step
         * rejected out of hand still reports a start and then a terminal `onStepError` like any
         * other failure - a guard placed above the call site would leave a phase that failed with
         * no step in it. `onStepEnd` fires only on success; a terminal failure ends with
         * `onStepError(willRetry = false)` and nothing else.
         *
         * `willRetry` is decided and reported before [sleeper] is asked for anything, so a
         * listener sees the decision when it is made and not after the delay it causes. It is not
         * `isTransient` alone: a transient failure on the last attempt reports false.
         */
        fun execute(phase: String, step: Step) {
            val ctx = events.step(phase, step.name)
            events.stepStart(ctx)
            // durationMs spans the whole step - every attempt and every backoff between them -
            // read from the injected clock rather than from the wall or from nanoTime.
            val startedAt = clock.millis()
            var attempt = 1
            while (true) {
                try {
                    val rows = runOnce(step, attempt)
                    val elapsed = clock.millis() - startedAt
                    events.stepEnd(ctx, StepResult(rows.rowsRead, rows.rowsWritten, elapsed, attempt))
                    return
                } catch (failure: Exception) {
                    val willRetry = attempt <= step.retries && isTransient(failure)
                    events.stepError(ctx, attempt, failure, willRetry)
                    if (!willRetry) throw failure
                    sleeper(backoffMillis(attempt))
                    attempt++
                }
            }
        }

        private fun runOnce(step: Step, attempt: Int): PipeResult {
            require(step.retries >= 0) { "step '${step.name}': retries must not be negative, got ${step.retries}." }
            return dispatch(step, attempt)
        }

        private fun dispatch(step: Step, attempt: Int): PipeResult = when (step) {
            is PipeStep -> pipe(step, attempt)
            is MaterializeStep -> materialize(step, attempt)
            is SqlStep -> sql(step, attempt)
            is ExportStep -> export(step, attempt)
            is CacheCopyStep -> throw NotImplementedError(
                "step '${step.name}': the cache copy step of spec 7.3 is P9's, not P5's.",
            )
        }

        /**
         * The one step type where rows pass through the JVM (spec 2.3), so the one type whose
         * [StepResult] carries a real pair. [RowPipe.run] already answers it; before P8a the
         * result was discarded only because the publish `if` was the last statement.
         */
        private fun pipe(step: PipeStep, attempt: Int): PipeResult {
            val target = step.target
            // Spec 5.5 is unconditional: every dataset produced inside scratch is written under an
            // attempt-suffixed name. REQUIRED cannot be, because the author created the table under
            // its stable name - so a retry would append on top of whatever the failed attempt
            // flushed, which spec 12 measures as anything from nothing to one chunk short of the
            // lot. Retries default to 3 for any scratch target, so that duplication would arrive on
            // a default nobody wrote. Loud instead.
            require(
                target !is TableTarget || target.datasource != SCRATCH ||
                    target.createTable != CreateTable.REQUIRED || step.retries == 0,
            ) {
                "step '${step.name}': a scratch target with createTable REQUIRED cannot be retried " +
                    "(retries ${step.retries}). Its table has no attempt-suffixed name, so a retry " +
                    "appends onto the rows the failed attempt already flushed. Use createTable AUTO, " +
                    "which gets the attempt suffix and the stable view of spec 5.5, or state " +
                    "retries: 0 to accept a single attempt."
            }
            val physical = physicalDataset(target, attempt)
            val rows = readFrom(step.source.datasource) { handle ->
                val parameters = variables(handle, step.source.sql, step.name, attempt)
                RowPipe(
                    source = JdbcSource(handle, step.source.sql, parameters),
                    target = writer(step, physical),
                    step = step.name,
                    chunkSize = step.chunkSize ?: definition.chunkSize,
                    transform = step.transform,
                ).run()
            }
            // Publishing is what makes an attempt the live one, so it happens only once the whole
            // attempt has succeeded (spec 5.5). DatasetNamer deliberately does not decide this.
            if (physical != null) namer.publishTable(scratch.connection(), (target as TableTarget).table, attempt)
            return rows
        }

        /**
         * Reports 0 / 0, like every step type but `pipe`: nothing here moves a row through the
         * JVM. Whether a non-scratch CTAS hands back a usable affected-row count is the driver's
         * business and is not measured here - on the one datasource this project can actually run,
         * DuckDB 1.1.3, it is -1 for DDL and for CREATE TABLE AS SELECT (see `update` below). The
         * ruling does not rest on that either way: one field meaning "rows piped" for one step
         * type and "rows the database says it touched" for another would be a number nobody could
         * aggregate.
         */
        private fun materialize(step: MaterializeStep, attempt: Int): PipeResult {
            if (step.datasource != SCRATCH) {
                require(step.format == MaterializeFormat.TABLE) {
                    "step '${step.name}': format PARQUET writes a file into the scratch directory, so it " +
                        "is available only on the '$SCRATCH' datasource (spec 5.6)."
                }
                // Unquoted, so that Oracle folds it to its own storage case exactly as
                // JdbcTableWriter's column names are folded.
                onDatasource(step.datasource) { handle ->
                    val ctas = "create table ${datasetIdentifier(step.output)} as ${step.sql}"
                    update(handle, ctas, step.name, attempt)
                }
                return NO_ROWS
            }
            val connection = scratch.connection()
            val statement = when (step.format) {
                MaterializeFormat.TABLE ->
                    "create table ${quoteIdentifier(namer.physical(step.output, attempt))} as ${step.sql}"
                MaterializeFormat.PARQUET ->
                    "copy (${step.sql}) to " +
                        "'${sqlLiteral(namer.parquetPath(step.output, attempt))}' (format parquet)"
            }
            Jdbi.create(connection).open().use { update(it, statement, step.name, attempt) }
            when (step.format) {
                MaterializeFormat.TABLE -> namer.publishTable(connection, step.output, attempt)
                MaterializeFormat.PARQUET -> namer.publishParquet(connection, step.output, attempt)
            }
            return NO_ROWS
        }

        /** Each statement is its own transaction (spec 5.2), so a retry re-runs all of them. */
        private fun sql(step: SqlStep, attempt: Int): PipeResult {
            onDatasource(step.datasource) { handle ->
                step.statements.forEach { update(handle, it, step.name, attempt) }
            }
            return NO_ROWS
        }

        /**
         * Every variable of the step is read first and defined only once all of them have
         * succeeded. Defining as we go would make a retry after a partial success fail on
         * "already defined" (spec 6.2) and bury the failure that caused the retry.
         */
        private fun export(step: ExportStep, attempt: Int): PipeResult {
            val exported = LinkedHashMap<String, Any?>()
            onDatasource(step.datasource) { handle ->
                step.vars.forEach { variable ->
                    // The scope is only reached once the whole step has succeeded, so without this
                    // two vars of one name would collide in here and spec 6.2's redefinition check
                    // would never see the first of them.
                    require(!exported.containsKey(variable.name)) {
                        "step '${step.name}': variable '${variable.name}' is exported twice by this " +
                            "step. A variable may not be redefined once set (spec 6.2)."
                    }
                    val parameters = variables(handle, variable.sql, step.name, attempt)
                    exported[variable.name] = handle.createQuery(variable.sql)
                        .bindMap(parameters)
                        .scanResultSet { rows, context ->
                            context.use { rows.get().use { single(it, step.name, variable.name) } }
                        }
                }
            }
            exported.forEach { (name, value) -> scope.define(name, value) }
            return NO_ROWS
        }

        /**
         * Spec 6.3: exactly one row and one column; more than one row is an error, zero yields null.
         *
         * **A null carries the export column's type.** Bound as a bare null it reaches the driver
         * as `setNull(pos, Types.OTHER)`, which Oracle rejects on some typed columns - measured on
         * JDBI 3.45.4 through a recording `PreparedStatement` (P5 scratchpad `P5Probe4`):
         * `bindMap` of a plain null records `setNull[1, 1111]`, and of a
         * [org.jdbi.v3.core.argument.NullArgument] records `setNull[1, 93]`, because JDBI binds an
         * `Argument` value **directly** rather than looking up an argument factory for its type.
         * That is what lets the type travel inside `JdbcSource`'s frozen `Map<String, Any?>`
         * without any signature change (spec 11.1, spec 12).
         *
         * The type comes from the result set metadata, which exists whether or not a row came back.
         * Both null shapes are wrapped: no row at all, and one row holding SQL NULL, which is what
         * `select max(ts)` returns over an empty table. Both were driven end to end through this
         * engine with a recording `PreparedStatement` behind the datasource (P5 scratchpad
         * `Drive2`): a genuinely rowless `select lot_code from src where lot_id > 999` bound
         * `setNull[1, 12]` (VARCHAR) into the next step, and a one-row `select max(lot_id) ...`
         * bound `setNull[1, -5]` (BIGINT).
         */
        private fun single(rows: ResultSet, step: String, variable: String): Any? {
            val mapper = RowMapper(rows.metaData, step)
            require(mapper.columns.size == 1) {
                "step '$step', variable '$variable': the export query returns ${mapper.columns.size} " +
                    "columns. An export query returns exactly one row and one column (spec 6.3)."
            }
            val present = rows.next()
            val value = if (present) mapper.map(rows)[mapper.columns.single().name] else null
            require(!present || !rows.next()) {
                "step '$step', variable '$variable': the export query returned more than one row. An " +
                    "export query returns exactly one row and one column; zero rows yields null (spec 6.3)."
            }
            return value ?: NullArgument(rows.metaData.getColumnType(1))
        }

        /**
         * The attempt-suffixed physical name a scratch dataset is written under, or null when the
         * step does not produce one. Only `createTable: AUTO` gets a suffix: under `REQUIRED` the
         * author created the table under its stable name with a `sql` step, and there is no
         * suffixed table for the framework to write or view for it to repoint (spec 5.5).
         */
        private fun physicalDataset(target: PipeTarget, attempt: Int): String? =
            (target as? TableTarget)
                ?.takeIf { it.datasource == SCRATCH && it.createTable == CreateTable.AUTO }
                ?.let { namer.physical(it.table, attempt) }

        private fun writer(step: PipeStep, physical: String?): RowWriter {
            val target = step.target
            val writer = when (target) {
                is TableTarget ->
                    if (target.datasource == SCRATCH) {
                        DuckDbTableWriter(
                            scratch.connection(), physical ?: target.table, target.createTable, step.name,
                        )
                    } else {
                        JdbcTableWriter(jdbi(target.datasource), target.table, step.name)
                    }

                is StatementTarget -> {
                    require(target.datasource != SCRATCH) {
                        "step '${step.name}': target.sql is not available on '$SCRATCH'. DuckDB writes go " +
                            "through the appender, which takes a table and not a statement (spec 4.4, " +
                            "validation rule 11)."
                    }
                    JdbcStatementWriter(jdbi(target.datasource), target.sql, step.name)
                }
            }
            return if (step.addColumns.isEmpty()) writer else DeclaredColumns(writer, step.addColumns, step.name)
        }

        private fun update(handle: Handle, sql: String, step: String, attempt: Int) {
            val parameters = variables(handle, sql, step, attempt)
            // The row count is ignored: DuckDB answers -1 for DDL and for CREATE TABLE AS SELECT.
            handle.createUpdate(sql).bindMap(parameters).execute()
        }

        /**
         * The variables [sql] actually binds, and the point at which spec 6.2's "a variable used
         * before its export is an error" is detected.
         *
         * JDBI's own parser is used so that the names checked here are exactly the names it will
         * look for: it already skips a colon inside a string literal and a `::` cast, both of which
         * appear in DuckDB SQL and in a Windows path literal. Only the parsed names are then bound -
         * see the class KDoc for what JDBI's superfluous-binding check does and does not catch.
         */
        private fun variables(handle: Handle, sql: String, step: String, attempt: Int): Map<String, Any?> {
            val parsed = handle.createUpdate(sql).use { statement ->
                handle.getConfig(SqlStatements::class.java).sqlParser.parse(sql, statement.context).parameters
            }
            require(!parsed.isPositional) {
                "step '$step': the SQL uses positional '?' parameters. Variables bind by name, so write " +
                    "':name' instead (spec 6.3)."
            }
            val undefined = parsed.parameterNames
                .filterNot { it == ATTEMPT || scope.contains(it) }.distinct().sorted()
            require(undefined.isEmpty()) {
                "step '$step': the SQL binds ${undefined.map { ":$it" }}, which no built-in, literal var " +
                    "or earlier export has defined. Variables resolve in step order, so an export step " +
                    "must come before its use (spec 6.2). Defined at this point: " +
                    "${(scope.names + ATTEMPT).sorted()}."
            }
            return parsed.parameterNames.associateWith { if (it == ATTEMPT) attempt else scope[it] }
        }

        /**
         * A handle for a read. A scratch read takes a [ScratchDb.duplicate] rather than the write
         * connection, because a single DuckDB connection must never carry a streaming read and an
         * appender at once - and a `Connection` used from two places is spec 7.2's crash, not an
         * error. The duplicate is closed here rather than left for the run to reclaim.
         */
        private fun <T> readFrom(datasource: String, block: (Handle) -> T): T =
            if (datasource == SCRATCH) scratch.duplicate().use { Jdbi.create(it).open().use(block) }
            else jdbi(datasource).open().use(block)

        /** A handle for a statement. Scratch statements run on the single write connection (spec 7.2). */
        private fun <T> onDatasource(datasource: String, block: (Handle) -> T): T =
            if (datasource == SCRATCH) Jdbi.create(scratch.connection()).open().use(block)
            else jdbi(datasource).open().use(block)
    }

    private fun jdbi(datasource: String): Jdbi = datasources[datasource] ?: throw IllegalArgumentException(
        "datasource '$datasource' is not configured. Known datasources are ${datasources.keys.sorted()}, " +
            "plus the reserved '$SCRATCH' (spec 7.1).",
    )
}

/**
 * Retry applies only to the four transient shapes of spec 5.3, and the cause chain is walked
 * because JDBI wraps every `SQLException` in an `UnableToExecuteStatementException`.
 *
 * Everything else - a type error, a constraint violation, a missing column - fails immediately.
 * Retrying a deterministic failure three times only turns a 10 minute failure into a 30 minute one.
 */
private fun isTransient(failure: Throwable): Boolean =
    generateSequence(failure) { if (it.cause === it) null else it.cause }
        .take(32)
        .any {
            it is SQLTransientException || it is SQLRecoverableException || it is SQLTimeoutException ||
                (it is SQLException && it.sqlState?.startsWith("08") == true)
        }

/** Spec 5.3: exponential from 2s, doubling, capped at 30s. [attempt] is 1 for the first failure. */
private fun backoffMillis(attempt: Int): Long = minOf(30_000L, 2_000L shl minOf(attempt - 1, 20))

/**
 * Adds the columns a transform declares in `transform.addColumns` to the list the target is
 * opened with (spec 9.1, validation rule 14).
 *
 * [RowPipe] opens its target with the source result set's columns, which is all that exists
 * before the first Row, so a column the transform adds is invisible to the target: under
 * `createTable: AUTO` the generated DDL has no such column and the value is silently dropped, and
 * [JdbcTableWriter] binds only the columns it was opened with, so it drops it too. Carrying the
 * declaration is Layer 2's job; doing it here rather than in [RowPipe] keeps Layer 1 free of a
 * concept the snapshot cache has no use for.
 */
private class DeclaredColumns(
    private val delegate: RowWriter,
    added: List<ColumnMeta>,
    private val step: String,
) : RowWriter {

    // Lower cased for the same reason Row keys are: the target is mapped by name and every name
    // it is mapped against is lower case (spec 4.5).
    private val added = added.map { ColumnMeta(it.name.lowercase(), it.type, it.nullable, it.precision, it.scale) }

    override fun open(columns: List<ColumnMeta>) {
        val produced = columns.mapTo(mutableSetOf()) { it.name }
        val clash = added.map { it.name }.filter { it in produced }.sorted()
        require(clash.isEmpty()) {
            "step '$step': transform.addColumns declares $clash, which the source query already " +
                "produces. Declare only the columns the transform adds (spec 9.1)."
        }
        delegate.open(columns + added)
    }

    override fun write(chunk: List<Row>): Int = delegate.write(chunk)

    override fun close() = delegate.close()
}
