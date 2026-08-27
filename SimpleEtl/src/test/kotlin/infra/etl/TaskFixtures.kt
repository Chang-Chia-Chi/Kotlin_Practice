package infra.etl

import infra.etl.duckdb.CreateTable
import infra.etl.pipe.ColumnMeta
import infra.etl.pipe.RowTransform
import infra.etl.task.CacheCopyStep
import infra.etl.task.ExportStep
import infra.etl.task.ExportVar
import infra.etl.task.LiteralVar
import infra.etl.task.MaterializeFormat
import infra.etl.task.MaterializeStep
import infra.etl.task.Outcome
import infra.etl.task.Phase
import infra.etl.task.PipeSource
import infra.etl.task.PipeStep
import infra.etl.task.SCRATCH
import infra.etl.task.SqlStep
import infra.etl.task.StatementTarget
import infra.etl.task.Step
import infra.etl.task.TableTarget
import infra.etl.task.TaskDefinition
import infra.etl.task.TaskEngine
import infra.etl.task.TaskHooks
import infra.etl.task.TaskMetrics
import infra.etl.task.TaskOutcome
import infra.etl.task.TaskRunListener
import infra.etl.task.TriggerSource
import java.io.File
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.io.path.isRegularFile
import org.duckdb.DuckDBConnection
import org.jdbi.v3.core.ConnectionFactory
import org.jdbi.v3.core.Jdbi

/**
 * P5 test support, written for this phase rather than shared with P1's `Duck`, P2's `Scratch`,
 * P3's `Pipe` or P4's `Scratchpad`, all of which belong to phases that may not be edited.
 *
 * **This file is the phase's reconciliation seam.** The engineer built `TaskEngine`,
 * `TaskDefinition`, the `Step` subtypes and `VariableScope` in parallel with these tests, and
 * neither side saw the other. Every place where the production shape is not frozen by spec 11.2
 * is funnelled through a builder here and marked `INTEGRATE:`. No test names a production
 * constructor, so a shape that came out differently is a one-line fix in this file rather than a
 * rewrite of six test classes.
 *
 * Nothing here INSERTs into DuckDB (non-negotiable rule 1), DELETEs, TRUNCATEs or DROPs a
 * dataset (spec 5.5), or creates a temporary table (spec 7.2). Datasets are built with
 * `CREATE TABLE AS SELECT ... FROM range(n)`.
 *
 * ### How a run is observed
 *
 * Scratch is deleted at run end (spec 7.2), so nothing inside it can be inspected afterwards -
 * and because `ScratchDb.close()` empties the directory whether or not opening it was justified,
 * an after-the-fact "no scratch file exists" assertion cannot tell a lazy engine from one that
 * opened scratch speculatively. Both observations therefore happen *during* the run, through
 * public API only:
 *
 * - [probeScratch] is an ordinary `sql` step on the `scratch` datasource that ATTACHes a second
 *   DuckDB file and copies the stable views, the catalog and the view definitions into it with
 *   `CREATE TABLE AS SELECT`. Measured on duckdb_jdbc 1.1.3: attach, cross-catalog CTAS and
 *   detach all work, and the file is readable from a fresh connection afterwards. This is the
 *   shape spec 5.4 already uses for a publish step - author SQL with side effects.
 * - [globCount] is an ordinary `sql` step on an *external* datasource that records how many
 *   files exist under a directory at the moment it runs. Measured: DuckDB's `glob` over a
 *   missing directory returns no rows rather than failing.
 *
 * A file being ATTACHed must not be held open elsewhere - measured, DuckDB 1.1.3 refuses with
 * "File is already open" - so a probe file is never opened by a test until the run has returned.
 */
object Etl {

    /** Matches P4's choice: far enough from DuckDB's default that a readback is unambiguous. */
    const val MEMORY_LIMIT_MB = 512

    /** The reserved datasource name of spec 7.1. */
    const val SCRATCH = "scratch"

    // -------------------------------------------------------------------------------------
    // Definition builders. INTEGRATE: everything in this block names a production constructor.
    // -------------------------------------------------------------------------------------

    /**
     * @param chunkSize null means *do not mention it*, so `TaskDefinition`'s own default of 5000
     *   applies. That distinction is the third case of the chunk-size done-when item, and it
     *   cannot be expressed by passing 5000 explicitly.
     */
    fun task(
        name: String = "wip-summary",
        vararg phases: Phase,
        vars: List<LiteralVar> = emptyList(),
        chunkSize: Int? = null,
    ): TaskDefinition =
        if (chunkSize == null) {
            TaskDefinition(name = name, vars = vars, phases = phases.toList())
        } else {
            TaskDefinition(name = name, vars = vars, chunkSize = chunkSize, phases = phases.toList())
        }

    fun phase(name: String, vararg steps: Step): Phase = Phase(name = name, steps = steps.toList())

    /** [value] is `Any?` so that the rejection of a null literal is expressible (spec 1.3, 6.1). */
    fun literal(name: String, value: Any?): LiteralVar = LiteralVar(name = name, value = value)

    /**
     * A `pipe` step (spec 3.2). [retries] null leaves the field at its declared default, which
     * spec 5.3 makes 3 for a scratch target and 0 for any other; not stating one is the only way
     * to observe that default.
     */
    /**
     * A `pipe` step whose target is a statement rather than a table (spec 4.4). Only the rejection
     * of the `scratch` case is reachable here: the happy path writes through `JdbcStatementWriter`,
     * which INSERTs, and the only non-DuckDB driver in the module is Oracle.
     */
    fun pipeToStatement(
        name: String,
        sourceDatasource: String,
        sql: String,
        targetDatasource: String,
        targetSql: String,
        retries: Int? = null,
    ): PipeStep {
        val source = PipeSource(datasource = sourceDatasource, sql = sql)
        val target = StatementTarget(datasource = targetDatasource, sql = targetSql)
        return if (retries == null) PipeStep(name = name, source = source, target = target)
        else PipeStep(name = name, source = source, target = target, retries = retries)
    }

    fun pipe(
        name: String,
        sourceDatasource: String,
        sql: String,
        table: String,
        createTable: CreateTable = CreateTable.AUTO,
        retries: Int? = null,
        chunkSize: Int? = null,
        transform: RowTransform? = null,
        addColumns: List<ColumnMeta> = emptyList(),
    ): PipeStep {
        val source = PipeSource(datasource = sourceDatasource, sql = sql)
        val target = TableTarget(datasource = SCRATCH, table = table, createTable = createTable)
        return if (retries == null) {
            PipeStep(
                name = name, source = source, target = target, transform = transform,
                addColumns = addColumns, chunkSize = chunkSize,
            )
        } else {
            PipeStep(
                name = name, source = source, target = target, transform = transform,
                addColumns = addColumns, chunkSize = chunkSize, retries = retries,
            )
        }
    }

    /** A `materialize` step (spec 3.3). */
    fun materialize(
        name: String,
        datasource: String = SCRATCH,
        output: String,
        sql: String,
        format: MaterializeFormat = MaterializeFormat.TABLE,
        retries: Int? = null,
    ): MaterializeStep =
        if (retries == null) {
            MaterializeStep(name = name, datasource = datasource, output = output, format = format, sql = sql)
        } else {
            MaterializeStep(
                name = name, datasource = datasource, output = output, format = format, sql = sql,
                retries = retries,
            )
        }

    /** A `sql` step (spec 3.4). Side effects only, no dataset output. */
    fun sql(name: String, datasource: String, vararg statements: String, retries: Int? = null): SqlStep =
        if (retries == null) {
            SqlStep(name = name, datasource = datasource, statements = statements.toList())
        } else {
            SqlStep(name = name, datasource = datasource, statements = statements.toList(), retries = retries)
        }

    /** An `export` step (spec 3.5). Each pair is a variable name and the query that produces it. */
    fun export(name: String, datasource: String, vararg vars: Pair<String, String>, retries: Int? = null): ExportStep {
        val exported = vars.map { (varName, varSql) -> ExportVar(name = varName, sql = varSql) }
        return if (retries == null) {
            ExportStep(name = name, datasource = datasource, vars = exported)
        } else {
            ExportStep(name = name, datasource = datasource, vars = exported, retries = retries)
        }
    }

    /** A `cacheCopy` step (spec 7.3). P9's; here only to prove its executor is not a no-op. */
    fun cacheCopy(name: String, cache: String, sql: String, output: String): CacheCopyStep =
        CacheCopyStep(name = name, cache = cache, sql = sql, output = output)

    // -------------------------------------------------------------------------------------
    // P8a additions (spec 9.2, 9.4). Additive only: every builder above keeps its name, its
    // signature, its defaults and its behaviour. These three are `copy` calls rather than new
    // parameters on [task] precisely so that they can be, and because the three fields they set
    // are carried by P5's model and acted on for the first time by P8a - so a P5 test that never
    // mentions them still means what it meant.
    // -------------------------------------------------------------------------------------

    /** Spec 9.2's per-task switch: `false` suppresses every listener call for that run. */
    fun withLogging(definition: TaskDefinition, logging: Boolean): TaskDefinition =
        definition.copy(logging = logging)

    /**
     * Spec 9.4's two hook names. A null name means *the task names no hook*, which is a different
     * thing from a name that is not in the registry - and telling those two apart is the whole of
     * contract 2.3's resolution rule.
     */
    fun withHooks(
        definition: TaskDefinition,
        onSuccess: String? = null,
        onFailure: String? = null,
    ): TaskDefinition = definition.copy(onSuccess = onSuccess, onFailure = onFailure)

    /**
     * The per-task DuckDB `memory_limit` (spec 7.2). Its only use here is 0, which `ScratchDb`'s
     * own `init` refuses - the one way a test can kill a run *before* scratch exists and still ask
     * what the listener saw.
     */
    fun withScratchMemoryLimitMb(definition: TaskDefinition, limit: Int): TaskDefinition =
        definition.copy(scratchMemoryLimitMb = limit)

    // -------------------------------------------------------------------------------------
    // Probe steps - ordinary steps whose side effect is a durable mid-run observation
    // -------------------------------------------------------------------------------------

    /**
     * A `sql` step on `scratch` that copies what scratch holds *right now* into [probeFile],
     * which the test reads once the run has returned and the scratch file is gone.
     *
     * [relations] are copied by name, so passing `wip_stg` reads the stable view of spec 5.5 and
     * therefore sees whichever attempt was published. Two catalogs come along unasked:
     * `probe_tables` names every physical relation scratch holds, which is where a failed
     * attempt's `wip_stg__a1` shows up, and `probe_views` carries each stable view's definition,
     * which is what says which attempt it points at.
     */
    fun probeScratch(name: String, probeFile: Path, vararg relations: String): SqlStep {
        val statements = buildList {
            add("attach '${sqlPath(probeFile)}' as probe")
            add(
                "create table probe.probe_tables as " +
                    "select table_name from duckdb_tables() where database_name <> 'probe'",
            )
            add(
                "create table probe.probe_views as " +
                    "select view_name, sql from duckdb_views() where internal = false",
            )
            relations.forEach { add("create table probe.$it as select * from $it") }
            add("detach probe")
        }
        return sql(name, SCRATCH, *statements.toTypedArray())
    }

    /**
     * A `sql` step recording how many files exist under [directory] at the moment it runs, into
     * [table] on [datasource].
     *
     * This is what makes the scratch-laziness item falsifiable. Asserting after a run that no
     * scratch file exists proves nothing, because `ScratchDb.close()` deletes the file whether or
     * not opening it was justified, so a speculative open and a lazy one leave the same empty
     * directory behind. Counting from inside the run tells them apart.
     */
    fun globCount(name: String, datasource: String, directory: Path, table: String): SqlStep =
        sql(
            name,
            datasource,
            "create or replace table $table as select count(*) as files from glob('${sqlPath(directory)}/**')",
        )

    /** Forward slashes: DuckDB accepts them on Windows and they need no escaping in a literal. */
    fun sqlPath(path: Path): String =
        path.toAbsolutePath().toString().replace(File.separatorChar, '/').replace("'", "''")

    // -------------------------------------------------------------------------------------
    // Reading a probe file back
    // -------------------------------------------------------------------------------------

    fun longAt(connection: Connection, sql: String): Long = scalar(connection, sql) { it.getLong(1) }

    fun strings(connection: Connection, sql: String): List<String?> = column(connection, sql) { it.getString(1) }

    fun tableExists(connection: Connection, table: String): Boolean =
        longAt(connection, "select count(*) from information_schema.tables where table_name = '$table'") == 1L

    private fun <T> scalar(connection: Connection, sql: String, read: (ResultSet) -> T): T =
        column(connection, sql, read).firstOrNull() ?: error("no rows from: $sql")

    private fun <T> column(connection: Connection, sql: String, read: (ResultSet) -> T): List<T> =
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rows ->
                val out = ArrayList<T>()
                while (rows.next()) out.add(read(rows))
                out
            }
        }
}

/**
 * The engine under test plus the world it runs in: named datasources, a scratch root, and the
 * recorded backoff.
 *
 * **The delay seam.** Spec 5.3's backoff doubles from 2s to a 30s cap, so a retry test that
 * actually waited would take a minute per case and be timing-flaky besides. The engine takes an
 * injected sleeper, and [delaysMillis] records what it asked for - which is the assertion this
 * phase calls for: the *requested* delays, never elapsed wall time. Nothing in this suite sleeps.
 *
 * ### P8a: the clock, the listener and the hook registry
 *
 * The sleeper now also advances [clock], which contract 1.3 makes the engine's only source of
 * time. Nothing that existed before P8a observes the engine's clock, so this changes no earlier
 * assertion; what it buys is that `StepResult.durationMs` is an exact number - a step retried
 * twice reports 6000, cross-checked against [delaysMillis] - instead of a stopwatch reading that
 * cannot tell a correct engine from one that times only the last attempt.
 *
 * [listener] is a `var` and is handed to the engine through a [ForwardingListener], which reads
 * it at every call. The engine below is built `by lazy`: a listener passed straight into that
 * constructor would be captured at the harness's first run, and a later `harness.listener = ...`
 * would silently never arrive - leaving a test asserting an empty recorder and passing for the
 * wrong reason. Two of this phase's tests swap the listener between runs of one harness, which is
 * the shape the paired `logging: true` / `logging: false` assertion needs.
 *
 * [hooks] is one registry, created eagerly and mutated in place, so the instance the engine
 * resolves names in is the same instance whose `.names` a test hands to `TaskFileLoader`. Spec
 * 9.4's startup validation only means something when both sides read the same registry.
 */
class TaskHarness(private val root: Path) : AutoCloseable {

    val scratchRoot: Path = root.resolve("scratch")

    private val recorded = ArrayList<Long>()

    /** Every delay the engine asked for, in order, across every run on this harness. */
    val delaysMillis: List<Long> get() = recorded

    /** P8a. The engine's only source of time, moved only by the sleeper below. */
    val clock: MutableClock = MutableClock()

    /** P8a. Read at every call site, never captured - see the class KDoc. */
    var listener: TaskRunListener = TaskRunListener.NONE

    /** P8a. The registry the engine resolves `onSuccess` / `onFailure` names in. */
    val hooks: TaskHooks = TaskHooks()

    /**
     * P8b. Spec 9.3's metrics seam, read at every call site through a [ForwardingMetrics] for the
     * same `by lazy` reason [listener] is.
     *
     * **The default is [TaskMetrics.NONE] and must stay that way.** Every P8a ordering test
     * asserts a *whole* trace with `assertEquals`, and those traces contain no `metric.` lines. A
     * harness that attached a recorder of its own by default would turn all of them red at once
     * while the engine was behaving perfectly - the single easiest way for this phase to break the
     * previous one. A test that wants metrics attaches its own.
     */
    var metrics: TaskMetrics = TaskMetrics.NONE

    private val datasources = LinkedHashMap<String, Jdbi>()
    private val files = ArrayList<DuckFile>()

    // INTEGRATE: spec 11.2 freezes only `run`, so TaskEngine's constructor is the engineer's.
    private val engine: TaskEngine by lazy {
        TaskEngine(
            datasources = datasources,
            scratchDirectory = scratchRoot,
            scratchMemoryLimitMb = Etl.MEMORY_LIMIT_MB,
            sleeper = { millis -> recorded += millis; clock.advance(millis) },
            listener = ForwardingListener { listener },
            hooks = hooks,
            metrics = ForwardingMetrics { metrics },
            clock = clock,
        )
    }

    fun run(definition: TaskDefinition, trigger: TriggerSource = TriggerSource.SCHEDULE): TaskOutcome =
        engine.run(definition, trigger)

    /**
     * P8a. A run carrying the caller identity of spec 8.2, which the two-argument [run] leaves
     * null. Separate rather than a defaulted parameter on [run], because [run] belongs to P5 and
     * its signature may not change.
     */
    fun runTriggeredBy(definition: TaskDefinition, trigger: TriggerSource, by: String?): TaskOutcome =
        engine.run(definition, trigger, triggeredBy = by)

    /** Runs, and fails the test if the run did not succeed, so a later assertion cannot be vacuous. */
    fun runExpectingSuccess(definition: TaskDefinition, trigger: TriggerSource = TriggerSource.SCHEDULE): TaskOutcome {
        val outcome = run(definition, trigger)
        check(outcome.outcome == Outcome.SUCCEEDED) {
            "task '${definition.name}' was expected to succeed but failed: ${outcome.failure}"
        }
        return outcome
    }

    /**
     * A DuckDB file registered as an ordinary, non-scratch datasource: the "external" target of
     * spec 5.4 whose writes must survive a later phase's failure. The harness owns the file, so a
     * test can read it after the run.
     */
    fun datasource(name: String): DuckFile =
        DuckFile(root.resolve("$name.duckdb")).also {
            files += it
            datasources[name] = Jdbi.create(it)
        }

    /**
     * Registers an arbitrary JDBI under [name], for a datasource this harness does not own - a
     * source wrapped in P3's `RecordingConnections`, say.
     */
    fun register(name: String, jdbi: Jdbi): Jdbi = jdbi.also { datasources[name] = it }

    /**
     * P8c. Starts [definition] on a plain thread of its own, through this harness's single engine,
     * and hands back the handle that joins it.
     *
     * Additive, and deliberately not a change to [run]: P5 owns that signature. What this adds is
     * the only shape in which two runs are live at once *inside one engine and one listener*, which
     * is what the P8c contract's interleaving criterion asks for. P7's `TaskRunner` cannot serve it
     * - it confines each task to its own dispatcher and is a phase whose fixtures may not be edited
     * - and spec 8.4 already says one engine serves concurrent tasks, so an ordinary thread per run
     * is the engine's real contract rather than a test-only liberty.
     *
     * A plain `Thread` and not a coroutine on purpose: `TaskEngine.run` is blocking by design
     * (spec 8.3), the two runs park inside a [ProbeDatasource] rather than suspending, and
     * [BackgroundRun.outcome]'s `Thread.join` is what supplies the happens-before that makes a
     * post-run read of the flow's `replayCache` safe from the test thread. Daemon, so a run that
     * never gets released cannot hold surefire open past its own loud timeout.
     */
    fun start(definition: TaskDefinition, trigger: TriggerSource = TriggerSource.SCHEDULE): BackgroundRun =
        BackgroundRun(definition.name) { engine.run(definition, trigger) }

    /** A path for a probe file. Deliberately not created: an ATTACH creates it and must own it. */
    fun probeFile(name: String): Path = root.resolve("$name-probe.duckdb")

    /** Reads a probe file once the run has released it. */
    fun <T> readProbe(file: Path, block: (Connection) -> T): T {
        check(Files.exists(file)) { "no probe file at $file - the probe step did not run" }
        return DriverManager.getConnection("jdbc:duckdb:${file.toAbsolutePath()}").use(block)
    }

    /** Every regular file under the scratch root, relative and slash-normalised. */
    fun scratchFiles(): List<String> =
        if (!Files.isDirectory(scratchRoot)) {
            emptyList()
        } else {
            Files.walk(scratchRoot).use { walk ->
                walk.filter { it.isRegularFile() }
                    .map { scratchRoot.relativize(it).toString().replace(File.separatorChar, '/') }
                    .sorted()
                    .toList()
            }
        }

    override fun close() = files.asReversed().forEach { runCatching { it.close() } }
}

/**
 * P8c. One run in flight on its own thread, and the join that ends it.
 *
 * [outcome] is the only way to read the result, and it joins first, so no test can read a run's
 * events before the thread that produced them has finished writing them. The bounded join with a
 * loud failure at the deadline is [TIMEOUT_SECONDS]'s discipline from P7: nothing here waits "long
 * enough", and an expiry means hung rather than slow.
 *
 * A throw from the run is re-raised out of [outcome] rather than being swallowed into a null - an
 * `Error` escaping `TaskEngine.run` is a real path (a `CacheCopyStep`'s `NotImplementedError`), and
 * a background thread that died quietly would present as a timeout on a latch somewhere else.
 */
class BackgroundRun internal constructor(private val label: String, body: () -> TaskOutcome) {

    private val result = AtomicReference<TaskOutcome>()
    private val failure = AtomicReference<Throwable>()

    private val thread = Thread(
        {
            try {
                result.set(body())
            } catch (e: Throwable) {
                failure.set(e)
            }
        },
        "p8c-run-$label",
    ).apply { isDaemon = true; start() }

    /** Joins the run's thread and returns what it produced, re-raising anything it threw. */
    fun outcome(): TaskOutcome {
        thread.join(SECONDS.toMillis(TIMEOUT_SECONDS))
        check(!thread.isAlive) {
            "run of '$label' had still not finished ${TIMEOUT_SECONDS}s after it was released"
        }
        failure.get()?.let { throw it }
        return result.get() ?: error("run of '$label' finished with neither an outcome nor a failure")
    }
}

/**
 * One DuckDB file behind a JDBI [ConnectionFactory], with optional failure injection.
 *
 * Every handle JDBI opens gets a `duplicate()` of one connection rather than its own instance,
 * which is how spec 7.2 says extra DuckDB connections are obtained. [connection] is the primary
 * and is never handed to JDBI, so a test can read the file outside a run without racing a step.
 *
 * ### Failure injection
 *
 * [failure] maps a 1-based execution ordinal to the exception that execution should raise, and
 * [failAfterRows] decides whether it is raised at execution or part way through the result set.
 * Mid-stream is the shape the attempt-suffix item needs: the target writer has already opened and
 * created `wip_stg__a1` by the time the source throws, so the failed attempt leaves a table
 * behind exactly as spec 5.5 describes. Failing at execution would leave no table at all, and the
 * test would be asserting something the engine never had the chance to do.
 *
 * ### Counting attempts
 *
 * [attempts] counts statements *prepared*, not statements executed, and it is the number a retry
 * test asserts on. Measured: a DuckDB syntax error is raised at prepare and never reaches an
 * execute at all - JDBI reports it as `UnableToCreateStatementException` - so a counter that
 * only saw executions would read zero for the case most worth counting. JDBI prepares exactly
 * once per statement, so for a step with one statement this is the attempt count. Every retry
 * test here uses one statement per step.
 *
 * [executions] is the separate ordinal that schedules [failure], so an injected failure lands on
 * a chosen attempt whether or not that attempt got as far as a result set.
 *
 * Measured on JDBI 3.45.4 and duckdb_jdbc 1.1.3: an exception thrown from `ResultSet.next()`
 * reaches the caller as `org.jdbi.v3.core.result.ResultSetException` **with the SQLException as
 * its cause**, and one raised while connecting arrives wrapped in `ConnectionException` the same
 * way. Spec 5.3's classification therefore only ever meets a transient exception through a cause
 * chain; a classifier that looked only at the exception it caught would retry nothing at all.
 */
class DuckFile(val file: Path) : ConnectionFactory, AutoCloseable {

    var failure: (Int) -> Throwable? = { null }

    /** Rows to hand over before failing. 0 fails at execution, before any row exists. */
    var failAfterRows: Int = 0

    val executions = AtomicInteger()

    /** Statements prepared through JDBI on this datasource: one per step attempt. */
    val attempts = AtomicInteger()

    private var primary: DuckDBConnection? = null
    private val issued = ArrayList<Connection>()

    fun connection(): DuckDBConnection = primary ?: openPrimary()

    override fun openConnection(): Connection {
        val real = connection().duplicate().also { issued += it }
        return Proxy.newProxyInstance(javaClass.classLoader, arrayOf(Connection::class.java), Handler(real))
            as Connection
    }

    fun exec(vararg sql: String) =
        connection().createStatement().use { statement -> sql.forEach { statement.execute(it) } }

    /**
     * A source table of [rows] rows, with [marker] woven into `lot_code` so that "the view
     * resolves to attempt 2" is answered by the data and not by a row count two attempts share.
     * DuckDB reports every column nullable, so an AUTO target creates all of them as
     * null-accepting types (spec 4.6) and no column needs a CAST.
     */
    fun createSourceTable(table: String, rows: Int, marker: String = "m") = exec(
        """
        create table $table as
        select cast(i as bigint)                as lot_id,
               cast('$marker-' || i as varchar) as lot_code,
               cast(i * 1.5 as decimal(18,3))   as qty
        from range(0, $rows) t(i)
        """,
    )

    /** Fails the first [count] executions with a fresh exception from [build], then succeeds. */
    fun failFirst(count: Int, afterRows: Int = 2, build: () -> Throwable) {
        failAfterRows = afterRows
        failure = { ordinal -> if (ordinal <= count) build() else null }
    }

    /** Fails every execution, for the backoff-cap case. */
    fun failAlways(afterRows: Int = 2, build: () -> Throwable) {
        failAfterRows = afterRows
        failure = { build() }
    }

    fun longAt(sql: String): Long = Etl.longAt(connection(), sql)

    fun strings(sql: String): List<String?> = Etl.strings(connection(), sql)

    fun tableExists(table: String): Boolean = Etl.tableExists(connection(), table)

    private fun openPrimary(): DuckDBConnection {
        Files.createDirectories(file.toAbsolutePath().parent)
        return (DriverManager.getConnection("jdbc:duckdb:${file.toAbsolutePath()}") as DuckDBConnection)
            .also { primary = it }
    }

    override fun close() {
        issued.forEach { runCatching { it.close() } }
        primary?.let { runCatching { it.close() } }
        primary = null
    }

    private inner class Handler(private val real: Any) : InvocationHandler {

        /** The exception the execution that just started is scheduled to raise, once it has fed rows. */
        private var armed: Throwable? = null
        private var rowsLeft = 0

        override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
            if (real is Connection && (method.name == "prepareStatement" || method.name == "createStatement")) {
                attempts.incrementAndGet()
            }
            if (real is Statement && method.name.startsWith("execute")) {
                val scheduled = failure(executions.incrementAndGet())
                if (scheduled != null && failAfterRows == 0) throw scheduled
                armed = scheduled
                rowsLeft = failAfterRows
            }
            if (real is ResultSet && method.name == "next") {
                if (rowsLeft <= 0) armed?.let { armed = null; throw it }
                rowsLeft--
            }
            val result = try {
                method.invoke(real, *(args ?: emptyArray()))
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
            return when {
                real is Connection && result is Statement -> wrap(result, statementFaces(result))
                real is Statement && result is ResultSet -> wrap(result, arrayOf(ResultSet::class.java))
                else -> result
            }
        }

        /** The result set inherits the arming of the execution that produced it. */
        private fun wrap(inner: Any, faces: Array<Class<*>>): Any {
            val handler = Handler(inner)
            handler.armed = armed
            handler.rowsLeft = rowsLeft
            return Proxy.newProxyInstance(javaClass.classLoader, faces, handler)
        }

        private fun statementFaces(statement: Statement): Array<Class<*>> =
            if (statement is PreparedStatement) arrayOf(PreparedStatement::class.java)
            else arrayOf(Statement::class.java)
    }
}
