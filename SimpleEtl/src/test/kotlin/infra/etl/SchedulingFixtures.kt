package infra.etl

import infra.etl.task.CronScheduler
import infra.etl.task.MaterializeStep
import infra.etl.task.Outcome
import infra.etl.task.Phase
import infra.etl.task.RunStatus
import infra.etl.task.SCRATCH
import infra.etl.task.SqlStep
import infra.etl.task.TaskAdmin
import infra.etl.task.TaskDefinition
import infra.etl.task.TaskEngine
import infra.etl.task.TaskFileLoader
import infra.etl.task.TaskOutcome
import infra.etl.task.TaskRunner
import infra.etl.task.TaskScheduler
import infra.etl.task.TriggerResult
import infra.etl.task.ValidationReport
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.isDirectory
import kotlin.io.path.writeText
import org.jdbi.v3.core.ConnectionFactory
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf

/**
 * P7 test support: the recording `CronScheduler`, the parking probe that lets a test assert on a
 * run *while it is in progress*, and the phase's single reconciliation seam.
 *
 * **This file is the phase's reconciliation seam.** `TaskScheduler`, `TaskRunner`, `TaskAdmin` and
 * `TriggerResult` were written by the engineer in parallel with these tests, and neither side saw
 * the other. Spec 11.2 freezes the *methods* and not the constructors - the seventh phase running
 * in which the plan names public surface spec 11 never fully declared - so every constructor call
 * in this phase lives in [P7World] and every result unwrapping lives in [Trig], each marked
 * `INTEGRATE:`. No test class names a production constructor.
 *
 * ### Why nothing here sleeps
 *
 * This is the first phase with real concurrency, and a `Thread.sleep` in it is a test that flakes
 * on CI rather than one that fails. Every wait in this file is either a [LinkedBlockingQueue.poll]
 * with a hard [TIMEOUT_SECONDS] deadline whose expiry is a loud failure, or [awaitFinished]'s
 * yielding spin on a public-API condition. Nothing encodes "long enough for the other thread to
 * get there".
 *
 * [awaitFinished] is the one wait that is not a latch, because P7's public surface offers no
 * completion callback at all - listeners are P8's - so "the run is over" is only observable by
 * asking [TaskAdmin.run] whether an outcome exists yet. It is not a sleep: it never pauses for a
 * fixed period, it returns the instant the condition holds, and it fails loudly at the deadline.
 *
 * ### Nothing here touches a DuckDB connection from two threads
 *
 * This is the first phase where two runs are live at once, so two scratch files exist at once
 * (spec 7.2, 8.4). [ProbeDatasource] therefore opens a **fresh in-memory DuckDB instance per
 * `openConnection`** rather than handing out duplicates of one shared connection: two parked runs
 * never meet on one `Connection`. In-memory is legitimate here because a probe datasource is an
 * ordinary external datasource, not the scratch working file that spec 7.2 requires to be a file.
 *
 * No fixture DELETEs, TRUNCATEs or DROPs a dataset, and none creates a temporary table - P4's
 * `NoTempTableTest` scans this file like any other.
 */

/** Every bounded wait in the phase. Generous, because its expiry means "hung", not "slow". */
const val TIMEOUT_SECONDS = 30L

// ---------------------------------------------------------------------------------------------
// The recording CronScheduler
// ---------------------------------------------------------------------------------------------

/**
 * The host's `CronScheduler` (spec 8.6) as a recording double: what was registered, what was
 * unregistered, and a way to fire a registered callback on demand.
 *
 * [registered] is the *state* - what the host's scheduler would fire from now on - and [events] is
 * the *churn*. Both are needed, because "registers exactly the enabled tasks carrying a cron"
 * is answered by the state while "re-registers only those whose cron changed" is answered only by
 * the churn: an implementation that cancels and re-registers everything on every `apply` produces
 * an identical state and a completely different event list.
 *
 * [rejectCron] makes this the "`CronScheduler` that throws on a bad cron" of spec 8.6's last row.
 * Throwing is the host's contractual obligation, so the double owes it too.
 */
class RecordingCron(@Volatile var rejectCron: (String) -> Boolean = { false }) : CronScheduler {

    val events: MutableList<String> = CopyOnWriteArrayList()

    private val live = ConcurrentHashMap<String, Registration>()

    override fun schedule(taskName: String, cron: String, run: () -> Unit): AutoCloseable {
        require(!rejectCron(cron)) { "cron '$cron' is not parseable (task '$taskName')" }
        val registration = Registration(taskName, cron, run)
        live[taskName] = registration
        events += "schedule:$taskName:$cron"
        return registration
    }

    /** Task name to cron, for every registration that has not been closed. */
    val registered: Map<String, String> get() = live.mapValues { it.value.cron }

    /** A cursor into [events], so a test can assert on what one `apply` did and not on history. */
    fun mark(): Int = events.size

    fun since(mark: Int): List<String> = events.drop(mark)

    /** Fires a registered callback on the calling thread, as the host's scheduler would. */
    fun fire(taskName: String) {
        val registration = live[taskName]
            ?: error("task '$taskName' is not registered; registered: ${registered.keys.sorted()}")
        registration.run()
    }

    private inner class Registration(
        val task: String,
        val cron: String,
        val run: () -> Unit,
    ) : AutoCloseable {

        private val closed = AtomicBoolean(false)

        override fun close() {
            if (closed.compareAndSet(false, true)) {
                live.remove(task, this)
                events += "cancel:$task"
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Observing a run from inside it
// ---------------------------------------------------------------------------------------------

/**
 * A place a run stops so that the test thread can assert while it is still in progress.
 *
 * [enter] is called on the run's own thread, from inside an ordinary `sql` step, which is what
 * makes the thread identity assertions of spec 8.3 possible **without reading a thread name**.
 * The `@taskName#1` suffix on a worker thread's name exists only under `-ea`, so a test asserting
 * the coroutine name through it would derive its discriminating power from a JVM flag surefire
 * happens to set - P4's Windows-file-lock finding in a new costume.
 *
 * [threads] is append-only and never cleared, so "the rejected trigger has still not run" is a
 * count and not the absence of an event within some window.
 */
class Probe(private val name: String) {

    private val entries = LinkedBlockingQueue<Thread>()
    private val releases = LinkedBlockingQueue<Unit>()

    /** Every thread that has reached this probe, in arrival order. */
    val threads: MutableList<Thread> = CopyOnWriteArrayList()

    /** False makes the probe a pure recorder: a run passes straight through it. */
    @Volatile
    var parking: Boolean = true

    /** Called on the run's thread. Records, announces, and parks until [release]. */
    fun enter() {
        threads += Thread.currentThread()
        entries.put(Thread.currentThread())
        if (parking) {
            checkNotNull(releases.poll(TIMEOUT_SECONDS, SECONDS)) {
                "probe '$name' was never released - the test parked a run and did not free it"
            }
        }
    }

    /** Blocks the test thread until a run reaches the probe, and returns that run's thread. */
    fun awaitEntry(): Thread =
        entries.poll(TIMEOUT_SECONDS, SECONDS)
            ?: error("probe '$name': no run reached it within ${TIMEOUT_SECONDS}s")

    /** Frees exactly one parked run. */
    fun release() {
        releases.put(Unit)
    }
}

/**
 * A `Jdbi` datasource whose only behaviour is to hand its [probe] the run's thread, and to park
 * there if the test asked it to.
 *
 * A fresh in-memory DuckDB instance per connection, so two concurrent runs never share a
 * `Connection` (spec 7.2's crash-not-error rule) and the parked run holds no lock on anything.
 */
class ProbeDatasource(val probe: Probe) : ConnectionFactory, AutoCloseable {

    private val issued: MutableList<Connection> = CopyOnWriteArrayList()

    override fun openConnection(): Connection {
        probe.enter()
        return DriverManager.getConnection("jdbc:duckdb:").also { issued += it }
    }

    override fun close() = issued.forEach { runCatching { it.close() } }
}

// ---------------------------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------------------------

/** `TaskDefinition` and the `Step` subtypes are P5's and frozen, so these name them directly. */
object P7Tasks {

    /**
     * A task whose one step stops at [probeDatasource].
     *
     * @param touchScratch prefixes a `materialize` into scratch, which is what forces the run to
     *   open a `ScratchDb` before it parks - the only way to observe two scratch files live at
     *   once (spec 8.4).
     */
    fun parking(
        name: String,
        probeDatasource: String,
        cron: String? = null,
        enabled: Boolean = true,
        touchScratch: Boolean = false,
    ): TaskDefinition {
        val work = Phase(
            name = "work",
            steps = listOf(
                SqlStep(
                    name = "park",
                    datasource = probeDatasource,
                    statements = listOf("create table probe_marker as select 1 as i"),
                    retries = 0,
                ),
            ),
        )
        val stage = Phase(
            name = "stage",
            steps = listOf(
                MaterializeStep(
                    name = "stage",
                    datasource = SCRATCH,
                    output = "staged",
                    sql = "select i from range(0, 10) t(i)",
                    retries = 0,
                ),
            ),
        )
        return TaskDefinition(
            name = name,
            enabled = enabled,
            cron = cron,
            phases = if (touchScratch) listOf(stage, work) else listOf(work),
        )
    }

    /** A definition that never runs: the scheduler tests only ever diff these. */
    fun scheduled(name: String, cron: String?, enabled: Boolean = true): TaskDefinition =
        TaskDefinition(
            name = name,
            enabled = enabled,
            cron = cron,
            phases = listOf(Phase("only", listOf(SqlStep("touch", SCRATCH, listOf("select 1"), retries = 0)))),
        )

    /**
     * One task file. `scratch` is the only datasource it names, so a loader configured with no
     * datasources at all still finds it valid (spec 7.1 reserves the name).
     *
     * @param datasource swapped for an unconfigured name to build the one-rule-away invalid file
     *   of validation rule 3. Everything else about the file stays valid, so a rejection cannot
     *   be satisfied by a loader that rejects everything.
     */
    fun yaml(
        name: String,
        cron: String? = null,
        datasource: String = SCRATCH,
        statement: String = "create table reload_marker as select 1 as i",
    ): String {
        val schedule = if (cron == null) emptyList() else listOf("schedule:", "  cron: \"$cron\"")
        return (
            listOf("name: $name") + schedule + listOf(
                "phases:",
                "  - name: only",
                "    steps:",
                "      - name: touch",
                "        type: sql",
                "        datasource: $datasource",
                "        statements:",
                "          - \"$statement\"",
            )
            ).joinToString("\n")
    }

    /** Writes task files into [directory] and returns it. */
    fun directory(directory: Path, vararg files: Pair<String, String>): Path {
        Files.createDirectories(directory)
        files.forEach { (fileName, text) -> directory.resolve(fileName).writeText(text) }
        return directory
    }
}

// ---------------------------------------------------------------------------------------------
// The world under test. INTEGRATE: every production constructor call in this phase is here.
// ---------------------------------------------------------------------------------------------

/**
 * The engine, the runner, the scheduler and the admin, plus the probe datasources they run
 * against and the scratch root they write into.
 *
 * INTEGRATE: spec 11.2 declares `TaskScheduler(cron)`, and bare `TaskRunner`, `TaskAdmin` and
 * `TaskEngine` types with no constructor at all. Every assumption about how they are wired lives
 * in the four `by lazy` blocks below and nowhere else.
 */
class P7World(private val root: Path) : AutoCloseable {

    val scratchRoot: Path = root.resolve("scratch")

    val cron: RecordingCron = RecordingCron()

    private val datasources = LinkedHashMap<String, Jdbi>()
    private val probes = LinkedHashMap<String, ProbeDatasource>()

    /** Registers a probe datasource under [name] and returns the probe behind it. */
    fun probe(name: String): Probe {
        val datasource = ProbeDatasource(Probe(name))
        probes[name] = datasource
        datasources[name] = Jdbi.create(datasource)
        return datasource.probe
    }

    // INTEGRATE: P5's constructor, already reconciled once in TaskFixtures.
    private val engine: TaskEngine by lazy {
        TaskEngine(
            datasources = datasources,
            scratchDirectory = scratchRoot,
            scratchMemoryLimitMb = Etl.MEMORY_LIMIT_MB,
            sleeper = { error("no step in this phase retries, so no backoff should be requested") },
        )
    }

    // INTEGRATE: the runner has to reach the engine somehow; spec 11.2 does not say how.
    val runner: TaskRunner by lazy { TaskRunner(engine) }

    // INTEGRATE: spec 11.2 shows `TaskScheduler(cron)`, but the callback it registers must submit
    // to the runner, so the runner has to arrive here too.
    val scheduler: TaskScheduler by lazy { TaskScheduler(cron, runner) }

    // INTEGRATE: the loader has no datasources, so only `scratch` is a valid name in a task file.
    private val loader: TaskFileLoader by lazy { TaskFileLoader() }

    private var admin: TaskAdmin? = null

    /** The admin over [definitions]. Built once per test, because reload replaces the set. */
    fun admin(vararg definitions: TaskDefinition): TaskAdmin {
        check(admin == null) { "one TaskAdmin per test: reload is how the definition set changes" }
        // INTEGRATE: TaskAdmin needs the runner, the scheduler, the loader and a starting set.
        return TaskAdmin(runner, scheduler, loader, definitions.toList()).also { admin = it }
    }

    /** Every run directory under the scratch root that currently holds a DuckDB file. */
    fun liveScratchFiles(): List<String> =
        if (!scratchRoot.isDirectory()) {
            emptyList()
        } else {
            Files.newDirectoryStream(scratchRoot).use { entries ->
                entries.filter { Files.isRegularFile(it.resolve("scratch.duckdb")) }
                    .map { it.fileName.toString() }
                    .sorted()
            }
        }

    override fun close() = probes.values.forEach { runCatching { it.close() } }
}

// ---------------------------------------------------------------------------------------------
// Reading a result. INTEGRATE: TriggerResult is frozen; TaskStatus is not.
// ---------------------------------------------------------------------------------------------

object Trig {

    /**
     * INTEGRATE: spec 11.2 writes `fun apply(definitions: List<TaskDefinition>)`, which returns
     * Unit, while the plan's own done-when says a bad cron "yields a `ValidationReport`". Assumed
     * to be `ValidationReport?` - null on success - mirroring the frozen `TaskAdmin.reload`. If it
     * came out as Unit-and-throws, this becomes a `runCatching` and no test class changes.
     */
    fun apply(scheduler: TaskScheduler, definitions: List<TaskDefinition>): ValidationReport? =
        scheduler.apply(definitions)

    /** The runId of an [TriggerResult.Accepted], failing with the actual result if it is not one. */
    fun acceptedRunId(result: TriggerResult): String {
        assertInstanceOf(TriggerResult.Accepted::class.java, result)
        return (result as TriggerResult.Accepted).runId
    }

    /**
     * Blocks the test thread until the run has an outcome.
     *
     * Assumes `TaskAdmin.run` answers null while a run is still in progress, which is the only
     * reading `TaskOutcome` allows: it carries `SUCCEEDED` or `FAILED` and has no in-progress
     * case. Not a sleep - see the file KDoc.
     */
    fun awaitFinished(admin: TaskAdmin, task: String, runId: String): TaskOutcome {
        val deadline = System.nanoTime() + SECONDS.toNanos(TIMEOUT_SECONDS)
        while (System.nanoTime() < deadline) {
            admin.run(task, runId)?.let { return it }
            Thread.yield()
        }
        error("run '$runId' of task '$task' had no outcome after ${TIMEOUT_SECONDS}s")
    }

    /**
     * Triggers until the task accepts, and returns the accepted runId.
     *
     * The retry exists only to cover the window between a run's outcome being recorded and the
     * task becoming triggerable again, which no public surface exposes a latch for. It does not
     * soften any rejection assertion: a trigger that was *queued* rather than rejected leaves the
     * task busy on a parked run that this loop never releases, so it expires loudly instead of
     * quietly passing.
     */
    fun awaitAccepted(admin: TaskAdmin, task: String, by: String?): String {
        val deadline = System.nanoTime() + SECONDS.toNanos(TIMEOUT_SECONDS)
        var last: TriggerResult? = null
        while (System.nanoTime() < deadline) {
            val result = admin.trigger(task, by)
            if (result is TriggerResult.Accepted) return result.runId
            last = result
            Thread.yield()
        }
        error("task '$task' never accepted a trigger within ${TIMEOUT_SECONDS}s; last result was $last")
    }

    /**
     * Blocks until [task]'s current run has ended, and returns its record.
     *
     * The scheduled path is where this is needed: `CronScheduler`'s callback returns `Unit`, so a
     * firing hands the test no runId and the listing is the only place to find one.
     */
    fun awaitFinishedRun(admin: TaskAdmin, task: String): RunStatus {
        val deadline = System.nanoTime() + SECONDS.toNanos(TIMEOUT_SECONDS)
        while (System.nanoTime() < deadline) {
            val last = admin.list().single { it.name == task }.lastRun
            if (last?.outcome != null) return last
            Thread.yield()
        }
        error("task '$task' had no finished run after ${TIMEOUT_SECONDS}s")
    }

    fun awaitSucceeded(admin: TaskAdmin, task: String, runId: String): TaskOutcome {
        val outcome = awaitFinished(admin, task, runId)
        assertEquals(Outcome.SUCCEEDED, outcome.outcome) { "run '$runId' failed: ${outcome.failure}" }
        return outcome
    }
}
