package infra.etl.task

import java.nio.file.Path
import org.jboss.logging.Logger

/**
 * One row of spec 8.2's `GET /admin/etl/tasks`: the task, its schedule, and its last run outcome.
 *
 * @param cron null for a task that only ever runs from the API (spec 8.1). What the *definition*
 *   asks for, which is not evidence that anything was registered - see [scheduled].
 * @param scheduled whether [TaskScheduler] currently holds a live registration for this name
 *   (E14). A non-null [cron] with `scheduled = false` has **two** causes, and they read
 *   identically here: spec 8.6's `TaskScheduler.apply` obligation was missed, or
 *   [WiringResult.Wired.close] has run and every task now renders this way, which is the shape a
 *   host's admin view keeps *because* `close` deliberately leaves the definitions in place (E16).
 *   Either way the task is listed with a schedule and will never fire. Always false for a `null`
 *   [cron], which is the normal API-only task.
 * @param lastRun null until the task has run at least once in this process. Run history does not
 *   survive a restart; nothing in spec 8 persists it.
 */
data class TaskStatus(
    val name: String,
    val enabled: Boolean,
    val cron: String?,
    val scheduled: Boolean,
    val lastRun: RunStatus?,
) {
    /**
     * True while a run is in flight. Derived rather than stored so it cannot disagree with
     * [lastRun], and needed because [TaskAdmin.run] answers with a [TaskOutcome], which has only
     * SUCCEEDED and FAILED (spec 11.2) and so cannot say "still running".
     */
    val running: Boolean get() = lastRun != null && lastRun.outcome == null
}

/**
 * The framework surface behind spec 8.2's four endpoints. The host's `AdminResource` maps these
 * results to 202 / 409 / 404 / 400 and carries `@RolesAllowed("etl-admin")`; **this class performs
 * no authorisation of its own** and only records the caller identity it is handed (spec 8.2, 8.6).
 *
 * **[reload] is also the startup path.** Startup runs the same call as the reload endpoint, so
 * there is one load path and one set of validation rules (spec 8.5). The `tasks` parameter exists
 * for the caller that already holds a loaded set - spec 2.1's programmatically built definitions
 * have no directory to be read from - and defaults to nothing, in which case [trigger] answers
 * [TriggerResult.Unknown] until the first reload.
 *
 * Note what the constructor does **not** do: it registers no cron. Scheduling is
 * [TaskScheduler.apply]'s, and a caller that supplies `tasks` here calls it itself.
 *
 * @param runner where a trigger goes, and where the run records live.
 * @param scheduler what registers the crons. [reload] rejects if it rejects.
 * @param loader spec 10's validation, already wired with the datasource, transform and hook names.
 */
class TaskAdmin(
    private val runner: TaskRunner,
    private val scheduler: TaskScheduler,
    private val loader: TaskFileLoader,
    tasks: List<TaskDefinition> = emptyList(),
    private val onTasksLoaded: (Set<String>) -> Unit = {},
) {

    @Volatile
    private var definitions: Map<String, TaskDefinition> = tasks.associateBy { it.name }

    init {
        // Spec 7.1 promises the pool minimum "at startup and on every reload". For the file-driven
        // host, `reload` *is* startup, so its call covers both; a host using `tasks` never calls
        // reload and would otherwise never see the number at all (E14).
        if (tasks.isNotEmpty()) reportPoolMinimums(tasks)
        // Fires beside reportPoolMinimums at both of the two moments a task-name set becomes
        // live - here and in reload - because the pairing is this class's own concern, not a
        // call-site discipline every host re-implements (spec 8.6's seed row, deepened
        // 2026-08-30). Invoking a (Set<String>) -> Unit names no metrics type, which is what
        // refuted the old row's claim that the adapter boundary forced the obligation outward.
        if (tasks.isNotEmpty()) onTasksLoaded(tasks.map { it.name }.toSet())
    }

    /**
     * `POST /admin/etl/tasks/{name}/runs`. Returns as soon as the run is submitted, never when it
     * finishes: a 30 minute run must not be held open behind an HTTP request (spec 8.2).
     *
     * Synchronised against [reload] because the lookup, the enabled check and the submit are one
     * admission decision. Left unsynchronised they are a check-then-act over a map another thread
     * replaces: an operator who disables a task, reloads, and is told the reload succeeded could
     * still watch a trigger that read the map a moment earlier launch a half-hour run of the old
     * enabled definition - the very thing the disable was meant to stop - while [list] reports the
     * task disabled. Spec 8.5's "a task currently running keeps the definition it started with"
     * covers runs already under way, not which runs are allowed to start.
     *
     * The cost is that a trigger waits out a concurrent reload's file read. A reload is an operator
     * action measured in hundreds of milliseconds, and this method only submits: the run itself is
     * launched on [TaskRunner]'s dispatcher and never holds this monitor.
     *
     * @param by the caller identity from the host's security context, recorded into the run
     *   ([RunStatus.triggeredBy]) so an API-triggered run is distinguishable in the listing from a
     *   scheduled one. Nothing here checks it.
     */
    @Synchronized
    fun trigger(name: String, by: String?): TriggerResult {
        val definition = definitions[name] ?: return TriggerResult.Unknown
        if (!definition.enabled) return TriggerResult.Disabled
        return runner.submit(definition, TriggerSource.API, by)
    }

    /**
     * `GET /admin/etl/tasks`, in load order.
     *
     * **Cross-checks the two definition maps against each other** (E14). A host that builds
     * definitions in code and forgets [TaskScheduler.apply] - or calls it and forgets `tasks` -
     * leaves the two permanently disagreeing rather than briefly, and spec 8.6 can only state
     * that as an obligation. Here it becomes observable in the direction each side can see: a
     * definition nothing registered is reported as `scheduled = false` alongside its `cron`, and a
     * registration with no definition is not listable at all, so it is logged instead.
     *
     * `definitions` is read once into a local. Re-reading the `@Volatile` per row would let a
     * concurrent [reload] serve a listing assembled from two different definition sets.
     */
    fun list(): List<TaskStatus> {
        val loaded = definitions
        val registered = scheduler.registeredNames()
        val orphaned = registered - loaded.keys
        if (orphaned.isNotEmpty()) {
            log.warnv(
                "{0} cron registration(s) name a task this TaskAdmin has no definition for: {1}. They will " +
                    "fire and be dropped, and they cannot appear in this listing. The host supplied " +
                    "TaskScheduler.apply a set it did not supply here (spec 8.6).",
                orphaned.size, orphaned.sorted(),
            )
        }
        return loaded.values.map {
            TaskStatus(it.name, it.enabled, it.cron, it.name in registered, runner.lastRun(it.name))
        }
    }

    /**
     * `GET /admin/etl/tasks/{name}/runs/{id}`. Null when that run is unknown or has not finished;
     * [list] distinguishes the two through [TaskStatus.running]. A run whose task a later reload
     * removed is still answered - the record belongs to the run, not to the definition.
     */
    fun run(name: String, runId: String): TaskOutcome? = runner.outcome(name, runId)

    /**
     * `POST /admin/etl/reload`, and the same call a host makes at startup (spec 8.5).
     *
     * Atomic in both halves. Every file is parsed and validated before anything changes, and a
     * batch of crons the host rejects is rolled back whole, so an invalid file or an unparseable
     * expression changes nothing at all and returns the errors instead.
     *
     * **A running task is untouched.** Its definition was captured by [TaskRunner.submit] before
     * the run was launched and travelled into the coroutine by value, so what this method replaces
     * is only what the *next* trigger will read.
     *
     * @return null when the reload succeeded, otherwise every error found and no change made.
     */
    @Synchronized
    fun reload(directory: Path): ValidationReport? {
        val loaded = when (val result = loader.load(directory)) {
            is LoadResult.Invalid -> return result.report
            is LoadResult.Loaded -> result.tasks
        }
        val rejected = scheduler.apply(loaded)
        if (rejected != null) return rejected
        definitions = loaded.associateBy { it.name }
        reportPoolMinimums(loaded)
        onTasksLoaded(loaded.map { it.name }.toSet())
        return null
    }

    /**
     * Spec 7.1's pool-sizing contract, as the half of it this framework can actually answer
     * (review finding H4).
     *
     * A pipe whose source and target name the same datasource holds two connections from that
     * pool at once - the streaming source handle for the whole step, and the target's, taken by
     * `RowWriter.open` from the same `Jdbi`. Two runs that each hold one and wait for the second
     * are in a circular wait, and no acquisition order can break it, because both connections come
     * from one pool. Undersized, that hangs both runs indefinitely with `busy = true` and every
     * later firing of either task is skipped as `AlreadyRunning`: the schedule stalls in silence.
     *
     * **Logged, not checked, and the asymmetry is the point.** The requirement's left-hand side is
     * knowable here - it is a property of the definitions just loaded. Its right-hand side is not:
     * `Jdbi` exposes neither its `ConnectionFactory` nor its `DataSource` (verified against
     * jdbi3-core 3.45.4), so reading the configured pool size would mean reflecting into a third
     * party's private fields, which is a worse thing to own than the problem. Emitting the number
     * an operator has to compare against is the honest half.
     *
     * Counted per *task*, not per step: `TaskRunner` admits one run per task at a time (spec 8.4),
     * so two same-datasource pipes in one task cannot overlap and must not be counted twice.
     */
    private fun reportPoolMinimums(tasks: List<TaskDefinition>) {
        sameDatasourcePipeUsers(tasks).forEach { (datasource, users) ->
            log.infov(
                "datasource {0} needs a connection pool of at least {1}: {2} task(s) {3} run a pipe step " +
                    "whose source and target are both {0}, and each such step holds two connections at " +
                    "once. A smaller pool deadlocks the runs against each other (spec 7.1).",
                datasource, users.size * 2, users.size, users,
            )
        }
    }
}

/**
 * The tasks that run a same-datasource pipe step, by datasource, in load order - the left-hand
 * side of spec 7.1's pool minimum, which is `2 × users.size`.
 *
 * Separate from the logging so it can be asserted on directly: a test that had to read a log
 * appender would be testing the logging framework, and one that asserted nothing would let the
 * arithmetic drift unnoticed.
 *
 * A task appears once per datasource however many such steps it has, because [TaskRunner] admits
 * one run per task at a time (spec 8.4) and two steps of one task therefore cannot overlap.
 */
internal fun sameDatasourcePipeUsers(tasks: List<TaskDefinition>): Map<String, List<String>> {
    val users = LinkedHashMap<String, MutableList<String>>()
    tasks.forEach { task ->
        task.phases.asSequence()
            .flatMap { it.steps.asSequence() }
            .filterIsInstance<PipeStep>()
            .filter { it.source.datasource != SCRATCH && it.source.datasource == it.target.datasource }
            .map { it.source.datasource }
            .distinct()
            .forEach { users.getOrPut(it) { mutableListOf() } += task.name }
    }
    return users
}

private val log: Logger = Logger.getLogger(TaskAdmin::class.java)
