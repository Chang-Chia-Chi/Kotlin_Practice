package infra.etl.task

import java.nio.file.Path

/**
 * One row of spec 8.2's `GET /admin/etl/tasks`: the task, its schedule, and its last run outcome.
 *
 * @param cron null for a task that only ever runs from the API (spec 8.1).
 * @param lastRun null until the task has run at least once in this process. Run history does not
 *   survive a restart; nothing in spec 8 persists it.
 */
data class TaskStatus(
    val name: String,
    val enabled: Boolean,
    val cron: String?,
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
) {

    @Volatile
    private var definitions: Map<String, TaskDefinition> = tasks.associateBy { it.name }

    /**
     * `POST /admin/etl/tasks/{name}/runs`. Returns as soon as the run is submitted, never when it
     * finishes: a 30 minute run must not be held open behind an HTTP request (spec 8.2).
     *
     * @param by the caller identity from the host's security context, recorded into the run
     *   ([RunStatus.triggeredBy]) so an API-triggered run is distinguishable in the listing from a
     *   scheduled one. Nothing here checks it.
     */
    fun trigger(name: String, by: String?): TriggerResult {
        val definition = definitions[name] ?: return TriggerResult.Unknown
        if (!definition.enabled) return TriggerResult.Disabled
        return runner.submit(definition, TriggerSource.API, by)
    }

    /** `GET /admin/etl/tasks`, in load order. */
    fun list(): List<TaskStatus> = definitions.values.map {
        TaskStatus(it.name, it.enabled, it.cron, runner.lastRun(it.name))
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
        return null
    }
}
