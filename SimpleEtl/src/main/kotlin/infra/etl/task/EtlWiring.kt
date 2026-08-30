package infra.etl.task

import infra.etl.pipe.RowTransform
import java.nio.file.Path
import java.time.Clock
import org.jdbi.v3.core.Jdbi

/**
 * What [EtlWiring.start] produced: the wired [TaskAdmin], or the errors that stopped it.
 *
 * Sealed for the reason [LoadResult] is. A wiring that failed has no `TaskAdmin` at all, so a
 * nullable admin beside a nullable report would leave three impossible states representable and
 * two of them silent. [Invalid] carries exactly what [TaskAdmin.reload] and [TaskScheduler.apply]
 * already answer with, so a host maps one report shape at startup and at reload rather than two.
 */
sealed interface WiringResult {

    /**
     * Everything is wired, the crons are registered, and this is what `AdminResource` serves.
     *
     * A plain class rather than a `data class` since E16: it holds the runner and the scheduler
     * as well, so that [close] can reach them, and a generated `equals` over those two would be
     * meaningless. [admin] is unchanged, and `copy`/`componentN` were used nowhere.
     */
    class Wired internal constructor(
        val admin: TaskAdmin,
        private val runner: TaskRunner,
        private val scheduler: TaskScheduler,
    ) : WiringResult, AutoCloseable {

        /**
         * **The shutdown seam** (E16; spec 8.6, 11.2). Until this existed a host could stop its
         * schedule only by reloading an empty directory - which cancels the registrations as a
         * *side effect* of throwing the task list away, blanking `GET /admin/etl/tasks` at exactly
         * the moment an operator is watching a shutdown, and which cannot stop the runner at all.
         *
         * Registrations first, then the runner's scope. The order is what makes the second half
         * meaningful: a firing the host's scheduler had already dispatched is past the point where
         * cancelling its registration can help, and is stopped by the scope instead. After this,
         * [TaskAdmin.trigger] answers [TriggerResult.AlreadyRunning] for every task - 409,
         * "rejected, not queued, it will not run later", which is what a shutting-down server
         * owes; spec 11.2 records why that is a reuse and not a fifth sealed case.
         *
         * **A run already in flight is not interrupted, and cannot be.** Spec 8.3 makes
         * `TaskEngine.run` an ordinary blocking function, so nothing under it suspends and a
         * cancelled `Job` has no cancellation point to act on. Such a run reaches its natural end
         * and records its real outcome, which [admin] still answers for afterwards. A host that
         * needs shutdown *bounded* owns that bound: spec 8.6's per-`DataSource` statement timeout
         * is the only lever on a wedged call, here as everywhere else.
         *
         * [admin] keeps its definitions and `list()` keeps answering. Idempotent.
         *
         * **A composed host calls this before `ManagedSnapshotCache.close()`**, which is the
         * cache's own spec 10.2 steps 2 and 3 - stop scheduling, stop starting new work - and
         * which no host previously had a way to perform. Reversed, a `cacheCopy` step can take a
         * lease on a cache that is already draining and be answered `ShuttingDownException`
         * mid-run.
         */
        override fun close() {
            scheduler.cancelAll()
            runner.close()
        }
    }

    /**
     * Nothing was started. The task files did not validate, or the [CronScheduler] rejected an
     * expression - in which case, as at reload, no registration survives (spec 8.5).
     */
    data class Invalid(val report: ValidationReport) : WiringResult
}

/**
 * **The one place a host states each of its parts once** (spec 11.2).
 *
 * [TaskEngine], [TaskFileLoader], [TaskRunner], [TaskScheduler] and [TaskAdmin] have to be built
 * in a particular order, and four of spec 8.6's host obligations are the same shape - *pass the
 * same thing to two constructors*. An adoption dry-run had a fresh reader build a host from spec
 * 11.2 alone and get three of the four constructors wrong on the first compile. This class is that
 * wiring written once, in the module that owns the types, so those four rows are discharged by
 * construction instead of by an operator reading a table:
 *
 * - [hooks] is one [TaskHooks]; the engine gets the registry and the loader gets `names` off it.
 * - [caches] is one map; the engine gets it and the loader gets its `keys`.
 * - [datasources] is one map; the engine gets it and the loader gets its `keys`.
 * - [transforms] reaches the loader, which is the only thing that validates against it.
 * - `start` calls [TaskScheduler.apply] and the initial load itself, so a host cannot forget
 *   either and be told nothing.
 *
 * A loader and an engine wired here cannot disagree about a name, which is precisely the failure
 * spec 9.4 exists to prevent: rule 5 passing vacuously for a typo that then dies at the end of a
 * 30 minute run.
 *
 * ### Why this is not `SimpleEtlBuilder`
 *
 * It is not a builder. There is no mutable accumulation, no `withX()` chain, no `build()`.
 * Kotlin's named and defaulted parameters *are* the builder pattern for a ten-argument
 * constructor, and a builder class over them would re-implement a language feature and add a
 * half-built state that cannot occur here. What this holds is the set of things the host supplies
 * - its wiring - stated once, with spec 2.1's two entry paths as two small overloads over it
 * rather than as two ten-argument functions.
 *
 * ### Why a factory is allowed here at all
 *
 * P7 rejected one (progress.md, P7 deviation 1). The decisive measurement was that **Quarkus does
 * not read `application.properties` from a dependency jar**, so shipping
 * `quarkus.scheduler.start-mode=forced` in this module would have put it in a file only this
 * module's own tests read while the real deployment fired nothing - a green test for a production
 * failure. That rejection turns on a factory having to own host *configuration*. This one owns
 * none: every parameter is a JDK type, a Layer 1 type, or a seam spec 11.2 already declares, and
 * nothing here reads a property, scans a classpath or names a framework. `infra.etl.task` still
 * compiles and tests without Quarkus on the classpath, and ArchUnit still says so.
 *
 * ### What it cannot absorb, and therefore names
 *
 * This list is load-bearing, not boilerplate. Everything below is still spec 8.6's, and a green
 * test in this repository is evidence about none of it:
 *
 * - **`quarkus.scheduler.start-mode=forced`** in the *application's* own `application.properties`.
 *   Missed, no task ever fires and no error is raised.
 * - **`@RolesAllowed("etl-admin")`** on every endpoint. [TaskAdmin] authorises nothing and records
 *   the identity it is handed; missed, an unauthenticated caller can trigger any task.
 * - **The `AdminResource` HTTP mapping** - [TriggerResult] and the rest to 202 / 409 / 404 / 400.
 * - **[CronScheduler.schedule] throwing on an unparseable expression.** Validation rule 16 is
 *   structural only, so the host's scheduler is the one thing that really parses a cron. A host
 *   that accepts a bad one silently makes spec 8.5's atomic reload a lie, and `start` inherits
 *   that: it can only report what the scheduler rejects.
 * - **`io.micrometer:micrometer-core` on the application's runtime classpath.** The framework
 *   declares it `optional` - compiled against, never inherited transitively.
 * - **Passing `metrics::seed` as [onTasksLoaded]** - the framework then invokes it after the
 *   initial load and after every reload, which absorbed what this list used to state as a
 *   call-site obligation. The old entry claimed this package could not absorb it because it may
 *   not name the binding; a depth review (2026-08-30) refuted that - invoking a
 *   `(Set<String>) -> Unit` names nothing. What stays the host's is only supplying the reference:
 *   forget it and a never-run task emits no `etl_task_runs_total` series (spec 8.6, 9.3).
 * - **A statement or query timeout on each `DataSource` behind a [Jdbi].** The framework has none
 *   anywhere and that is the design (spec 3.6, 8.6).
 *
 * @param scratchDirectory spec 7.2's disk-backed scratch root. Required, as on [TaskEngine].
 * @param cron the host's binding. Required and undefaulted on purpose: a no-op default would make
 *   a wiring that registers nothing look exactly like one that works.
 * @param hooks the registry the host fills at startup. `TaskHooks.names` is a live view, so a hook
 *   registered *after* this call still reaches validation rule 5 - which is why one instance here
 *   is enough and the ordering of the host's startup beans does not matter.
 */
class EtlWiring(
    private val scratchDirectory: Path,
    private val cron: CronScheduler,
    private val datasources: Map<String, Jdbi> = emptyMap(),
    private val transforms: Map<String, RowTransform> = emptyMap(),
    private val hooks: TaskHooks = TaskHooks(),
    private val caches: Map<String, CacheBinding> = emptyMap(),
    private val scratchMemoryLimitMb: Int = 4096,
    private val listener: TaskRunListener = TaskRunListener.NONE,
    private val metrics: TaskMetrics = TaskMetrics.NONE,
    private val clock: Clock = Clock.systemUTC(),
    // The host's metric-series seeding (or any other reaction to a live task-name set),
    // invoked by TaskAdmin after the initial load and after every successful reload. Pass the
    // binding's `seed`; see spec 8.6's seed row and 9.3's idempotence contract.
    private val onTasksLoaded: (Set<String>) -> Unit = {},
) {

    /**
     * Spec 2.1's file-driven path.
     *
     * Delegates to [TaskAdmin.reload], which *is* the startup path (spec 8.5) - so there is one
     * load path, one set of validation rules and one place [TaskScheduler.apply] is called from.
     * A directory that does not validate leaves nothing registered and nothing loaded.
     *
     * @throws java.io.IOException if [taskDirectory] cannot be read at all, which [TaskFileLoader]
     *   treats as a deployment fault rather than an authoring one and this call does not reshape.
     */
    fun start(taskDirectory: Path): WiringResult {
        val runner = runner()
        val scheduler = TaskScheduler(cron, runner)
        val admin = TaskAdmin(runner, scheduler, loader(), onTasksLoaded = onTasksLoaded)
        val report = admin.reload(taskDirectory)
        return if (report == null) {
            WiringResult.Wired(admin, runner, scheduler)
        } else {
            WiringResult.Invalid(report)
        }
    }

    /**
     * Spec 2.1's programmatic path, for the caller that builds definitions in code and has no
     * directory to read.
     *
     * **[TaskScheduler.apply] runs first, and that is the point of this overload.** Spec 8.6's
     * longest row is the obligation to call it yourself on this path - missed, `list()` reports
     * every task and not one of them ever fires, with no error raised. Applying before the
     * [TaskAdmin] exists also means a rejected batch produces no half-wired admin: on [Invalid]
     * nothing is registered and nothing was constructed to hold the definitions.
     *
     * These definitions never meet [TaskFileLoader]'s rules - they were not parsed from a file -
     * and this call does not change that. [TaskEngine] checks the task-shaped rules on the way in,
     * which is where spec 2.1 has always put it.
     */
    fun start(definitions: List<TaskDefinition>): WiringResult {
        val runner = runner()
        val scheduler = TaskScheduler(cron, runner)
        val rejected = scheduler.apply(definitions)
        return if (rejected != null) {
            WiringResult.Invalid(rejected)
        } else {
            WiringResult.Wired(TaskAdmin(runner, scheduler, loader(), definitions, onTasksLoaded), runner, scheduler)
        }
    }

    private fun runner() = TaskRunner(
        TaskEngine(
            datasources = datasources,
            scratchDirectory = scratchDirectory,
            scratchMemoryLimitMb = scratchMemoryLimitMb,
            listener = listener,
            hooks = hooks,
            metrics = metrics,
            clock = clock,
            caches = caches,
        ),
    )

    /**
     * The loader, with all four name sets derived from the same values [runner] hands the engine
     * (spec 8.6). `TaskEngine`'s `sleeper` is left at its default here and is not a parameter of
     * this class: spec 11.2 declares it as spec 5.3's backoff injected for tests, and a host
     * replacing it would be turning off retry backoff in production.
     */
    private fun loader() = TaskFileLoader(datasources.keys, transforms, hooks.names, caches.keys)
}
