package infra.etl.task

/**
 * The cron binding, implemented by the **host** over Quarkus's programmatic `Scheduler`
 * (spec 8.1, 8.6). It is an interface here for the same reason spec 7.1's datasources arrive as a
 * `Map<String, Jdbi>` and spec 9.1's transforms as a `Map<String, RowTransform>`: the framework
 * names what it needs and the host supplies it, so this module boots no framework to test itself.
 *
 * Two obligations the host carries, both stated in spec 8.6 and neither testable here:
 *
 * - [schedule] **must throw on an unparseable expression**. [TaskScheduler.apply] converts that
 *   throw into a [ValidationError] and rejects the whole batch; a host that silently accepts a bad
 *   cron makes spec 8.5's atomic reload a lie. Validation rule 16 is only structural - field count
 *   and legal characters - so this is where an expression is really parsed.
 * - [run] **must not execute inline on the scheduler's own thread**. It hands off to [TaskRunner]
 *   and returns immediately, so the Vert.x worker Quarkus fires on is free long before its
 *   60-second blocked-thread checker notices a 5-to-30 minute run.
 *
 * @return a handle whose `close()` unregisters that job. [TaskScheduler] closes it when the task
 *   is removed, disabled, or given a different cron.
 */
fun interface CronScheduler {
    fun schedule(taskName: String, cron: String, run: () -> Unit): AutoCloseable
}

/**
 * Keeps the [CronScheduler]'s registrations in step with the loaded task definitions (spec 8.1,
 * 8.5). It owns no threads and executes nothing: a firing is handed straight to [TaskRunner],
 * which confines and guards it.
 *
 * **A batch either applies whole or not at all.** If the host rejects any expression, every
 * registration this call made is closed and every one it cancelled is restored, so the throw is
 * reported as a [ValidationReport] and the live registry is exactly what it was. That is spec
 * 8.5's "a bad edit cannot take the scheduler down", and it is why [apply] answers rather than
 * throws.
 *
 * **A firing looks the current definition up by name.** Capturing it in the callback would pin a
 * task whose cron did not change to the definition it had when that cron was first registered, so
 * an edit to its SQL would never take effect - the one reload case that re-registration does not
 * cover. The definition a *run* uses is captured later still, by [TaskRunner.submit].
 *
 * @param cron the host's binding.
 * @param runner where a firing goes. Its self-concurrency guard is what turns a scheduled firing
 *   during a run into a skip rather than a backlog (spec 8.4), which is why the [TriggerResult] is
 *   discarded here: `CronScheduler.schedule` takes a `() -> Unit`, so there is no one to tell.
 */
class TaskScheduler(private val cron: CronScheduler, private val runner: TaskRunner) {

    private val registrations = LinkedHashMap<String, Registration>()

    @Volatile
    private var current: Map<String, TaskDefinition> = emptyMap()

    /**
     * Registers exactly the enabled tasks that carry a cron, unregisters the ones that no longer
     * qualify, and re-registers **only** those whose expression changed. A task whose definition
     * changed but whose cron did not keeps its registration and simply fires the new definition.
     *
     * Cancels run before registrations, so a changed cron is unregistered and registered in that
     * order rather than briefly being live twice.
     *
     * **The definitions are published before anything is registered**, because [fire] reads them
     * from the host's own scheduler thread and cannot be made to wait. Registering first leaves a
     * window in which a callback exists but the map it looks in does not yet hold its task: at
     * startup that window is the whole of `apply`, and a task registered a moment before its cron
     * boundary fires into an empty map and is dropped in silence - indistinguishable from the
     * removed-task skip [fire] documents. Published first, the only firing that can straddle the
     * swap is one for a registration that already existed, which then runs the new definition -
     * which is what a reload is for. The error path puts the previous map back with the previous
     * registrations.
     *
     * @return null when every registration succeeded, or the errors when the [CronScheduler]
     *   rejected at least one expression - in which case nothing has changed.
     */
    @Synchronized
    fun apply(definitions: List<TaskDefinition>): ValidationReport? {
        val wanted = definitions.filter { it.enabled && it.cron != null }.associate { it.name to it.cron!! }
        val previous = current
        current = definitions.associateBy { it.name }

        // Removed, disabled, cron dropped, or cron changed - all four are "what is registered is not
        // what is wanted", and all four are cancelled before anything new is registered.
        val cancelled = registrations.filter { (name, registration) -> wanted[name] != registration.cron }
        cancelled.forEach { (name, registration) ->
            registrations.remove(name)
            close(registration.handle)
        }

        val errors = mutableListOf<ValidationError>()
        val added = mutableListOf<String>()
        for ((name, expression) in wanted) {
            if (name in registrations) continue
            try {
                register(name, expression)
                added += name
            } catch (e: Exception) {
                errors += ValidationError(
                    file = name,
                    step = null,
                    line = null,
                    message = "task '$name': the scheduler rejected cron '$expression' " +
                        "(${e.javaClass.simpleName}: ${e.message}). Validation rule 16 checks only the shape " +
                        "of an expression; the scheduler is the only thing that can parse one " +
                        "(spec 8.5, 8.6).",
                )
            }
        }

        if (errors.isNotEmpty()) {
            added.forEach { close(registrations.remove(it)!!.handle) }
            // Restoring re-runs the same call that just succeeded for these, with the same
            // stateless callback. A host that now rejects what it accepted a moment ago has lost
            // that schedule either way, and there is nothing better to do than leave it out.
            cancelled.forEach { (name, registration) -> runCatching { register(name, registration.cron) } }
            current = previous
            return ValidationReport(errors)
        }

        return null
    }

    /**
     * The task names that currently hold a live cron registration, for [TaskAdmin.list] to report
     * as [TaskStatus.scheduled] (E14).
     *
     * **Reads [registrations], never [current].** The definition map staying in two places is
     * E11's finding, declined three times; a second *reader* of it here is that ruling's named
     * tripwire for a fourth attempt. This answers a question only the registry can answer - "is
     * this name actually wired to the host's scheduler" - which is precisely what a caller holding
     * its own definitions cannot derive, and it is why the two maps disagreeing is now visible
     * instead of permanent and silent (spec 8.6's `apply` row).
     *
     * `@Synchronized` because [registrations] is a plain `LinkedHashMap` published under this
     * monitor by [apply] alone. `list()` is called from an HTTP worker thread, so an unsynchronised
     * read of it is a data race, not merely a stale answer.
     */
    @Synchronized
    internal fun registeredNames(): Set<String> = registrations.keys.toSet()

    private fun register(name: String, expression: String) {
        registrations[name] = Registration(expression, cron.schedule(name, expression) { fire(name) })
    }

    /**
     * One scheduled firing. A task removed by a concurrent reload is silently not fired, which is
     * the same answer unregistering it gives and is why the lookup may come back null.
     */
    private fun fire(name: String) {
        val definition = current[name] ?: return
        runner.submit(definition, TriggerSource.SCHEDULE, null)
    }

    /**
     * A host handle that throws on close is a host bug this class can do nothing about:
     * propagating it would abandon [apply] with half the registry swapped, which is worse than
     * the leak. P8's listener is where it becomes reportable.
     */
    private fun close(handle: AutoCloseable) {
        runCatching { handle.close() }
    }

    private class Registration(val cron: String, val handle: AutoCloseable)
}
