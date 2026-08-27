package infra.etl.task

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

/**
 * Host code the framework runs at the end of a task (spec 9.4): invalidate a cache, notify a
 * downstream system, page someone.
 *
 * A hook receives only the [TaskContext] and holds nothing of the run - by the time it runs the
 * scratch file is closed and deleted, so there is nothing left for it to read.
 */
fun interface TaskHook {
    fun run(ctx: TaskContext)
}

/**
 * Where a host publishes its hooks (spec 9.4). Names, not classes, because a task file names its
 * `onSuccess` and `onFailure` as strings and validation rule 5 checks those strings at startup.
 */
interface TaskHookRegistry {
    fun register(name: String, hook: TaskHook)
}

/**
 * The registry the application actually holds: one instance, filled at startup, read for the rest
 * of the process.
 *
 * Backed by a [ConcurrentHashMap] because the two sides run on different threads with no
 * happens-before between them - `register` from the host's startup beans, [get] from the N task
 * threads of spec 8.4.
 *
 * **[names] is a live view, not a snapshot.** The set a caller holds reflects registrations made
 * after it was taken, which is what lets `TaskFileLoader(hooks = registry.names)` be constructed
 * without depending on the order the host's startup beans happen to fire in. It is unmodifiable:
 * the only way in is [register].
 */
class TaskHooks : TaskHookRegistry {

    private val hooks = ConcurrentHashMap<String, TaskHook>()

    /**
     * @throws IllegalArgumentException if [name] is already registered, and **the first
     *   registration stands**. Two startup beans both claiming one name is a deployment mistake,
     *   and the alternative to refusing is that which hook runs depends on bean initialisation
     *   order - a difference nothing in the task file or the log would reveal.
     */
    override fun register(name: String, hook: TaskHook) {
        val existing = hooks.putIfAbsent(name, hook)
        require(existing == null) {
            "hook '$name' is already registered. A hook name identifies exactly one hook (spec 9.4); " +
                "the first registration stands and this one was refused, because otherwise which hook " +
                "runs would depend on the order the registering beans happened to start in."
        }
    }

    /** The registered names, for validation rule 5. Unmodifiable, and live - see the class KDoc. */
    val names: Set<String> get() = Collections.unmodifiableSet(hooks.keys)

    /**
     * The hook, or null when nothing is registered under [name].
     *
     * Names resolve **at invocation**, not at run start, and [TaskEngine] treats the two answers
     * differently by position: an absent `onSuccess` fails the run, an absent `onFailure` is
     * logged and swallowed, because the failure-reporting path may never change the failure it is
     * reporting.
     */
    operator fun get(name: String): TaskHook? = hooks[name]
}
