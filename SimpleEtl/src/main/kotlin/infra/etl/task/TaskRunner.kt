package infra.etl.task

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

/**
 * What a trigger did (spec 8.2). Sealed rather than a boolean or a nullable runId, because the
 * host's `AdminResource` has four distinct answers to give - 202, 409, 404, 400 - and a caller
 * that forgets one should not compile.
 *
 * [TaskRunner] only ever answers [Accepted] or [AlreadyRunning]: it is handed a definition, so
 * "no such task" and "that task is disabled" are not questions it can be asked. [TaskAdmin],
 * which resolves a name, answers all four.
 */
sealed interface TriggerResult {

    /** The run was submitted. It has a runId now and an outcome later (spec 8.2). */
    data class Accepted(val runId: String) : TriggerResult

    /**
     * The task is already running and this trigger was **rejected, not queued** (spec 8.4), so a
     * slow run cannot accumulate a backlog. It will not run later.
     *
     * **Also the answer after [WiringResult.Wired.close]**, when nothing is running and nothing
     * ever will be again (E16). The three words the case is named for are then untrue and the
     * three the paragraph above is written in - rejected, not queued, will not run later - are
     * exactly true, which is why this is a reuse and not a fifth case. A fifth case would be the
     * more precise name and would break the exhaustive `when` in every host that maps this sealed
     * type to a status code, to describe a state 409 already describes on the wire.
     */
    data object AlreadyRunning : TriggerResult

    /** No task of that name is loaded. */
    data object Unknown : TriggerResult

    /** The task exists but carries `enabled: false`. */
    data object Disabled : TriggerResult
}

/**
 * The current or most recent run of one task, as [TaskAdmin.list] reports it.
 *
 * [finishedAt] and [outcome] are both null while the run is in progress and both non-null once it
 * has ended; that pair is what [TaskStatus.running] reads.
 *
 * @param triggeredBy the caller identity [TaskAdmin.trigger] was given, null for a scheduled
 *   firing. The framework records it and authorises nothing (spec 8.2, 8.6).
 */
data class RunStatus(
    val runId: String,
    val trigger: TriggerSource,
    val triggeredBy: String?,
    val startedAt: Instant,
    val finishedAt: Instant?,
    val outcome: Outcome?,
)

/**
 * The confinement layer between a trigger and [TaskEngine] (spec 8.3, 8.4). Each task gets one
 * `Dispatchers.IO.limitedParallelism(1)` view tagged with a [CoroutineName]; both the scheduled
 * callback and the API trigger submit to it, and neither waits for the run.
 *
 * **The guard sits in front of the submit, not behind it.** A `limitedParallelism(1)` view
 * *serialises* - measured in the P7 scratchpad (`p7probe/Probe.kt`): two coroutines launched into
 * one view both ran, the second after the first, which is exactly the queueing spec 8.4 forbids.
 * So a task is claimed with a compare-and-set **before** anything is launched, and a losing
 * trigger is answered [TriggerResult.AlreadyRunning] and dropped. The dispatcher is what confines;
 * the flag is what rejects.
 *
 * **The definition is captured here, at submit time**, and travels into the coroutine by value.
 * That is what makes spec 8.5's reload safe: a reload replaces what [TaskScheduler] and
 * [TaskAdmin] hand to the *next* trigger and cannot reach a run already in flight.
 *
 * Measured on kotlinx-coroutines 1.10.1 / Kotlin 2.2.0 (P7 scratchpad `p7probe/Probe.kt` and
 * `Probe2.kt`, run with assertions off), because this is the first phase in the project with two
 * threads in it:
 *
 * - `limitedParallelism(1)` needs no `@OptIn` on this version; it compiles clean.
 * - Two runs of one task ran on one worker thread (`DefaultDispatcher-worker-1` twice), and the
 *   triggering thread (`main`) was not it.
 * - Two tasks on two views met inside a `CyclicBarrier`, so different tasks genuinely run at the
 *   same instant - each therefore reaching [TaskEngine] with its own [ScratchDb] and its own
 *   DuckDB connection, never a shared one (spec 7.2, 8.4).
 * - `coroutineContext[CoroutineName]` reads the task name inside the launched block, and the
 *   blocking engine body has no suspending frame to read it from at all, which is why [context]
 *   exists.
 * - `Job.invokeOnCompletion` runs after the block, including when the block throws an `Error`,
 *   and hands the handler that `Error` as the cause. That is the release path.
 *
 * @param engine the executor. One instance serves every task: its fields are configuration, and
 *   every run builds its own scratch state, so nothing mutable is shared across threads.
 */
class TaskRunner(private val engine: TaskEngine) {

    /**
     * `SupervisorJob` so one failed run does not cancel the others.
     *
     * [close] cancels it, and cancelling is worth exactly one thing: **no new run starts**. It
     * does not stop a run already under way, because the engine is blocking JDBC with no
     * suspension point to cancel at (spec 8.3) - such a run reaches its natural end and records
     * its own outcome, which is what [TaskSlot.release] not overwriting a written outcome
     * preserves.
     */
    private val scope = CoroutineScope(SupervisorJob())

    private val tasks = ConcurrentHashMap<String, TaskSlot>()

    /**
     * Claims the task, allocates the runId, and returns **while the run is still parked** - so a
     * 30 minute run never holds an HTTP request open (spec 8.2). Nothing here blocks.
     *
     * @param by the caller identity, recorded into [RunStatus.triggeredBy]. Null for a scheduled
     *   firing. No authorisation happens here, or anywhere in this module (spec 8.6).
     */
    fun submit(definition: TaskDefinition, trigger: TriggerSource, by: String?): TriggerResult {
        // Before the claim, not after: a submit into a cancelled scope launches a job that never
        // runs its block, and claiming first would record a run that has no way to end (E16).
        if (!scope.isActive) return TriggerResult.AlreadyRunning
        val slot = slot(definition.name)
        if (!slot.claim()) return TriggerResult.AlreadyRunning
        val runId = UUID.randomUUID().toString()
        slot.begin(Run(runId, trigger, by, Instant.now()))
        val job = scope.launch(slot.context) { slot.end(engine.run(definition, trigger, runId, by)) }
        job.invokeOnCompletion { cause -> slot.release(cause) }
        return TriggerResult.Accepted(runId)
    }

    /** The current or most recent run of [name], or null if it has never run in this process. */
    internal fun lastRun(name: String): RunStatus? = tasks[name]?.last?.status()

    /**
     * The outcome of [runId], or null when that run is unknown **or has not finished yet**.
     * [TaskOutcome] has no in-progress state and its signature is frozen (spec 11.2), so
     * "is it still running" is read from [TaskStatus.running] instead.
     *
     * ponytail: only the current-or-last run per task is retained, which is what spec 8.2's
     * "poll the runId you were just handed" needs and what stops a long-lived process
     * accumulating a record per firing. A bounded ring per task, if an operator ever needs to
     * look further back than one run.
     */
    internal fun outcome(name: String, runId: String): TaskOutcome? =
        tasks[name]?.last?.takeIf { it.runId == runId }?.outcome

    /**
     * The context task [name] is confined to: its own `limitedParallelism(1)` view of
     * `Dispatchers.IO` plus its [CoroutineName]. Internal rather than private because [submit]
     * launches with exactly this value; internal rather than public because spec 11.2 declares only
     * `submit`, so this is an internal seam its own tests read and not surface a host is offered.
     * The name is observable nowhere else:
     *
     * - not from the run, which is blocking code with no suspending frame to read
     *   `coroutineContext` from (spec 8.3);
     * - not from the worker's thread name, whose `@name` tag exists only under `-ea` (spec 8.3),
     *   so an assertion on it would mean nothing in production;
     * - not from underneath the view either - read in kotlinx-coroutines-core-jvm 1.10.1,
     *   `LimitedDispatcher.dispatch` hands *itself* to the underlying dispatcher as the context
     *   and never the coroutine's, so a wrapper below it sees no name at all.
     *
     * Creates the task's view if it has none yet, which costs no thread: a limited view shares
     * the IO pool rather than owning one (spec 8.3).
     */
    internal fun context(name: String): CoroutineContext = slot(name).context

    /**
     * Spec 8.6's shutdown row, the runner's half (E16): after this, [submit] launches nothing and
     * answers [TriggerResult.AlreadyRunning]. Runs already in flight are left alone - see [scope].
     *
     * Internal, not public and not `AutoCloseable`: spec 11.2 declares `submit` as this class's
     * whole surface, and the seam a host is offered is [WiringResult.Wired.close], which owns the
     * ordering this is one step of. Idempotent, because `Job.cancel` is.
     */
    internal fun close() = scope.cancel()

    private fun slot(name: String): TaskSlot = tasks.computeIfAbsent(name, ::TaskSlot)
}

/** One run, replaced wholesale rather than mutated, so a reader never sees a half-written record. */
private data class Run(
    val runId: String,
    val trigger: TriggerSource,
    val by: String?,
    val startedAt: Instant,
    val finishedAt: Instant? = null,
    val outcome: TaskOutcome? = null,
) {
    fun status() = RunStatus(runId, trigger, by, startedAt, finishedAt, outcome?.outcome)
}

/**
 * One task's confinement and its guard. [busy] is released last, after [last] has been written, so
 * the trigger that wins the next claim cannot observe an unfinished record.
 */
private class TaskSlot(name: String) {

    val context: CoroutineContext = Dispatchers.IO.limitedParallelism(1) + CoroutineName(name)

    private val busy = AtomicBoolean(false)

    @Volatile
    var last: Run? = null
        private set

    fun claim(): Boolean = busy.compareAndSet(false, true)

    fun begin(run: Run) {
        last = run
    }

    fun end(outcome: TaskOutcome) {
        last = last?.copy(finishedAt = Instant.now(), outcome = outcome)
    }

    /**
     * @param cause non-null when the coroutine died on something [TaskEngine.run] does not catch -
     *   an `Error`, such as an `OutOfMemoryError` or one raised by host code inside a listener.
     *   No step type raises one since P9 removed [CacheCopyStep]'s `NotImplementedError` stub, but
     *   the branch is not dead: an `Error` from anywhere under `run` arrives here, and without it
     *   the run stays `running` for the life of the process. Recorded as the run's failure rather
     *   than left looking like a run that never ended.
     */
    fun release(cause: Throwable?) {
        last?.let {
            if (it.outcome == null) {
                last = it.copy(finishedAt = Instant.now(), outcome = TaskOutcome(it.runId, Outcome.FAILED, cause))
            }
        }
        busy.set(false)
    }
}
