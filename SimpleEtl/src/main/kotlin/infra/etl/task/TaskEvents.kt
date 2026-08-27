package infra.etl.task

import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow

/**
 * One observable moment of a run: the flow-shaped form of spec 9.2's seven call sites.
 *
 * The vocabulary is 1:1 with [TaskRunListener] - seven call sites, seven subtypes, no collapsing
 * of `onStepEnd` and `onStepError` into one "step finished" event, because a collector that has to
 * inspect a nullable field to learn whether the step worked is worse off than one that matched on
 * a type.
 *
 * [task] is the run every event belongs to and is the **same instance** the engine handed the
 * listener, for every event of one run - so a collector reassembling a run may compare by identity
 * and does not have to key on [TaskContext.runId] unless it wants to. On the five phase- and
 * step-shaped subtypes it is a delegating getter declared in the class *body*, which keeps it out
 * of the generated `equals` / `hashCode` / `copy`: `PhaseStarted` compares by its `phase` alone,
 * which already carries the same context.
 */
sealed interface TaskEvent {

    val task: TaskContext

    /** Spec 9.2's `onTaskStart`. */
    data class TaskStarted(override val task: TaskContext) : TaskEvent

    /** Spec 9.2's `onTaskEnd`. Reached from a `finally`, so a run that started always ends. */
    data class TaskEnded(override val task: TaskContext, val outcome: Outcome) : TaskEvent

    /** Spec 9.2's `onPhaseStart`. */
    data class PhaseStarted(val phase: PhaseContext) : TaskEvent {
        override val task get() = phase.task
    }

    /** Spec 9.2's `onPhaseEnd`. [Outcome.FAILED] when a step of the phase failed terminally. */
    data class PhaseEnded(val phase: PhaseContext, val outcome: Outcome) : TaskEvent {
        override val task get() = phase.task
    }

    /** Spec 9.2's `onStepStart`. Fires before the first attempt - see the pairing note on [TaskEventFlow]. */
    data class StepStarted(val step: StepContext) : TaskEvent {
        override val task get() = step.task
    }

    /** Spec 9.2's `onStepEnd`. Success only: a terminally failed step ends with [StepError] instead. */
    data class StepEnded(val step: StepContext, val result: StepResult) : TaskEvent {
        override val task get() = step.task
    }

    /**
     * Spec 9.2's `onStepError`: **one failed attempt**, not a failed step. Named for the call site
     * it mirrors rather than "StepFailed", which would read as terminal when this fires for attempt
     * 1 of 3.
     *
     * @param attempt numbered from 1.
     * @param error the failure as the step threw it, by identity. On every JDBI path that is an
     *   `UnableToExecuteStatementException` around the `SQLException`, so a collector classifying a
     *   failure walks the cause chain rather than testing the top-level type.
     * @param willRetry decided and reported **before** the backoff sleep. False both for a
     *   non-transient failure and for a transient one that has run out of attempts (spec 5.3).
     */
    data class StepError(
        val step: StepContext,
        val attempt: Int,
        val error: Throwable,
        val willRetry: Boolean,
    ) : TaskEvent {
        override val task get() = step.task
    }
}

/**
 * A [TaskRunListener] that republishes spec 9.2's call sites as a `SharedFlow` of [TaskEvent],
 * for a host that would rather `collect` than implement seven methods.
 *
 * It is an **ordinary listener** and adds no call site to [TaskEngine]. Attach it like any other,
 * beside the host's own logging if there is any:
 *
 * ```kotlin
 * val events = TaskEventFlow()
 * val engine = TaskEngine(..., listener = TaskRunListener.of(inHouseLogging, events))
 * scope.launch { events.events.collect { … } }   // on a real dispatcher - see [events]
 * ```
 *
 * `logging: false` on a task therefore silences this stream for that task's runs entirely: the
 * engine binds the run's sink to [TaskRunListener.NONE] and never reaches the flow. Correct, and
 * surprising enough to say out loud.
 *
 * **Emission is `tryEmit`, never `emit`.** `emit` suspends, cannot be called from the blocking
 * engine at all, and would let a slow collector back-pressure a live ETL run. `tryEmit` never
 * suspends under any overflow policy; what it can do instead is refuse the value, and [dropped]
 * counts exactly that. Overflow is `SUSPEND` deliberately - measured, `DROP_OLDEST` combined with
 * `tryEmit` never refuses anything, which would make [dropped] structurally always zero.
 *
 * **Thread safety.** Every method here is called from N task threads at once (spec 8.4).
 * `MutableSharedFlow` is thread safe, the two counters are `AtomicLong`, and nothing else is
 * mutable. Retention is bounded by `replay + extraBufferCapacity` events; note that a
 * [TaskEvent.StepError] holds a `Throwable` and its stack trace, so a wedged collector at the
 * default buffer of 256 pins a real - bounded, but not small - amount of memory.
 *
 * @param replay events a newly attached collector is handed before live ones. A truncating ceiling
 *   that silently keeps the **tail**: `replay = 4` over ten events retains the last four.
 * @param extraBufferCapacity events accepted past `replay` while a collector is behind.
 */
class TaskEventFlow(replay: Int = 0, extraBufferCapacity: Int = 256) : TaskRunListener {

    init {
        // Not the library's guard: measured, MutableSharedFlow(0, 0, SUSPEND) constructs happily,
        // because a zero capacity is only illegal under a non-default overflow policy. It is this
        // class's, because such a flow refuses *every* emission once a subscriber is present and
        // would report 100% loss forever.
        require(replay + extraBufferCapacity > 0) {
            "replay + extraBufferCapacity must be positive, was replay=$replay, " +
                "extraBufferCapacity=$extraBufferCapacity: a flow with neither buffers nothing, so " +
                "every emission to a live collector would be refused and counted as dropped"
        }
    }

    private val sink = MutableSharedFlow<TaskEvent>(
        replay = replay,
        extraBufferCapacity = extraBufferCapacity,
        onBufferOverflow = BufferOverflow.SUSPEND,
    )

    private val emittedCount = AtomicLong()
    private val droppedCount = AtomicLong()

    /**
     * The stream. **Read the list of what it does not promise before you build on it** - a host
     * reaching for this *because* it is the Kotlin-shaped one gets a weaker guarantee than the
     * listener's, and gets it silently.
     *
     * **Promised: order, per run.** Each subscriber is delivered events in emission order, and that
     * order is the engine's: task start, then per phase a phase start, its steps, a phase end, then
     * the task end. What a lagging subscriber receives is always an in-order prefix-with-gaps.
     *
     * Not promised, all of it:
     *
     * - **Not completeness.** A lagging collector loses events, and the stream carries no gap
     *   marker. [dropped] is the only evidence, and it is global.
     * - **Not per-run isolation.** One [TaskEngine] serves concurrent tasks (spec 8.4), so one
     *   `TaskEventFlow` interleaves runs. Reconstructing one run means filtering on
     *   [TaskEvent.task]`.runId`. A per-run or per-task flow is not obtainable from this design
     *   without a per-task engine.
     * - **Not delivery at all when nothing collects.** With `replay = 0` an event emitted while no
     *   collector is attached is discarded, and [dropped] does not count it - measured, `tryEmit`
     *   returns true with no subscriber under every policy. No counter of any design can report
     *   "nobody was listening"; [subscriptionCount] is how you ask that question instead.
     * - **Not completion.** A `SharedFlow` never completes and `collect` never returns. There is no
     *   `close()` and none will be added: a collector must be owned by a scope the host cancels,
     *   and at JVM exit it is killed mid-collect with no drain, consistent with spec 8's "no
     *   shutdown drain". [TaskEvent.TaskEnded] is **not** a usable terminator either - it is
     *   exactly as droppable as any other event.
     * - **Not pairing.** Everything [TaskRunListener.onStepStart] says carries over: an `Error`
     *   escaping the engine means no `StepEnded`, no `StepError` and no `PhaseEnded` for that step
     *   - only `TaskEnded`, which is reached from a `finally`. A collector holding per-run state
     *   must unwind it on `TaskEnded` or it leaks on exactly that path.
     * - **Not "you only lose the boring ones".** `SUSPEND` refuses the **newest**, so loss falls on
     *   the tail of a burst, which is where terminal failures cluster. Do not alert on this stream.
     * - **Not isolation between subscribers.** This is `SUSPEND`'s price: `tryEmit` refuses when
     *   **any** subscriber cannot accept the value, so one wedged debug collector causes - and
     *   inflates - loss for a healthy metrics collector beside it, and one global [dropped] cannot
     *   say which of them was at fault.
     * - **Not a whole run for a late subscriber.** Subscribing mid-run yields an unmarked partial
     *   run; `replay` backfills a fixed count of events, not "the run so far".
     * - **Not immune to being switched off.** `logging: false` on a task silences this stream for
     *   that task's runs entirely.
     * - **Not "the producer cannot be blocked".** `tryEmit` never *suspends*; that is not the same
     *   as never blocking. A collector on `Dispatchers.Unconfined`, or started `UNDISPATCHED`, runs
     *   its body **inline on the ETL thread, inside `tryEmit`** - measured: an emitting thread did
     *   not return from `tryEmit` for 300 ms while such a collector blocked. That is precisely the
     *   inversion this design exists to prevent, and no overflow policy prevents it. **Collect on a
     *   real dispatcher.**
     * - **Not exception-isolated.** A collector that throws does not surface through `tryEmit`,
     *   which returns `true` regardless; the exception lands on the **ETL thread's
     *   uncaught-exception handler**, where neither [TaskRunListener.of]'s per-listener guard nor
     *   the engine's own catch will ever see it. Those guards protect against *this listener*
     *   throwing, which it does not do. Guard your collector's body yourself.
     */
    val events: SharedFlow<TaskEvent> = sink.asSharedFlow()

    /**
     * How many collectors are attached right now, live.
     *
     * The one question [dropped] structurally cannot answer: with nothing collecting, `tryEmit`
     * accepts and discards, so zero drops means "nothing was lost by a lagging collector", never
     * "somebody was listening". This is declared on `MutableSharedFlow` rather than on
     * `SharedFlow`, which is why it is surfaced here rather than reached through [events].
     */
    val subscriptionCount: StateFlow<Int> get() = sink.subscriptionCount

    /**
     * Every call site this flow was reached at, whatever `tryEmit` answered.
     *
     * It exists so [dropped] has a denominator: `dropped = 400` is uninterpretable alone - 400 of
     * 401 is a catastrophe and 400 of 40 million is noise - and a host cannot derive the total
     * without attaching a second counting listener, which is the duplication this class exists to
     * avoid.
     */
    val emitted: Long get() = emittedCount.get()

    /**
     * Events a **lagging subscriber** missed: `tryEmit` refused them because the buffer was full.
     *
     * It does **not** count events discarded because nothing was collecting - those are accepted by
     * `tryEmit` and dropped on the floor, and are invisible to any counter (see [events]). Nor does
     * it attribute a loss to a subscriber: under `SUSPEND` one wedged collector's back-pressure is
     * counted here on behalf of every collector, including the healthy ones.
     *
     * The accounting equation is `delivered + buffered + dropped == emitted`, and `buffered` is not
     * observable from outside: at any moment up to `replay + extraBufferCapacity` events are
     * accepted but not yet handed to a lagging collector. `delivered + dropped == emitted` is
     * therefore **false against correct behaviour**, and closes only once the collector drains.
     */
    val dropped: Long get() = droppedCount.get()

    override fun onTaskStart(ctx: TaskContext) = publish(TaskEvent.TaskStarted(ctx))

    override fun onTaskEnd(ctx: TaskContext, outcome: Outcome) = publish(TaskEvent.TaskEnded(ctx, outcome))

    override fun onPhaseStart(ctx: PhaseContext) = publish(TaskEvent.PhaseStarted(ctx))

    override fun onPhaseEnd(ctx: PhaseContext, outcome: Outcome) = publish(TaskEvent.PhaseEnded(ctx, outcome))

    override fun onStepStart(ctx: StepContext) = publish(TaskEvent.StepStarted(ctx))

    override fun onStepEnd(ctx: StepContext, result: StepResult) = publish(TaskEvent.StepEnded(ctx, result))

    override fun onStepError(ctx: StepContext, attempt: Int, error: Throwable, willRetry: Boolean) =
        publish(TaskEvent.StepError(ctx, attempt, error, willRetry))

    /**
     * The only emission in this file, and the only place either counter moves.
     *
     * `tryEmit` rather than `emit`, so this returns on the ETL thread without suspending whatever
     * any collector is doing - subject to the undispatched-collector hazard [events] documents,
     * which is the host's to avoid and not something this method can defend against.
     */
    private fun publish(event: TaskEvent) {
        emittedCount.incrementAndGet()
        if (!sink.tryEmit(event)) {
            droppedCount.incrementAndGet()
        }
    }
}
