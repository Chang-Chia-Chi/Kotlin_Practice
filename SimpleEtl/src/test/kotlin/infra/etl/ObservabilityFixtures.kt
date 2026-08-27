package infra.etl

import infra.etl.task.Outcome
import infra.etl.task.PhaseContext
import infra.etl.task.StepContext
import infra.etl.task.StepResult
import infra.etl.task.TaskContext
import infra.etl.task.TaskEvent
import infra.etl.task.TaskHook
import infra.etl.task.TaskMetrics
import infra.etl.task.TaskRunListener
import infra.etl.task.TriggerSource
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * P8a test support: the observation apparatus for spec 9.2's listener and spec 9.4's hooks.
 *
 * **One trace, two kinds of assertion.** Everything a run reports - every listener call site and
 * every hook invocation - lands as a line in a single [EventTrace], in the order it happened, and
 * separately as a typed record on the [RecordingListener] or [RecordingHook] that saw it. The two
 * exist for different questions and neither substitutes for the other:
 *
 * - the trace answers *ordering*, which is most of contract 2.1 to 2.3, and it answers it with
 *   `containsExactly` over the whole run. `containsSubsequence` is not used anywhere in this
 *   phase: an empty trace satisfies a subsequence assertion, which is exactly the shape of test
 *   that passes against an engine with no call sites in it at all;
 * - the typed records answer *payload* - `StepResult.durationMs`, the read/written pair, the
 *   attempt number, `willRetry`, and the identity of a `Throwable` - none of which survives being
 *   rendered into a string.
 *
 * Hooks write into the same trace as listener calls because the two interleave by contract:
 * spec 9.4's `onSuccess` runs after the last `onPhaseEnd` and before `onTaskEnd`, and a
 * throwing `onSuccess` is followed by `onFailure` and *then* by `onTaskEnd(FAILED)`. Two separate
 * recordings could not express that, and asserting it is the point.
 *
 * **The tests write their expected traces out as literals.** No helper here builds an expected
 * line, deliberately: a fixture that formatted both sides would assert that the engine agrees
 * with itself. The one formatter lives below, in [RecordingListener], and only the actual side
 * goes through it. The grammar it produces is:
 *
 * ```
 * onTaskStart(wip-summary)
 * onPhaseStart(extract)
 * onStepStart(extract/load-wip)
 * onStepError(extract/load-wip, attempt=1, willRetry=true)
 * onStepEnd(extract/load-wip, attempt=2)
 * onPhaseEnd(extract, SUCCEEDED)
 * hook(notify-downstream)
 * onTaskEnd(wip-summary, SUCCEEDED)
 * ```
 *
 * with a `label:` prefix when a listener was given one, which is how a fan-out through
 * `TaskRunListener.of` shows *which* listener received an event and in what order.
 *
 * ### Recording happens before throwing
 *
 * A [RecordingListener] built with a non-empty `failAt` records the call and only then throws.
 * That ordering is load-bearing for contract 2.5's test: "a throwing listener did not fail the
 * run" is satisfied for free by an engine that never called the listener at all, so the test also
 * asserts that the site under test fired ([RecordingListener.thrown]) and that every later site
 * still fired ([RecordingListener.calls]).
 *
 * ### Nothing here sleeps, and nothing here reads a wall clock
 *
 * [MutableClock] is the engine's only source of time (contract 1.3) and `TaskHarness`'s sleeper
 * advances it by exactly the backoff the engine asked for. A step that was retried twice
 * therefore reports `durationMs = 6000` exactly, not "a few milliseconds", and the assertion is
 * an equality rather than a range. That is the whole reason the clock is injected: without it
 * "durationMs spans all attempts" has no observation that separates a correct engine from one
 * that times the last attempt only.
 */
class EventTrace {

    private val recorded = Collections.synchronizedList(ArrayList<String>())

    /** Every event so far, in order. Copied out, so an assertion cannot race a late arrival. */
    val entries: List<String> get() = synchronized(recorded) { ArrayList(recorded) }

    fun record(entry: String) {
        recorded += entry
    }

    /** Forgets everything, for a test that asserts one run of a harness at a time. */
    fun clear() = recorded.clear()

    /**
     * A listener writing into this trace.
     *
     * @param label prefixes every line, for the fan-out tests where several listeners share one
     *   trace and the question is which of them saw what, in which order.
     * @param failAt the call sites this listener throws from, *after* recording them.
     */
    fun listener(
        label: String = "",
        failAt: Set<ListenerCall> = emptySet(),
        failure: () -> Throwable = { IllegalStateException("probe: the listener threw") },
    ): RecordingListener = RecordingListener(this, label, failAt, failure)

    /**
     * A hook writing into this trace.
     *
     * @param failure thrown after the invocation is recorded, so "the hook was reached" and "the
     *   hook failed" stay separately observable - which is what contract 2.3's swallowed
     *   `onFailure` needs, since an engine that never calls the hook also never propagates from it.
     */
    fun hook(name: String, failure: Throwable? = null): RecordingHook = RecordingHook(this, name, failure)

    /**
     * P8b. A [infra.etl.task.TaskMetrics] writing into this trace, alongside the listener and the
     * hooks, because spec 9.3's ordering clauses are all *relative* ones - `stepRetried` before
     * `onStepError(willRetry = true)`, `scratchBytes` before the `onSuccess` hook. Two separate
     * recordings could express neither, and those two orderings are the ones that carry a failure
     * mode.
     *
     * @param failAt the call sites this recorder throws from, *after* recording them - the same
     *   discipline [listener] follows and for the same reason: "a throwing `TaskMetrics` did not
     *   fail the run" is satisfied for free by an engine with no metrics call sites in it at all,
     *   so the test also asserts [RecordingMetrics.thrown] and that the later sites still fired.
     */
    fun metrics(
        failAt: Set<MetricsCall> = emptySet(),
        failure: () -> Throwable = { IllegalStateException("probe: the metrics recorder threw") },
    ): RecordingMetrics = RecordingMetrics(this, failAt, failure)
}

/** The seven call sites of spec 9.2, as a value a parameterized test can enumerate. */
enum class ListenerCall { TASK_START, TASK_END, PHASE_START, PHASE_END, STEP_START, STEP_END, STEP_ERROR }

/**
 * A [TaskRunListener] that records what it was told, and optionally throws from chosen call sites.
 *
 * Thread safe, because spec 9.2 says a listener is called from N task threads at once. Nothing in
 * this phase runs two tasks concurrently, but a fixture that would corrupt under the contract it
 * is testing is not a fixture worth having.
 */
class RecordingListener internal constructor(
    private val trace: EventTrace,
    private val label: String,
    private val failAt: Set<ListenerCall>,
    private val failure: () -> Throwable,
) : TaskRunListener {

    /** What `onStepEnd` carried, kept whole so a test can read any field of [StepResult]. */
    data class StepEnd(val ctx: StepContext, val result: StepResult)

    /** What `onStepError` carried. [error] is kept by reference, for the identity assertions. */
    data class StepError(val ctx: StepContext, val attempt: Int, val error: Throwable, val willRetry: Boolean)

    private val callsSeen = Collections.synchronizedList(ArrayList<ListenerCall>())
    private val taskStartsSeen = Collections.synchronizedList(ArrayList<TaskContext>())
    private val taskEndsSeen = Collections.synchronizedList(ArrayList<Pair<TaskContext, Outcome>>())
    private val phaseStartsSeen = Collections.synchronizedList(ArrayList<PhaseContext>())
    private val phaseEndsSeen = Collections.synchronizedList(ArrayList<Pair<PhaseContext, Outcome>>())
    private val stepStartsSeen = Collections.synchronizedList(ArrayList<StepContext>())
    private val stepEndsSeen = Collections.synchronizedList(ArrayList<StepEnd>())
    private val stepErrorsSeen = Collections.synchronizedList(ArrayList<StepError>())
    private val thrownCount = AtomicInteger()

    /** Every call site that fired, in order. Empty is how `logging: false` looks. */
    val calls: List<ListenerCall> get() = copyOf(callsSeen)

    val taskStarts: List<TaskContext> get() = copyOf(taskStartsSeen)
    val taskEnds: List<Pair<TaskContext, Outcome>> get() = copyOf(taskEndsSeen)
    val phaseStarts: List<PhaseContext> get() = copyOf(phaseStartsSeen)
    val phaseEnds: List<Pair<PhaseContext, Outcome>> get() = copyOf(phaseEndsSeen)
    val stepStarts: List<StepContext> get() = copyOf(stepStartsSeen)
    val stepEnds: List<StepEnd> get() = copyOf(stepEndsSeen)
    val stepErrors: List<StepError> get() = copyOf(stepErrorsSeen)

    /** How many times this listener threw. A site that never fired cannot have thrown. */
    val thrown: Int get() = thrownCount.get()

    /** The [StepResult] the named step ended with. Fails loudly rather than returning null. */
    fun result(step: String): StepResult =
        stepEnds.singleOrNull { it.ctx.step == step }?.result
            ?: error("no single onStepEnd for step '$step'; saw ${stepEnds.map { it.ctx.step }}")

    override fun onTaskStart(ctx: TaskContext) {
        taskStartsSeen += ctx
        fired(ListenerCall.TASK_START, "onTaskStart(${ctx.taskName})")
    }

    override fun onTaskEnd(ctx: TaskContext, outcome: Outcome) {
        taskEndsSeen += ctx to outcome
        fired(ListenerCall.TASK_END, "onTaskEnd(${ctx.taskName}, $outcome)")
    }

    override fun onPhaseStart(ctx: PhaseContext) {
        phaseStartsSeen += ctx
        fired(ListenerCall.PHASE_START, "onPhaseStart(${ctx.phase})")
    }

    override fun onPhaseEnd(ctx: PhaseContext, outcome: Outcome) {
        phaseEndsSeen += ctx to outcome
        fired(ListenerCall.PHASE_END, "onPhaseEnd(${ctx.phase}, $outcome)")
    }

    override fun onStepStart(ctx: StepContext) {
        stepStartsSeen += ctx
        fired(ListenerCall.STEP_START, "onStepStart(${ctx.phase}/${ctx.step})")
    }

    override fun onStepEnd(ctx: StepContext, result: StepResult) {
        stepEndsSeen += StepEnd(ctx, result)
        fired(ListenerCall.STEP_END, "onStepEnd(${ctx.phase}/${ctx.step}, attempt=${result.attempt})")
    }

    override fun onStepError(ctx: StepContext, attempt: Int, error: Throwable, willRetry: Boolean) {
        stepErrorsSeen += StepError(ctx, attempt, error, willRetry)
        fired(
            ListenerCall.STEP_ERROR,
            "onStepError(${ctx.phase}/${ctx.step}, attempt=$attempt, willRetry=$willRetry)",
        )
    }

    /** Records, then throws. Never the other way round - see the file KDoc. */
    private fun fired(call: ListenerCall, entry: String) {
        callsSeen += call
        trace.record(if (label.isEmpty()) entry else "$label:$entry")
        if (call in failAt) {
            thrownCount.incrementAndGet()
            throw failure()
        }
    }

    private fun <T> copyOf(source: MutableList<T>): List<T> = synchronized(source) { ArrayList(source) }
}

/** A [TaskHook] that records the contexts it was handed, and optionally throws afterwards. */
class RecordingHook internal constructor(
    private val trace: EventTrace,
    val name: String,
    private val failure: Throwable?,
) : TaskHook {

    private val seen = Collections.synchronizedList(ArrayList<TaskContext>())

    /** Every invocation's context, in order. */
    val contexts: List<TaskContext> get() = synchronized(seen) { ArrayList(seen) }

    /** Spec 9.4 says `onSuccess` runs *once*, so this is asserted rather than assumed non-zero. */
    val runs: Int get() = contexts.size

    override fun run(ctx: TaskContext) {
        seen += ctx
        trace.record("hook($name)")
        failure?.let { throw it }
    }
}

/**
 * Reads [target] at every call instead of capturing it once.
 *
 * `TaskHarness` builds its `TaskEngine` `by lazy`, so a listener passed straight into that
 * constructor is frozen at the first run of the harness and a later `harness.listener = ...`
 * would silently never reach the engine - the test would then assert an empty recorder and pass
 * for the wrong reason. This is the seam that makes swapping a listener between two runs of one
 * harness mean what it looks like it means, which is what contract 4's paired `logging` assertion
 * needs.
 */
class ForwardingListener(private val target: () -> TaskRunListener) : TaskRunListener {
    override fun onTaskStart(ctx: TaskContext) = target().onTaskStart(ctx)
    override fun onTaskEnd(ctx: TaskContext, outcome: Outcome) = target().onTaskEnd(ctx, outcome)
    override fun onPhaseStart(ctx: PhaseContext) = target().onPhaseStart(ctx)
    override fun onPhaseEnd(ctx: PhaseContext, outcome: Outcome) = target().onPhaseEnd(ctx, outcome)
    override fun onStepStart(ctx: StepContext) = target().onStepStart(ctx)
    override fun onStepEnd(ctx: StepContext, result: StepResult) = target().onStepEnd(ctx, result)
    override fun onStepError(ctx: StepContext, attempt: Int, error: Throwable, willRetry: Boolean) =
        target().onStepError(ctx, attempt, error, willRetry)
}

/**
 * The engine's clock, moved only by the injected sleeper.
 *
 * The cost the contract records is real and is accepted here: a `Clock` is wall-clock and not
 * monotonic. What it buys is that every `durationMs` in this suite is an exact number rather than
 * a stopwatch reading, and that `TaskContext.startedAt` is checkable at all - an engine that
 * called `Instant.now()` reports today's date, which is not this clock's instant.
 *
 * [withZone] shares the elapsed counter rather than forking it, so a production call to
 * `clock.withZone(...)` cannot quietly hand back a clock this fixture no longer controls.
 */
class MutableClock private constructor(
    private val origin: Instant,
    private val zone: ZoneId,
    private val elapsed: AtomicLong,
) : Clock() {

    constructor(origin: Instant = ORIGIN, zone: ZoneId = ZoneOffset.UTC) : this(origin, zone, AtomicLong(0))

    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock = MutableClock(origin, zone, elapsed)

    override fun instant(): Instant = origin.plusMillis(elapsed.get())

    /** Moves time forward. Called by `TaskHarness`'s sleeper with the backoff the engine asked for. */
    fun advance(millis: Long) {
        require(millis >= 0) { "a clock does not run backwards, was asked for $millis ms" }
        elapsed.addAndGet(millis)
    }

    /** Total time this clock has been advanced by, across every run of the harness that owns it. */
    val elapsedMillis: Long get() = elapsed.get()

    companion object {
        /** Far from any real `Instant.now()`, so a wall-clock reading is unmistakable. */
        val ORIGIN: Instant = Instant.parse("2026-01-01T00:00:00Z")
    }
}

/**
 * A [TaskContext] built by a test rather than by a run, for the two `TaskRunListener.of` cases
 * that assert fan-out semantics directly and have no engine in them at all.
 */
fun taskContext(
    runId: String = "run-1",
    taskName: String = "wip-summary",
    triggerSource: TriggerSource = TriggerSource.SCHEDULE,
    triggeredBy: String? = null,
    startedAt: Instant = MutableClock.ORIGIN,
): TaskContext = TaskContext(
    runId = runId,
    taskName = taskName,
    triggerSource = triggerSource,
    triggeredBy = triggeredBy,
    startedAt = startedAt,
)

/** The four call sites of spec 9.3's metrics seam, as a value a parameterized test can enumerate. */
enum class MetricsCall { TASK_ENDED, STEP_ENDED, STEP_RETRIED, SCRATCH_BYTES }

/**
 * P8b. A [TaskMetrics] that records what it was told, and optionally throws from chosen call sites.
 *
 * Thread safe for the same reason [RecordingListener] is: spec 8.4 runs N tasks through one engine
 * and every method here is reached from all of them.
 *
 * **`scratchBytes`'s byte count is deliberately absent from the trace line.** It is the size of a
 * DuckDB file this suite does not control to the byte, so a whole-trace `assertEquals` carrying it
 * would be a flaky equality. The number is asserted where it is falsifiable - through
 * [scratchSamples], as `> 0` for a run that used scratch and `== 0` for one that did not. The same
 * reasoning does *not* apply to `stepEnded`: its attempt and its two row counts are exact under
 * the harness's clock and its fixed source table, so they are in the line, where a whole-trace
 * assertion checks them for free.
 */
class RecordingMetrics internal constructor(
    private val trace: EventTrace,
    private val failAt: Set<MetricsCall>,
    private val failure: () -> Throwable,
) : TaskMetrics {

    /** What `taskEnded` carried. [durationMs] is the engine's own clock reading, kept for assertion. */
    data class TaskEnded(val ctx: TaskContext, val outcome: Outcome, val durationMs: Long)

    /** What `stepEnded` carried. Unlike the listener's, this fires on terminal failure too. */
    data class StepEnded(val ctx: StepContext, val result: StepResult)

    /** What `scratchBytes` carried. */
    data class ScratchSample(val ctx: TaskContext, val bytes: Long)

    private val callsSeen = Collections.synchronizedList(ArrayList<MetricsCall>())
    private val taskEndsSeen = Collections.synchronizedList(ArrayList<TaskEnded>())
    private val stepEndsSeen = Collections.synchronizedList(ArrayList<StepEnded>())
    private val retriesSeen = Collections.synchronizedList(ArrayList<StepContext>())
    private val scratchSeen = Collections.synchronizedList(ArrayList<ScratchSample>())
    private val thrownCount = AtomicInteger()

    /** Every call site that fired, in order. Empty is what a `TaskMetrics.NONE` binding looks like. */
    val calls: List<MetricsCall> get() = copyOf(callsSeen)

    val taskEndings: List<TaskEnded> get() = copyOf(taskEndsSeen)
    val stepEndings: List<StepEnded> get() = copyOf(stepEndsSeen)

    /** One entry per *retried attempt*, not per retried step (spec 9.3, contract 3.2). */
    val retries: List<StepContext> get() = copyOf(retriesSeen)

    val scratchSamples: List<ScratchSample> get() = copyOf(scratchSeen)

    /** How many times this recorder threw. A site that never fired cannot have thrown. */
    val thrown: Int get() = thrownCount.get()

    /** The [StepResult] the named step was metered with. Fails loudly rather than returning null. */
    fun result(step: String): StepResult =
        stepEndings.singleOrNull { it.ctx.step == step }?.result
            ?: error("no single stepEnded for step '$step'; saw ${stepEndings.map { it.ctx.step }}")

    override fun taskEnded(ctx: TaskContext, outcome: Outcome, durationMs: Long) {
        taskEndsSeen += TaskEnded(ctx, outcome, durationMs)
        fired(MetricsCall.TASK_ENDED, "metric.taskEnded(${ctx.taskName}, $outcome)")
    }

    override fun stepEnded(ctx: StepContext, result: StepResult) {
        stepEndsSeen += StepEnded(ctx, result)
        fired(
            MetricsCall.STEP_ENDED,
            "metric.stepEnded(${ctx.phase}/${ctx.step}, attempt=${result.attempt}, " +
                "read=${result.rowsRead}, written=${result.rowsWritten})",
        )
    }

    override fun stepRetried(ctx: StepContext) {
        retriesSeen += ctx
        fired(MetricsCall.STEP_RETRIED, "metric.stepRetried(${ctx.phase}/${ctx.step})")
    }

    override fun scratchBytes(ctx: TaskContext, bytes: Long) {
        scratchSeen += ScratchSample(ctx, bytes)
        fired(MetricsCall.SCRATCH_BYTES, "metric.scratchBytes(${ctx.taskName})")
    }

    /** Records, then throws. Never the other way round - see [RecordingListener]. */
    private fun fired(call: MetricsCall, entry: String) {
        callsSeen += call
        trace.record(entry)
        if (call in failAt) {
            thrownCount.incrementAndGet()
            throw failure()
        }
    }

    private fun <T> copyOf(source: MutableList<T>): List<T> = synchronized(source) { ArrayList(source) }
}

/**
 * Reads [target] at every call instead of capturing it once - [ForwardingListener]'s trap, in the
 * seam P8b adds.
 *
 * `TaskHarness` builds its `TaskEngine` `by lazy`, so a `TaskMetrics` passed straight into that
 * constructor is frozen at the harness's first run and a later `harness.metrics = ...` would never
 * arrive. Two of this phase's tests swap the recorder between runs of one harness - the paired
 * `logging: true` / `logging: false` assertion is one of them - and both would otherwise assert an
 * empty recorder and pass for the wrong reason.
 */
class ForwardingMetrics(private val target: () -> TaskMetrics) : TaskMetrics {
    override fun taskEnded(ctx: TaskContext, outcome: Outcome, durationMs: Long) =
        target().taskEnded(ctx, outcome, durationMs)

    override fun stepEnded(ctx: StepContext, result: StepResult) = target().stepEnded(ctx, result)

    override fun stepRetried(ctx: StepContext) = target().stepRetried(ctx)

    override fun scratchBytes(ctx: TaskContext, bytes: Long) = target().scratchBytes(ctx, bytes)
}

// -------------------------------------------------------------------------------------------
// P8c. The flow-shaped form of the same seven call sites (spec 9.2, P8c contract 1.1).
// Additive: nothing above changes, and no existing trace line moves.
// -------------------------------------------------------------------------------------------

/**
 * One [TaskEvent] rendered into the grammar [RecordingListener] already writes, with the event's
 * own noun in place of the call site's verb - `TaskStarted(wip-summary)` where the listener writes
 * `onTaskStart(wip-summary)`.
 *
 * **Only the actual side goes through this**, exactly as the file KDoc says of the listener's
 * formatter: every expected list in `TaskEventFlowTest` is a literal. A fixture that formatted both
 * sides would assert that the flow agrees with itself.
 *
 * **Why a rendering and not value equality over whole `TaskEvent`s.** Every event carries the run's
 * [TaskContext], whose `runId` is a fresh UUID and whose `startedAt` is the harness clock's origin,
 * so an expected `TaskEvent` could only be built by reading the actual one first. The rendering
 * carries the identifying half - task, phase, step, attempt, `willRetry`, outcome - and the payload
 * that does not survive a string (a `StepResult`, a `Throwable`'s identity, a `TaskContext`'s
 * identity) is asserted from the typed events themselves, against a co-attached
 * [RecordingListener]. Same division of labour as P8a's, for the same reason.
 *
 * The event names deliberately stay 1:1 with the seven call sites: that vocabulary is what the
 * contract's criterion 7 asserts, and a renderer that collapsed `StepError` and `StepEnded` into
 * one line shape would hide exactly the mapping under test.
 */
fun rendered(event: TaskEvent): String = when (event) {
    is TaskEvent.TaskStarted -> "TaskStarted(${event.task.taskName})"
    is TaskEvent.TaskEnded -> "TaskEnded(${event.task.taskName}, ${event.outcome})"
    is TaskEvent.PhaseStarted -> "PhaseStarted(${event.phase.phase})"
    is TaskEvent.PhaseEnded -> "PhaseEnded(${event.phase.phase}, ${event.outcome})"
    is TaskEvent.StepStarted -> "StepStarted(${event.step.phase}/${event.step.step})"
    is TaskEvent.StepEnded -> "StepEnded(${event.step.phase}/${event.step.step}, attempt=${event.result.attempt})"
    is TaskEvent.StepError ->
        "StepError(${event.step.phase}/${event.step.step}, attempt=${event.attempt}, " +
            "willRetry=${event.willRetry})"
}

/** [rendered] over a whole replay cache, which is the order half of every ordering assertion. */
fun rendered(events: List<TaskEvent>): List<String> = events.map { rendered(it) }
