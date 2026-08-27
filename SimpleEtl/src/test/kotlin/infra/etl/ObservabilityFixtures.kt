package infra.etl

import infra.etl.task.Outcome
import infra.etl.task.PhaseContext
import infra.etl.task.StepContext
import infra.etl.task.StepResult
import infra.etl.task.TaskContext
import infra.etl.task.TaskHook
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
