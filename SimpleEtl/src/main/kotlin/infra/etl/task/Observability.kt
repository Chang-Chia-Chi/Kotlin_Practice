package infra.etl.task

import java.time.Instant
import org.jboss.logging.Logger

private val log = Logger.getLogger(TaskRunListener::class.java)

/**
 * What one run is, as every listener call site and every hook is told about it (spec 9.2, 9.4).
 *
 * The same instance is handed to every call of one run, so a listener that keys a log scope or a
 * tracing span off it can compare by identity and does not have to reassemble the run from four
 * separate fields.
 *
 * @param runId the one id that names this run everywhere: the scratch directory, the `runId` task
 *   variable, [TaskOutcome.runId] and the admin API (spec 8.2).
 * @param triggeredBy the caller identity of spec 8.2, null for a scheduled firing. Recorded, never
 *   authorised against - authorisation is the host's (spec 8.6).
 * @param startedAt read from the engine's injected `Clock` at the top of the run, before scratch
 *   exists.
 */
data class TaskContext(
    val runId: String,
    val taskName: String,
    val triggerSource: TriggerSource,
    val triggeredBy: String?,
    val startedAt: Instant,
)

/** One phase of [task]. Phases group steps for logs and metrics and nothing else (spec 2.2). */
data class PhaseContext(val task: TaskContext, val phase: String)

/**
 * One step of [phase], which is the phase's **name** and not a nested [PhaseContext]: a listener
 * formatting `phase/step` wants two strings, and nesting would make it reach through a context to
 * a context to get the second of them.
 */
data class StepContext(val task: TaskContext, val phase: String, val step: String)

/**
 * What a step did, reported once, on success (spec 9.2).
 *
 * @param rowsRead rows the source produced, and [rowsWritten] rows the target accepted. They are
 *   two numbers because a `transform` returning null drops a row (spec 9.1). Both are 0 for every
 *   step type except `pipe`: only `pipe` moves rows through the JVM (spec 2.3), and a `sql` or
 *   `materialize` affected-row count would make one field mean a different thing per step type.
 * @param durationMs the whole step, from its first attempt to the attempt that succeeded,
 *   including the retry backoff in between, measured on the engine's injected `Clock`.
 * @param attempt the attempt that succeeded, numbered from 1. With `retries: n` the attempts run
 *   1..n+1.
 */
data class StepResult(val rowsRead: Long, val rowsWritten: Long, val durationMs: Long, val attempt: Int)

/**
 * The observation seam of spec 9.2: the host's own logging, plugged into the run.
 *
 * **Every method is called from N task threads at once.** One [TaskEngine] serves every task and
 * different tasks run concurrently, each on its own confined dispatcher (spec 8.4), so an
 * implementation holding state must be thread safe, and none of these methods may block - a
 * listener that waits parks an ETL run behind it.
 *
 * **A listener never fails a run.** Every call site catches, logs at WARN and continues, and
 * [of] applies the same isolation per listener. A logging plug-in that failed the task it was
 * logging would invert the point of the seam.
 *
 * `logging: false` on a task suppresses every method here for that task's runs (spec 9.2). Hooks
 * are unaffected.
 */
interface TaskRunListener {

    fun onTaskStart(ctx: TaskContext)

    /** The run is over. Reached from a `finally`, so a run that started always ends. */
    fun onTaskEnd(ctx: TaskContext, outcome: Outcome)

    fun onPhaseStart(ctx: PhaseContext)

    /**
     * [Outcome.FAILED] when a step of the phase failed terminally; no later phase then starts.
     *
     * Carries the same unpaired hazard as [onStepStart]: an `Error` escaping the engine skips
     * this call, so [onPhaseStart] can be the last phase event a listener sees.
     */
    fun onPhaseEnd(ctx: PhaseContext, outcome: Outcome)

    /**
     * Once per step, before its first attempt and before any guard that can reject the step.
     *
     * **Not always paired.** A step normally closes with [onStepEnd] or [onStepError], but an
     * `Error` - not an `Exception` - escapes the engine uncaught, and then neither fires: the
     * step, and its phase, end with no closing event at all. Only [onTaskEnd] is guaranteed,
     * because it is reached from a `finally`. A listener holding per-step state - an MDC push, a
     * log scope, a tracing span - must therefore be able to unwind that state at [onTaskEnd] as
     * well, or it leaks on exactly the path a not-yet-implemented step takes.
     */
    fun onStepStart(ctx: StepContext)

    /**
     * Success only. A step that fails terminally ends with `onStepError(willRetry = false)` and no
     * `onStepEnd`. A listener that pairs start with end - an MDC push/pop, a log scope, a tracing
     * span - must close on either of those, **and** on [onTaskEnd] for the `Error` path where
     * neither of them fires at all (see [onStepStart]).
     */
    fun onStepEnd(ctx: StepContext, result: StepResult)

    /**
     * One failed attempt.
     *
     * @param attempt numbered from 1.
     * @param error the failure as the step threw it - the engine adds no wrapper of its own, but
     *   removes none either. On every JDBI path this is an `UnableToExecuteStatementException`
     *   around the `SQLException`, so a listener classifying a failure walks the cause chain
     *   rather than testing the top-level type (spec 5.3 does the same).
     * @param willRetry decided and reported **before** the backoff sleep, so a listener sees the
     *   decision at the moment it is made rather than after the delay it causes. False both for a
     *   non-transient failure and for a transient one that has run out of attempts (spec 5.3).
     */
    fun onStepError(ctx: StepContext, attempt: Int, error: Throwable, willRetry: Boolean)

    companion object {

        /** Discards everything. What a [TaskEngine] built without a listener reports to. */
        val NONE: TaskRunListener = NoOpTaskRunListener

        /**
         * Fans out to [listeners] in argument order. `of()` returns [NONE]; `of(one)` returns its
         * argument unwrapped, so neither adds a frame that would have to be reasoned about.
         *
         * Each listener is isolated from the others exactly as it is from the engine: one that
         * throws is logged and the event still reaches the listeners after it. That is why this
         * exists at all - [TaskEngine] takes one listener, so a host wanting spec 9.2's "existing
         * in-house logging mechanism" *and* anything else has nowhere else to compose them.
         *
         * The engine's own catch is not made redundant by this: a host may attach a bare listener,
         * and both guards log, so a throw is never lost whichever of them sees it.
         */
        fun of(vararg listeners: TaskRunListener): TaskRunListener = when (listeners.size) {
            0 -> NONE
            1 -> listeners.single()
            else -> CompositeTaskRunListener(listeners.toList())
        }
    }
}

private object NoOpTaskRunListener : TaskRunListener {
    override fun onTaskStart(ctx: TaskContext) = Unit
    override fun onTaskEnd(ctx: TaskContext, outcome: Outcome) = Unit
    override fun onPhaseStart(ctx: PhaseContext) = Unit
    override fun onPhaseEnd(ctx: PhaseContext, outcome: Outcome) = Unit
    override fun onStepStart(ctx: StepContext) = Unit
    override fun onStepEnd(ctx: StepContext, result: StepResult) = Unit
    override fun onStepError(ctx: StepContext, attempt: Int, error: Throwable, willRetry: Boolean) = Unit
}

/** The fan-out behind [TaskRunListener.of]. Immutable, so it is as thread safe as its members. */
private class CompositeTaskRunListener(private val listeners: List<TaskRunListener>) : TaskRunListener {

    override fun onTaskStart(ctx: TaskContext) = each("onTaskStart", ctx.describe()) { it.onTaskStart(ctx) }

    override fun onTaskEnd(ctx: TaskContext, outcome: Outcome) =
        each("onTaskEnd", ctx.describe()) { it.onTaskEnd(ctx, outcome) }

    override fun onPhaseStart(ctx: PhaseContext) = each("onPhaseStart", ctx.task.describe()) { it.onPhaseStart(ctx) }

    override fun onPhaseEnd(ctx: PhaseContext, outcome: Outcome) =
        each("onPhaseEnd", ctx.task.describe()) { it.onPhaseEnd(ctx, outcome) }

    override fun onStepStart(ctx: StepContext) = each("onStepStart", ctx.task.describe()) { it.onStepStart(ctx) }

    override fun onStepEnd(ctx: StepContext, result: StepResult) =
        each("onStepEnd", ctx.task.describe()) { it.onStepEnd(ctx, result) }

    override fun onStepError(ctx: StepContext, attempt: Int, error: Throwable, willRetry: Boolean) =
        each("onStepError", ctx.task.describe()) { it.onStepError(ctx, attempt, error, willRetry) }

    /**
     * Every member gets the event, whatever the ones before it did. Aborting on the first throw
     * would let one broken plug-in silently blind every other.
     */
    private fun each(site: String, run: String, call: (TaskRunListener) -> Unit) {
        listeners.forEach { listener ->
            try {
                call(listener)
            } catch (e: Exception) {
                log.warn(
                    "$run: listener ${listener.javaClass.name} threw from $site and was skipped. The " +
                        "run is unaffected and the remaining listeners still received the event.",
                    e,
                )
            }
        }
    }
}

/** The `runId / taskName` prefix every isolation warning carries, so a log line names its run. */
internal fun TaskContext.describe(): String = "run $runId of task '$taskName'"
