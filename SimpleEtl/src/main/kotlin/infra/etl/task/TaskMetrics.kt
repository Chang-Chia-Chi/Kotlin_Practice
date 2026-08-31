package infra.etl.task

/**
 * The metrics seam: four calls the engine makes, out of which a host publishes six
 * meters. The engine names no metrics library - `infra.etl.micrometer.MicrometerTaskMetrics` is
 * the binding this project ships, and it is the only class in the module allowed to name one.
 *
 * **Every method is called from N task threads at once.** One [TaskEngine] serves every task and
 * different tasks run concurrently, each on its own confined dispatcher, so an
 * implementation holding state must be thread safe, and none of these methods may block - a
 * recorder that waits parks an ETL run behind it. That is the same obligation [TaskRunListener]
 * carries, for the same reason.
 *
 * **A metrics recorder never fails a run.** Every call site catches [Exception], logs at WARN
 * naming the seam and the site, and continues, exactly as the listener's sites do.
 *
 * **`logging: false` does not suppress metrics.** That flag binds the run's *listener*
 * to [TaskRunListener.NONE]; a task whose logging is turned off is still counted, still timed and
 * still gauged, because an operator's dashboard is not the task author's to switch off.
 *
 * **Every method is abstract, and a host that wants only some of them binds [NONE] to the rest by
 * composing.** No default bodies: the sibling snapshot-cache module's `CacheEvents` has them and
 * the divergence is deliberate, because it means a seventh metric cannot be added here without
 * every implementation noticing at compile time.
 */
interface TaskMetrics {

    /**
     * The run is over, whatever it did. Called from `run`'s own `finally`, **before**
     * [TaskRunListener.onTaskEnd] and after the hooks.
     *
     * @param durationMs the whole run on the engine's injected `Clock`, and therefore inclusive of
     *   `onSuccess` / `onFailure` hook execution and of `ScratchDb.close()` - both of which happen
     *   after the last step and before this reading is taken. It is not "the engine's own work".
     */
    fun taskEnded(ctx: TaskContext, outcome: Outcome, durationMs: Long)

    /**
     * One step ended, on **success and on terminal failure alike** - the asymmetry with
     * [TaskRunListener.onStepEnd], which is success-only.
     *
     * The timer this feeds carries no `outcome` tag, so without the failure call a step that
     * always fails would have no duration series at all, which is the one shape an operator is
     * most likely to go looking for. On terminal failure [result] carries rows 0/0 - a partial
     * count of what the failed attempt managed to flush would make the same field mean two
     * different things - a `durationMs` spanning every attempt and the backoff between them, and
     * the attempt that failed terminally rather than one that succeeded.
     */
    fun stepEnded(ctx: StepContext, result: StepResult)

    /**
     * One **retried attempt**, not one retried step: a step with `retries: 2` that failed twice
     * before succeeding calls this twice. Called before the matching
     * `onStepError(willRetry = true)` and before the backoff sleep.
     */
    fun stepRetried(ctx: StepContext)

    /**
     * The run's scratch footprint, sampled after the last step and **before `ScratchDb.close()`**,
     * which is the only moment the directory still exists (the engine deletes it on every path).
     *
     * Not once per run. A run that dies before the scratch object is constructed - a
     * non-positive `memory_limit` is the reachable case - reports [taskEnded] and no
     * [scratchBytes] at all. A run that simply never referenced `scratch` does call this, with 0.
     *
     * @param bytes every regular file under the run's scratch directory, summed. See
     *   `ScratchDb.diskBytes` for what that number does and does not contain; the short version is
     *   that it includes the write-ahead log and any Parquet materialisation, and usually excludes
     *   spill, which is normally already reclaimed by the time the sample is taken.
     */
    fun scratchBytes(ctx: TaskContext, bytes: Long)

    companion object {

        /** Discards everything. What a [TaskEngine] built without a recorder reports to. */
        val NONE: TaskMetrics = NoOpTaskMetrics
    }
}

private object NoOpTaskMetrics : TaskMetrics {
    override fun taskEnded(ctx: TaskContext, outcome: Outcome, durationMs: Long) = Unit
    override fun stepEnded(ctx: StepContext, result: StepResult) = Unit
    override fun stepRetried(ctx: StepContext) = Unit
    override fun scratchBytes(ctx: TaskContext, bytes: Long) = Unit
}
