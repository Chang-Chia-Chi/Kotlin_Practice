package etlhost

import infra.etl.task.TaskEvent
import infra.etl.task.TaskRunListener
import jakarta.inject.Singleton
import org.jboss.logging.Logger

/**
 * SimpleEtl spec 9.2's seam, attached. This is spec 8.6's listener row, and the row is worth
 * reading twice: `TaskEngine`'s parameter defaults to `TaskRunListener.NONE`, a **specified no-op**
 * and not a default binding, so a host that omits it gets a 30-minute run that emits nothing at all
 * - no start, no step, no failure - while every call site exists and every one of them succeeds.
 * There is nothing to notice.
 *
 * `ListenerSilenceTest` is written against that symptom rather than against these lines: it runs
 * the same task twice, once wired and once with the listener replaced by `NONE`, and asserts the
 * second run says nothing. A test that only asserted "the wired listener logged" would still pass
 * on the day someone deletes this producer.
 *
 * Logs through `org.jboss.logging.Logger`, which is what Quarkus is built on, so `quarkus.log.*`
 * configures it unchanged and the frameworks' own logging and this land in one stream.
 *
 * Thread safe and non-blocking as the seam requires: it holds no state, and a log call is the
 * whole body.
 */
@Singleton
class LoggingRunListener : TaskRunListener {

    override fun on(event: TaskEvent) {
        val run = event.task
        when (event) {
            is TaskEvent.TaskStart ->
                log.infov("run {0} task {1} started, triggered by {2}/{3}",
                    run.runId, run.taskName, event.task.triggerSource, run.triggeredBy ?: "schedule")
            is TaskEvent.TaskEnd ->
                log.infov("run {0} task {1} ended {2}", run.runId, run.taskName, event.outcome)
            is TaskEvent.PhaseStart ->
                log.debugv("run {0} phase {1} started", run.runId, event.phase.phase)
            is TaskEvent.PhaseEnd ->
                log.debugv("run {0} phase {1} ended {2}", run.runId, event.phase.phase, event.outcome)
            is TaskEvent.StepStart ->
                log.debugv("run {0} step {1}/{2} started", run.runId, event.step.phase, event.step.step)
            is TaskEvent.StepEnd ->
                log.infov("run {0} step {1}/{2} ok: {3} read, {4} written, {5} ms, attempt {6}",
                    run.runId, event.step.phase, event.step.step,
                    event.result.rowsRead, event.result.rowsWritten, event.result.durationMs, event.result.attempt)
            // WARN when it will retry, ERROR when it will not: the second one ends the run, and an
            // operator's saved search should not have to distinguish them by reading the message.
            is TaskEvent.StepError -> if (event.willRetry) {
                log.warnv(event.error, "run {0} step {1}/{2} attempt {3} failed, retrying",
                    run.runId, event.step.phase, event.step.step, event.attempt)
            } else {
                log.errorv(event.error, "run {0} step {1}/{2} attempt {3} failed terminally",
                    run.runId, event.step.phase, event.step.step, event.attempt)
            }
        }
    }

    private companion object {
        val log: Logger = Logger.getLogger("etl.run")
    }
}
