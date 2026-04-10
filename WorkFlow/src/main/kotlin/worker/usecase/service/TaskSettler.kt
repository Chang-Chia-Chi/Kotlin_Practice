package com.workflow.worker.usecase.service

import com.workflow.infrastructure.coroutine.suspendCatching
import com.workflow.workflow.model.TaskCompletionEvent
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import jakarta.enterprise.context.ApplicationScoped
import java.time.Instant

/**
 * Outcome of [TaskSettler.retryOrFail]: the task was either reset for
 * another attempt or permanently marked as FAILED.
 */
sealed interface RetryOutcome {
    data object Retried : RetryOutcome

    data object Failed : RetryOutcome
}

/**
 * Centralises the retry-or-fail decision and task settlement through
 * [PhaseGate.onTaskCompleted]. Both [WorkerLoop][com.workflow.worker.usecase.service.execution.WorkerLoop]
 * and [TriggerLoop][com.workflow.worker.usecase.service.trigger.TriggerLoop] delegate here so the
 * business rule lives in one place.
 *
 * **Error contract:** Both methods propagate exceptions (CancellationException-safe
 * via [suspendCatching]). Callers are responsible for their own error boundaries.
 * [retryOrFail] internally catches [resetForRetry][TaskRepository.resetForRetry]
 * failures and falls through to settling as FAILED — but if
 * [PhaseGate.onTaskCompleted] itself throws, that propagates to the caller.
 */
@ApplicationScoped
class TaskSettler(
    private val taskRepo: TaskRepository,
    private val phaseGate: PhaseGate,
) {
    /**
     * Settles a task by delegating to [PhaseGate.onTaskCompleted].
     */
    suspend fun settle(event: TaskCompletionEvent) {
        phaseGate.onTaskCompleted(event)
    }

    /**
     * Retries the task if attempts remain, otherwise settles it as FAILED.
     *
     * If [retryCount] < [maxRetries], attempts [TaskRepository.resetForRetry].
     * Returns [RetryOutcome.Retried] only when the DB confirms the row was actually
     * reset (returns `true`). If `resetForRetry` returns `false` (task already
     * terminal, e.g. TIMED_OUT by watchdog) or throws, falls through to settling as FAILED.
     *
     * If retries are exhausted, calls [PhaseGate.onTaskCompleted] with
     * [TaskStatus.FAILED] and returns [RetryOutcome.Failed].
     */
    suspend fun retryOrFail(
        taskId: String,
        workflowId: String,
        sequenceNumber: Int,
        retryCount: Int,
        maxRetries: Int,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
    ): RetryOutcome {
        if (retryCount < maxRetries) {
            val resetResult =
                suspendCatching {
                    taskRepo.resetForRetry(taskId, retryCount + 1, claimedBy, claimedAt)
                }
            // Only retry if the DB confirmed the row was actually reset (returns true).
            // false means the task was already terminal (e.g., TIMED_OUT by watchdog) —
            // fall through to settle as FAILED so PhaseGate can finalize the workflow.
            if (resetResult.getOrDefault(false)) return RetryOutcome.Retried
            // resetForRetry returned false or threw — fall through to settle as FAILED
        }

        phaseGate.onTaskCompleted(
            TaskCompletionEvent(
                taskId = taskId,
                workflowId = workflowId,
                sequenceNumber = sequenceNumber,
                status = TaskStatus.FAILED,
                resultJson = null,
                claimedBy = claimedBy,
                claimedAt = claimedAt,
            )
        )
        return RetryOutcome.Failed
    }
}
