package com.workflow.worker.usecase.service

import com.workflow.infrastructure.coroutine.suspendCatching
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
    suspend fun settle(
        taskId: String, workflowId: String, sequenceNumber: Int,
        status: TaskStatus, resultJson: String?,
        claimedBy: String? = null, claimedAt: Instant? = null,
    ) {
        phaseGate.onTaskCompleted(
            taskId = taskId,
            workflowId = workflowId,
            sequenceNumber = sequenceNumber,
            status = status,
            resultJson = resultJson,
            claimedBy = claimedBy,
            claimedAt = claimedAt,
        )
    }

    /**
     * Retries the task if attempts remain, otherwise settles it as FAILED.
     *
     * If [retryCount] < [maxRetries], attempts [TaskRepository.resetForRetry].
     * On success returns [RetryOutcome.Retried]. If `resetForRetry` throws
     * (non-CancellationException), falls through to settling as FAILED.
     *
     * If retries are exhausted, calls [PhaseGate.onTaskCompleted] with
     * [TaskStatus.FAILED] and returns [RetryOutcome.Failed].
     */
    suspend fun retryOrFail(
        taskId: String, workflowId: String, sequenceNumber: Int,
        retryCount: Int, maxRetries: Int,
        claimedBy: String? = null, claimedAt: Instant? = null,
    ): RetryOutcome {
        if (retryCount < maxRetries) {
            val resetResult = suspendCatching {
                taskRepo.resetForRetry(taskId, retryCount + 1)
            }
            if (resetResult.isSuccess) return RetryOutcome.Retried
            // resetForRetry failed — fall through to settle as FAILED
        }

        phaseGate.onTaskCompleted(
            taskId = taskId,
            workflowId = workflowId,
            sequenceNumber = sequenceNumber,
            status = TaskStatus.FAILED,
            resultJson = null,
            claimedBy = claimedBy,
            claimedAt = claimedAt,
        )
        return RetryOutcome.Failed
    }
}
