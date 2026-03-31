package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import org.jdbi.v3.core.Handle
import java.time.Instant

interface TaskRepository {
    suspend fun insertBatch(tasks: List<Task>)
    suspend fun claimNext(workerId: String, limit: Int, queueName: String = "default"): List<Task>
    suspend fun updateStatus(id: String, newStatus: TaskStatus, resultJson: String? = null): Boolean
    suspend fun countNonTerminal(workflowId: String, sequenceNumber: Int): Int
    suspend fun countFailed(workflowId: String, sequenceNumber: Int): Int
    suspend fun countTotal(workflowId: String, sequenceNumber: Int): Int
    suspend fun findByWorkflowAndSequence(workflowId: String, sequenceNumber: Int): List<Task>
    suspend fun resetForRetry(id: String, newRetryCount: Int)
    suspend fun replayDeadLetterTask(taskId: String): Boolean
    suspend fun replayDeadLetterBatch(workflowId: String): Int
    suspend fun findExpired(now: Instant): List<Task>
    suspend fun resetStaleTasks(staleThreshold: Instant): Int
    suspend fun deadLetterExhaustedTasks(staleThreshold: Instant): Int

    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: TaskStatus, resultJson: String? = null, claimedBy: String? = null, claimedAt: Instant? = null): Boolean
    fun countNonTerminalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun countFailedWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun countTotalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun findByWorkflowAndSequenceWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): List<Task>
    fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int
    fun insertBatchWithHandle(handle: Handle, tasks: List<Task>)
    fun replayDeadLetterBatchWithHandle(handle: Handle, workflowId: String): Int
    fun findDistinctQueuesByWorkflowId(handle: Handle, workflowId: String, statuses: List<String>): List<String>
}
