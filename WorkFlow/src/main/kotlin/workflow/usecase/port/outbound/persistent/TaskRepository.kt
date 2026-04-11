package com.workflow.workflow.usecase.port.outbound.persistent

import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import org.jdbi.v3.core.Handle
import java.time.Instant

interface TaskRepository {
    suspend fun insertBatch(tasks: List<Task>)
    suspend fun claimNext(workerId: String, limit: Int, queueName: String = "default"): List<Task>
    suspend fun findByWorkflowAndSequence(workflowId: String, sequenceNumber: Int): List<Task>
    suspend fun resetForRetry(id: String, newRetryCount: Int, claimedBy: String?, claimedAt: java.time.Instant?): Boolean
    suspend fun replayDeadLetterTask(taskId: String): Boolean
    suspend fun replayDeadLetterBatch(workflowId: String): Int
    suspend fun findExpired(now: Instant): List<Task>
    suspend fun resetStaleTasks(now: Instant): Int
    suspend fun deadLetterExhaustedTasks(now: Instant): Int
    suspend fun defer(taskId: String, triggerType: String, triggerMeta: String): Boolean
    suspend fun findDeferred(): List<DeferredTaskRef>

    fun updateStatusWithHandle(handle: Handle, id: String, newStatus: TaskStatus, resultJson: String? = null, claimedBy: String? = null, claimedAt: Instant? = null, fanOutPayloadsJson: String? = null): Boolean
    fun countNonTerminalWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun countTotalBySequenceWithHandle(handle: Handle, workflowId: String, sequenceNumber: Int): Int
    fun cancelPendingTasksWithHandle(handle: Handle, workflowId: String): Int
    fun insertBatchWithHandle(handle: Handle, tasks: List<Task>)
    fun replayDeadLetterBatchWithHandle(handle: Handle, workflowId: String): Int
    fun countAllNonTerminalWithHandle(handle: Handle, workflowId: String): Int
    fun findDistinctQueuesByWorkflowId(handle: Handle, workflowId: String, statuses: List<String>): List<String>
    fun countStatusSummariesByWorkflowWithHandle(handle: Handle, workflowId: String): Map<Int, TaskStatusCounts>
    fun findByWorkflowIdWithHandle(handle: Handle, workflowId: String): List<Task>
    fun cancelTasksForOverdueWorkflowsWithHandle(handle: Handle, now: java.time.LocalDateTime): Int
}
