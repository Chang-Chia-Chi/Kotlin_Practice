package com.mapreduce.schedule.observer

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.event.TaskCompleted
import com.mapreduce.event.TaskResultType
import com.mapreduce.schedule.ScheduleMetrics
import com.mapreduce.schedule.model.ScheduleExecutionStatus
import com.mapreduce.schedule.model.ScheduleTaskMetadata
import com.mapreduce.schedule.repository.CronScheduleRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.ObservesAsync
import org.jboss.logging.Logger
import java.time.Instant

/**
 * Observes [TaskCompleted] events and updates the `cron_schedule` table
 * when the completed task was enqueued by the cron trigger.
 *
 * Correlation: the task's `metadata` JSON contains `scheduleId` if it
 * was produced by the trigger loop.
 */
@ApplicationScoped
class ScheduleCompletionObserver(
    private val scheduleRepository: CronScheduleRepository,
    private val objectMapper: ObjectMapper,
    private val scheduleMetrics: ScheduleMetrics,
) {

    private val log = Logger.getLogger(ScheduleCompletionObserver::class.java)

    fun onTaskCompleted(@ObservesAsync event: TaskCompleted) {
        // Quick exit: most tasks are not schedule-produced
        val metadata = extractScheduleMetadata(event) ?: return

        val status = when (event.result) {
            TaskResultType.SUCCESS -> ScheduleExecutionStatus.SUCCESS
            TaskResultType.FAILED, TaskResultType.RETRY -> ScheduleExecutionStatus.FAILED
            TaskResultType.DEAD_LETTERED -> ScheduleExecutionStatus.DEAD_LETTERED
        }

        try {
            scheduleRepository.recordCompletion(
                scheduleId = metadata.scheduleId,
                status = status,
                completedAt = Instant.now(),
            )
            scheduleMetrics.recordLastDuration(metadata.scheduleName, event.durationMs)
            log.debugf(
                "Schedule '%s' task %s completed with %s (duration=%dms)",
                metadata.scheduleName, event.taskId, status, event.durationMs,
            )
        } catch (e: Exception) {
            log.warnf(e, "Failed to record schedule completion for task %s (schedule=%s)",
                event.taskId, metadata.scheduleName)
        }
    }

    private fun extractScheduleMetadata(event: TaskCompleted): ScheduleTaskMetadata? {
        // TaskCompleted doesn't carry metadata directly — we need to check
        // the task record. However, to avoid an extra DB round-trip for every
        // task completion, we rely on the handler field convention. If the
        // task was enqueued by the trigger, its metadata JSON will have been
        // read during execution and is available on the completion event's
        // groupId field or we can inspect the task.
        //
        // Since TaskCompleted doesn't include metadata, we use a lightweight
        // approach: look up by task ID only when the handler is known to be
        // a scheduled handler. For generic detection without handler knowledge,
        // we query the task table for metadata.
        return try {
            // Use JDBI to get task metadata — cheap SELECT by PK
            val metadata = scheduleRepository.getTaskMetadata(event.taskId) ?: return null
            objectMapper.readValue(metadata, ScheduleTaskMetadata::class.java)
        } catch (_: Exception) {
            null
        }
    }
}
