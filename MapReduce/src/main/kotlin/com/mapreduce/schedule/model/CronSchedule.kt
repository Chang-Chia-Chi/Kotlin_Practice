package com.mapreduce.schedule.model

import org.jdbi.v3.core.mapper.reflect.ColumnName
import java.time.Instant

/**
 * What to do when the cron ticks but the previous task is still running.
 *
 * - **SKIP** — do not enqueue; wait for the current one to finish (default).
 * - **ENQUEUE** — enqueue regardless; allows concurrent executions.
 * - **REPLACE** — dead-letter the running task and enqueue a fresh one.
 */
enum class OverlapPolicy {
    SKIP, ENQUEUE, REPLACE,
}

/**
 * Outcome of the most recent scheduled task execution.
 * Stored in `cron_schedule.last_status` and updated by the completion observer.
 */
enum class ScheduleExecutionStatus {
    SUCCESS, FAILED, DEAD_LETTERED, PENDING, RUNNING,
}

/**
 * Persistent schedule definition — one row per cron schedule.
 *
 * The trigger loop reads enabled schedules where `next_fire_at <= NOW`,
 * enqueues a task, and updates the tracking columns.
 */
data class CronSchedule(
    @ColumnName("schedule_id") val scheduleId: String,
    val name: String,
    val handler: String,
    @ColumnName("cron_expression") val cronExpression: String,
    val payload: String = "{}",
    val queue: String = "default",
    @ColumnName("max_retries") val maxRetries: Int = 3,
    @ColumnName("overlap_policy") val overlapPolicy: OverlapPolicy = OverlapPolicy.SKIP,
    val enabled: Boolean = true,
    @ColumnName("last_fired_at") val lastFiredAt: Instant? = null,
    @ColumnName("last_completed_at") val lastCompletedAt: Instant? = null,
    @ColumnName("last_task_id") val lastTaskId: String? = null,
    @ColumnName("last_status") val lastStatus: ScheduleExecutionStatus? = null,
    @ColumnName("next_fire_at") val nextFireAt: Instant? = null,
    val version: Long = 0,
    @ColumnName("created_at") val createdAt: Instant? = null,
    @ColumnName("updated_at") val updatedAt: Instant? = null,
)

/**
 * Metadata embedded in the task's `metadata` JSON column to link
 * a scheduled task back to its schedule definition.
 */
data class ScheduleTaskMetadata(
    val scheduleName: String,
    val scheduleId: String,
    val fireTime: Instant,
    val sequenceNumber: Long,
)
