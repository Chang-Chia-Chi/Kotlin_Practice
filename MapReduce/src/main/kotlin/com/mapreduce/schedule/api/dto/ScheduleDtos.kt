package com.mapreduce.schedule.api.dto

import com.mapreduce.schedule.model.OverlapPolicy
import com.mapreduce.schedule.model.ScheduleExecutionStatus
import java.time.Instant

/**
 * Request to create a new cron schedule.
 */
data class CreateScheduleRequest(
    val name: String,
    val handler: String,
    val cronExpression: String,
    val payload: String = "{}",
    val queue: String = "default",
    val maxRetries: Int = 3,
    val overlapPolicy: OverlapPolicy = OverlapPolicy.SKIP,
    val enabled: Boolean = true,
)

/**
 * Request to update an existing cron schedule.
 * All fields are optional — only non-null fields are applied.
 */
data class UpdateScheduleRequest(
    val handler: String? = null,
    val cronExpression: String? = null,
    val payload: String? = null,
    val queue: String? = null,
    val maxRetries: Int? = null,
    val overlapPolicy: OverlapPolicy? = null,
)

/**
 * Response DTO for schedule endpoints.
 */
data class ScheduleResponse(
    val scheduleId: String,
    val name: String,
    val handler: String,
    val cronExpression: String,
    val payload: String,
    val queue: String,
    val maxRetries: Int,
    val overlapPolicy: OverlapPolicy,
    val enabled: Boolean,
    val lastFiredAt: Instant?,
    val lastCompletedAt: Instant?,
    val lastTaskId: String?,
    val lastStatus: ScheduleExecutionStatus?,
    val nextFireAt: Instant?,
    val createdAt: Instant?,
    val updatedAt: Instant?,
)

/**
 * Response for the manual trigger endpoint.
 */
data class TriggerResponse(
    val taskId: String,
    val scheduleName: String,
    val message: String,
)
