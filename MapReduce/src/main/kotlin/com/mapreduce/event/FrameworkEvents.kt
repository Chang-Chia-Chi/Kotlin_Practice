package com.mapreduce.event

import java.time.Instant

/**
 * CDI event fired when a task is moved to DEAD_LETTER status.
 *
 * Consumers:
 * - [com.mapreduce.deadletter.alerting.DeadLetterAlertEvaluator] — threshold alerting
 * - [com.mapreduce.deadletter.DeadLetterMetrics] — Prometheus counters
 *
 * Producers:
 * - [com.mapreduce.queue.worker.TaskDispatcher] — handler failure / no handler
 * - [com.mapreduce.queue.worker.StaleTaskReaper] — stale reclaim exhausts retries
 */
data class TaskDeadLettered(
    val taskId: String,
    val handler: String,
    val queue: String,
    val groupId: String?,
    val retryCount: Int,
    val lastError: String,
    val createdAt: Instant?,
    val deadLetteredAt: Instant = Instant.now(),
)
