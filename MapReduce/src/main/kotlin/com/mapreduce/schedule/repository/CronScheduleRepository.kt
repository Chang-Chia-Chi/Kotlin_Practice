package com.mapreduce.schedule.repository

import com.mapreduce.schedule.model.CronSchedule
import com.mapreduce.schedule.model.OverlapPolicy
import com.mapreduce.schedule.model.ScheduleExecutionStatus
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.time.Instant
import java.util.UUID

/**
 * Persistence layer for the `cron_schedule` table.
 *
 * All queries use named parameters for SQL injection safety.
 * Optimistic locking via `version` column prevents lost updates.
 */
@ApplicationScoped
class CronScheduleRepository(private val jdbi: Jdbi) {

    fun insert(schedule: CronSchedule): String {
        val id = schedule.scheduleId.ifBlank { UUID.randomUUID().toString() }
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO cron_schedule (
                    schedule_id, name, handler, cron_expression, payload,
                    queue, max_retries, overlap_policy, enabled, next_fire_at,
                    version, created_at, updated_at
                ) VALUES (
                    :id, :name, :handler, :cronExpression, :payload,
                    :queue, :maxRetries, :overlapPolicy, :enabled, :nextFireAt,
                    0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """
            )
                .bind("id", id)
                .bind("name", schedule.name)
                .bind("handler", schedule.handler)
                .bind("cronExpression", schedule.cronExpression)
                .bind("payload", schedule.payload)
                .bind("queue", schedule.queue)
                .bind("maxRetries", schedule.maxRetries)
                .bind("overlapPolicy", schedule.overlapPolicy.name)
                .bind("enabled", if (schedule.enabled) 1 else 0)
                .bind("nextFireAt", schedule.nextFireAt)
                .execute()
        }
        return id
    }

    fun findById(scheduleId: String): CronSchedule? =
        jdbi.withHandle<CronSchedule?, Exception> { h ->
            h.createQuery("SELECT * FROM cron_schedule WHERE schedule_id = :id")
                .bind("id", scheduleId)
                .mapTo(CronSchedule::class.java)
                .findOne().orElse(null)
        }

    fun findByName(name: String): CronSchedule? =
        jdbi.withHandle<CronSchedule?, Exception> { h ->
            h.createQuery("SELECT * FROM cron_schedule WHERE name = :name")
                .bind("name", name)
                .mapTo(CronSchedule::class.java)
                .findOne().orElse(null)
        }

    fun findAll(): List<CronSchedule> =
        jdbi.withHandle<List<CronSchedule>, Exception> { h ->
            h.createQuery("SELECT * FROM cron_schedule ORDER BY name")
                .mapTo(CronSchedule::class.java)
                .list()
        }

    /**
     * Find all enabled schedules whose `next_fire_at` is at or before [now].
     * This is the core query for the trigger loop.
     */
    fun findDueSchedules(now: Instant): List<CronSchedule> =
        jdbi.withHandle<List<CronSchedule>, Exception> { h ->
            h.createQuery(
                """
                SELECT * FROM cron_schedule
                WHERE enabled = 1
                  AND next_fire_at IS NOT NULL
                  AND next_fire_at <= :now
                ORDER BY next_fire_at ASC
                """
            )
                .bind("now", now)
                .mapTo(CronSchedule::class.java)
                .list()
        }

    /**
     * Atomically update the schedule after firing: set last_fired_at, last_task_id,
     * last_status, next_fire_at, and bump version. Uses optimistic locking.
     *
     * @return true if the update succeeded (version matched)
     */
    fun recordFire(
        scheduleId: String,
        version: Long,
        taskId: String,
        firedAt: Instant,
        nextFireAt: Instant?,
    ): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            val updated = h.createUpdate(
                """
                UPDATE cron_schedule
                SET last_fired_at  = :firedAt,
                    last_task_id   = :taskId,
                    last_status    = :status,
                    next_fire_at   = :nextFireAt,
                    version        = version + 1,
                    updated_at     = CURRENT_TIMESTAMP
                WHERE schedule_id = :id AND version = :version
                """
            )
                .bind("id", scheduleId)
                .bind("version", version)
                .bind("taskId", taskId)
                .bind("firedAt", firedAt)
                .bind("status", ScheduleExecutionStatus.PENDING.name)
                .bind("nextFireAt", nextFireAt)
                .execute()
            updated > 0
        }

    /**
     * Update last_completed_at and last_status after a scheduled task finishes.
     * Called by the completion observer.
     */
    fun recordCompletion(scheduleId: String, status: ScheduleExecutionStatus, completedAt: Instant) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE cron_schedule
                SET last_completed_at = :completedAt,
                    last_status       = :status,
                    updated_at        = CURRENT_TIMESTAMP
                WHERE schedule_id = :id
                """
            )
                .bind("id", scheduleId)
                .bind("completedAt", completedAt)
                .bind("status", status.name)
                .execute()
        }
    }

    /**
     * Update a schedule's definition (cron, payload, policy, etc.).
     * Uses optimistic locking.
     *
     * @return true if the update succeeded
     */
    fun update(
        scheduleId: String,
        version: Long,
        handler: String?,
        cronExpression: String?,
        payload: String?,
        queue: String?,
        maxRetries: Int?,
        overlapPolicy: OverlapPolicy?,
        nextFireAt: Instant?,
    ): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            val setClauses = mutableListOf<String>()
            val bindings = mutableMapOf<String, Any?>()

            handler?.let { setClauses += "handler = :handler"; bindings["handler"] = it }
            cronExpression?.let { setClauses += "cron_expression = :cronExpression"; bindings["cronExpression"] = it }
            payload?.let { setClauses += "payload = :payload"; bindings["payload"] = it }
            queue?.let { setClauses += "queue = :queue"; bindings["queue"] = it }
            maxRetries?.let { setClauses += "max_retries = :maxRetries"; bindings["maxRetries"] = it }
            overlapPolicy?.let { setClauses += "overlap_policy = :overlapPolicy"; bindings["overlapPolicy"] = it.name }
            nextFireAt?.let { setClauses += "next_fire_at = :nextFireAt"; bindings["nextFireAt"] = it }

            if (setClauses.isEmpty()) return@withHandle true

            setClauses += "version = version + 1"
            setClauses += "updated_at = CURRENT_TIMESTAMP"

            val sql = "UPDATE cron_schedule SET ${setClauses.joinToString(", ")} WHERE schedule_id = :id AND version = :version"
            val update = h.createUpdate(sql)
                .bind("id", scheduleId)
                .bind("version", version)
            bindings.forEach { (k, v) -> update.bind(k, v) }
            update.execute() > 0
        }

    fun setEnabled(scheduleId: String, enabled: Boolean): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            h.createUpdate(
                """
                UPDATE cron_schedule
                SET enabled = :enabled, updated_at = CURRENT_TIMESTAMP
                WHERE schedule_id = :id
                """
            )
                .bind("id", scheduleId)
                .bind("enabled", if (enabled) 1 else 0)
                .execute() > 0
        }

    fun delete(scheduleId: String): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            h.createUpdate("DELETE FROM cron_schedule WHERE schedule_id = :id")
                .bind("id", scheduleId)
                .execute() > 0
        }

    /**
     * Check if a task for the given schedule is currently in-flight
     * (PENDING or CLAIMED). Used by the SKIP overlap policy.
     */
    fun hasInFlightTask(scheduleName: String): Boolean =
        jdbi.withHandle<Boolean, Exception> { h ->
            val count = h.createQuery(
                """
                SELECT COUNT(*) FROM task
                WHERE status IN ('PENDING', 'CLAIMED')
                  AND metadata LIKE :pattern
                """
            )
                .bind("pattern", "%\"scheduleName\":\"$scheduleName\"%")
                .mapTo(Int::class.java)
                .one()
            count > 0
        }

    /**
     * Find the in-flight task ID for REPLACE overlap policy.
     */
    fun findInFlightTaskId(scheduleName: String): String? =
        jdbi.withHandle<String?, Exception> { h ->
            h.createQuery(
                """
                SELECT task_id FROM task
                WHERE status IN ('PENDING', 'CLAIMED')
                  AND metadata LIKE :pattern
                FETCH FIRST 1 ROWS ONLY
                """
            )
                .bind("pattern", "%\"scheduleName\":\"$scheduleName\"%")
                .mapTo(String::class.java)
                .findOne().orElse(null)
        }

    /**
     * Read a task's metadata column by task ID.
     * Used by the completion observer to correlate tasks back to schedules.
     */
    fun getTaskMetadata(taskId: String): String? =
        jdbi.withHandle<String?, Exception> { h ->
            h.createQuery("SELECT metadata FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java)
                .findOne().orElse(null)
        }
}
