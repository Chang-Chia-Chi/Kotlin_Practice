package com.mapreduce.schedule.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.ScheduleFired
import com.mapreduce.event.ScheduleSkipped
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.schedule.ScheduleMetrics
import com.mapreduce.schedule.cron.CronExpressionParser
import com.mapreduce.schedule.model.CronSchedule
import com.mapreduce.schedule.model.OverlapPolicy
import com.mapreduce.schedule.model.ScheduleTaskMetadata
import com.mapreduce.schedule.repository.CronScheduleRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.time.Instant

/**
 * Leader-only trigger loop that polls due cron schedules and enqueues
 * tasks into the generic task queue.
 *
 * Lifecycle: starts on [StartupEvent], cancelled during shutdown Phase 1.
 * Only the leader pod executes the trigger logic; follower pods skip each tick.
 *
 * On leader failover, the new leader picks up where the old one left off
 * by reading `next_fire_at` from Oracle. No backfilling of missed ticks —
 * if the gap exceeds the schedule interval, exactly one task is enqueued.
 */
@ApplicationScoped
class CronTriggerLoop(
    private val config: FrameworkConfig,
    private val leaderManager: LeaderManager,
    private val scheduleRepository: CronScheduleRepository,
    private val taskRepository: TaskRepository,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val objectMapper: ObjectMapper,
    private val scheduleMetrics: ScheduleMetrics,
    private val scheduleFiredEvent: Event<ScheduleFired>,
    private val scheduleSkippedEvent: Event<ScheduleSkipped>,
) {

    private val log = Logger.getLogger(CronTriggerLoop::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** Monotonic counter per schedule for sequenceNumber in task metadata. */
    private val sequenceCounters = mutableMapOf<String, Long>()

    fun onStart(@Observes ev: StartupEvent) {
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val tickInterval = config.schedule().triggerInterval().toMillis()

        scope.launch {
            delay(tickInterval) // skip first tick at startup
            while (isActive) {
                if (leaderManager.isActive) {
                    try {
                        withContext(Dispatchers.IO) { tick() }
                    } catch (e: Exception) {
                        log.errorf(e, "Error in cron trigger loop")
                    }
                }
                delay(tickInterval)
            }
        }
    }

    /**
     * One tick: find all due schedules and process each.
     */
    private fun tick() {
        val now = Instant.now()
        val dueSchedules = scheduleRepository.findDueSchedules(now)
        for (schedule in dueSchedules) {
            try {
                processSchedule(schedule, now)
            } catch (e: Exception) {
                log.warnf(e, "Failed to process schedule '%s' (id=%s)", schedule.name, schedule.scheduleId)
            }
        }
    }

    private fun processSchedule(schedule: CronSchedule, now: Instant) {
        // Evaluate overlap policy
        when (schedule.overlapPolicy) {
            OverlapPolicy.SKIP -> {
                if (scheduleRepository.hasInFlightTask(schedule.name)) {
                    log.debugf("Schedule '%s' skipped — previous task still in-flight (SKIP policy)", schedule.name)
                    scheduleMetrics.recordSkipped(schedule.name)
                    fireSkippedEvent(schedule, "Previous task still in-flight (SKIP policy)")
                    // Recompute next fire time so the trigger re-evaluates on next tick
                    advanceNextFireAt(schedule, now)
                    return
                }
            }
            OverlapPolicy.REPLACE -> {
                val inFlightTaskId = scheduleRepository.findInFlightTaskId(schedule.name)
                if (inFlightTaskId != null) {
                    log.infof("Schedule '%s' replacing in-flight task %s (REPLACE policy)", schedule.name, inFlightTaskId)
                    taskRepository.deadLetter(inFlightTaskId, "Replaced by schedule '${schedule.name}' (REPLACE overlap policy)")
                }
            }
            OverlapPolicy.ENQUEUE -> { /* no check needed */ }
        }

        // Build schedule metadata for the task
        val seq = sequenceCounters.merge(schedule.scheduleId, 1L) { old, _ -> old + 1 }!!
        val metadata = ScheduleTaskMetadata(
            scheduleName = schedule.name,
            scheduleId = schedule.scheduleId,
            fireTime = now,
            sequenceNumber = seq,
        )
        val metadataJson = objectMapper.writeValueAsString(metadata)

        // Enqueue the task
        val taskId = taskRepository.enqueue(
            EnqueueRequest(
                handler = schedule.handler,
                payload = schedule.payload,
                queue = schedule.queue,
                maxRetries = schedule.maxRetries,
                metadata = metadataJson,
            ),
        )

        // Compute next fire time
        val nextFireAt = CronExpressionParser.nextFireTime(schedule.cronExpression, now)

        // Update schedule state (optimistic lock)
        val updated = scheduleRepository.recordFire(
            scheduleId = schedule.scheduleId,
            version = schedule.version,
            taskId = taskId,
            firedAt = now,
            nextFireAt = nextFireAt,
        )

        if (updated) {
            log.infof("Schedule '%s' fired → task %s (next_fire_at=%s)", schedule.name, taskId, nextFireAt)
            scheduleMetrics.recordFired(schedule.name)
            fireFiredEvent(schedule, taskId)
        } else {
            log.warnf("Schedule '%s' fire lost optimistic lock race — task %s still enqueued", schedule.name, taskId)
        }
    }

    private fun fireFiredEvent(schedule: CronSchedule, taskId: String) {
        try {
            scheduleFiredEvent.fireAsync(ScheduleFired(
                scheduleId = schedule.scheduleId,
                scheduleName = schedule.name,
                taskId = taskId,
                handler = schedule.handler,
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire ScheduleFired event for schedule '%s'", schedule.name)
        }
    }

    private fun fireSkippedEvent(schedule: CronSchedule, reason: String) {
        try {
            scheduleSkippedEvent.fireAsync(ScheduleSkipped(
                scheduleId = schedule.scheduleId,
                scheduleName = schedule.name,
                reason = reason,
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire ScheduleSkipped event for schedule '%s'", schedule.name)
        }
    }

    /**
     * Advance `next_fire_at` without enqueuing a task (used when SKIP skips).
     */
    private fun advanceNextFireAt(schedule: CronSchedule, now: Instant) {
        val nextFireAt = CronExpressionParser.nextFireTime(schedule.cronExpression, now)
        scheduleRepository.update(
            scheduleId = schedule.scheduleId,
            version = schedule.version,
            handler = null,
            cronExpression = null,
            payload = null,
            queue = null,
            maxRetries = null,
            overlapPolicy = null,
            nextFireAt = nextFireAt,
        )
    }
}
