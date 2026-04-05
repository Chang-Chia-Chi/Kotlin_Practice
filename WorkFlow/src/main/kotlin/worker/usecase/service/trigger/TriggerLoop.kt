package com.workflow.worker.usecase.service.trigger

import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.infrastructure.shutdown.ShutdownParticipant
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

const val SHUTDOWN_ORDER_TRIGGER = 5

/**
 * Leader-gated sweep loop that periodically loads DEFERRED tasks, dispatches
 * them to [TriggerDriver] instances, polls for results, and settles
 * completed/failed/expired tasks through the [PhaseGate].
 *
 * Runs on a single-threaded coroutine dispatcher (`limitedParallelism(1)`)
 * with a [SupervisorJob] so that individual sweep failures do not cancel
 * the loop.
 */
@ApplicationScoped
class TriggerLoop(
    private val taskRepo: TaskRepository,
    private val driverBeans: Instance<TriggerDriver>,
    private val phaseGate: PhaseGate,
    private val leaderGuard: LeaderGuard,
    private val meterRegistry: MeterRegistry,
    private val triggerLoopConfig: TriggerLoopConfig,
    private val shutdownConfig: ShutdownConfig,
) : ShutdownParticipant {

    private val log = LoggerFactory.getLogger(TriggerLoop::class.java)
    private val _running = AtomicBoolean(false)
    private val deferredGauge = AtomicInteger(0)

    @Volatile
    private var activeJob: Job? = null

    private var drivers: Map<String, TriggerDriver> = emptyMap()
    private var pollCounter: Counter? = null
    private var sweepTimer: Timer? = null
    private var gaugeRegistered = false

    private fun settledCounter(type: String, outcome: String): Counter =
        meterRegistry.counter("trigger_settled_total", "type", type, "outcome", outcome)

    fun onStart(@Observes ev: StartupEvent) {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO.limitedParallelism(1))
        start(scope)
    }

    /**
     * Starts the sweep loop in the given [scope]. Returns the [Job] for
     * testing and lifecycle control.
     */
    fun start(scope: CoroutineScope): Job {
        drivers = driverBeans.associateBy { it.type() }
        pollCounter = meterRegistry.counter("trigger_poll_total")
        sweepTimer = Timer.builder("trigger_sweep_duration_seconds")
            .publishPercentileHistogram()
            .register(meterRegistry)
        if (!gaugeRegistered) {
            meterRegistry.gauge("trigger_deferred_tasks", deferredGauge) { it.get().toDouble() }
            gaugeRegistered = true
        }

        _running.set(true)
        val interval = triggerLoopConfig.sweepInterval()

        val job = scope.launch {
            while (isActive && _running.get()) {
                try {
                    sweep()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.error("Trigger sweep failed", e)
                }
                delay(interval.toMillis())
            }
        }
        activeJob = job
        log.info("TriggerLoop started: sweepInterval={}, drivers={}", interval, drivers.keys)
        return job
    }

    /**
     * Single sweep iteration: dispatch DEFERRED tasks to drivers, poll for
     * results, enforce deadlines.
     */
    internal suspend fun sweep() {
        if (!leaderGuard.isLeader) return

        val sample = Timer.start(meterRegistry)
        pollCounter?.increment()

        val deferred = taskRepo.findDeferred()
        val taskIndex = deferred.associateBy { it.taskId }
        deferredGauge.set(deferred.size)

        val grouped = deferred.groupBy { it.triggerType }

        // Dispatch to drivers
        for ((type, tasks) in grouped) {
            val driver = drivers[type]
            if (driver == null) {
                log.warn("No TriggerDriver registered for type '{}', {} tasks orphaned", type, tasks.size)
                continue
            }
            try {
                driver.start(tasks)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("TriggerDriver '{}' start() failed", type, e)
            }
        }

        // Poll all drivers for results
        val settledTaskIds = mutableSetOf<String>()
        for ((type, driver) in drivers) {
            val results = try {
                driver.poll()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("TriggerDriver '{}' poll() failed", type, e)
                continue
            }
            for (result in results) {
                if (settleResult(type, result, taskIndex)) {
                    settledTaskIds += result.taskId
                }
            }
        }

        // Deadline enforcement — skip tasks already settled via poll results
        val now = Instant.now()
        for (task in deferred) {
            if (task.taskId !in settledTaskIds && task.deadlineAt != null && now.isAfter(task.deadlineAt)) {
                expireTask(task)
            }
        }

        sweepTimer?.let { sample.stop(it) }
    }

    /**
     * Settles a single trigger result. Returns `true` if the task was settled
     * (phaseGate called or retry reset), used to skip duplicate deadline expiry.
     */
    private suspend fun settleResult(
        triggerType: String,
        result: TriggerResult,
        taskIndex: Map<String, DeferredTaskRef>,
    ): Boolean {
        val task = taskIndex[result.taskId]
        if (task == null) {
            log.warn("TriggerResult for unknown task {} (type={}), skipping", result.taskId, triggerType)
            return false
        }
        return try {
            when (result) {
                is TriggerResult.Succeeded -> {
                    phaseGate.onTaskCompleted(
                        taskId = result.taskId,
                        workflowId = task.workflowId,
                        sequenceNumber = task.sequenceNumber,
                        status = TaskStatus.COMPLETED,
                        resultJson = result.result,
                        claimedBy = null,
                        claimedAt = null,
                    )
                    settledCounter(triggerType, "succeeded").increment()
                    log.info("Trigger settled task {} as COMPLETED (type={})", result.taskId, triggerType)
                    true
                }
                is TriggerResult.Failed -> {
                    handleTriggerFailure(result.taskId, triggerType, result.reason, taskIndex)
                    true
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Failed to settle trigger result for task {}", result.taskId, e)
            false
        }
    }

    private suspend fun handleTriggerFailure(
        taskId: String,
        triggerType: String,
        reason: String,
        taskIndex: Map<String, DeferredTaskRef>,
    ) {
        val task = taskIndex[taskId]
        if (task == null) {
            log.warn("TriggerFailure for unknown task {} (type={}), skipping", taskId, triggerType)
            return
        }

        if (task.retryCount < task.maxRetries) {
            try {
                taskRepo.resetForRetry(taskId, task.retryCount + 1)
                settledCounter(triggerType, "retried").increment()
                log.info(
                    "Trigger task {} failed ({}), retrying ({}/{})",
                    taskId, reason, task.retryCount + 1, task.maxRetries,
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error("Failed to reset task {} for retry, reporting as FAILED", taskId, e)
                phaseGate.onTaskCompleted(
                    taskId = taskId,
                    workflowId = task.workflowId,
                    sequenceNumber = task.sequenceNumber,
                    status = TaskStatus.FAILED,
                    resultJson = null,
                    claimedBy = null,
                    claimedAt = null,
                )
                settledCounter(triggerType, "failed").increment()
            }
        } else {
            phaseGate.onTaskCompleted(
                taskId = taskId,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                status = TaskStatus.FAILED,
                resultJson = null,
                claimedBy = null,
                claimedAt = null,
            )
            settledCounter(triggerType, "failed").increment()
            log.warn("Trigger task {} failed permanently ({})", taskId, reason)
        }
    }

    private suspend fun expireTask(task: DeferredTaskRef) {
        try {
            drivers[task.triggerType]?.let { driver ->
                try {
                    driver.cancel(task.taskId)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.warn("Failed to cancel trigger for expired task {}", task.taskId, e)
                }
            }
            phaseGate.onTaskCompleted(
                taskId = task.taskId,
                workflowId = task.workflowId,
                sequenceNumber = task.sequenceNumber,
                status = TaskStatus.TIMED_OUT,
                resultJson = null,
                claimedBy = null,
                claimedAt = null,
            )
            settledCounter(task.triggerType, "expired").increment()
            log.warn("DEFERRED task {} expired (deadline={})", task.taskId, task.deadlineAt)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("Failed to expire DEFERRED task {}", task.taskId, e)
        }
    }

    override val shutdownOrder: Int = SHUTDOWN_ORDER_TRIGGER

    override val shutdownTimeout: Duration get() = shutdownConfig.globalTimeout()

    override suspend fun shutdown() {
        log.info("TriggerLoop shutting down")
        _running.set(false)
        withTimeoutOrNull(shutdownTimeout.toMillis()) {
            activeJob?.join()
        }
        activeJob?.cancelAndJoin()
        for ((type, driver) in drivers) {
            try {
                driver.close()
            } catch (e: Exception) {
                log.warn("TriggerDriver '{}' close() failed", type, e)
            }
        }
        log.info("TriggerLoop shutdown complete")
    }
}
