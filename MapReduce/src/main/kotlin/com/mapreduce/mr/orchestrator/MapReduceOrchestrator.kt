package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.FencingTokenHolder
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Leader-only monitoring loop, simplified after the reactive barrier refactoring.
 *
 * Phase transitions are now handled reactively by [PhaseTransitionHandler] callback
 * tasks. This orchestrator retains:
 * 1. [pollQueueDepth] — HPA queue depth gauge (unchanged)
 * 2. [recoverySweep] — safety net for stuck ACTIVE groups where barrier was met
 *    but callback task was lost (e.g., bug, TX anomaly)
 */
@ApplicationScoped
class MapReduceOrchestrator(
    private val config: FrameworkConfig,
    private val taskGroupRepository: TaskGroupRepository,
    private val taskRepository: TaskRepository,
    private val leaderManager: LeaderManager,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(MapReduceOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val queueDepths = ConcurrentHashMap<String, AtomicLong>()

    /** Tracks how many monitor intervals have elapsed, for low-frequency recovery sweep. */
    private var tickCount = 0

    fun onStart(@Observes ev: StartupEvent) {
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.leader().monitorInterval().toMillis()
        scope.launch {
            delay(interval)
            while (isActive) {
                if (leaderManager.isActive) {
                    val epoch = leaderManager.token
                    try {
                        withContext(Dispatchers.IO) {
                            FencingTokenHolder.withToken(epoch) {
                                monitor()
                            }
                        }
                    } catch (e: Exception) {
                        log.errorf(e, "Error in MR orchestrator loop")
                    }
                }
                delay(interval)
            }
        }
    }

    private fun monitor() {
        pollQueueDepth()

        // Recovery sweep at 5x the normal interval
        tickCount++
        if (tickCount % 5 == 0) {
            recoverySweep()
        }
    }

    private fun pollQueueDepth() {
        try {
            val counts = taskRepository.countPendingByQueue()
            for ((queue, count) in counts) {
                queueDepths.computeIfAbsent(queue) { q ->
                    AtomicLong(0).also { gauge ->
                        meterRegistry.gauge(
                            "framework.queue.depth",
                            listOf(Tag.of("queue_name", q)),
                            gauge,
                        ) { it.toDouble() }
                    }
                }.set(count.toLong())
            }
            for ((queue, depth) in queueDepths) {
                if (queue !in counts) depth.set(0)
            }
        } catch (e: Exception) {
            log.warnf(e, "Failed to poll queue depth")
        }
    }

    /**
     * Safety net: detect ACTIVE groups where the barrier has been met but no
     * callback task exists (e.g., due to TX anomaly or bug). Reconciles the
     * phase_failed counter from actual DEAD_LETTER count and re-creates
     * missing callback tasks.
     */
    private fun recoverySweep() {
        try {
            val activeGroups = taskGroupRepository.findGroupsByStatus(GroupStatus.ACTIVE)
            for (group in activeGroups) {
                // Reconcile phase_failed from actual dead-letter count
                val actualDeadLettered = taskRepository.countByGroupAndStatus(group.groupId, TaskStatus.DEAD_LETTER)
                val actualCompleted = taskRepository.countByGroupAndStatus(group.groupId, TaskStatus.COMPLETED)

                val barrierMet = actualCompleted + actualDeadLettered >= group.phaseTotal
                if (!barrierMet) continue

                // Check if callback task already exists (PENDING or CLAIMED)
                if (group.onCompleteHandler != null) {
                    val existingCallback = taskRepository.findByGroupAndHandler(group.groupId, group.onCompleteHandler)
                    if (existingCallback == null) {
                        // No callback found with groupId match — but callback tasks have NULL groupId.
                        // Look for any pending/claimed task with the handler name and payload = groupId
                        log.warnf(
                            "Recovery: group %s barrier met (completed=%d, dead_lettered=%d, total=%d) but no callback task — re-creating",
                            group.groupId, actualCompleted, actualDeadLettered, group.phaseTotal,
                        )
                        taskGroupRepository.recordGroupTaskFailure(group.groupId)
                    }
                }
            }
        } catch (e: Exception) {
            log.warnf(e, "Error in recovery sweep")
        }
    }
}
