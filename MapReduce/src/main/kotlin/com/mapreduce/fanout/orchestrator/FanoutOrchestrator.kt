package com.mapreduce.fanout.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.FanoutJobStateChanged
import com.mapreduce.fanout.model.FanoutJob
import com.mapreduce.fanout.model.FanoutJobStatus
import com.mapreduce.fanout.registry.FanoutRegistrar
import com.mapreduce.fanout.repository.FanoutJobRepository
import com.mapreduce.fanout.spi.FanoutSummary
import com.mapreduce.fanout.spi.unsafeCast
import com.mapreduce.leader.FencingTokenHolder
import com.mapreduce.leader.LeaderManager
import com.mapreduce.mr.model.evaluateFailurePolicy
import com.mapreduce.observability.AutoscalingMetrics
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
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

/**
 * Leader-only monitoring loop for fan-out jobs.
 *
 * Responsibilities:
 * - Detect barriers (all execute tasks resolved)
 * - Apply failure policies
 * - Invoke OnCompleted callback inline
 * - Recover from leader failover
 *
 * Unlike [com.mapreduce.mr.orchestrator.MapReduceOrchestrator], there is no
 * reduce phase. The barrier transitions directly from RUNNING to COMPLETED,
 * with the OnCompleted callback running inline on the leader.
 *
 * All leader writes propagate the fencing epoch via [FencingTokenHolder]
 * so repository SQL includes the `WHERE last_epoch <= :epoch` guard.
 */
@ApplicationScoped
class FanoutOrchestrator(
    private val config: FrameworkConfig,
    private val fanoutJobRepository: FanoutJobRepository,
    private val taskRepository: TaskRepository,
    private val registrar: FanoutRegistrar,
    private val leaderManager: LeaderManager,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val autoscalingMetrics: AutoscalingMetrics,
    private val fanoutJobStateEvent: Event<FanoutJobStateChanged>,
) {

    private val log = Logger.getLogger(FanoutOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun onStart(@Observes ev: StartupEvent) {
        shutdownCoordinator.registerLeaderScopeCallback { scope.cancel() }

        val interval = config.leader().monitorInterval().toMillis()
        scope.launch {
            delay(interval) // initial delay
            while (isActive) {
                if (leaderManager.isActive) {
                    val epoch = leaderManager.token
                    try {
                        withContext(Dispatchers.IO) {
                            FencingTokenHolder.withToken(epoch) {
                                monitorRunningJobs()
                            }
                        }
                    } catch (e: Exception) {
                        log.errorf(e, "Error in fanout orchestrator loop")
                    }
                }
                delay(interval)
            }
        }
    }

    /**
     * For RUNNING jobs: count completed + dead-lettered tasks from the task table.
     * When barrier is met (completed + dead_lettered >= total), apply failure policy
     * and either fail the job or invoke OnCompleted + transition to COMPLETED.
     */
    private fun monitorRunningJobs() {
        val runningJobs = fanoutJobRepository.findJobsByStatus(FanoutJobStatus.RUNNING)
        for (job in runningJobs) {
            val deadLettered = taskRepository.countByGroupAndStatus(job.jobId, TaskStatus.DEAD_LETTER)

            // Sync the failed_tasks counter from authoritative source (task table)
            if (deadLettered != job.failedTasks) {
                fanoutJobRepository.updateFailedTasks(job.jobId, deadLettered)
            }

            if (job.completedTasks + deadLettered >= job.totalTasks) {
                handleBarrierMet(job, deadLettered)
            }
        }
    }

    private fun handleBarrierMet(job: FanoutJob, deadLettered: Int) {
        log.infof("Barrier met for fanout job %s: completed=%d, dead_lettered=%d, total=%d",
            job.jobId, job.completedTasks, deadLettered, job.totalTasks)

        // Apply failure policy (stored on the job row at creation time)
        val failureReason = evaluateFailurePolicy(
            job.failurePolicy, deadLettered, job.totalTasks, job.failureThreshold,
        )
        if (failureReason != null) {
            failJob(job, failureReason)
            return
        }

        // Invoke OnCompleted inline, then transition to COMPLETED
        val definition = registrar.getDefinition(job.jobType)
        if (definition != null) {
            try {
                val summary = FanoutSummary(
                    jobId = job.jobId,
                    jobType = job.jobType,
                    totalTasks = job.totalTasks,
                    completedTasks = job.completedTasks,
                    failedTasks = deadLettered,
                )
                definition.unsafeCast().onCompleted(summary)
            } catch (e: Exception) {
                log.errorf(e, "OnCompleted failed for fanout job %s — transitioning to FAILED", job.jobId)
                failJob(job, "OnCompleted failed: ${e.message}")
                return
            }
        }

        val transitioned = fanoutJobRepository.casJobStatus(
            job.jobId, FanoutJobStatus.RUNNING, FanoutJobStatus.COMPLETED, job.version,
        )
        if (transitioned) {
            fireFanoutJobStateChanged(job, FanoutJobStatus.RUNNING, FanoutJobStatus.COMPLETED)
            log.infof("Fanout job %s completed", job.jobId)
            if (job.createdAt != null) {
                autoscalingMetrics.recordOrchestrationDuration("FanOut", job.jobType, job.createdAt)
            }
        }
    }

    private fun fireFanoutJobStateChanged(
        job: FanoutJob,
        previousStatus: FanoutJobStatus,
        newStatus: FanoutJobStatus,
    ) {
        try {
            fanoutJobStateEvent.fireAsync(FanoutJobStateChanged(
                jobId = job.jobId,
                jobType = job.jobType,
                previousStatus = previousStatus,
                newStatus = newStatus,
                completedTasks = job.completedTasks,
                failedTasks = job.failedTasks,
                totalTasks = job.totalTasks,
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire FanoutJobStateChanged event for job %s", job.jobId)
        }
    }

    private fun failJob(job: FanoutJob, reason: String) {
        val transitioned = fanoutJobRepository.casJobStatus(
            job.jobId, FanoutJobStatus.RUNNING, FanoutJobStatus.FAILED, job.version,
        )
        if (transitioned) {
            fireFanoutJobStateChanged(job, FanoutJobStatus.RUNNING, FanoutJobStatus.FAILED)
            log.warnf("Fanout job %s failed: %s", job.jobId, reason)
            if (job.createdAt != null) {
                autoscalingMetrics.recordOrchestrationDuration("FanOut", job.jobType, job.createdAt)
            }
        }
    }
}
