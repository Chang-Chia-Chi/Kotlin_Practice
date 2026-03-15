package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.JobStateChanged
import com.mapreduce.leader.FencingTokenHolder
import com.mapreduce.leader.LeaderManager
import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.model.Job
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.mr.repository.JobRepository
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
 * Leader-only monitoring loop for map-reduce jobs.
 *
 * Responsibilities:
 * - Detect barriers (all map tasks resolved)
 * - Apply failure policies
 * - Dispatch reduce tasks
 * - Monitor reduce completion
 * - Recover from leader failover
 *
 * All leader writes propagate the fencing epoch via [FencingTokenHolder]
 * so repository SQL includes the `WHERE last_epoch <= :epoch` guard.
 */
@ApplicationScoped
class MapReduceOrchestrator(
    private val config: FrameworkConfig,
    private val jobRepository: JobRepository,
    private val taskRepository: TaskRepository,
    private val registrar: MapReduceRegistrar,
    private val leaderManager: LeaderManager,
    private val speculativeExecutor: SpeculativeExecutor,
    private val shutdownCoordinator: ShutdownCoordinator,
    private val autoscalingMetrics: AutoscalingMetrics,
    private val jobStateEvent: Event<JobStateChanged>,
) {

    private val log = Logger.getLogger(MapReduceOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun onStart(@Observes ev: StartupEvent) {
        // Register scope cancellation with shutdown coordinator for Phase 1
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
                                monitorJobs()
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

    private fun monitorJobs() {
        val runningJobs = monitorRunningJobs()
        speculativeExecutor.evaluateRunningJobs(runningJobs)
        monitorReducingJobs()
    }

    /**
     * For RUNNING jobs: count completed + dead-lettered tasks from the task table.
     * When barrier is met (completed + dead_lettered >= total), apply failure policy
     * and either fail the job or dispatch the reduce task.
     */
    private fun monitorRunningJobs(): List<Job> {
        val runningJobs = jobRepository.findJobsByStatus(JobStatus.RUNNING)
        for (job in runningJobs) {
            val deadLettered = taskRepository.countByGroupAndStatus(job.jobId, TaskStatus.DEAD_LETTER)

            // Sync the failed_tasks counter from authoritative source (task table)
            if (deadLettered != job.failedTasks) {
                jobRepository.updateFailedTasks(job.jobId, deadLettered)
            }

            if (job.completedTasks + deadLettered >= job.totalTasks) {
                handleBarrierMet(job, deadLettered)
            }
        }
        return runningJobs
    }

    /**
     * For REDUCING jobs: check reduce task(s) status.
     * Supports sharded reduce — multiple parallel reduce tasks per job.
     * - No reduce tasks → recovery: enqueue them
     * - All COMPLETED → transition to COMPLETED
     * - Any DEAD_LETTER → transition to FAILED (localized retry for partitioned)
     */
    private fun monitorReducingJobs() {
        val reducingJobs = jobRepository.findJobsByStatus(JobStatus.REDUCING)
        for (job in reducingJobs) {
            val reduceHandler = "${job.jobType}.reduce"
            val reduceTasks = taskRepository.findAllByGroupAndHandler(job.jobId, reduceHandler)

            when {
                reduceTasks.isEmpty() -> {
                    log.warnf("Job %s in REDUCING without reduce tasks — recovering", job.jobId)
                    dispatchReduceTask(job)
                }
                reduceTasks.all { it.status == TaskStatus.COMPLETED } -> {
                    val transitioned = jobRepository.casJobStatus(
                        job.jobId, JobStatus.REDUCING, JobStatus.COMPLETED, job.version,
                    )
                    if (transitioned) {
                        fireJobStateChanged(job, JobStatus.REDUCING, JobStatus.COMPLETED)
                        log.infof("Job %s completed (%d reduce partitions)", job.jobId, reduceTasks.size)
                        if (job.createdAt != null) {
                            autoscalingMetrics.recordOrchestrationDuration("MapReduce", job.jobType, job.createdAt)
                        }
                    }
                }
                reduceTasks.any { it.status == TaskStatus.DEAD_LETTER } -> {
                    val transitioned = jobRepository.casJobStatus(
                        job.jobId, JobStatus.REDUCING, JobStatus.FAILED, job.version,
                    )
                    if (transitioned) {
                        fireJobStateChanged(job, JobStatus.REDUCING, JobStatus.FAILED)
                        val failed = reduceTasks.count { it.status == TaskStatus.DEAD_LETTER }
                        log.errorf("Job %s failed: %d/%d reduce partition(s) dead-lettered",
                            job.jobId, failed, reduceTasks.size)
                        if (job.createdAt != null) {
                            autoscalingMetrics.recordOrchestrationDuration("MapReduce", job.jobType, job.createdAt)
                        }
                    }
                }
            }
        }
    }

    private fun handleBarrierMet(job: Job, deadLettered: Int) {
        log.infof("Barrier met for job %s: completed=%d, dead_lettered=%d, total=%d",
            job.jobId, job.completedTasks, deadLettered, job.totalTasks)

        // Apply failure policy (stored on the job row at creation time)
        when (job.failurePolicy) {
            FailurePolicy.FAIL_JOB -> {
                if (deadLettered > 0) {
                    failJob(job, "FAIL_JOB: $deadLettered task(s) dead-lettered")
                    return
                }
            }
            FailurePolicy.THRESHOLD -> {
                val failureRate = deadLettered.toDouble() / job.totalTasks
                if (failureRate > job.failureThreshold) {
                    failJob(job, "THRESHOLD: %.1f%% > %.1f%%".format(
                        failureRate * 100, job.failureThreshold * 100))
                    return
                }
            }
            FailurePolicy.BEST_EFFORT -> { /* always proceed */ }
        }

        val transitioned = jobRepository.casJobStatus(
            job.jobId, JobStatus.RUNNING, JobStatus.REDUCING, job.version,
        )
        if (transitioned) {
            fireJobStateChanged(job, JobStatus.RUNNING, JobStatus.REDUCING)
            dispatchReduceTask(job)
        }
    }

    private fun dispatchReduceTask(job: Job) {
        val definition = registrar.getDefinition(job.jobType)
        val maxRetries = definition?.maxRetries ?: 3
        val queue = definition?.queue ?: "mr"
        jobRepository.insertReduceTasks(job.jobId, job.jobType, maxRetries, queue, job.totalPartitions)
        log.infof("Dispatched %d reduce task(s) for job %s", job.totalPartitions, job.jobId)
    }

    private fun fireJobStateChanged(job: Job, previousStatus: JobStatus, newStatus: JobStatus) {
        try {
            jobStateEvent.fireAsync(JobStateChanged(
                jobId = job.jobId,
                jobType = job.jobType,
                previousStatus = previousStatus,
                newStatus = newStatus,
                completedTasks = job.completedTasks,
                failedTasks = job.failedTasks,
                totalTasks = job.totalTasks,
            ))
        } catch (e: Exception) {
            log.warnf(e, "Failed to fire JobStateChanged event for job %s", job.jobId)
        }
    }

    private fun failJob(job: Job, reason: String) {
        val transitioned = jobRepository.casJobStatus(
            job.jobId, JobStatus.RUNNING, JobStatus.FAILED, job.version,
        )
        if (transitioned) {
            fireJobStateChanged(job, JobStatus.RUNNING, JobStatus.FAILED)
            log.warnf("Job %s failed: %s", job.jobId, reason)
            if (job.createdAt != null) {
                autoscalingMetrics.recordOrchestrationDuration("MapReduce", job.jobType, job.createdAt)
            }
        }
    }
}
