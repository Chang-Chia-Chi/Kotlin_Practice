package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderElection
import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.model.Job
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import io.quarkus.runtime.ShutdownEvent
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

/**
 * Leader-only monitoring loop for map-reduce jobs.
 *
 * Responsibilities:
 * - Detect barriers (all map tasks resolved)
 * - Apply failure policies
 * - Dispatch reduce tasks
 * - Monitor reduce completion
 * - Recover from leader failover
 */
@ApplicationScoped
class MapReduceOrchestrator(
    private val config: FrameworkConfig,
    private val jobRepository: JobRepository,
    private val taskRepository: TaskRepository,
    private val registrar: MapReduceRegistrar,
    private val leaderElection: LeaderElection,
) {

    private val log = Logger.getLogger(MapReduceOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun onStart(@Observes ev: StartupEvent) {
        val interval = config.leader().monitorInterval().toMillis()
        scope.launch {
            delay(interval) // initial delay
            while (isActive) {
                if (leaderElection.isLeader) {
                    try {
                        withContext(Dispatchers.IO) { monitorJobs() }
                    } catch (e: Exception) {
                        log.errorf(e, "Error in MR orchestrator loop")
                    }
                }
                delay(interval)
            }
        }
    }

    fun onStop(@Observes ev: ShutdownEvent) {
        scope.cancel()
    }

    private fun monitorJobs() {
        monitorRunningJobs()
        monitorReducingJobs()
    }

    /**
     * For RUNNING jobs: count completed + dead-lettered tasks from the task table.
     * When barrier is met (completed + dead_lettered >= total), apply failure policy
     * and either fail the job or dispatch the reduce task.
     */
    private fun monitorRunningJobs() {
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
    }

    /**
     * For REDUCING jobs: check the reduce task's status.
     * - No reduce task → recovery: enqueue it
     * - Reduce COMPLETED → transition to COMPLETED
     * - Reduce DEAD_LETTER → transition to FAILED
     */
    private fun monitorReducingJobs() {
        val reducingJobs = jobRepository.findJobsByStatus(JobStatus.REDUCING)
        for (job in reducingJobs) {
            val reduceHandler = "${job.jobType}.reduce"
            val reduceTask = taskRepository.findByGroupAndHandler(job.jobId, reduceHandler)

            when {
                reduceTask == null -> {
                    log.warnf("Job %s in REDUCING without reduce task — recovering", job.jobId)
                    dispatchReduceTask(job)
                }
                reduceTask.status == TaskStatus.COMPLETED -> {
                    val transitioned = jobRepository.casJobStatus(
                        job.jobId, JobStatus.REDUCING, JobStatus.COMPLETED,
                        job.version, leaderElection.fenceToken
                    )
                    if (transitioned) log.infof("Job %s completed", job.jobId)
                }
                reduceTask.status == TaskStatus.DEAD_LETTER -> {
                    val transitioned = jobRepository.casJobStatus(
                        job.jobId, JobStatus.REDUCING, JobStatus.FAILED,
                        job.version, leaderElection.fenceToken
                    )
                    if (transitioned) log.errorf("Job %s failed: reduce task dead-lettered", job.jobId)
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
            job.jobId, JobStatus.RUNNING, JobStatus.REDUCING,
            job.version, leaderElection.fenceToken
        )
        if (transitioned) {
            dispatchReduceTask(job)
        }
    }

    private fun dispatchReduceTask(job: Job) {
        val definition = registrar.getDefinition(job.jobType)
        val maxRetries = definition?.maxRetries ?: 3
        val queue = definition?.queue ?: "mr"
        jobRepository.insertReduceTask(job.jobId, job.jobType, maxRetries, queue)
        log.infof("Dispatched reduce task for job %s", job.jobId)
    }

    private fun failJob(job: Job, reason: String) {
        val transitioned = jobRepository.casJobStatus(
            job.jobId, JobStatus.RUNNING, JobStatus.FAILED,
            job.version, leaderElection.fenceToken
        )
        if (transitioned) log.warnf("Job %s failed: %s", job.jobId, reason)
    }
}
