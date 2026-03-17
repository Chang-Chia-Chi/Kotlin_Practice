package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.FencingTokenHolder
import com.mapreduce.leader.LeaderManager
import com.mapreduce.mr.model.Job
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.mr.model.evaluateFailurePolicy
import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
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
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Leader-only monitoring loop for map-reduce jobs.
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
    private val shutdownCoordinator: ShutdownCoordinator,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(MapReduceOrchestrator::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val queueDepths = ConcurrentHashMap<String, AtomicLong>()

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
        monitorRunningJobs()
        monitorReducingJobs()
        pollQueueDepth()
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

    private fun monitorRunningJobs() {
        val runningJobs = jobRepository.findJobsByStatus(JobStatus.RUNNING)
        for (job in runningJobs) {
            val deadLettered = taskRepository.countByGroupAndStatus(job.jobId, TaskStatus.DEAD_LETTER)

            if (deadLettered != job.failedTasks) {
                jobRepository.updateFailedTasks(job.jobId, deadLettered)
            }

            if (job.completedTasks + deadLettered >= job.totalTasks) {
                handleBarrierMet(job, deadLettered)
            }
        }
    }

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
                        log.infof("Job %s completed (%d reduce partitions)", job.jobId, reduceTasks.size)
                        recordOrchestrationDuration(job)
                    }
                }
                reduceTasks.any { it.status == TaskStatus.DEAD_LETTER } -> {
                    val transitioned = jobRepository.casJobStatus(
                        job.jobId, JobStatus.REDUCING, JobStatus.FAILED, job.version,
                    )
                    if (transitioned) {
                        val failed = reduceTasks.count { it.status == TaskStatus.DEAD_LETTER }
                        log.errorf("Job %s failed: %d/%d reduce partition(s) dead-lettered",
                            job.jobId, failed, reduceTasks.size)
                        recordOrchestrationDuration(job)
                    }
                }
            }
        }
    }

    private fun handleBarrierMet(job: Job, deadLettered: Int) {
        log.infof("Barrier met for job %s: completed=%d, dead_lettered=%d, total=%d",
            job.jobId, job.completedTasks, deadLettered, job.totalTasks)

        val failureReason = evaluateFailurePolicy(
            job.failurePolicy, deadLettered, job.totalTasks, job.failureThreshold,
        )
        if (failureReason != null) {
            failJob(job, failureReason)
            return
        }

        val definition = registrar.getDefinition(job.jobType)
        val maxRetries = definition?.maxRetries ?: 3
        val queue = definition?.queue ?: "mr"

        val transitioned = jobRepository.transitionToReducing(
            job.jobId, job.version, job.jobType, maxRetries, queue, job.totalPartitions,
        )
        if (transitioned) {
            log.infof("Dispatched %d reduce task(s) for job %s", job.totalPartitions, job.jobId)
        }
    }

    /** Recovery-only: insert reduce tasks for a job already in REDUCING state. */
    private fun dispatchReduceTask(job: Job) {
        val definition = registrar.getDefinition(job.jobType)
        val maxRetries = definition?.maxRetries ?: 3
        val queue = definition?.queue ?: "mr"
        jobRepository.insertReduceTasks(job.jobId, job.jobType, maxRetries, queue, job.totalPartitions)
        log.infof("Dispatched %d reduce task(s) for job %s", job.totalPartitions, job.jobId)
    }

    private fun failJob(job: Job, reason: String) {
        val transitioned = jobRepository.casJobStatus(
            job.jobId, JobStatus.RUNNING, JobStatus.FAILED, job.version,
        )
        if (transitioned) {
            log.warnf("Job %s failed: %s", job.jobId, reason)
            recordOrchestrationDuration(job)
        }
    }

    private fun recordOrchestrationDuration(job: Job) {
        if (job.createdAt == null) return
        val duration = Duration.between(job.createdAt, Instant.now())
        Timer.builder("framework.orchestration.duration.seconds")
            .tag("orchestration_type", "MapReduce")
            .tag("identifier", job.jobType)
            .register(meterRegistry)
            .record(duration.toMillis(), TimeUnit.MILLISECONDS)
    }
}
