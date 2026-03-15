package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.mr.model.Job
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant

/**
 * Straggler mitigation via speculative execution.
 *
 * In large clusters, tasks rarely fail outright — they hang due to hardware
 * degradation, noisy neighbors, or GC pauses. This component detects stragglers
 * and proactively enqueues duplicate tasks.
 *
 * **Detection:** Calculates the median execution time of completed map tasks.
 * If an active task exceeds a configurable multiple of the median, a speculative
 * duplicate is enqueued.
 *
 * **Resolution:** Both the slow worker and the speculative worker race.
 * Zombie worker fencing (execution_generation) ensures the first to write wins.
 * The latecomer's write is rejected.
 */
@ApplicationScoped
class SpeculativeExecutor(
    private val config: FrameworkConfig,
    private val taskRepository: TaskRepository,
    private val registrar: MapReduceRegistrar,
) {
    private val log = Logger.getLogger(SpeculativeExecutor::class.java)

    fun evaluateRunningJobs(jobs: List<Job>) {
        if (!config.speculative().enabled()) return

        for (job in jobs) {
            if (job.status != JobStatus.RUNNING) continue
            evaluateJob(job)
        }
    }

    private fun evaluateJob(job: Job) {
        val mapHandler = "${job.jobType}.map"
        val completedTasks = taskRepository.findCompletedByGroupAndHandler(
            job.jobId, mapHandler
        )

        if (completedTasks.size < config.speculative().minCompleted()) return

        val durations = completedTasks.mapNotNull { task ->
            val claimed = task.claimedAt ?: return@mapNotNull null
            val completed = task.completedAt ?: return@mapNotNull null
            Duration.between(claimed, completed).toMillis()
        }.sorted()

        if (durations.isEmpty()) return

        val median = durations[durations.size / 2]
        val threshold = (median * config.speculative().medianMultiplier()).toLong()
        val now = Instant.now()

        val claimedTasks = taskRepository.findClaimedByGroupAndHandler(
            job.jobId, mapHandler
        )

        for (task in claimedTasks) {
            if (task.speculative == 1) continue

            val claimedAt = task.claimedAt ?: continue
            val elapsed = Duration.between(claimedAt, now).toMillis()

            if (elapsed > threshold) {
                val definition = registrar.getDefinition(job.jobType) ?: continue

                log.warnf(
                    "SPECULATIVE: task %s for job %s has been running %dms (median=%dms, threshold=%dms) — enqueuing duplicate",
                    task.taskId, job.jobId, elapsed, median, threshold
                )

                taskRepository.enqueue(
                    EnqueueRequest(
                        handler = mapHandler,
                        payload = task.payload,
                        queue = task.queue,
                        maxRetries = task.maxRetries,
                        priority = task.priority,
                        groupId = task.groupId,
                        metadata = task.metadata,
                    )
                )

                taskRepository.markSpeculative(task.taskId)
            }
        }
    }
}
