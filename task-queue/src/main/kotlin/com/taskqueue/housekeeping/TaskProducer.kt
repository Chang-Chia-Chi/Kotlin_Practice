package com.taskqueue.housekeeping

import com.taskqueue.election.LeaderElectionService
import com.taskqueue.queue.TaskQueueDao
import jakarta.inject.Singleton
import org.jboss.logging.Logger
import java.time.Instant

/**
 * A single root-task production request.
 *
 * Implementations return a list of these from [TaskProducerJob.produce].
 * The [TaskProducer] inserts them into TASK_QUEUE as root tasks (PARENT_TASK_ID = NULL).
 *
 * Set [uniqueKey] to enable deduplication — if a non-terminal task with the same key
 * already exists, the insert is silently skipped.
 */
data class RootTaskRequest(
    val taskType: String,
    val payload: String? = null,
    val priority: Int = 5,
    val deadlineAt: Instant? = null,
    val uniqueKey: String? = null,
)

/**
 * Implement this interface to define which root tasks should be produced on a schedule.
 *
 * Each implementation is a CDI bean discovered at startup. The [TaskProducer] calls
 * all registered jobs on its cron schedule (leader-only).
 *
 * Separation of concerns: the *what* (this interface) is decoupled from the *when*
 * (the cron schedule in [TaskProducer]) and the *how* (INSERT logic in [TaskQueueDao]).
 */
interface TaskProducerJob {
    /** Human-readable name for logging. */
    val name: String

    /** Produce zero or more root tasks. Called on each cron tick. */
    fun produce(): List<RootTaskRequest>
}

/**
 * Leader-only task producer. Iterates all [TaskProducerJob] beans and inserts
 * their root tasks into the queue.
 *
 * Not @Scheduled itself — called from [LeaderCronJobs] or a custom cron.
 * This allows the production schedule to be configured externally.
 */
@Singleton
class TaskProducer(
    private val dao: TaskQueueDao,
    private val leaderElection: LeaderElectionService,
    private val jobs: jakarta.enterprise.inject.Instance<TaskProducerJob>,
) {

    private val log = Logger.getLogger(TaskProducer::class.java)

    /**
     * Run all registered producer jobs and insert their root tasks.
     * Safe to call from a @Scheduled method — guards on leader status.
     *
     * If a [RootTaskRequest] has a [RootTaskRequest.uniqueKey], the insert uses
     * deduplication — duplicate active tasks are silently skipped.
     */
    fun produceAll() {
        if (!leaderElection.isLeader.value) return

        for (job in jobs) {
            try {
                val requests = job.produce()
                for (req in requests) {
                    val taskId = if (req.uniqueKey != null) {
                        dao.insertRootTaskUnique(
                            taskType = req.taskType,
                            payload = req.payload,
                            priority = req.priority,
                            deadlineAt = req.deadlineAt,
                            uniqueKey = req.uniqueKey,
                        )
                    } else {
                        dao.insertRootTask(
                            taskType = req.taskType,
                            payload = req.payload,
                            priority = req.priority,
                            deadlineAt = req.deadlineAt,
                        )
                    }

                    if (taskId != null) {
                        log.debugf("Produced root task %d (type=%s) from job '%s'", taskId, req.taskType, job.name)
                    } else {
                        log.debugf("Duplicate task skipped (type=%s, key=%s) from job '%s'", req.taskType, req.uniqueKey, job.name)
                    }
                }
                if (requests.isNotEmpty()) {
                    log.infof("Job '%s' produced %d root task(s)", job.name, requests.size)
                }
            } catch (e: Exception) {
                log.errorf(e, "Producer job '%s' failed — skipping this cycle", job.name)
            }
        }
    }
}
