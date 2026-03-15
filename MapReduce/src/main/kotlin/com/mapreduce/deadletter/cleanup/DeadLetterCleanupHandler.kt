package com.mapreduce.deadletter.cleanup

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.deadletter.DeadLetterMetrics
import com.mapreduce.deadletter.repository.DeadLetterRepository
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Self-referential cleanup: a task handler that deletes old dead-lettered tasks.
 *
 * Registered as handler `"system.dead-letter-cleanup"` and periodically enqueued
 * by the leader. Gets all standard queue guarantees (retry, dead-letter, monitoring).
 *
 * The leader's orchestration loop is responsible for scheduling this task
 * at the configured interval (see [DeadLetterCleanupScheduler]).
 */
@ApplicationScoped
class DeadLetterCleanupHandler(
    private val config: FrameworkConfig,
    private val repository: DeadLetterRepository,
    private val metrics: DeadLetterMetrics,
) : TaskHandler {

    private val log = Logger.getLogger(DeadLetterCleanupHandler::class.java)

    override val handlerName: String = HANDLER_NAME

    override suspend fun handle(ctx: TaskContext): TaskResult {
        val retentionDays = config.deadLetter().retentionDays()
        val cutoff = Instant.now().minus(retentionDays.toLong(), ChronoUnit.DAYS)

        log.infof("Dead-letter cleanup: deleting tasks older than %s (retention=%dd)", cutoff, retentionDays)

        val deleted = repository.deleteOlderThan(cutoff)
        if (deleted > 0) {
            metrics.recordCleaned(deleted)
            log.infof("Dead-letter cleanup: deleted %d task(s)", deleted)
        } else {
            log.info("Dead-letter cleanup: no tasks to delete")
        }

        return TaskResult.Success("""{"deleted":$deleted}""")
    }

    companion object {
        const val HANDLER_NAME = "system.dead-letter-cleanup"
    }
}
