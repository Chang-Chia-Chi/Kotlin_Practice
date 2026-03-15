package com.mapreduce.deadletter

import com.mapreduce.deadletter.api.dto.BulkReplayFilter
import com.mapreduce.deadletter.api.dto.DeadLetterDetail
import com.mapreduce.deadletter.api.dto.DeadLetterListItem
import com.mapreduce.deadletter.api.dto.DeadLetterSummaryResponse
import com.mapreduce.deadletter.api.dto.ErrorPatternDto
import com.mapreduce.deadletter.api.dto.GroupSummaryDto
import com.mapreduce.deadletter.api.dto.HandlerSummaryDto
import com.mapreduce.deadletter.repository.DeadLetterRepository
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.time.Instant

/**
 * Business logic for dead-letter inspection and replay.
 *
 * Stateless — all pods serve these operations. No leader requirement.
 */
@ApplicationScoped
class DeadLetterService(private val repository: DeadLetterRepository) {

    private val log = Logger.getLogger(DeadLetterService::class.java)

    // ── Inspection ────────────────────────────────────────────────

    fun list(
        handler: String?,
        groupId: String?,
        since: Instant?,
        until: Instant?,
        errorPattern: String?,
        limit: Int,
        offset: Int,
    ): List<DeadLetterListItem> {
        val clamped = limit.coerceIn(1, 200)
        return repository.findDeadLetters(handler, groupId, since, until, errorPattern, clamped, offset)
            .map {
                DeadLetterListItem(
                    taskId = it.taskId,
                    handler = it.handler,
                    queue = it.queue,
                    groupId = it.groupId,
                    retryCount = it.retryCount,
                    errorMessage = it.errorMessage,
                    createdAt = it.createdAt,
                    metadata = it.metadata,
                )
            }
    }

    fun getDetail(taskId: String): DeadLetterDetail? {
        val task = repository.findDeadLetterById(taskId) ?: return null
        return DeadLetterDetail(
            taskId = task.taskId,
            handler = task.handler,
            queue = task.queue,
            payload = task.payload,
            groupId = task.groupId,
            metadata = task.metadata,
            retryCount = task.retryCount,
            maxRetries = task.maxRetries,
            errorMessage = task.errorMessage,
            createdAt = task.createdAt,
            claimedBy = task.claimedBy,
            claimedAt = task.claimedAt,
        )
    }

    fun summary(since: Instant?): DeadLetterSummaryResponse {
        val byHandler = repository.summaryByHandler(since).map {
            HandlerSummaryDto(it.handler, it.count, it.latestError, it.earliest, it.latest)
        }
        val byGroup = repository.summaryByGroupId(since).map {
            GroupSummaryDto(it.groupId, it.handler, it.count, it.latestError, it.earliest, it.latest)
        }
        val totalCount = byHandler.sumOf { it.count }
        return DeadLetterSummaryResponse(byHandler, byGroup, totalCount)
    }

    fun errorPatterns(handler: String?, since: Instant?): List<ErrorPatternDto> =
        repository.errorPatternGroups(handler, since).map {
            ErrorPatternDto(it.errorPattern, it.count)
        }

    // ── Replay ────────────────────────────────────────────────────

    /**
     * Replay a single task: DEAD_LETTER → PENDING.
     *
     * @return the replayed count (1) or null if task not found / already replayed
     */
    fun replaySingle(taskId: String, maxRetries: Int?, scheduledAt: Instant?): Int? {
        val success = repository.replaySingle(taskId, maxRetries, scheduledAt)
        if (success) {
            log.infof("Replayed dead-lettered task %s", taskId)
            return 1
        }
        return null
    }

    /** Bulk replay by filter. */
    fun replayByFilter(filter: BulkReplayFilter, maxRetries: Int?, scheduledAt: Instant?): Int {
        val count = repository.replayByFilter(
            handler = filter.handler,
            groupId = filter.groupId,
            since = filter.since,
            errorPattern = filter.errorPattern,
            maxRetries = maxRetries,
            scheduledAt = scheduledAt,
        )
        log.infof("Bulk-replayed %d dead-lettered task(s) with filter: %s", count, filter)
        return count
    }

    /**
     * Replay all dead-lettered tasks for a map-reduce job and resurrect the job (§4.4).
     *
     * @return replayed count, or -1 if the job is COMPLETED (rejected without force)
     */
    fun replayJob(jobId: String, force: Boolean = false): Int {
        val result = repository.replayJob(jobId, force)
        when {
            result == -1 -> log.warnf("Replay rejected for COMPLETED job %s (force=%s)", jobId, force)
            result == 0 -> log.infof("No dead-lettered tasks to replay for job %s", jobId)
            else -> log.infof("Replayed %d dead-lettered task(s) for job %s", result, jobId)
        }
        return result
    }
}
