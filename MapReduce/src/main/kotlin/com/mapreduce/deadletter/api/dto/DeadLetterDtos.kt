package com.mapreduce.deadletter.api.dto

import java.time.Instant

/** Response for list endpoint — payload excluded for performance. */
data class DeadLetterListItem(
    val taskId: String,
    val handler: String,
    val queue: String,
    val groupId: String?,
    val retryCount: Int,
    val errorMessage: String?,
    val createdAt: Instant?,
    val metadata: String?,
)

/** Response for single task detail — includes full payload. */
data class DeadLetterDetail(
    val taskId: String,
    val handler: String,
    val queue: String,
    val payload: String,
    val groupId: String?,
    val metadata: String?,
    val retryCount: Int,
    val maxRetries: Int,
    val errorMessage: String?,
    val createdAt: Instant?,
    val claimedBy: String?,
    val claimedAt: Instant?,
)

/** Summary response — counts by handler and by group. */
data class DeadLetterSummaryResponse(
    val byHandler: List<HandlerSummaryDto>,
    val byGroup: List<GroupSummaryDto>,
    val totalCount: Int,
)

data class HandlerSummaryDto(
    val handler: String,
    val count: Int,
    val latestError: String?,
    val earliest: Instant?,
    val latest: Instant?,
)

data class GroupSummaryDto(
    val groupId: String,
    val handler: String,
    val count: Int,
    val latestError: String?,
    val earliest: Instant?,
    val latest: Instant?,
)

/** Error pattern grouping response. */
data class ErrorPatternDto(
    val errorPattern: String,
    val count: Int,
)

/** Request body for single replay. */
data class ReplaySingleRequest(
    val maxRetries: Int? = null,
    val scheduledAt: Instant? = null,
)

/** Request body for bulk replay by filter. */
data class BulkReplayRequest(
    val filter: BulkReplayFilter,
    val maxRetries: Int? = null,
    val scheduledAt: Instant? = null,
)

data class BulkReplayFilter(
    val handler: String? = null,
    val groupId: String? = null,
    val since: Instant? = null,
    val errorPattern: String? = null,
)

/** Response for replay operations. */
data class ReplayResponse(
    val replayed: Int,
    val message: String,
)
