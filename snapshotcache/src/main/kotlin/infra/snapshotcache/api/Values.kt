package infra.snapshotcache.api

import java.sql.Connection
import java.time.Instant

/** What a published generation contains and which point in time it represents (spec 5.1). */
data class GenerationInfo(
    val generation: Long,
    val dataAsOf: Instant,
    val publishedAt: Instant,
    val rowCounts: Map<String, Long>,
)

/**
 * A single SQL statement to run against a generation, writing its result into
 * [targetTable] on [targetConnection] (spec 5.1, 6.5).
 */
data class CopyOutSpec(
    val sql: String,
    val targetTable: String,
    val targetConnection: Connection,
)

/** Result of a copy-out, carrying the lineage the consumer must record (spec 6.4). */
data class CopyOutResult(
    val generation: Long,
    val dataAsOf: Instant,
    val rowsCopied: Long,
)

/** Administrative view of one live generation (spec 5.3, 12.7). */
data class GenerationState(
    val generation: Long,
    val isCurrent: Boolean,
    val refCount: Int,
    val fileBytes: Long,
    val leases: List<LeaseInfo>,
)

/**
 * One outstanding lease. [deadline] is diagnostic only - it is never enforced by
 * force-reclaiming the generation (spec 6.2, D8).
 */
data class LeaseInfo(
    val owner: String,
    val acquiredAt: Instant,
    val deadline: Instant,
)
