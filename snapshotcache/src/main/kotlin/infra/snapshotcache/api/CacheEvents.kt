package infra.snapshotcache.api

import java.time.Duration

/**
 * Outcome of one refresh round. The constants are the `result` label values of
 * `snapshot_refresh_total`; `name.lowercase()` is the label verbatim.
 */
enum class RefreshResult {
    SUCCESS,
    VERIFY_FAILED,
    SOURCE_ERROR,

    /** Not enough disk space to build the candidate; the round also triggers an emergency GC. */
    DISK_ERROR,

    /** Shutdown interrupted an in-flight build; the candidate is discarded, never promoted. */
    SHUTDOWN_ABORTED,
    SKIPPED_OVERLAP,
    BLOCKED_BY_K,
}

/**
 * Stage of a refresh round. The constants are the `phase` label values of
 * `snapshot_refresh_duration_seconds`; `name.lowercase()` is the label verbatim.
 */
enum class RefreshPhase { QUERY, FETCH, APPEND, CHECKPOINT, VERIFY, PUBLISH }

/**
 * Why an acquire gave up. The constants are the `reason` label values of
 * `snapshot_acquire_unavailable_total`; `name.lowercase()` is the label verbatim.
 */
enum class AcquireUnavailableReason { NOT_READY, TIMEOUT, SHUTTING_DOWN }

/**
 * The single sink for discrete occurrences inside the framework.
 *
 * Gauges - current generation, dataAsOf, row counts, live generations, active leases,
 * file bytes - are NOT events. Callers poll them from [SnapshotCache.currentInfo] and
 * [CacheAdmin.liveGenerations]. Every method defaults to doing nothing, so an
 * implementation overrides only what it reports.
 *
 * Implementations must not block: they run on the refresh thread and inside consumer
 * acquire/release paths.
 */
interface CacheEvents {

    /** A refresh round ended. [generation] is set only when [result] is [RefreshResult.SUCCESS]. */
    fun refreshFinished(group: GroupId, result: RefreshResult, generation: Long?) {}

    /** One stage of a refresh round completed - timing every phase separately is mandatory. */
    fun refreshPhase(group: GroupId, phase: RefreshPhase, elapsed: Duration) {}

    /** A verify rule rejected a candidate - never report "verification failed" alone. */
    fun verifyFailed(group: GroupId, rule: String, detail: String) {}

    /** Consecutive verify failures reached the configured threshold. */
    fun verifyFailureEscalated(group: GroupId, consecutiveFailures: Int) {}

    /** A lease was released normally; feeds `snapshot_lease_duration_seconds`. */
    fun leaseReleased(group: GroupId, lease: LeaseInfo, heldFor: Duration) {}

    /** A lease passed its diagnostic deadline and is still open. */
    fun leaseExpired(group: GroupId, lease: LeaseInfo, heldFor: Duration) {}

    /** A lease was force-released by the cleaner because its handle was collected. Always a bug. */
    fun leaseOrphaned(group: GroupId, lease: LeaseInfo) {}

    /** An acquire waited before a generation became available; empty in steady state. */
    fun acquireWaited(group: GroupId, waited: Duration) {}

    /** An acquire gave up. */
    fun acquireUnavailable(group: GroupId, reason: AcquireUnavailableReason) {}

    /** A generation was detached and its file deleted. */
    fun generationReclaimed(group: GroupId, generation: Long) {}
}

/** Default sink: discards everything. Shipped so callers that want no metrics wire nothing. */
object NoOpCacheEvents : CacheEvents
