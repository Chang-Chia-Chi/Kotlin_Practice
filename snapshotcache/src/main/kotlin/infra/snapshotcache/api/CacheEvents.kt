package infra.snapshotcache.api

import java.time.Duration

/**
 * Outcome of one refresh round. The constants are the `result` label values of
 * `snapshot_refresh_total` (spec 12.2); `name.lowercase()` is the label verbatim.
 */
enum class RefreshResult {
    SUCCESS,
    VERIFY_FAILED,
    SOURCE_ERROR,

    /** Not enough disk space to build the candidate; the round also triggers an emergency GC (spec 9.2). */
    DISK_ERROR,

    /** Shutdown interrupted an in-flight build; the candidate is discarded, never promoted (spec 10.2 step 3, D23). */
    SHUTDOWN_ABORTED,
    SKIPPED_OVERLAP,
    BLOCKED_BY_K,
}

/**
 * Stage of a refresh round. The constants are the `phase` label values of
 * `snapshot_refresh_duration_seconds` (spec 12.2); `name.lowercase()` is the label verbatim.
 */
enum class RefreshPhase { QUERY, FETCH, APPEND, CHECKPOINT, VERIFY, PUBLISH }

/**
 * Why an acquire gave up. The constants are the `reason` label values of
 * `snapshot_acquire_unavailable_total` (spec 12.3); `name.lowercase()` is the label verbatim.
 */
enum class AcquireUnavailableReason { NOT_READY, TIMEOUT, SHUTTING_DOWN }

/**
 * The single sink for discrete occurrences inside the framework (plan 2.3).
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

    /** One stage of a refresh round completed (spec 12.2 - per-phase timing is mandatory). */
    fun refreshPhase(group: GroupId, phase: RefreshPhase, elapsed: Duration) {}

    /** A verify rule rejected a candidate (spec 8.5 - never log "verification failed" alone). */
    fun verifyFailed(group: GroupId, rule: String, detail: String) {}

    /** Consecutive verify failures reached the configured threshold (spec 8.5, D15). */
    fun verifyFailureEscalated(group: GroupId, consecutiveFailures: Int) {}

    /** A lease was released normally; feeds `snapshot_lease_duration_seconds`. */
    fun leaseReleased(group: GroupId, lease: LeaseInfo, heldFor: Duration) {}

    /** A lease passed its diagnostic deadline and is still open (spec 6.2). */
    fun leaseExpired(group: GroupId, lease: LeaseInfo, heldFor: Duration) {}

    /** A lease was force-released by the cleaner because its handle was collected. Always a bug (spec 6.3). */
    fun leaseOrphaned(group: GroupId, lease: LeaseInfo) {}

    /** An acquire waited before a generation became available; empty in steady state (spec 9.3). */
    fun acquireWaited(group: GroupId, waited: Duration) {}

    /** An acquire gave up (spec 9.2). */
    fun acquireUnavailable(group: GroupId, reason: AcquireUnavailableReason) {}

    /** A generation was detached and its file deleted (spec 4.2 GC). */
    fun generationReclaimed(group: GroupId, generation: Long) {}
}

/** Default sink: discards everything. Shipped so callers that want no metrics wire nothing. */
object NoOpCacheEvents : CacheEvents
