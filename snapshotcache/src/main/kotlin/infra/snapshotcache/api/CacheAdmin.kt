package infra.snapshotcache.api

/** Operations surface: manual trigger, manual reclaim, and full live state (spec 5.3, 12.7). */
interface CacheAdmin {

    /** Runs one refresh round now. Returns [RefreshResult.SKIPPED_OVERLAP] if a round is already running (spec 4.4). */
    fun triggerRefresh(group: GroupId): RefreshOutcome

    /** Runs one reclaim pass now over non-current generations with refcount zero (spec 4.2 GC). */
    fun gc(group: GroupId): GcOutcome

    /** Every generation the framework still holds, with its leases (spec 12.7). */
    fun liveGenerations(group: GroupId): List<GenerationState>
}

/** What one refresh round did. [detail] carries the failing rule or error message, if any. */
data class RefreshOutcome(
    val result: RefreshResult,
    val generation: Long? = null,
    val detail: String? = null,
)

/**
 * What one reclaim pass did. [deferred] holds generations that could not be released yet -
 * for example a DETACH that failed because a connection is still in use (spec 9.2).
 */
data class GcOutcome(
    val reclaimed: List<Long> = emptyList(),
    val deferred: List<Long> = emptyList(),
)
