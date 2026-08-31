package infra.snapshotcache.api

/** Operations surface: manual trigger, manual reclaim, and full live state. */
interface CacheAdmin {

    /** Runs one refresh round now. Returns [RefreshResult.SKIPPED_OVERLAP] if a round is already running. */
    fun triggerRefresh(group: GroupId): RefreshOutcome

    /** Runs one reclaim pass now over non-current generations with refcount zero. */
    fun gc(group: GroupId): GcOutcome

    /** Every generation the framework still holds, with its leases. */
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
 * for example a DETACH that failed because a connection is still in use.
 */
data class GcOutcome(
    val reclaimed: List<Long> = emptyList(),
    val deferred: List<Long> = emptyList(),
)
