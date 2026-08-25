package infra.snapshotcache.api

import java.sql.Connection
import java.time.Duration
import java.time.Instant

/** Identifies a set of tables that must stay mutually consistent (spec 2, 3.2). */
@JvmInline
value class GroupId(val value: String) {
    override fun toString(): String = value
}

/**
 * Consumer-facing surface of the snapshot cache (spec 5.1).
 *
 * Every method hands out data from a single, internally consistent generation.
 * Prefer [withSnapshot] and [copyOut]: they own the lease lifecycle, so a caller
 * cannot leak a lease by forgetting to close a handle.
 */
interface SnapshotCache {

    /** Fallback for the `waitBudget` parameters below; comes from [SnapshotCacheConfig.defaultWaitBudget]. */
    val defaultWaitBudget: Duration

    /**
     * Runs [block] against the current generation, releasing the lease on every exit path.
     * The preferred entry point (spec 5.1, D9).
     *
     * @throws NotReadyException nothing published yet and [waitBudget] elapsed (or was zero).
     * @throws ShuttingDownException shutdown has begun (spec 10.2 step 1).
     */
    fun <T> withSnapshot(
        group: GroupId,
        waitBudget: Duration = defaultWaitBudget,
        block: (Snapshot) -> T,
    ): T

    /**
     * Copies a subset out into the caller's own connection, then releases the lease immediately.
     *
     * Successive calls may observe different generations, so the returned
     * `(generation, dataAsOf)` must be recorded as lineage (spec 6.4).
     */
    fun copyOut(
        group: GroupId,
        spec: CopyOutSpec,
        waitBudget: Duration = defaultWaitBudget,
    ): CopyOutResult

    /**
     * Long lease; the caller owns [Snapshot.close]. Advanced path - wrap it in try/finally.
     *
     * Reading the current pointer and incrementing its refcount happen in one critical
     * section, so a publish plus reclaim cannot slip in between (spec 5.1 atomicity).
     */
    fun acquire(group: GroupId, waitBudget: Duration = defaultWaitBudget): Snapshot

    /** Current generation status without taking a lease; null before the first publish (spec 5.1, D24). */
    fun currentInfo(group: GroupId): GenerationInfo?
}

/**
 * A lease on one generation. The data it exposes never changes underneath the holder
 * (invariant I8), and the generation is not reclaimed while the lease is open (invariant I2).
 */
interface Snapshot : AutoCloseable {
    val generation: Long
    val dataAsOf: Instant

    /** Read-only connection bound to this generation; writes are rejected (spec 3.1, A3). */
    fun connection(): Connection

    /** Idempotent: repeated calls release the lease once and never drive refcount negative (invariant I6). */
    override fun close()
}

/**
 * No generation is available to hand out.
 *
 * [reason] is [AcquireUnavailableReason.NOT_READY] when the wait budget was zero and
 * [AcquireUnavailableReason.TIMEOUT] when a positive budget expired (spec 9.2, 9.3).
 */
class NotReadyException(
    val group: GroupId,
    val reason: AcquireUnavailableReason,
) : RuntimeException("no generation available for group '$group' (${reason.name.lowercase()})")

/** Shutdown has begun; acquires fail at once and existing waiters are released (spec 10.2 step 1). */
class ShuttingDownException(
    val group: GroupId,
) : RuntimeException("snapshot cache is shutting down; group '$group' is no longer served")
