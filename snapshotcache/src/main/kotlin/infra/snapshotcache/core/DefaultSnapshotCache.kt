package infra.snapshotcache.core

import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.CacheAdmin
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.CopyOutResult
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.api.GcOutcome
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GenerationState
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.NoOpCacheEvents
import infra.snapshotcache.api.NotReadyException
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.ShuttingDownException
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCache
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.spi.GenerationStore
import infra.snapshotcache.spi.SnapshotHandle
import org.jboss.logging.Logger
import java.time.Clock
import java.time.Duration

private val log = Logger.getLogger(DefaultSnapshotCache::class.java)

/** Everything the facade needs to serve one group (plan 2.4: a map and a per-group loop). */
internal class GroupRuntime(
    val registry: GenerationRegistry,
    val store: GenerationStore,
    /** Null only in wirings that never refresh (registry-only tests); the admin surface requires it. */
    val cycle: RefreshCycle? = null,
)

/**
 * Consumer-facing surface over [GenerationRegistry] (spec 5.1). One class implements both
 * interfaces on purpose (plan 2.3): callers that should not see the admin surface are
 * handed the [SnapshotCache] type, not a second object.
 *
 * The facade owns no mutable state and holds no lock: atomicity lives in the registry
 * (spec 5.1), and events fire from here, always outside the registry lock. It also holds
 * no schedule state (D24) - `waitBudget` is per call and [currentInfo] is the caller's
 * freshness seam.
 */
internal class DefaultSnapshotCache(
    config: SnapshotCacheConfig,
    private val groups: Map<GroupId, GroupRuntime>,
    private val events: CacheEvents = NoOpCacheEvents,
    private val clock: Clock = Clock.systemUTC(),
) : SnapshotCache, CacheAdmin {

    override val defaultWaitBudget: Duration = config.defaultWaitBudget
    private val leaseDrainTimeout: Duration = config.leaseDrainTimeout

    override fun <T> withSnapshot(group: GroupId, waitBudget: Duration, block: (Snapshot) -> T): T {
        val snapshot = acquire(group, waitBudget)
        try {
            return block(snapshot)
        } finally {
            snapshot.close()
        }
    }

    override fun copyOut(group: GroupId, spec: CopyOutSpec, waitBudget: Duration): CopyOutResult {
        val runtime = runtimeOf(group)
        val lease = acquireLease(runtime, group, waitBudget)
        try {
            val rows = runtime.store.copyOut(lease.opened, spec)
            return CopyOutResult(lease.generation, lease.generationInfo.dataAsOf, rows)
        } finally {
            release(runtime, group, lease, orphaned = false)
        }
    }

    override fun acquire(group: GroupId, waitBudget: Duration): Snapshot {
        val runtime = runtimeOf(group)
        val lease = acquireLease(runtime, group, waitBudget)
        try {
            // Constructed at the spi boundary, held here only as api.Snapshot (D28).
            return SnapshotHandle(lease.opened, lease.generationInfo.dataAsOf) { orphaned ->
                release(runtime, group, lease, orphaned)
            }
        } catch (failure: Throwable) {
            runtime.registry.release(lease)
            throw failure
        }
    }

    override fun currentInfo(group: GroupId): GenerationInfo? = runtimeOf(group).registry.currentInfo()

    override fun triggerRefresh(group: GroupId): RefreshOutcome = cycleOf(group).runOnce()

    override fun gc(group: GroupId): GcOutcome = cycleOf(group).reclaimPass()

    override fun liveGenerations(group: GroupId): List<GenerationState> = runtimeOf(group).registry.liveGenerations()

    /**
     * Spec 10.2 steps 1 + 4. Step 1: every group is marked shutting down first, so new
     * acquires are refused everywhere and all budget-waiters release at once. Step 4:
     * leases drain under ONE total [leaseDrainTimeout] deadline across all groups
     * (nanoTime-based like the waits themselves, since an injected [Clock] cannot drive
     * `awaitNanos`). Every lease still outstanding at the deadline is WARN-logged with
     * owner and hold duration - the only way to identify what is delaying shutdown - and
     * returned. Logging runs outside the registry lock (plan 2.5). Steps 2 (stop
     * scheduling) and 3 (interrupting an in-flight build) are P9 wiring. Idempotent: a
     * repeated call re-checks and returns the current outstanding list without error.
     */
    fun shutdown(): List<LeaseInfo> {
        groups.values.forEach { it.registry.beginShutdown() }
        val deadline = System.nanoTime() + leaseDrainTimeout.toNanos()
        val outstanding = groups.flatMap { (group, runtime) ->
            runtime.registry.awaitQuiescence(Duration.ofNanos(deadline - System.nanoTime()))
                .map { lease -> group to lease }
        }
        val now = clock.instant()
        for ((group, lease) in outstanding) {
            log.warnf(
                "Shutdown lease drain timed out with a lease still outstanding. " +
                    "group=%s owner=%s heldFor=%s (spec 10.2 step 4).",
                group,
                lease.owner,
                Duration.between(lease.acquiredAt, now),
            )
        }
        return outstanding.map { it.second }
    }

    // ---- internals ----

    private fun runtimeOf(group: GroupId): GroupRuntime =
        requireNotNull(groups[group]) { "unknown group '$group'" }

    private fun cycleOf(group: GroupId): RefreshCycle =
        checkNotNull(runtimeOf(group).cycle) { "group '$group' was wired without a RefreshCycle" }

    /**
     * The `waitBudget` path (spec 5.1, 9.3, D21/D22). Shutdown is checked first; a zero
     * budget never enters a wait; a positive budget waits on the registry condition -
     * signalled by publish and shutdown, never polled - and a shutdown signal during the
     * wait wins over the remaining budget (spec 10.2 step 1).
     */
    private fun acquireLease(runtime: GroupRuntime, group: GroupId, waitBudget: Duration): RegistryLease {
        val registry = runtime.registry
        val owner = Thread.currentThread().name
        // tryAcquire refuses under shutdown from inside the registry lock, so the refusal
        // below cannot be raced past by a caller preempted between the two calls.
        registry.tryAcquire(owner)?.let { return it }
        if (registry.isShuttingDown()) refuseShuttingDown(group)
        if (waitBudget <= Duration.ZERO) refuseUnavailable(group, AcquireUnavailableReason.NOT_READY)
        val waitedFrom = clock.instant()
        val available = try {
            registry.awaitCurrent(waitBudget)
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
            if (registry.isShuttingDown()) refuseShuttingDown(group)
            refuseUnavailable(group, AcquireUnavailableReason.TIMEOUT)
        }
        if (registry.isShuttingDown()) refuseShuttingDown(group)
        if (available) {
            registry.tryAcquire(owner)?.let { lease ->
                emit(group) { events.acquireWaited(group, Duration.between(waitedFrom, clock.instant())) }
                return lease
            }
        }
        refuseUnavailable(group, AcquireUnavailableReason.TIMEOUT)
    }

    /**
     * Single release path for copyOut and the handle callback. The registry makes the
     * refcount decrement idempotent (I6); the handle's cleanup runs at most once, so each
     * lease produces exactly one leaseReleased or leaseOrphaned event, never both.
     */
    private fun release(runtime: GroupRuntime, group: GroupId, lease: RegistryLease, orphaned: Boolean) {
        runtime.registry.release(lease)
        val heldFor = Duration.between(lease.info.acquiredAt, clock.instant())
        if (orphaned) {
            log.warnf(
                "Snapshot lease orphaned - handle garbage-collected without close(); force-released. " +
                    "group=%s generation=%d owner=%s heldFor=%s. This is a consumer bug (spec 6.3).",
                group,
                lease.generation,
                lease.info.owner,
                heldFor,
            )
            emit(group) { events.leaseOrphaned(group, lease.info) }
        } else {
            emit(group) { events.leaseReleased(group, lease.info, heldFor) }
        }
    }

    private fun refuseShuttingDown(group: GroupId): Nothing {
        emit(group) { events.acquireUnavailable(group, AcquireUnavailableReason.SHUTTING_DOWN) }
        throw ShuttingDownException(group)
    }

    private fun refuseUnavailable(group: GroupId, reason: AcquireUnavailableReason): Nothing {
        emit(group) { events.acquireUnavailable(group, reason) }
        throw NotReadyException(group, reason)
    }
}
