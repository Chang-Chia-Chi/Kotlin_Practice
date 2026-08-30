package infra.snapshotcache.bootstrap

import infra.snapshotcache.api.CacheAdmin
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.GenerationSource
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.NoOpCacheEvents
import infra.snapshotcache.api.SnapshotCache
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.core.DefaultSnapshotCache
import infra.snapshotcache.core.GenerationRegistry
import infra.snapshotcache.core.GroupRuntime
import infra.snapshotcache.core.RefreshCycle
import infra.snapshotcache.duckdb.DuckDbGenerationStore
import java.time.Clock
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The composition root (plan 2.2, amended 2026-08-30; spec 5.4).
 *
 * Everything that implements the `api` interfaces is Kotlin `internal`, so without this
 * function a downstream module could hold a [SnapshotCache] and never obtain one. A
 * factory in `api` would have been the obvious repair and is the wrong one: it makes the
 * innermost layer depend on `core` and `duckdb`. This package carries that dependency
 * instead, and nothing depends on this package - both facts are ArchUnit rules.
 *
 * It owns nothing at runtime. It assembles the graph, hands it back, and the returned
 * [ManagedSnapshotCache] is only a close handle over the objects it built.
 *
 * ### One store per group, derived
 *
 * Each group gets its own [DuckDbGenerationStore] rooted at
 * `config.storagePath.resolve(group.value)` - spec 3.1's `/data/cache/<group>/` layout,
 * implemented here for the first time. The path is derived rather than configured because
 * generation numbering restarts at 1 for every group: two groups aimed at one directory
 * would both write `gen_0000000001.db`. Deriving it makes that misconfiguration
 * unrepresentable rather than merely documented.
 *
 * ### Which config fields become real here
 *
 * - [SnapshotCacheConfig.storagePath] - the per-group store directory, as above.
 * - [SnapshotCacheConfig.tempDirectory], [SnapshotCacheConfig.servingMemoryLimit],
 *   [SnapshotCacheConfig.servingThreads] - passed to each store, which applies them to its
 *   serving DuckDB instance (spec 10.1 step 2, D29).
 * - [SnapshotCacheConfig.clearStaleFilesOnStartup] - spec 10.1 step 1, performed below as
 *   `listOnDisk` + `delete` per group. The current pointer is never persisted (D10), so
 *   every file found at startup is unowned.
 * - [SnapshotCacheConfig.maxLiveGenerations], [SnapshotCacheConfig.leaseDeadline],
 *   [SnapshotCacheConfig.defaultWaitBudget], [SnapshotCacheConfig.leaseDrainTimeout],
 *   [SnapshotCacheConfig.verify] - already honored by the registry, the facade and the
 *   verify gate; this is the path that wires them consistently.
 *
 * Two fields stay dormant on purpose, and naming them is the point - a config whose unread
 * fields are unexplained is a manifest that silently diverges from behavior:
 *
 * - [SnapshotCacheConfig.jdbcFetchSize] is the caller's [GenerationSource]'s to apply
 *   (spec 7.2). The framework opens no source connection, so it has nothing to set it on.
 * - [SnapshotCacheConfig.refreshInterval] is the host scheduler's (spec 4.4). The framework
 *   ships no scheduler, and one here would be the async orchestration plan 2.4 forbids.
 * - [SnapshotCacheConfig.consumerMemoryLimit] belongs to the host's second, consumer-side
 *   DuckDB instance (spec 6.5), which the host owns because it is the host's jobs that
 *   pass its connection as `CopyOutSpec.targetConnection`.
 *
 * ### What this seam deliberately does not absorb
 *
 * All of it needs a thread, a schedule or a registry the framework has no business owning:
 *
 * - **Refresh scheduling** (spec 4.4). The host's tick calls [CacheAdmin.triggerRefresh].
 * - **The `expiredLeases()` poll** (spec 6.2, 12.3). A lease still *held* past its deadline
 *   is visible only to something periodic, and the host's tick is the only periodic thing.
 * - **Thread naming.** `LeaseInfo.owner` is the acquiring thread's name; useful attribution
 *   is the host naming its pools.
 * - **Metrics binders** (spec 12). Gauges are polled from [SnapshotCache.currentInfo] and
 *   [CacheAdmin.liveGenerations]; occurrences arrive through the [CacheEvents] passed in.
 * - **Readiness, and the first refresh** (spec 10.1 steps 3-5). Readiness is the host's
 *   health surface.
 * - **Spec 10.2 steps 2 and 3** - stop scheduling, interrupt the in-flight build. [close]
 *   runs steps 1 and 4; the host owns the scheduler and the build thread.
 *
 * @param sources one [GenerationSource] per group; the map's keys are the groups this cache
 *   serves, and a group absent from it is unknown to every method on the returned surfaces.
 * @param checks caller-supplied verify rules, composed after the built-in gate (spec 5.2, 8.1).
 */
fun openSnapshotCache(
    config: SnapshotCacheConfig,
    sources: Map<GroupId, GenerationSource>,
    events: CacheEvents = NoOpCacheEvents,
    checks: List<GenerationCheck> = emptyList(),
    clock: Clock = Clock.systemUTC(),
): ManagedSnapshotCache {
    require(sources.isNotEmpty()) { "at least one group is required; a cache serving no group serves nothing" }
    val stores = mutableListOf<DuckDbGenerationStore>()
    try {
        val runtimes = sources.mapValues { (group, source) ->
            val store = DuckDbGenerationStore(
                directory = config.storagePath.resolve(group.value),
                tempDirectory = config.tempDirectory,
                memoryLimit = config.servingMemoryLimit,
                servingThreads = config.servingThreads,
            )
            stores += store
            // Spec 10.1 step 1, before anything is attached: nothing on disk is owned yet.
            if (config.clearStaleFilesOnStartup) store.listOnDisk().forEach(store::delete)
            val registry = GenerationRegistry(config.maxLiveGenerations, config.leaseDeadline, clock)
            GroupRuntime(
                registry = registry,
                store = store,
                cycle = RefreshCycle(
                    group = group,
                    registry = registry,
                    store = store,
                    source = source,
                    config = config,
                    events = events,
                    checks = checks,
                    clock = clock,
                ),
            )
        }
        return ManagedSnapshotCache(DefaultSnapshotCache(config, runtimes, events, clock), stores.toList())
    } catch (failure: Throwable) {
        // A store opened before the failing one already holds a DuckDB instance and a
        // directory handle; leaving them would leak an instance per failed startup attempt.
        stores.forEach { runCatching { it.close() } }
        throw failure
    }
}

/**
 * What [openSnapshotCache] hands back: the two public surfaces of spec 5.1 and 5.3, plus
 * the [close] the host owes them.
 *
 * A class rather than an interface - plan 2.3's five-interface budget is a budget, and
 * plan 2.4 bans single-implementation interfaces. It is a holder, not a seam.
 */
class ManagedSnapshotCache internal constructor(
    private val delegate: DefaultSnapshotCache,
    private val stores: List<DuckDbGenerationStore>,
) : AutoCloseable {

    /** The consumer surface (spec 5.1). Hand this to jobs; it cannot reach [admin]. */
    val cache: SnapshotCache get() = delegate

    /** The operations surface (spec 5.3): manual trigger, manual reclaim, live state. */
    val admin: CacheAdmin get() = delegate

    private val closed = AtomicBoolean(false)

    /**
     * Spec 10.2 in the order the spec fixes: mark shutting down and release every waiter
     * at once (step 1), drain leases under one [SnapshotCacheConfig.leaseDrainTimeout] and
     * WARN-log whatever is still outstanding (step 4), then close the stores - detaching
     * every generation and releasing the DuckDB instances. Steps 2 and 3 are the host's,
     * and the host runs them before calling this.
     *
     * Idempotent, and the store close runs even if the drain throws: a half-closed cache
     * that still holds file handles is the failure mode that makes the next startup's wipe
     * fail too.
     */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        try {
            delegate.shutdown()
        } finally {
            stores.forEach { runCatching { it.close() } }
        }
    }
}
