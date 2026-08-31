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
import org.jboss.logging.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.util.concurrent.atomic.AtomicBoolean

private val log = Logger.getLogger("infra.snapshotcache.bootstrap")

/**
 * Every form a generation file takes on disk: promoted, candidate, and the WAL sibling of
 * either. Strictness is the safety of the startup wipe - the pattern is what makes
 * deleting inside a caller-supplied directory tree defensible, so nothing that does not
 * look exactly like a generation file is ever touched.
 */
private val GENERATION_FILE = Regex("gen_\\d{10}\\.db(\\.tmp)?(\\.wal)?")

/**
 * The composition root.
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
 * `config.storagePath.resolve(group.value)` - the `/data/cache/<group>/` layout,
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
 *   serving DuckDB instance.
 * - [SnapshotCacheConfig.clearStaleFilesOnStartup] - the startup wipe, performed below as
 *   `listOnDisk` + `delete` per group. The current pointer is never persisted, so every
 *   file found at startup is unowned.
 * - [SnapshotCacheConfig.maxLiveGenerations], [SnapshotCacheConfig.leaseDeadline],
 *   [SnapshotCacheConfig.defaultWaitBudget], [SnapshotCacheConfig.leaseDrainTimeout],
 *   [SnapshotCacheConfig.verify] - already honored by the registry, the facade and the
 *   verify gate; this is the path that wires them consistently.
 *
 * Four fields stay dormant on purpose, and naming them is the point - a config whose unread
 * fields are unexplained is a manifest that silently diverges from behavior:
 *
 * - [SnapshotCacheConfig.jdbcFetchSize] is the caller's [GenerationSource]'s to apply. The
 *   framework opens no source connection, so it has nothing to set it on.
 * - [SnapshotCacheConfig.refreshInterval] is the host scheduler's. The framework ships no
 *   scheduler, and one here would be the async orchestration this framework does not build.
 * - [SnapshotCacheConfig.consumerMemoryLimit] belongs to the host's second, consumer-side
 *   DuckDB instance, which the host owns because it is the host's jobs that
 *   pass its connection as `CopyOutSpec.targetConnection`. **With SimpleEtl as the consumer
 *   it is inert**: a `cacheCopy` step passes its own per-run scratch instance's write
 *   connection, whose `memory_limit` came from `EtlWiring.scratchMemoryLimitMb` (4096 MB by
 *   default). The field stays because a host that does own one shared consumer instance
 *   reads it; a SimpleEtl host sizes its pod off `scratchMemoryLimitMb` times the number of
 *   tasks whose schedules can overlap, which is the amended pod-budget arithmetic.
 * - [SnapshotCacheConfig.allowOverlap] can never be true: overlapping refresh rounds are
 *   forbidden and the refresh cycle refuses one unconditionally, without consulting the
 *   flag. It is a config row with no reachable `true` branch, not a knob this seam declined.
 *
 * ### What this seam deliberately does not absorb
 *
 * All of it needs a thread, a schedule or a registry the framework has no business owning:
 *
 * - **Refresh scheduling.** The host's tick calls [CacheAdmin.triggerRefresh].
 * - **The `expiredLeases()` poll.** A lease still *held* past its deadline
 *   is visible only to something periodic, and the host's tick is the only periodic thing.
 * - **Reading [CacheAdmin.liveGenerations] on that same tick.** [CacheAdmin.gc] answers
 *   `GcOutcome(reclaimed = [], deferred = [])` both when there was nothing to reclaim and
 *   when a consumer is pinning a non-current generation, and the outcome cannot tell those
 *   apart. `GenerationState.refCount` and its lease list can. Unpolled, "a job is holding a
 *   generation open" stays invisible until the live count reaches K and refresh pauses -
 *   an alert one stage downstream of the fact it is about.
 * - **Lease attribution, which on a coroutine-dispatched host is a JVM flag rather than a
 *   thread name.** `LeaseInfo.owner` is the acquiring thread's name. "Name your pools" is no
 *   remedy for the primary consumer: SimpleEtl runs on `Dispatchers.IO.limitedParallelism(1)`,
 *   a view over the shared IO pool with no `ThreadFactory` to name. What attributes a lease
 *   there is **`-Dkotlinx.coroutines.debug=on`**, which appends the coroutine name and id to
 *   the worker thread's name for each dispatch. Measured on the first composed host: with
 *   assertions on, `DefaultDispatcher-worker-1 @wip-summary#5`; on a production JVM, bare
 *   `DefaultDispatcher-worker-1`. The flag is JVM-global and costs a small per-dispatch
 *   rename.
 *
 *   **No test can catch its absence.** kotlinx-coroutines' debug mode is `AUTO`, which turns
 *   itself on whenever assertions are enabled - and surefire enables them - so every test JVM
 *   shows attribution working while production silently has none. This is a deployment flag to
 *   read off a running pod, not a suite to keep green.
 * - **Metrics binders.** Gauges are polled from [SnapshotCache.currentInfo] and
 *   [CacheAdmin.liveGenerations]; occurrences arrive through the [CacheEvents] passed in.
 * - **Readiness, and the first refresh.** Readiness is the host's health surface.
 * - **Stopping the scheduler and interrupting an in-flight build** - the two shutdown steps
 *   that come before [close]; the host owns the scheduler and the build thread.
 *
 * ### The default verify gate assumes an `id` column, and fails two systems away
 *
 * `VerifyConfig.keyUnique` defaults to true, so the gate runs `COUNT(id), COUNT(DISTINCT id)`
 * over **every** table of a candidate. A group whose source tables have no `id`
 * column therefore fails its very first refresh, and the failure is loud only here: nothing is
 * published, so a consumer sees `NotReadyException` from `acquire`/`copyOut` - in SimpleEtl, a
 * failed `cacheCopy` step - with the actual cause in a different system's verify log. Either
 * give every table in the group an `id` column, or set `verify = VerifyConfig(keyUnique = false)`
 * deliberately. This is the one gate default that turns a schema mismatch into a symptom two
 * systems from its cause, which is why it is named at the surface adopters read.
 *
 * @param sources one [GenerationSource] per group; the map's keys are the groups this cache
 *   serves, and a group absent from it is unknown to every method on the returned surfaces.
 * @param checks caller-supplied verify rules, composed after the built-in gate.
 */
fun openSnapshotCache(
    config: SnapshotCacheConfig,
    sources: Map<GroupId, GenerationSource>,
    events: CacheEvents = NoOpCacheEvents,
    checks: List<GenerationCheck> = emptyList(),
    clock: Clock = Clock.systemUTC(),
): ManagedSnapshotCache {
    require(sources.isNotEmpty()) { "at least one group is required; a cache serving no group serves nothing" }
    // The startup wipe, before any store exists: every gen_* file under the cache
    // directory means the whole tree, not the directories this call happens to name. A group
    // dropped from [sources] between deploys leaves a directory nothing would revisit.
    if (config.clearStaleFilesOnStartup) wipeStaleFiles(config.storagePath)
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
            // With the wipe off, whatever survived is unowned but NOT unreachable: numbering
            // restarts at 1, so promote's ATOMIC_MOVE would silently overwrite a lower
            // leftover, and a higher one would never be reclaimed because no record names
            // it. Starting above the highest file on disk makes both impossible.
            val startAfter = if (config.clearStaleFilesOnStartup) 0L else store.listOnDisk().maxOrNull() ?: 0L
            val registry = GenerationRegistry(
                config.maxLiveGenerations,
                config.leaseDeadline,
                clock,
                startAfterGeneration = startAfter,
            )
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
 * The startup wipe: delete every generation file under [storagePath] - directly in it (the
 * flat layout that predates the per-group directories) and in each first-level
 * subdirectory, whether or not a group by that name is still being served. The current
 * pointer is never persisted, so every one of them is unowned by construction.
 *
 * Membership in `sources` is deliberately not the filter; [GENERATION_FILE] is. A group
 * removed from the config is exactly the case where files rot forever, and it is the case a
 * membership filter would skip. Entries are collected before deleting: on Windows, removing
 * a file while its directory stream is open can fail.
 */
private fun wipeStaleFiles(storagePath: Path) {
    if (!Files.isDirectory(storagePath)) return
    val subdirectories = Files.list(storagePath).use { entries ->
        entries.filter { Files.isDirectory(it) }.toList()
    }
    // plusElement, not `+`: Path is Iterable<Path>, so `list + path` appends the path's NAME
    // ELEMENTS ("Users", "maxch", ...) instead of the path itself.
    for (directory in subdirectories.plusElement(storagePath)) {
        val stale = Files.list(directory).use { entries ->
            entries.filter { GENERATION_FILE.matches(it.fileName.toString()) }.toList()
        }
        stale.forEach { Files.deleteIfExists(it) }
        // Named per directory: the P9 operator round found the wipe correct and invisible - a
        // restart that collected a crashed predecessor's leftovers looked identical to one that
        // found a clean directory, so "did the wipe run?" was unanswerable from the log.
        if (stale.isNotEmpty()) {
            log.infof(
                "startup wipe: removed %d stale generation file(s) left by a previous process from %s: %s",
                stale.size, directory, stale.map { it.fileName.toString() },
            )
        }
    }
}

/**
 * What [openSnapshotCache] hands back: the consumer surface and the operations surface,
 * plus the [close] the host owes them.
 *
 * A class rather than an interface - the framework's five-interface budget is a budget, and
 * single-implementation interfaces are banned. Not a seam - one implementation, no variation - and since the drain guard, not a mere holder either: `close()` owns the one conditional whose wrong answer is a SIGSEGV, not an exception.
 */
class ManagedSnapshotCache internal constructor(
    private val delegate: DefaultSnapshotCache,
    private val stores: List<DuckDbGenerationStore>,
) : AutoCloseable {

    /** The consumer surface. Hand this to jobs; it cannot reach [admin]. */
    val cache: SnapshotCache get() = delegate

    /** The operations surface: manual trigger, manual reclaim, live state. */
    val admin: CacheAdmin get() = delegate

    private val closed = AtomicBoolean(false)

    /**
     * The shutdown sequence, in its fixed order: mark shutting down and release every waiter
     * at once, then drain leases under one [SnapshotCacheConfig.leaseDrainTimeout]. Stopping
     * the scheduler and interrupting the in-flight build come in between and are the host's,
     * and the host runs them before calling this.
     *
     * **The store sweep runs only after a clean drain, and that split is a safety
     * requirement rather than tidiness.** `DuckDbGenerationStore.close()` closes every
     * connection it issued. An outstanding lease means a consumer thread may be mid-query on
     * one of them, and a DuckDB connection touched from two threads crashes the JVM instead
     * of raising - so the sweep would turn a slow consumer into a SIGSEGV, which no
     * `runCatching` can catch. When the drain times out, the leases are WARN-logged by
     * [infra.snapshotcache.core.DefaultSnapshotCache.shutdown] and the files are left to the
     * last shutdown step: the connections die with the process. The next startup's wipe
     * removes what is left, which is what that step relies on.
     *
     * A clean drain proves no consumer holds a connection, so the full close runs and the
     * process keeps no file handles. If the drain itself throws, the sweep is skipped for
     * the same reason - an unknown drain state is not a proven-clean one.
     *
     * **After a dirty drain the outstanding leases stay readable through [admin].**
     * `liveGenerations` still reports the pinned generations and their `LeaseInfo` owners, so
     * the WARN this logs is a summary and not the only record - a host with a shutdown hook can
     * name what is holding it up. Measured on Windows: a dirty drain also leaves the generation
     * files undeletable until the process exits, because the store connections were left open on
     * purpose and Windows will not unlink an open file. That is the same shutdown step working
     * as written, not a leak; the next startup's wipe clears them. On Linux - the deployment
     * target - the same state is benign twice over (measured, WSL2 Ubuntu 22.04, 2026-08-30):
     * unlink succeeds immediately, the open connection keeps its inode, and the disk space
     * frees at last close, byte-exact. The undeletable-file artifact is Windows-only.
     *
     * Idempotent.
     */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        val outstanding = delegate.shutdown()
        if (outstanding.isEmpty()) {
            stores.forEach { runCatching { it.close() } }
        } else {
            log.warnf(
                "Lease drain timed out with %d lease(s) outstanding; leaving the DuckDB stores open " +
                    "rather than closing connections a consumer may still be querying; they die with the process, and the next startup wipe collects the files.",
                outstanding.size,
            )
        }
    }
}
