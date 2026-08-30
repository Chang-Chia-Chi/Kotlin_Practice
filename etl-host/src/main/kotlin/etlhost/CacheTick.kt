package etlhost

import infra.snapshotcache.api.GenerationState
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.quarkus.scheduler.Scheduled
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Duration
import org.jboss.logging.Logger

/**
 * The host's periodic half of the cache: snapshotcache spec 4.4's refresh schedule, and the two
 * polls spec 5.4 assigns to whatever is periodic - because the framework ships no scheduler and
 * this tick is the only periodic thing in the system.
 *
 * ### The refresh
 *
 * `refresh.interval` is a gap measured from the end of the previous round, not a cron, which is
 * what `@Scheduled(every = ...)` with `SKIP` gives. `SKIP` and the framework's own overlap guard
 * are not redundant: the framework answers `SKIPPED_OVERLAP` and counts it, which would turn a slow
 * round into a stream of counted skips rather than a quiet wait.
 *
 * ### The two polls, and why a poll is needed at all
 *
 * **Expired leases.** The core fires `CacheEvents.leaseExpired` on the *release* path, so it can
 * only ever report leases that have already ended. A lease still **held** past its deadline - the
 * case the metric exists to catch, a job that is stuck right now - is invisible to it. Only
 * something periodic can see that, so plan P9 assigns it here.
 *
 * The plan names `GenerationRegistry.expiredLeases()`, which is `internal` to the cache's core and
 * unreachable from a host; `CacheAdmin.liveGenerations` carries the same leases with the same
 * deadlines, so this computes the identical set from the public surface. See progress.md - the
 * frameworks are read-only for this phase and nothing needed to change.
 *
 * **Pinned generations.** `CacheAdmin.gc()` answers `GcOutcome([], [])` for two different worlds -
 * nothing to reclaim, and a consumer pinning a non-current generation - and the outcome cannot tell
 * them apart. `refCount` on each `GenerationState` can. Unpolled, "a job is holding a generation
 * open" stays invisible until the live count reaches K and spec 6.1 pauses refresh, which is an
 * alert firing one stage after the fact it describes.
 *
 * The tick stops refreshing once shutdown has begun (spec 10.2 step 2, "no new refresh cycle
 * starts"). The `@Scheduled` job outlives the [ShutdownEvent] observer by however long the
 * container takes to stop the scheduler, and a refresh started in that window would race the drain.
 */
@Singleton
class CacheTick @Inject constructor(
    private val host: EtlHost,
    private val managed: ManagedSnapshotCache,
    private val events: CacheMetrics,
    private val clock: Clock,
) {

    @Scheduled(
        every = "{etl-host.cache.refresh-interval}",
        concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
    )
    fun tick() {
        if (host.shuttingDown) return
        host.groups.forEach { group ->
            val outcome = managed.admin.triggerRefresh(group)
            log.debugv("tick: group {0} refresh {1}", group, outcome.result)
            poll(group)
        }
    }

    /**
     * Separate from [tick] and public because it is the part with logic in it. Asserting that
     * Quarkus fires an `@Scheduled` method is asserting something about Quarkus; asserting that a
     * lease held past its deadline produces exactly one `leaseExpired` is asserting something about
     * this host.
     *
     * Returns the pinned generations it warned about, for the same reason `TaskAdmin` factors
     * `sameDatasourcePipeUsers` out of its own logging: a test that had to read a log appender would
     * be testing the logging framework, and one that asserted nothing would let the condition drift
     * unnoticed. The log line is still the operational output; the return value is what is checked.
     */
    fun poll(group: GroupId): List<GenerationState> {
        val now = clock.instant()
        val live = managed.admin.liveGenerations(group)

        live.asSequence()
            .flatMap { it.leases }
            .filter { it.deadline.isBefore(now) }
            .forEach { events.leaseExpired(group, it, Duration.between(it.acquiredAt, now)) }

        val pinned = live.filter { !it.isCurrent && it.refCount > 0 }
        if (pinned.isNotEmpty()) {
            log.warnv(
                "group {0}: {1} non-current generation(s) still pinned - {2}. Refresh pauses at " +
                    "generation.maxLive; these are the holders (spec 6.1, 6.2).",
                group, pinned.size,
                pinned.joinToString { "gen ${it.generation} refCount=${it.refCount} ${owners(it.leases)}" },
            )
        }
        return pinned
    }

    private companion object {
        val log: Logger = Logger.getLogger(CacheTick::class.java)

        /**
         * Reads `LeaseInfo.owner` verbatim. On a coroutine-dispatched host this is
         * `DefaultDispatcher-worker-1 @task-name#5` **only when `-Dkotlinx.coroutines.debug=on` is
         * set**, and a bare worker name otherwise - which is the incident where this line is read
         * and the one place it has to name a task. See application.properties.
         */
        fun owners(leases: List<LeaseInfo>) = leases.joinToString(prefix = "[", postfix = "]") { it.owner }
    }
}
