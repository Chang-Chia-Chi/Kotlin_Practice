package etlhost

import infra.snapshotcache.api.GroupId
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import java.time.Clock
import java.time.Duration
import java.time.ZoneOffset
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * **The poll snapshotcache plan P9 assigns to the host**, and the two things it sees that nothing
 * else can.
 *
 * ### A note on `expiredLeases()`
 *
 * The plan entry names `GenerationRegistry.expiredLeases()`. That method is `internal` to the
 * cache's core and unreachable from a host, and the frameworks are read-only for this phase - so
 * [CacheTick] computes the identical set from `CacheAdmin.liveGenerations`, which carries the same
 * `LeaseInfo` objects with the same deadlines. Nothing was added to the framework and nothing needed
 * to be; see progress.md.
 *
 * ### Why a poll at all
 *
 * The core fires `leaseExpired` on the **release** path, so it reports only leases that have already
 * ended. A lease still *held* past its deadline - a job stuck right now, which is the case the
 * metric exists for - is invisible to it. And `gc()` answers `GcOutcome([], [])` both when there was
 * nothing to reclaim and when a consumer is pinning a generation, so the outcome cannot tell those
 * apart either; `refCount` can.
 */
@QuarkusTest
@WithTestResource(HostFixture::class)
class CachePollTest {

    @Inject
    lateinit var managed: ManagedSnapshotCache

    @Inject
    lateinit var host: EtlHost

    @Inject
    lateinit var events: CacheMetrics

    @Inject
    lateinit var registry: PrometheusMeterRegistry

    private val group = GroupId(HostFixture.GROUP)

    /**
     * A clock past the lease deadline is the whole mechanism: `LeaseInfo.deadline` is stamped when
     * the lease is taken, so "expired" is a question about *now*, and moving now is how a test asks
     * it without waiting five minutes.
     */
    private fun tickAt(offset: Duration) = CacheTick(
        host, managed, events, Clock.offset(Clock.system(ZoneOffset.UTC), offset),
    )

    @Test
    fun `a lease still held past its deadline is reported once, which release-path events never see`() {
        val before = counter("snapshot_lease_expired_total")

        managed.cache.acquire(group).use {
            assertThat(counter("snapshot_lease_expired_total"))
                .withFailMessage("a lease inside its deadline must not be reported")
                .isEqualTo(before)

            tickAt(Duration.ofMinutes(10)).poll(group)

            assertThat(counter("snapshot_lease_expired_total"))
                .withFailMessage("the poll did not see a lease held past its deadline")
                .isEqualTo(before + 1.0)
        }
    }

    @Test
    fun `a non-current generation pinned by a consumer is named while it is still pinned`() {
        val pinnedGeneration = managed.cache.acquire(group).use {
            // A successful refresh publishes a successor, so the generation this lease pins stops
            // being current - and cannot be reclaimed while the lease is open. That is the exact
            // state that piles generations up to K and pauses refresh, one stage later.
            managed.admin.triggerRefresh(group)

            val pinned = tickAt(Duration.ZERO).poll(group)
            assertThat(pinned).hasSize(1)
            assertThat(pinned.single().refCount).isEqualTo(1)
            assertThat(pinned.single().isCurrent).isFalse()
            pinned.single().generation
        }

        // Released: the same poll now says nothing, so the report tracks the condition rather than
        // the history of it.
        assertThat(tickAt(Duration.ZERO).poll(group).map { it.generation }).doesNotContain(pinnedGeneration)
    }

    private fun counter(name: String): Double =
        registry.find(name).tag("group", HostFixture.GROUP).counter()?.count() ?: 0.0
}
