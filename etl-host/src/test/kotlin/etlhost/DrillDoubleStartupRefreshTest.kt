package etlhost

import io.micrometer.prometheus.PrometheusMeterRegistry
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * **Boot must run the first refresh exactly once**, and for eight phases it ran it twice.
 *
 * Quarkus fires an `@Scheduled(every = ...)` trigger for the first time when the scheduler starts,
 * not one interval later, so `CacheTick` raced [EtlHost.onStart]'s spec 10.1 step 4 refresh on
 * every boot - while [EtlHost]'s KDoc said "the tick's first firing is one whole interval away"
 * and [HostFixture] was built on the same belief ("long enough that the tick never fires inside a
 * test"). `delayed` is the knob that makes both true.
 *
 * The pair below is the evidence and stays as the guard. Same fixture, same 30-minute interval,
 * one difference: the scheduler's start mode. Both must now count one round - the forced case
 * because the fix holds, the halted case because it is the control that would catch a second
 * refresher appearing from somewhere other than the tick.
 */
class HaltedTick : QuarkusTestProfile {
    override fun getConfigOverrides() = mapOf(
        "quarkus.scheduler.start-mode" to "halted",
        // Distinguishes this instance from CronDoesNotFireWhenTheSchedulerIsHaltedTest's, which
        // shares the halted start mode and would otherwise reuse its already-booted host.
        "etl-host.cache.serving-memory-limit" to "384MB",
    )
}

@QuarkusTest
@WithTestResource(HostFixture::class)
@TestProfile(HaltedTick::class)
class DrillSingleRefreshWhenSchedulerHaltedTest {

    @Inject
    lateinit var registry: PrometheusMeterRegistry

    @Test
    fun `with the scheduler halted the host refreshes exactly once at boot`() {
        assertThat(successfulRefreshes(registry))
            .withFailMessage("the control is wrong: something other than the tick is refreshing")
            .isEqualTo(1.0)
    }
}

@QuarkusTest
@WithTestResource(HostFixture::class)
class DrillDoubleStartupRefreshTest {

    @Inject
    lateinit var registry: PrometheusMeterRegistry

    /**
     * The regression guard. Before `delayed` was added to [CacheTick] this read 2.0 with the
     * scheduler in the mode production runs it in, and the two rounds raced: whichever lost, lost
     * differently. The startup refresh losing is the expensive one - it comes back
     * `SKIPPED_OVERLAP`, which is not `SUCCESS`, and the host used to derive readiness from that.
     *
     * Against the Oracle this host is built for a refresh is spec 10.1's "minutes", so the removed
     * cost is a doubled startup and a generation that is garbage the moment it publishes.
     */
    @Test
    fun `with the scheduler forced the host still refreshes exactly once at boot`() {
        assertThat(successfulRefreshes(registry))
            .withFailMessage(
                "boot ran more than one refresh round with refresh-interval = PT30M, so CacheTick's " +
                    "first firing is racing EtlHost.onStart again - which is what `delayed` exists " +
                    "to prevent. See CacheTick's KDoc for both failure modes.",
            )
            .isEqualTo(1.0)
    }
}

private fun successfulRefreshes(registry: PrometheusMeterRegistry): Double =
    registry.scrape().lines()
        .first { it.startsWith("""snapshot_refresh_total{group="${HostFixture.GROUP}",result="success",}""") }
        .substringAfterLast(' ')
        .toDouble()
