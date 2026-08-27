package infra.snapshotcache.core

import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.RefreshPhase
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.testkit.StoreOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

/**
 * Regression tests for the 2026-08-28 code-review fix pass, one per finding whose defect
 * is observable at core level. Each asserts the behavior the pre-fix code got wrong, so a
 * revert of the fix fails the test rather than the reasoning.
 *
 * Zero sleeps: the one interleaving is driven by [HookDriver], with bounded joins as
 * bounds on a broken implementation (plan 1.5, spec 17.4).
 */
internal class ReviewFixRegressionTest : RefreshCycleTestBase() {

    private fun cycleWith(hookRunner: HookRunner = hooks, sink: CacheEvents = events): RefreshCycle =
        RefreshCycle(
            group = group,
            registry = registry,
            store = stubStore,
            source = source,
            config = config,
            events = sink,
            checks = emptyList(),
            clock = clock,
            hooks = hookRunner,
        )

    // ------------------------------------------------------------------ H1: reclaim retries the whole unit

    @Test
    fun reclaim_deleteFailsAfterASuccessfulDetach_nextPassCompletesInsteadOfDeferringForever() {
        val c = cycle()
        runSuccess(c) // gen 1

        // Round 2 publishes gen 2 and reclaims gen 1: the DETACH succeeds, the delete
        // throws (a transient file lock in production). The generation goes back to LIVE.
        store.failOnGen(StoreOp.DELETE, 1L)
        runSuccess(c) // gen 2
        assertThat(store.generationsOnDisk())
            .describedAs("the undeleted file is still on disk and the generation still LIVE")
            .contains(1L)

        // The retry re-runs close + delete as one unit. close(1) must be a no-op, not a
        // second DETACH: before the fix it threw, and the generation deferred forever.
        val gc = c.reclaimPass()

        assertThat(gc.reclaimed).containsExactly(1L)
        assertThat(gc.deferred).isEmpty()
        assertThat(store.generationsOnDisk()).containsExactly(2L)
        assertThat(events.reclaimedGens).contains(1L)
    }

    // ------------------------------------------------------------------ H2: a throwing event sink

    @Test
    fun throwingEventSink_isIgnored_roundStillPublishesAndLeavesNoZombieRecord() {
        val boom = object : CacheEvents {
            override fun refreshPhase(group: GroupId, phase: RefreshPhase, elapsed: Duration) {
                throw RuntimeException("metrics sink exploded")
            }
        }

        // Before the fix this escaped round(), skipping abort(): the record stayed in the
        // registry for the process lifetime and the candidate file was never cleaned up.
        val out = cycleWith(sink = boom).runOnce()

        assertThat(out.result).isEqualTo(RefreshResult.SUCCESS)
        assertThat(registry.current()).isEqualTo(1L)
        assertThat(registry.liveGenerations().map { it.generation }).containsExactly(1L)
    }

    // ------------------------------------------------------------------ M2: no lease after shutdown begins

    @Test
    fun tryAcquire_returnsNullOnceShutdownHasBegun_evenWithACurrentGeneration() {
        runSuccess(cycle()) // gen 1 is current and healthy

        registry.beginShutdown()

        assertThat(registry.tryAcquire("late-consumer"))
            .describedAs("the refusal is decided under the registry lock (spec 10.2 step 1)")
            .isNull()
    }

    // ------------------------------------------------------------------ M4: shutdown across the verify window

    @Test
    fun shutdownDuringVerify_abortsBeforeThePointerSwap_currentUntouched() {
        runSuccess(cycle()) // gen 1 published and current

        val driver = HookDriver()
        val gate = driver.arm(Hook.AFTER_VERIFY)
        val outcome = AtomicReference<RefreshOutcome>()
        val round = Thread({ outcome.set(cycleWith(hookRunner = driver).runOnce()) }, "verify-window-round")
        round.isDaemon = true
        round.start()
        driver.awaitParked(gate)

        // Verify has passed; shutdown begins while the round sits in the window that used
        // to run straight on into publish + reclaim.
        registry.beginShutdown()
        driver.release(gate)
        joinOrFail(round)

        assertThat(outcome.get().result).isEqualTo(RefreshResult.SHUTDOWN_ABORTED)
        assertThat(registry.current()).describedAs("the pointer swap must not happen").isEqualTo(1L)
        assertThat(store.generationsOnDisk()).describedAs("the candidate is cleaned up").containsExactly(1L)
        assertThat(store.openedGenerations()).describedAs("the candidate is detached again").containsExactly(1L)
    }
}
