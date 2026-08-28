package infra.snapshotcache.core

import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.RefreshPhase
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.testkit.StoreOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit
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

    @Test
    fun throwingEventSink_onAWaitedAcquire_isIgnored_andTheLeaseIsNotLeaked() {
        val boom = object : CacheEvents {
            override fun acquireWaited(group: GroupId, waited: Duration) {
                throw RuntimeException("metrics sink exploded")
            }
        }
        val c = cycle()
        val cache = DefaultSnapshotCache(config, mapOf(group to GroupRuntime(registry, stubStore, c)), boom, clock)

        // Nothing is published yet, so the acquire parks on the registry condition - and
        // the wait it is about to record is exactly what fires the throwing sink.
        val outcome = AtomicReference<Any?>()
        val waiter = Thread({
            outcome.set(runCatching { cache.acquire(group, Duration.ofSeconds(30)) }.getOrElse { it })
        }, "budget-waiter")
        waiter.isDaemon = true
        waiter.start()
        awaitParked(waiter)

        runSuccess(c) // publishes gen 1; the waiter wakes, takes the lease, fires the sink
        joinOrFail(waiter)

        // Before the fix the sink's exception escaped between the registry's refcount
        // increment and the SnapshotHandle that owns the matching release: the caller got
        // a RuntimeException instead of a Snapshot, and the lease it never received stayed
        // outstanding for the process lifetime, eventually wedging refresh at the K guard.
        val result = outcome.get()
        assertThat(result)
            .describedAs("a throwing sink must not break the acquire, got %s", result)
            .isInstanceOf(Snapshot::class.java)
        (result as Snapshot).close()
        assertThat(registry.liveGenerations().single().refCount)
            .describedAs("the lease reached a handle that released it; nothing is leaked")
            .isEqualTo(0)
    }

    /** Bounded spin until the waiter is parked in the registry's condition wait. */
    private fun awaitParked(thread: Thread) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (true) {
            when (thread.state) {
                Thread.State.WAITING, Thread.State.TIMED_WAITING -> return
                Thread.State.TERMINATED ->
                    throw AssertionError("waiter terminated before parking; acquire did not wait")
                else -> {
                    if (System.nanoTime() >= deadline) throw AssertionError("waiter never parked; state=${thread.state}")
                    Thread.onSpinWait()
                }
            }
        }
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
