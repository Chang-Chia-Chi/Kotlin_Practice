package infra.snapshotcache.core

import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.api.VerifyResult
import infra.snapshotcache.testkit.StoreOp
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * The refresh failure taxonomy, P4 scope: source error, disk error (+ emergency GC),
 * blocked-by-K (pause + auto-resume), DETACH-fails (defer, no blocking), in-flight
 * shutdown abort, overlap skip - every row asserting RETURN TO A USABLE STATE (the next
 * runOnce succeeds / current keeps serving), never merely that an error surfaced.
 * Plus I7. The verify_failed row lives in RefreshCycleTest.
 *
 * Interleavings are latch-parked sources with bounded joins; zero sleeps.
 */
internal class RefreshCycleFailureTest : RefreshCycleTestBase() {

    // ------------------------------------------------------------------ source error

    @Test
    fun sourceError_abortsRound_deletesCandidate_nextRunOnceSucceeds() {
        val c = cycle()
        runSuccess(c) // gen 1

        source.behavior = { throw RuntimeException("boom-source") }
        val out = c.runOnce()

        assertThat(out.result).isEqualTo(RefreshResult.SOURCE_ERROR)
        assertThat(out.generation).isNull()
        assertThat(listOf("boom-source", "RuntimeException").any { out.detail?.contains(it) == true })
            .describedAs("detail (%s) must name the exception", out.detail)
            .isTrue()
        assertThat(events.finished.last()).isEqualTo(RefreshResult.SOURCE_ERROR to null)
        assertThat(store.generationsOnDisk()).describedAs("candidate deleted").containsExactly(1L)
        assertThat(registry.current()).isEqualTo(1L)

        source.behavior = {}
        assertThat(runSuccess(c)).isEqualTo(3L) // gen 2 was allocated and discarded by the failed round
    }

    // ------------------------------------------------------------------ disk error

    @Test
    fun diskError_onCreateCandidate_runsEmergencyGc_nextRunOnceSucceeds() {
        val c = cycle()
        runSuccess(c) // gen 1

        // Keep gen 1 alive with a lease across the next round, then release it: a
        // reclaimable non-current generation now exists for the emergency GC to find.
        val lease = checkNotNull(registry.tryAcquire("holder"))
        runSuccess(c) // gen 2
        registry.release(lease)
        assertThat(store.generationsOnDisk()).contains(1L, 2L)

        store.failOnGen(StoreOp.CREATE_CANDIDATE, 3L)
        val out = c.runOnce()

        assertThat(out.result).isEqualTo(RefreshResult.DISK_ERROR)
        assertThat(out.generation).isNull()
        assertThat(out.detail).describedAs("detail names the exception").containsIgnoringCase("scripted")
        assertThat(events.finished.last()).isEqualTo(RefreshResult.DISK_ERROR to null)
        assertThat(events.reclaimedGens).describedAs("emergency GC ran during the failed round").contains(1L)
        assertThat(store.generationsOnDisk()).containsExactly(2L)
        assertThat(registry.current()).isEqualTo(2L)

        assertThat(runSuccess(c)).isEqualTo(4L)
    }

    @Test
    fun diskError_onPromote_candidateDeleted_nextRunOnceSucceeds() {
        val c = cycle()
        runSuccess(c) // gen 1

        store.failOnGen(StoreOp.PROMOTE, 2L)
        val out = c.runOnce()

        assertThat(out.result).describedAs("store exceptions classify as DISK_ERROR (lead pin)")
            .isEqualTo(RefreshResult.DISK_ERROR)
        assertThat(store.calls().any { it.op == StoreOp.DELETE && it.gen == 2L && !it.failed })
            .describedAs("the unpromotable candidate must be deleted")
            .isTrue()
        assertThat(store.generationsOnDisk()).containsExactly(1L)
        assertThat(registry.current()).isEqualTo(1L)

        assertThat(runSuccess(c)).isEqualTo(3L)
    }

    // ------------------------------------------------------------------ blocked by K

    @Test
    fun blockedByK_pausesWithoutAllocating_autoResumesAfterLeaseRelease() {
        val regK = GenerationRegistry(1, Duration.ofMinutes(5), clock, hooks)
        trackedRegistry = regK
        val c = cycle(cfg = config.copy(maxLiveGenerations = 1), reg = regK)

        runSuccess(c) // gen 1
        val lease = checkNotNull(regK.tryAcquire("stuck-consumer"))
        runSuccess(c) // gen 2; gen 1 survives on the lease -> live = 2 > K = 1

        val mutatingBefore = store.calls().count { it.op != StoreOp.LIST_ON_DISK }
        val out = c.runOnce()

        assertThat(out.result).isEqualTo(RefreshResult.BLOCKED_BY_K)
        assertThat(out.generation).describedAs("blocked round allocates no generation number").isNull()
        assertThat(events.finished.last()).isEqualTo(RefreshResult.BLOCKED_BY_K to null)
        assertThat(store.calls().count { it.op != StoreOp.LIST_ON_DISK })
            .describedAs("a blocked round performs no candidate/publish/reclaim I/O")
            .isEqualTo(mutatingBefore)
        assertThat(regK.current()).describedAs("current keeps serving while paused").isEqualTo(2L)

        // Auto-resume: releasing the lease makes the very next runOnce succeed.
        regK.release(lease)
        val resumed = c.runOnce()
        assertThat(resumed.result).describedAs("refresh must auto-resume after release, got %s (%s)", resumed.result, resumed.detail)
            .isEqualTo(RefreshResult.SUCCESS)
        assertThat(resumed.generation)
            .describedAs("the blocked round must not have consumed a generation number")
            .isEqualTo(3L)
        assertThat(events.reclaimedGens).contains(1L)
    }

    // ------------------------------------------------------------------ DETACH fails

    @Test
    fun detachFailure_defersReclaim_withoutBlockingTheRound_nextPassReclaims() {
        val c = cycle()
        runSuccess(c) // gen 1

        // The round's own GC hits the failing DETACH: reclaim is deferred, the round is not.
        store.failOnGen(StoreOp.CLOSE, 1L)
        assertThat(runSuccess(c)).isEqualTo(2L)
        assertThat(store.generationsOnDisk()).describedAs("deferred gen stays on disk").contains(1L)
        assertThat(events.reclaimedGens).isEmpty()

        // An explicit pass reports the deferral in GcOutcome.
        store.failOnGen(StoreOp.CLOSE, 1L)
        val deferred = c.reclaimPass()
        assertThat(deferred.deferred).containsExactly(1L)
        assertThat(deferred.reclaimed).isEmpty()
        assertThat(events.reclaimedGens).isEmpty()

        // Next pass reclaims: a failed DETACH leaves the generation for the next GC pass.
        val reclaimed = c.reclaimPass()
        assertThat(reclaimed.reclaimed).containsExactly(1L)
        assertThat(reclaimed.deferred).isEmpty()
        assertThat(events.reclaimedGens).containsExactly(1L)
        assertThat(store.generationsOnDisk()).containsExactly(2L)
    }

    // ------------------------------------------------------------------ overlap

    @Test
    fun overlappingRunOnce_skipped_neverTwoCandidates_nextRunOnceSucceeds() {
        val c = cycle()
        val entered = CountDownLatch(1)
        val proceed = CountDownLatch(1)
        source.behavior = {
            entered.countDown()
            check(proceed.await(10, TimeUnit.SECONDS)) { "round was never unparked" }
        }

        val first = AtomicReference<RefreshOutcome>()
        val round = Thread({ first.set(c.runOnce()) }, "refresh-round")
        round.isDaemon = true
        round.start()
        try {
            await(entered) // the first round is parked inside the source

            val skipped = c.runOnce() // concurrent trigger from this thread
            assertThat(skipped.result).isEqualTo(RefreshResult.SKIPPED_OVERLAP)
            assertThat(skipped.generation).isNull()
            assertThat(store.calls().count { it.op == StoreOp.CREATE_CANDIDATE })
                .describedAs("a second candidate must never exist (spec 4.4)")
                .isEqualTo(1)
        } finally {
            proceed.countDown()
        }
        joinOrFail(round)

        assertThat(first.get()!!.result).describedAs("the parked round completes normally").isEqualTo(RefreshResult.SUCCESS)
        assertThat(first.get()!!.generation).isEqualTo(1L)

        source.behavior = {}
        assertThat(runSuccess(c)).describedAs("the skip must not consume a generation number").isEqualTo(2L)
        assertThat(events.finished).containsExactly(
            RefreshResult.SKIPPED_OVERLAP to null,
            RefreshResult.SUCCESS to 1L,
            RefreshResult.SUCCESS to 2L,
        )
    }

    // ------------------------------------------------------------------ in-flight shutdown abort

    @Test
    fun shutdownMidBuild_abortsRound_noTmpNoPromotion_currentUntouched() {
        val c = cycle()
        runSuccess(c) // gen 1

        val entered = CountDownLatch(1)
        val proceed = CountDownLatch(1)
        source.behavior = {
            entered.countDown()
            proceed.await() // interruptible park; throws InterruptedException on interrupt
        }

        val outcome = AtomicReference<RefreshOutcome>()
        val round = Thread({ outcome.set(c.runOnce()) }, "aborting-round")
        round.isDaemon = true
        round.start()
        try {
            await(entered)
            registry.beginShutdown()
            round.interrupt() // shutdown interrupts the in-flight source
            joinOrFail(round)
        } finally {
            proceed.countDown()
        }

        assertThat(outcome.get()!!.result).isEqualTo(RefreshResult.SHUTDOWN_ABORTED)
        assertThat(outcome.get()!!.generation).isNull()
        assertThat(events.finished.last()).isEqualTo(RefreshResult.SHUTDOWN_ABORTED to null)
        assertThat(store.calls().none { it.op == StoreOp.PROMOTE && it.gen == 2L })
            .describedAs("the candidate must never be promoted")
            .isTrue()
        assertThat(store.generationsOnDisk()).describedAs("no .tmp remains").containsExactly(1L)
        assertThat(registry.current()).describedAs("current pointer untouched; keeps serving").isEqualTo(1L)
    }

    @Test
    fun interruptedSource_classifiedShutdownAborted_nextRunOnceSucceeds() {
        val c = cycle()
        runSuccess(c) // gen 1

        source.behavior = { throw InterruptedException("interrupted mid-fetch") }
        val out = c.runOnce()
        Thread.interrupted() // clear a re-set flag so this test thread's later waits are unaffected

        assertThat(out.result).describedAs("InterruptedException classifies as SHUTDOWN_ABORTED (lead pin)")
            .isEqualTo(RefreshResult.SHUTDOWN_ABORTED)
        assertThat(store.generationsOnDisk()).containsExactly(1L)
        assertThat(registry.current()).isEqualTo(1L)

        // No shutdown was actually begun, so the cycle must remain usable.
        source.behavior = {}
        assertThat(runSuccess(c)).isEqualTo(3L)
    }

    // ------------------------------------------------------------------ I7

    @Test
    fun I7_afterFailedRefresh_currentUnchangedAndNoCandidateResourcesRemain() {
        var failVerify = false
        val gate = GenerationCheck { _, _ ->
            if (failVerify) VerifyResult.Fail("i7_rule", "forced failure") else VerifyResult.Pass
        }
        val c = cycle(checks = listOf(gate))
        runSuccess(c) // gen 1
        val info1 = registry.currentInfo()

        fun assertNoResidue() {
            assertThat(registry.current()).describedAs("current unchanged").isEqualTo(1L)
            assertThat(registry.currentInfo()).isEqualTo(info1)
            assertThat(store.generationsOnDisk()).describedAs("no candidate file remains").containsExactly(1L)
            assertThat(store.openedGenerations()).containsExactly(1L)
            assertThat(store.tracker.unclosed()).describedAs("no leaked connections").isEmpty()
        }

        // Generation numbers below follow monotonic allocation: each failed round consumed one.
        source.behavior = { throw RuntimeException("boom-source") } // gen 2
        assertThat(c.runOnce().result).isEqualTo(RefreshResult.SOURCE_ERROR)
        assertNoResidue()
        source.behavior = {}

        failVerify = true // gen 3: promoted, opened, then rejected
        assertThat(c.runOnce().result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertNoResidue()
        failVerify = false

        store.failOnGen(StoreOp.PROMOTE, 4L)
        assertThat(c.runOnce().result).isEqualTo(RefreshResult.DISK_ERROR)
        assertNoResidue()

        store.failOnGen(StoreOp.CREATE_CANDIDATE, 5L)
        assertThat(c.runOnce().result).isEqualTo(RefreshResult.DISK_ERROR)
        assertNoResidue()

        // Return to a usable state after every failure class in sequence.
        assertThat(runSuccess(c)).isEqualTo(6L)
        assertThat(registry.current()).isEqualTo(6L)
    }
}
