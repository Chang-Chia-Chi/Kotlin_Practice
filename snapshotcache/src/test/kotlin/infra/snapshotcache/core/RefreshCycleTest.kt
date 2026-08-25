package infra.snapshotcache.core

import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.RefreshPhase
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.VerifyResult
import infra.snapshotcache.testkit.StoreOp
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * P4 tests for [RefreshCycle]: the spec 4.1 state machine on the happy path, the pinned
 * round sequence, admin wiring through [DefaultSnapshotCache], generation lineage
 * (spec 5.1 GenerationInfo), and invariants I1 / I5 (spec 17.2).
 *
 * Failure taxonomy rows live in RefreshCycleFailureTest; the verify gate rules in
 * VerifyGateTest. No sleeps anywhere (plan 1.5).
 */
internal class RefreshCycleTest : RefreshCycleTestBase() {

    // ------------------------------------------------------------------ happy path

    @Test
    fun happyRound_publishes_populatesInfo_firesPhasesAndHooksInPinnedOrder() {
        // Round start is dataAsOf; the source advancing the clock separates publishedAt.
        source.behavior = { clock.advance(Duration.ofSeconds(5)) }

        val gen = runSuccess(cycle())

        assertThat(gen).isEqualTo(1L)
        assertThat(events.finished).containsExactly(RefreshResult.SUCCESS to 1L)

        // BuildContext pinned: group, gen, candidate connection, dataAsOf = round-start instant, previous = null.
        val ctx = source.contexts.single()
        assertThat(ctx.group).isEqualTo(group)
        assertThat(ctx.generation).isEqualTo(1L)
        assertThat(ctx.previous).isNull()
        assertThat(ctx.dataAsOf).describedAs("dataAsOf is the round-start clock instant").isEqualTo(t0)
        assertThat(ctx.target.toString())
            .describedAs("the source must write through the candidate's connection")
            .contains("write connection")
        assertThat(ctx.target.isClosed)
            .describedAs("candidate close (CHECKPOINT seam) must have released the write connection")
            .isTrue()

        // GenerationInfo lineage (pinned item 7): dataAsOf = round start, publishedAt = publish time,
        // rowCounts from verify.
        assertThat(registry.current()).isEqualTo(1L)
        assertThat(registry.currentInfo()).isEqualTo(
            GenerationInfo(1L, t0, t0.plusSeconds(5), mapOf("t_a" to 10L, "t_b" to 20L)),
        )

        // Phase timings for CHECKPOINT/VERIFY/PUBLISH only; QUERY/FETCH/APPEND are P10 source-side.
        assertThat(events.phases.map { it.first })
            .containsExactlyInAnyOrder(RefreshPhase.CHECKPOINT, RefreshPhase.VERIFY, RefreshPhase.PUBLISH)
        assertThat(events.phases.map { it.second }).allSatisfy { assertThat(it).isGreaterThanOrEqualTo(Duration.ZERO) }

        // Pinned hook order: AFTER_VERIFY (after the gate) -> BEFORE_POINTER_SWAP -> publish.
        assertThat(hooks.fired).containsSubsequence(
            Hook.AFTER_VERIFY,
            Hook.BEFORE_POINTER_SWAP,
            Hook.AFTER_POINTER_SWAP,
        )
        assertThat(store.tracker.unclosed()).describedAs("verify connection closed on the happy path").isEmpty()
    }

    // ------------------------------------------------------------------ facade admin wiring (pinned item 3)

    @Test
    fun facade_triggerRefreshRunsRound_gcRunsReclaimPass_liveGenerationsFromRegistry() {
        val cache = DefaultSnapshotCache(
            config,
            mapOf(group to GroupRuntime(registry, stubStore, cycle())),
            events,
            clock,
        )

        val out = cache.triggerRefresh(group)
        assertThat(out.result).isEqualTo(RefreshResult.SUCCESS)
        assertThat(out.generation).isEqualTo(1L)
        assertThat(cache.currentInfo(group)!!.generation).isEqualTo(1L)

        // A held lease keeps gen 1 alive across the next round; releasing it makes gc(group) reclaim it.
        val snap = cache.acquire(group)
        assertThat(cache.triggerRefresh(group).generation).isEqualTo(2L)
        snap.close()

        val gc = cache.gc(group)
        assertThat(gc.reclaimed).containsExactly(1L)
        assertThat(gc.deferred).isEmpty()
        assertThat(events.reclaimedGens).contains(1L)

        val states = cache.liveGenerations(group)
        assertThat(states).describedAs("liveGenerations reads the registry").hasSize(1)
        assertThat(states.single().generation).isEqualTo(2L)
        assertThat(states.single().isCurrent).isTrue()
        assertThat(states.single().refCount).isEqualTo(0)
    }

    @Test
    fun facade_nullCycle_triggerRefreshAndGcThrowIllegalState() {
        val cache = DefaultSnapshotCache(
            config,
            mapOf(group to GroupRuntime(registry, stubStore)),
            events,
            clock,
        )
        assertThatThrownBy { cache.triggerRefresh(group) }.isInstanceOf(IllegalStateException::class.java)
        assertThatThrownBy { cache.gc(group) }.isInstanceOf(IllegalStateException::class.java)
    }

    // ------------------------------------------------------------------ invariants (spec 17.2; P4 owns I1, I5)

    @Test
    fun I1_currentOnlyEverPointsToAVerifiedGeneration() {
        // Verify failure is driven by a caller check: builtin-rule details are P4b's.
        var failVerify = true
        val c = cycle(checks = listOf(gate("i1_rule") { failVerify }))

        // Cold start: a candidate that fails verify must never become current.
        assertThat(c.runOnce().result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertThat(registry.current()).describedAs("failed build must not be published").isNull()
        assertThat(registry.currentInfo()).isNull()

        // A verified generation publishes.
        failVerify = false
        val gen = runSuccess(c)
        val info = registry.currentInfo()

        // Warm: another failing candidate leaves the verified current untouched.
        failVerify = true
        assertThat(c.runOnce().result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertThat(registry.current()).isEqualTo(gen)
        assertThat(registry.currentInfo()).isEqualTo(info)

        // The swap seam is only ever crossed for the verified publish.
        assertThat(hooks.fired.count { it == Hook.BEFORE_POINTER_SWAP })
            .describedAs("BEFORE_POINTER_SWAP fires once per publish, never for a failed candidate")
            .isEqualTo(1)

        failVerify = false
        runSuccess(c) // returns to a usable state
    }

    // ------------------------------------------------------------------ verify_failed row (spec 9.2) + escalation (spec 8.5)

    @Test
    fun verifyFailure_abortsRound_cleansCandidate_nextRunOnceSucceeds() {
        var failVerify = true
        val c = cycle(checks = listOf(gate("caller_rule") { failVerify }))

        val out = c.runOnce()
        assertThat(out.result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertThat(out.generation).describedAs("generation only on SUCCESS").isNull()
        assertThat(out.detail).describedAs("detail names the failing rule").contains("caller_rule")
        assertThat(events.verifyFailures).containsExactly("caller_rule" to "forced failure")
        assertThat(events.finished).containsExactly(RefreshResult.VERIFY_FAILED to null)

        // Candidate fully cleaned: verify runs post-promote (pinned), so the file was
        // promoted and opened, and must now be closed, deleted and gone.
        assertThat(store.generationsOnDisk()).isEmpty()
        assertThat(store.openedGenerations()).isEmpty()
        assertThat(store.tracker.unclosed()).isEmpty()
        assertThat(registry.current()).isNull()

        // Spec 17.8: return to a usable state, not merely an error surfaced.
        failVerify = false
        assertThat(runSuccess(c)).isEqualTo(2L)
        assertThat(events.escalations).describedAs("one failure, threshold 3").isEmpty()
    }

    @Test
    fun verifyFailure_escalatesExactlyOnceAtThreshold_successResetsAndRearms() {
        var failVerify = true
        val c = cycle(checks = listOf(gate("caller_rule") { failVerify }))

        repeat(2) { assertThat(c.runOnce().result).isEqualTo(RefreshResult.VERIFY_FAILED) }
        assertThat(events.escalations).describedAs("two consecutive failures stay below threshold 3").isEmpty()

        failVerify = false
        runSuccess(c) // resets the consecutive counter and re-arms escalation

        failVerify = true
        repeat(2) { assertThat(c.runOnce().result).isEqualTo(RefreshResult.VERIFY_FAILED) }
        assertThat(events.escalations).describedAs("counter was reset by the success").isEmpty()

        assertThat(c.runOnce().result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertThat(events.escalations)
            .describedAs("fires exactly once, when the counter reaches the threshold (lead pin, spec 8.5)")
            .containsExactly(3)

        assertThat(c.runOnce().result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertThat(events.escalations).describedAs("a 4th consecutive failure does not re-fire").containsExactly(3)

        failVerify = false
        runSuccess(c) // usable again after sustained failure
        assertThat(events.verifyFailures).describedAs("every failure carries rule + detail").hasSize(6)
    }

    /** Caller [GenerationCheck] failing while [failing] returns true; rule name is [rule]. */
    private fun gate(rule: String, failing: () -> Boolean) = GenerationCheck { _, _ ->
        if (failing()) VerifyResult.Fail(rule, "forced failure") else VerifyResult.Pass
    }

    @Test
    fun I5_nonCurrentGenerationWithZeroRefcountIsEventuallyDeleted() {
        val c = cycle()
        runSuccess(c) // gen 1

        // The round's own GC reclaims the superseded, unleased generation.
        runSuccess(c) // gen 2; gen 1 now non-current, refcount 0
        assertThat(events.reclaimedGens).containsExactly(1L)
        assertThat(store.generationsOnDisk()).containsExactly(2L)
        assertThat(store.calls().any { it.op == StoreOp.CLOSE && it.gen == 1L }).isTrue()
        assertThat(store.calls().any { it.op == StoreOp.DELETE && it.gen == 1L }).isTrue()

        // A leased generation is not reclaimed; after release, reclaimPass deletes it.
        val lease = checkNotNull(registry.tryAcquire("holder"))
        runSuccess(c) // gen 3; gen 2 held by the lease
        assertThat(store.generationsOnDisk()).describedAs("leased gen 2 must survive the round").contains(2L)

        registry.release(lease)
        val gc = c.reclaimPass()
        assertThat(gc.reclaimed).containsExactly(2L)
        assertThat(gc.deferred).isEmpty()
        assertThat(events.reclaimedGens).containsExactly(1L, 2L)
        assertThat(store.generationsOnDisk()).containsExactly(3L)
    }
}
