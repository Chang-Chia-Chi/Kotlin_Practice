package infra.snapshotcache.core

import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import infra.snapshotcache.api.NoOpHooks
import infra.snapshotcache.spi.OpenGeneration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * P1 tests for [GenerationRegistry] (plan P1; spec 17.2 registry half: I2, I3, I4, I6, I8;
 * spec 5.1 acquire atomicity; plan 2.5 lifecycle).
 *
 * No sleeps. Interleavings are driven by [Hook] latches only; join/await timeouts below are
 * bounds on broken implementations, never sequencing.
 */
class GenerationRegistryTest {

    private val t0: Instant = Instant.parse("2026-01-01T00:00:00Z")
    private val leaseDeadline: Duration = Duration.ofMinutes(5)

    private fun registry(
        maxLive: Int = 3,
        clock: Clock = Clock.fixed(t0, ZoneOffset.UTC),
        hooks: HookRunner = NoOpHooks,
    ) = GenerationRegistry(maxLive, leaseDeadline, clock, hooks)

    /**
     * Stands in for the registry's former fileBytes-only publish overload, which existed
     * only for this suite and forced a nullable `RegistryLease.opened` through production
     * code. The stub generation is never read from - these tests exercise the registry,
     * not a store.
     */
    private fun GenerationRegistry.publish(gen: Long, bytes: Long) {
        val opened = object : OpenGeneration {
            override val generation = gen
            override fun connection() = error("registry tests never read from a generation")
            override fun fileBytes() = bytes
        }
        publish(gen, opened, GenerationInfo(gen, t0, t0, emptyMap()))
    }

    /** Full happy build path: beginBuild -> beginPublish -> publish. Returns the generation. */
    private fun GenerationRegistry.publishGen(fileBytes: Long = 100): Long {
        val gen = beginBuild()
        beginPublish(gen)
        publish(gen, fileBytes)
        return gen
    }

    private fun GenerationRegistry.refCountOf(gen: Long): Int? =
        liveGenerations().firstOrNull { it.generation == gen }?.refCount

    // ------------------------------------------------------------------ invariants

    @Test
    fun I2_leasedGenerationIsNeverSelectedForReclaim() {
        val reg = registry()
        val g1 = reg.publishGen()
        val lease = reg.tryAcquire("job-x")
        assertThat(lease).isNotNull
        reg.publishGen()

        assertThat(reg.beginReclaim()).describedAs("refcount > 0 must exclude g1 from reclaim").isEmpty()
        assertThat(reg.refCountOf(g1)).isEqualTo(1)

        reg.release(lease!!)
        assertThat(reg.beginReclaim()).containsExactly(g1)
    }

    @Test
    fun I3_generationNumbersStrictlyIncreasing() {
        val reg = registry()
        val a = reg.beginBuild()
        reg.discardBuild(a)
        val b = reg.beginBuild()
        reg.beginPublish(b)
        reg.publish(b, 10)
        val c = reg.beginBuild()
        reg.discardBuild(c)
        val d = reg.beginBuild()

        val numbers = listOf(a, b, c, d)
        numbers.zipWithNext().forEach { (prev, next) ->
            assertThat(next).describedAs("numbering must be strictly increasing, got %s", numbers).isGreaterThan(prev)
        }
    }

    @Test
    fun I4_liveBeyondKReportsBlockedWithHoldingLeases_neverThrows() {
        val reg = registry(maxLive = 2)
        assertThatCode { reg.blockedByK() }.doesNotThrowAnyException()
        assertThat(reg.blockedByK()).describedAs("empty registry is not blocked").isNull()

        val g1 = reg.publishGen()
        val l1 = reg.tryAcquire("holder-1")!!
        val g2 = reg.publishGen()
        assertThat(reg.blockedByK()).describedAs("live == K proceeds normally (spec 6.1)").isNull()

        val l2 = reg.tryAcquire("holder-2")!!
        reg.publishGen()
        val blocked = reg.blockedByK()
        assertThat(blocked).describedAs("live == 3 > K = 2 must report blocked").isNotNull
        assertThat(blocked!!.map { it.owner }).containsExactlyInAnyOrder("holder-1", "holder-2")

        reg.release(l1)
        reg.release(l2)
        val marked = reg.beginReclaim()
        assertThat(marked).containsExactlyInAnyOrder(g1, g2)
        marked.forEach { reg.reclaimed(it) }
        assertThat(reg.blockedByK()).describedAs("auto-unblocks once live <= K again").isNull()
    }

    @Test
    fun I6_doubleReleaseDecrementsOnce_refcountNeverNegative() {
        val reg = registry()
        val g = reg.publishGen()
        val a = reg.tryAcquire("a")!!
        val b = reg.tryAcquire("b")!!
        assertThat(reg.refCountOf(g)).isEqualTo(2)

        reg.release(a)
        reg.release(a) // double close: idempotent per lease instance
        assertThat(reg.refCountOf(g)).isEqualTo(1)

        reg.release(b)
        reg.release(b)
        assertThat(reg.refCountOf(g)).isEqualTo(0)
        reg.liveGenerations().forEach { assertThat(it.refCount).isGreaterThanOrEqualTo(0) }
    }

    @Test
    fun I8_leaseObservesSameGenerationAcrossPublishes() {
        val reg = registry()
        val g1 = reg.publishGen()
        val lease = reg.tryAcquire("long-runner")!!
        assertThat(lease.generation).isEqualTo(g1)

        reg.publishGen()
        reg.publishGen()

        assertThat(lease.generation).isEqualTo(g1)
        assertThat(reg.current()).isNotEqualTo(g1)
        assertThat(reg.refCountOf(g1)).describedAs("g1 must remain live-readable while leased").isEqualTo(1)
    }

    // ------------------------------------------------------------------ acquire atomicity (spec 5.1)

    @Test
    fun acquireDuringSwap_afterReadCurrentHook_leaseRemainsLiveReadable() {
        val gate = GateHooks(Hook.AFTER_READ_CURRENT)
        val reg = GenerationRegistry(3, leaseDeadline, Clock.fixed(t0, ZoneOffset.UTC), gate)
        val genA = reg.publishGen()

        val leaseRef = AtomicReference<RegistryLease?>()
        val acquirer = Thread { leaseRef.set(reg.tryAcquire("swap-race")) }
        acquirer.isDaemon = true
        acquirer.start()
        assertThat(gate.reached.await(5, TimeUnit.SECONDS))
            .describedAs("acquire must pass AFTER_READ_CURRENT outside the registry monitor").isTrue()

        // Full swap + GC marking while the acquirer is parked mid-acquire.
        val genB = reg.publishGen()
        val marked = reg.beginReclaim()

        gate.proceed.countDown()
        acquirer.join(5_000)
        assertThat(acquirer.isAlive).isFalse()

        val lease = leaseRef.get()
        assertThat(lease).describedAs("a current generation exists; acquire must not fail").isNotNull
        val gen = lease!!.generation
        assertThat(gen).isIn(genA, genB)
        assertThat(marked).describedAs("the leased generation must not have been marked RECLAIMING (I2)")
            .doesNotContain(gen)
        val state = reg.liveGenerations().firstOrNull { it.generation == gen }
        assertThat(state).describedAs("leased generation must still be LIVE-readable").isNotNull
        assertThat(state!!.refCount).isEqualTo(1)
    }

    // ------------------------------------------------------------------ lifecycle

    @Test
    fun tryAcquire_returnsNullUntilFirstPublish() {
        val reg = registry()
        assertThat(reg.tryAcquire("early")).isNull()
        val g = reg.beginBuild()
        assertThat(reg.tryAcquire("early")).describedAs("BUILDING is not acquirable").isNull()
        reg.beginPublish(g)
        assertThat(reg.tryAcquire("early")).describedAs("OPENING is not acquirable").isNull()
        reg.publish(g, 10)
        assertThat(reg.tryAcquire("early")).isNotNull
    }

    @Test
    fun tryAcquire_leaseRecordsOwnerAcquiredAtAndDeadlineFromClock() {
        val reg = registry()
        val g = reg.publishGen()
        val lease = reg.tryAcquire("etl-job-x")!!
        assertThat(lease.generation).isEqualTo(g)
        assertThat(lease.info.owner).isEqualTo("etl-job-x")
        assertThat(lease.info.acquiredAt).isEqualTo(t0)
        assertThat(lease.info.deadline).isEqualTo(t0.plus(leaseDeadline))
    }

    @Test
    fun current_tracksLatestPublishedGeneration() {
        val reg = registry()
        assertThat(reg.current()).isNull()
        val g1 = reg.publishGen()
        assertThat(reg.current()).isEqualTo(g1)
        val g2 = reg.publishGen()
        assertThat(reg.current()).isEqualTo(g2)
    }

    @Test
    fun discardBuild_leavesCurrentUntouchedAndCandidateGone() {
        val reg = registry()
        val g1 = reg.publishGen()
        val candidate = reg.beginBuild()
        reg.discardBuild(candidate)
        assertThat(reg.current()).isEqualTo(g1)
        assertThat(reg.liveGenerations().map { it.generation }).doesNotContain(candidate)
        assertThat(reg.tryAcquire("after-discard")!!.generation).isEqualTo(g1)
    }

    @Test
    fun reclaimLifecycle_markThenDeferThenComplete() {
        val reg = registry()
        val g1 = reg.publishGen()
        reg.publishGen()

        assertThat(reg.beginReclaim()).containsExactly(g1)
        assertThat(reg.beginReclaim()).describedAs("RECLAIMING gens are not re-marked").isEmpty()

        reg.deferReclaim(g1) // DETACH failed outside; back to LIVE for the next pass
        assertThat(reg.beginReclaim()).containsExactly(g1)

        reg.reclaimed(g1) // detach + delete succeeded outside
        assertThat(reg.liveGenerations().map { it.generation }).doesNotContain(g1)
        assertThat(reg.beginReclaim()).isEmpty()
    }

    @Test
    fun beginReclaim_neverMarksCurrentEvenAtZeroRefcount() {
        val reg = registry()
        reg.publishGen()
        assertThat(reg.beginReclaim()).isEmpty()
    }

    @Test
    fun liveGenerations_reportCurrentFlagFileBytesRefcountAndLeases() {
        val reg = registry()
        val g1 = reg.publishGen(fileBytes = 111)
        reg.tryAcquire("viewer")
        val g2 = reg.publishGen(fileBytes = 222)

        val states = reg.liveGenerations().associateBy { it.generation }
        assertThat(states.keys).contains(g1, g2)
        with(states.getValue(g1)) {
            assertThat(isCurrent).isFalse()
            assertThat(fileBytes).isEqualTo(111)
            assertThat(refCount).isEqualTo(1)
            assertThat(leases.map { it.owner }).containsExactly("viewer")
        }
        with(states.getValue(g2)) {
            assertThat(isCurrent).isTrue()
            assertThat(fileBytes).isEqualTo(222)
            assertThat(refCount).isEqualTo(0)
            assertThat(leases).isEmpty()
        }
    }

    // ------------------------------------------------------------------ lease deadline (diagnostic, spec 6.2)

    @Test
    fun expiredLeases_reportedOnlyPastDeadline_viaInjectedClock() {
        val clock = MutableClock(t0)
        val reg = registry(clock = clock)
        reg.publishGen()
        val held = reg.tryAcquire("slow-job")!!
        val done = reg.tryAcquire("done-job")!!
        reg.release(done)

        assertThat(reg.expiredLeases()).isEmpty()
        clock.advance(leaseDeadline.minusSeconds(1))
        assertThat(reg.expiredLeases()).isEmpty()
        clock.advance(Duration.ofSeconds(2))
        assertThat(reg.expiredLeases().map { it.owner })
            .describedAs("only the still-held, past-deadline lease is reported").containsExactly("slow-job")

        // Diagnostic only (D8): expiry must not have revoked anything.
        assertThat(reg.refCountOf(held.generation)).isEqualTo(1)
        reg.release(held)
    }

    // ------------------------------------------------------------------ waiters: publish and shutdown

    @Test
    fun awaitCurrent_trueImmediatelyWhenCurrentExists() {
        val reg = registry()
        reg.publishGen()
        assertThat(reg.awaitCurrent(Duration.ZERO)).isTrue()
    }

    @Test
    fun awaitCurrent_falseOnZeroBudgetWithoutCurrent() {
        val reg = registry()
        assertThat(reg.awaitCurrent(Duration.ZERO)).isFalse()
    }

    @Test
    fun awaitCurrent_falseWhenBudgetExpiresWithoutPublish() {
        val reg = registry()
        assertThat(reg.awaitCurrent(Duration.ofMillis(50))).isFalse()
    }

    @Test
    fun awaitCurrent_waiterReleasedByPublish() {
        val reg = registry()
        val started = CountDownLatch(1)
        val result = AtomicReference<Boolean>()
        val waiter = Thread {
            started.countDown()
            result.set(reg.awaitCurrent(Duration.ofSeconds(10)))
        }
        waiter.isDaemon = true
        waiter.start()
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue()

        reg.publishGen()

        waiter.join(5_000) // bound, not sequencing: publish must signal well before the 10s budget
        assertThat(waiter.isAlive).describedAs("publish must release the waiter").isFalse()
        assertThat(result.get()).isTrue()
    }

    @Test
    fun awaitCurrent_waiterReleasedImmediatelyByShutdown() {
        val reg = registry()
        val started = CountDownLatch(1)
        val result = AtomicReference<Boolean>()
        val waiter = Thread {
            started.countDown()
            result.set(reg.awaitCurrent(Duration.ofSeconds(10)))
        }
        waiter.isDaemon = true
        waiter.start()
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue()

        reg.beginShutdown()

        waiter.join(5_000) // bound: shutdown releases at once, never serving out the budget (spec 10.2 step 1)
        assertThat(waiter.isAlive).describedAs("shutdown must release the waiter at once").isFalse()
        assertThat(result.get()).isFalse()
        assertThat(reg.isShuttingDown()).isTrue()
    }

    @Test
    fun beginShutdown_flagFalseUntilSet() {
        val reg = registry()
        assertThat(reg.isShuttingDown()).isFalse()
        reg.beginShutdown()
        assertThat(reg.isShuttingDown()).isTrue()
    }

    // ------------------------------------------------------------------ hook placement (pinned)

    @Test
    fun hooks_runAtPinnedPoints() {
        val recorded = CopyOnWriteArrayList<Hook>()
        val reg = GenerationRegistry(3, leaseDeadline, Clock.fixed(t0, ZoneOffset.UTC), HookRunner { recorded += it })

        reg.publishGen()
        assertThat(recorded).contains(Hook.AFTER_POINTER_SWAP)

        recorded.clear()
        val lease = reg.tryAcquire("hooked")!!
        assertThat(recorded).contains(Hook.AFTER_READ_CURRENT)
        reg.release(lease)

        reg.publishGen()
        recorded.clear()
        assertThat(reg.beginReclaim()).isNotEmpty
        assertThat(recorded).contains(Hook.BEFORE_DETACH)
    }

    // ------------------------------------------------------------------ test helpers

    /** Deterministic advancing clock; no real waiting (spec 17.1). */
    private class MutableClock(@Volatile private var now: Instant) : Clock() {
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
        fun advance(by: Duration) {
            now = now.plus(by)
        }
    }

    /**
     * Parks the first passage of [target]: signals [reached], then blocks until [proceed].
     * Subsequent passages (e.g. an internal acquire retry) run through unparked.
     */
    private class GateHooks(private val target: Hook) : HookRunner {
        val reached = CountDownLatch(1)
        val proceed = CountDownLatch(1)
        private val armed = AtomicBoolean(true)

        override fun at(hook: Hook) {
            if (hook == target && armed.compareAndSet(true, false)) {
                reached.countDown()
                check(proceed.await(5, TimeUnit.SECONDS)) { "GateHooks: proceed latch never released" }
            }
        }
    }
}
