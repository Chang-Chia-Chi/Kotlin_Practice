package infra.snapshotcache.core

import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.ShuttingDownException
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.testkit.AccountingFixture
import infra.snapshotcache.testkit.InMemoryGenerationStore
import infra.snapshotcache.testkit.StoreOp
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * P5: the deterministic concurrency suite, at the integration level -
 * [DefaultSnapshotCache] + [RefreshCycle] + [GenerationRegistry] + fake storage wired
 * together as production would be, driven through the facade and triggerRefresh.
 *
 * The six specified interleavings, the two P5 shutdown interleavings, and the
 * N=20 / M=100 stress test with invariants checked after every round.
 *
 * Zero sleeps. Every interleaving is driven by [Hook] latches ([HookDriver]) or latch-parked
 * source behavior; bounded joins/awaits are bounds on broken implementations, never
 * sequencing. The one permitted nondeterminism is the Cleaner orphan case, bounded by
 * awaiting the orphan event while forcing GC.
 */
internal class ConcurrencySuiteTest {

    private val t0: Instant = Instant.parse("2026-01-01T00:00:00Z")
    private val leaseDeadline: Duration = Duration.ofMinutes(5)
    private val group = GroupId("orders")

    private val store = InMemoryGenerationStore()
    private val script = QueryScript().apply {
        tables["t_a"] = 10L
        tables["t_b"] = 20L
    }
    private val stubStore = QueryStubGenerationStore(store, script)
    private val clock = MutableTestClock(t0)
    private val driver = HookDriver()
    private val events = P5RecordingEvents()
    private val source = ScriptedSource()
    private val config = SnapshotCacheConfig(
        storagePath = Path.of("unused-storage"),
        tempDirectory = Path.of("unused-temp"),
    )

    private val registry = GenerationRegistry(3, leaseDeadline, clock, driver)
    private val cycle = RefreshCycle(
        group = group,
        registry = registry,
        store = stubStore,
        source = source,
        config = config,
        events = events,
        clock = clock,
        hooks = driver,
    )
    private val cache = DefaultSnapshotCache(
        config,
        mapOf(group to GroupRuntime(registry, stubStore, cycle)),
        events,
        clock,
    )

    /** Tests that build their own registry (the K=1 case) point the fixture at it. */
    private var trackedRegistry: GenerationRegistry? = null

    @RegisterExtension
    @JvmField
    val accounting = AccountingFixture(store).apply {
        currentGeneration = { (trackedRegistry ?: registry).current() }
        refCounts = { (trackedRegistry ?: registry).liveGenerations().associate { it.generation to it.refCount } }
    }

    // ------------------------------------------------------------- case 1: publish + GC mid-acquire

    @Test
    fun midAcquire_fullPublishAndGcCycleAtAfterReadCurrent_handleValidAndQueryable_I2Holds() {
        val gen1 = triggerSuccess()
        val gate = driver.arm(Hook.AFTER_READ_CURRENT)

        val handleRef = AtomicReference<Snapshot?>()
        val acquirer = Thread({ handleRef.set(cache.acquire(group)) }, "mid-acquire")
        acquirer.isDaemon = true
        acquirer.start()
        driver.awaitParked(gate)

        // The acquire atomicity seam: a COMPLETE publish + GC cycle runs while acquire
        // sits between reading the pointer and refcount++. gen1's refcount is still 0,
        // so GC detaches and deletes it before the acquirer resumes.
        val gen2 = triggerSuccess()
        assertThat(store.calls().filter { it.op == StoreOp.DELETE && it.gen == gen1 })
            .describedAs("GC must have reclaimed gen1 while the acquirer was parked")
            .hasSize(1)

        driver.release(gate)
        joinOrFail(acquirer)

        // Handle valid: a current generation exists, so acquire must have succeeded -
        // on the post-swap generation, never the deleted one.
        val snap = handleRef.get()
        assertThat(snap).describedAs("handle must be valid: a current generation exists").isNotNull
        assertThat(snap!!.generation)
            .describedAs("the atomic critical section must re-read the pointer and count the LIVE generation")
            .isEqualTo(gen2)

        // Queryable against the fake: connection() works and answers a query.
        val connection = snap.connection()
        assertThat(connection.isClosed).isFalse()
        val resultSet = connection.createStatement().executeQuery("SELECT 1")
        assertThat(resultSet.next()).isTrue()
        assertThat(resultSet.getLong(1)).isEqualTo(1L)

        // I2 holds: the handle's generation (refcount > 0) is opened, never closed or deleted.
        assertThat(refCountOf(gen2)).isEqualTo(1)
        assertThat(store.openedGenerations()).contains(gen2)
        assertThat(store.calls().none { (it.op == StoreOp.CLOSE || it.op == StoreOp.DELETE) && it.gen == gen2 })
            .describedAs("I2: the leased generation must never have been closed or deleted")
            .isTrue()

        snap.close()
        assertThat(refCountOf(gen2)).isEqualTo(0)
    }

    // ------------------------------------------------------------- case 2: lease held while refreshing up to K

    @Test
    fun leaseHeldOnOldGenWhileRefreshingUpToK_refreshBlocksWithExplicitState_autoResumesAfterRelease() {
        // K = 1: one lease on the previous generation pushes live past K after the next publish.
        val configK1 = config.copy(maxLiveGenerations = 1)
        val registryK1 = GenerationRegistry(1, leaseDeadline, clock, driver)
        val cycleK1 = RefreshCycle(
            group = group,
            registry = registryK1,
            store = stubStore,
            source = source,
            config = configK1,
            events = events,
            clock = clock,
            hooks = driver,
        )
        val cacheK1 = DefaultSnapshotCache(
            configK1,
            mapOf(group to GroupRuntime(registryK1, stubStore, cycleK1)),
            events,
            clock,
        )
        trackedRegistry = registryK1

        val gen1 = triggerSuccess(cacheK1)
        val held = cacheK1.acquire(group)
        assertThat(held.generation).isEqualTo(gen1)
        val gen2 = triggerSuccess(cacheK1) // gen1 leased -> survives GC; live = 2 > K

        val blocked = cacheK1.triggerRefresh(group)
        assertThat(blocked.result)
            .describedAs("refresh must block with explicit state while live > K (spec 6.1, I4)")
            .isEqualTo(RefreshResult.BLOCKED_BY_K)
        assertThat(blocked.generation).isNull()
        assertThat(store.calls().count { it.op == StoreOp.CREATE_CANDIDATE })
            .describedAs("a blocked round must not build a candidate")
            .isEqualTo(2)
        assertThat(events.finished.count { it.first == RefreshResult.BLOCKED_BY_K })
            .describedAs("the blocked state is recorded, not silent")
            .isEqualTo(1)
        assertThat(registryK1.liveGenerations().map { it.generation }).containsExactlyInAnyOrder(gen1, gen2)

        held.close()

        // Auto-resume: the very next trigger reclaims the released generation and proceeds.
        val resumed = cacheK1.triggerRefresh(group)
        assertThat(resumed.result)
            .describedAs("refresh must auto-resume after the lease releases, got %s (%s)", resumed.result, resumed.detail)
            .isEqualTo(RefreshResult.SUCCESS)
        assertThat(registryK1.liveGenerations().map { it.generation })
            .describedAs("the released old generation must have been reclaimed")
            .doesNotContain(gen1)
    }

    // ------------------------------------------------------------- case 3: close() called twice

    @Test
    fun closeCalledTwice_refcountDecrementedOnce_I6Holds() {
        val gen1 = triggerSuccess()
        val a = cache.acquire(group)
        val b = cache.acquire(group)
        assertThat(refCountOf(gen1)).isEqualTo(2)

        a.close()
        a.close() // second close: must be a no-op
        assertThat(refCountOf(gen1)).describedAs("double close must decrement exactly once (I6)").isEqualTo(1)
        assertThat(events.released.get()).describedAs("one release event per lease, not per close call").isEqualTo(1)

        b.close()
        b.close()
        b.close()
        assertThat(refCountOf(gen1)).isEqualTo(0)
        assertThat(events.released.get()).isEqualTo(2)
        registry.liveGenerations().forEach {
            assertThat(it.refCount).describedAs("I6: refcount never negative").isGreaterThanOrEqualTo(0)
        }
    }

    // ------------------------------------------------------------- case 4: handle GC'd without close

    @Test
    fun handleGarbageCollectedWithoutClose_cleanerForceReleases_orphanCounterPlusOne() {
        val gen1 = triggerSuccess()
        acquireAndDrop()

        // The one permitted nondeterminism: bounded await of the orphan event while forcing GC.
        var fired = false
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        while (!fired && System.nanoTime() < deadline) {
            System.gc()
            fired = events.orphanPermits.tryAcquire(200, TimeUnit.MILLISECONDS)
        }
        assertThat(fired).describedAs("the Cleaner must force-release the dropped handle").isTrue()
        assertThat(events.orphaned.get()).describedAs("orphan counter incremented by exactly 1").isEqualTo(1)
        assertThat(events.released.get()).describedAs("an orphan is never also a normal release").isEqualTo(0)
        assertThat(refCountOf(gen1)).describedAs("the force-release must return the refcount").isEqualTo(0)
    }

    /** Separate frame so the returned handle is unreachable the moment this returns. */
    private fun acquireAndDrop() {
        cache.acquire(group)
    }

    // ------------------------------------------------------------- case 5: overlapping schedule trigger

    @Test
    fun overlappingScheduleTrigger_secondRunSkipped_neverTwoCandidatesAtOnce() {
        triggerSuccess()

        val building = CountDownLatch(1)
        val finishBuild = CountDownLatch(1)
        source.behavior = {
            building.countDown()
            check(finishBuild.await(10, TimeUnit.SECONDS)) { "parked build was never released" }
        }
        val outcome = AtomicReference<RefreshOutcome>()
        val firstRun = Thread({ outcome.set(cache.triggerRefresh(group)) }, "refresh-in-flight")
        firstRun.isDaemon = true
        firstRun.start()
        await(building) // round 2 is deterministically mid-build; its candidate exists

        val callsBefore = store.calls().size
        val second = cache.triggerRefresh(group)
        assertThat(second.result).describedAs("the overlapping trigger must be skipped").isEqualTo(RefreshResult.SKIPPED_OVERLAP)
        assertThat(store.calls().size)
            .describedAs("the skipped run must not touch the store: never two candidates at once")
            .isEqualTo(callsBefore)
        assertThat(store.calls().count { it.op == StoreOp.CREATE_CANDIDATE })
            .describedAs("only round 1 and the in-flight round 2 ever created a candidate")
            .isEqualTo(2)

        finishBuild.countDown()
        joinOrFail(firstRun)
        assertThat(outcome.get()!!.result).describedAs("the in-flight round completes normally").isEqualTo(RefreshResult.SUCCESS)
        assertThat(events.finished.count { it.first == RefreshResult.SKIPPED_OVERLAP }).isEqualTo(1)
    }

    // ------------------------------------------------------------- case 6: one handle spans two publishes

    @Test
    fun oneHandleSpansTwoPublishes_generationNumberUnchanged_I8Holds() {
        val gen1 = triggerSuccess()
        val snap = cache.acquire(group)
        assertThat(snap.generation).isEqualTo(gen1)
        val dataAsOf = snap.dataAsOf

        val gen2 = triggerSuccess()
        assertThat(snap.generation).describedAs("the first publish must not move the handle (I8)").isEqualTo(gen1)
        val gen3 = triggerSuccess()
        assertThat(gen2).isGreaterThan(gen1)
        assertThat(gen3).isGreaterThan(gen2)

        assertThat(snap.generation)
            .describedAs("generation number unchanged across two publishes (I8)")
            .isEqualTo(gen1)
        assertThat(snap.dataAsOf).isEqualTo(dataAsOf)
        assertThat(registry.current()).isEqualTo(gen3)

        // Still readable from the pinned generation: opened, never closed or deleted (I2).
        assertThat(store.openedGenerations()).contains(gen1)
        val connection = snap.connection()
        assertThat(connection.isClosed).isFalse()
        connection.close()

        snap.close()
        cache.gc(group)
        assertThat(registry.liveGenerations().map { it.generation })
            .describedAs("once released, the spanned generation is reclaimable")
            .doesNotContain(gen1)
    }

    // ------------------------------------------------------------- plan-P5 shutdown interleaving 1

    @Test
    fun shutdownWhileThreadSitsInWaitBudget_releasedAtOnce_notAfterTheBudget() {
        val outcome = AtomicReference<Throwable?>()
        val waiter = Thread({
            outcome.set(catchThrowable { cache.acquire(group, Duration.ofSeconds(30)) })
        }, "budget-waiter")
        waiter.isDaemon = true
        waiter.start()
        awaitParkedInWait(waiter)

        registry.beginShutdown()

        joinOrFail(waiter, 5_000) // far below the 30s budget: released at once, never serving it out
        assertThat(outcome.get())
            .describedAs("a waiter released by shutdown must see ShuttingDownException (spec 10.2 step 1)")
            .isInstanceOf(ShuttingDownException::class.java)
        assertThat(events.unavailable).containsExactly(AcquireUnavailableReason.SHUTTING_DOWN)
    }

    // ------------------------------------------------------------- plan-P5 shutdown interleaving 2

    @Test
    fun shutdownWhileBuildMidFlight_candidateDiscarded_currentUnchanged() {
        val gen1 = triggerSuccess()
        val infoBefore = cache.currentInfo(group)

        val building = CountDownLatch(1)
        val finishBuild = CountDownLatch(1)
        source.behavior = {
            building.countDown()
            check(finishBuild.await(10, TimeUnit.SECONDS)) { "parked build was never released" }
        }
        val outcome = AtomicReference<RefreshOutcome>()
        val run = Thread({ outcome.set(cache.triggerRefresh(group)) }, "mid-build-refresh")
        run.isDaemon = true
        run.start()
        await(building) // the build is deterministically mid-flight

        registry.beginShutdown()
        finishBuild.countDown()
        joinOrFail(run)

        assertThat(outcome.get()!!.result)
            .describedAs("shutdown mid-build must abort the round (spec 10.2 step 3, D23)")
            .isEqualTo(RefreshResult.SHUTDOWN_ABORTED)

        val candidateGen = store.calls()
            .filter { it.op == StoreOp.CREATE_CANDIDATE }
            .mapNotNull { it.gen }
            .single { it != gen1 }
        assertThat(store.calls().none { it.op == StoreOp.PROMOTE && it.gen == candidateGen })
            .describedAs("the candidate must never be promoted")
            .isTrue()
        assertThat(store.calls().filter { it.op == StoreOp.DELETE && it.gen == candidateGen }.map { it.detail })
            .describedAs("the candidate file must be deleted")
            .containsExactly("candidate")
        assertThat(store.generationsOnDisk()).containsExactly(gen1)
        assertThat(registry.current()).describedAs("the current pointer is untouched").isEqualTo(gen1)
        assertThat(cache.currentInfo(group)).isEqualTo(infoBefore)
    }

    // ------------------------------------------------------------- stress

    @Test
    fun stress_twentyConsumers_hundredRounds_invariantsHoldAfterEveryRound() {
        val consumers = 20
        val rounds = 100
        val stop = AtomicBoolean(false)
        val failures = ConcurrentLinkedQueue<Throwable>()
        val acquires = AtomicLong()

        var lastPublished = triggerSuccess() // round 1 of 100: consumers never see "not ready"
        sweepInvariants(RefreshResult.SUCCESS)

        val threads = (1..consumers).map { i ->
            Thread({
                val rnd = ThreadLocalRandom.current()
                try {
                    while (!stop.get()) {
                        if (rnd.nextBoolean()) {
                            val snap = cache.acquire(group, Duration.ZERO)
                            try {
                                acquires.incrementAndGet()
                                val pinned = snap.generation
                                val connection = snap.connection()
                                check(!connection.isClosed) { "issued connection must be open" }
                                connection.close()
                                check(snap.generation == pinned) { "I8: handle moved from $pinned to ${snap.generation}" }
                            } finally {
                                snap.close()
                            }
                        } else {
                            cache.withSnapshot(group, Duration.ZERO) { snap ->
                                acquires.incrementAndGet()
                                val pinned = snap.generation
                                snap.connection().close()
                                check(snap.generation == pinned) { "I8: handle moved from $pinned to ${snap.generation}" }
                            }
                        }
                    }
                } catch (failure: Throwable) {
                    failures += failure
                }
            }, "consumer-$i").apply { isDaemon = true }
        }
        threads.forEach { it.start() }

        try {
            repeat(rounds - 1) {
                val outcome = cache.triggerRefresh(group)
                assertThat(outcome.result)
                    .describedAs("only SUCCESS or an explicit BLOCKED_BY_K may occur, got %s (%s)", outcome.result, outcome.detail)
                    .isIn(RefreshResult.SUCCESS, RefreshResult.BLOCKED_BY_K)
                if (outcome.result == RefreshResult.SUCCESS) {
                    val gen = checkNotNull(outcome.generation)
                    assertThat(gen).describedAs("I3: published generations strictly increasing").isGreaterThan(lastPublished)
                    lastPublished = gen
                }
                sweepInvariants(outcome.result)
            }
        } finally {
            stop.set(true)
            threads.forEach { joinOrFail(it, 30_000) }
        }

        failures.firstOrNull()?.let { throw AssertionError("consumer-side invariant failure: $it", it) }
        assertThat(events.orphaned.get()).describedAs("every stress handle was closed; no orphan may fire").isEqualTo(0)
        assertThat(events.released.get())
            .describedAs("I6 via final accounting: exactly one release per acquire")
            .isEqualTo(acquires.get().toInt())

        cache.gc(group) // consumers are gone: one pass reclaims every non-current generation
        val remaining = registry.liveGenerations()
        assertThat(remaining).describedAs("only the current generation survives the final GC").hasSize(1)
        assertThat(remaining.single().isCurrent).isTrue()
        assertThat(remaining.single().refCount).isEqualTo(0)
        // The AccountingFixture asserts the accounting equations at test end.
    }

    /**
     * All checkable invariants after one round: I6 (no negative refcount), I2 (every leased
     * generation opened, never closed/deleted), I4 with the blocked-state exception. Live may
     * transiently exceed K when a lease released after this round's GC marking left an
     * unreclaimed generation behind; a bounded gc-and-recheck loop absorbs that race
     * without sleeping.
     */
    private fun sweepInvariants(result: RefreshResult) {
        var attempts = 0
        while (true) {
            val live = registry.liveGenerations()
            live.forEach { state ->
                assertThat(state.refCount)
                    .describedAs("I6: refcount of generation %d never negative", state.generation)
                    .isGreaterThanOrEqualTo(0)
            }
            val opened = store.openedGenerations()
            live.filter { it.refCount > 0 }.forEach { state ->
                assertThat(opened)
                    .describedAs("I2: leased generation %d must be opened, never closed or deleted", state.generation)
                    .contains(state.generation)
            }
            val overK = live.size > 3
            val excessExplained = result == RefreshResult.BLOCKED_BY_K ||
                live.filter { !it.isCurrent }.all { it.refCount > 0 }
            if (!overK || excessExplained) return
            assertThat(++attempts)
                .describedAs("I4: live=%d > K=3 with reclaimable generations GC never reclaimed", live.size)
                .isLessThanOrEqualTo(100)
            cache.gc(group)
        }
    }

    // ------------------------------------------------------------- helpers

    private fun triggerSuccess(admin: DefaultSnapshotCache = cache): Long {
        val out = admin.triggerRefresh(group)
        assertThat(out.result)
            .describedAs("expected SUCCESS, got %s (detail=%s)", out.result, out.detail)
            .isEqualTo(RefreshResult.SUCCESS)
        return checkNotNull(out.generation) { "SUCCESS outcome must carry its generation" }
    }

    private fun refCountOf(gen: Long): Int? =
        (trackedRegistry ?: registry).liveGenerations().firstOrNull { it.generation == gen }?.refCount

    /** Bounded latch wait: a precondition check, never sequencing by sleep. */
    private fun await(latch: CountDownLatch) {
        assertThat(latch.await(10, TimeUnit.SECONDS)).describedAs("latch must open").isTrue()
    }

    /** Bounded join: a bound on broken implementations, never sequencing. */
    private fun joinOrFail(thread: Thread, bound: Long = 10_000) {
        thread.join(bound)
        assertThat(thread.isAlive).describedAs("thread %s must have finished", thread.name).isFalse()
    }

    /**
     * Bounded wait until [thread] is parked in a wait (the acquire's condition await).
     * Establishes "the waiter is actually waiting" before shutdown - a condition check
     * with a deadline, not sequencing by sleep.
     */
    private fun awaitParkedInWait(thread: Thread) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (true) {
            when (thread.state) {
                Thread.State.WAITING, Thread.State.TIMED_WAITING -> return
                Thread.State.TERMINATED ->
                    throw AssertionError("waiter terminated before parking; acquire did not wait")
                else -> {
                    if (System.nanoTime() >= deadline) {
                        throw AssertionError("waiter never parked; state=${thread.state}")
                    }
                    Thread.onSpinWait()
                }
            }
        }
    }

    /**
     * P5 event recorder. Counters instead of lists for the release/orphan side: the stress
     * test produces tens of thousands of releases, where list appends would be quadratic.
     */
    private class P5RecordingEvents : CacheEvents {
        val finished = ConcurrentLinkedQueue<Pair<RefreshResult, Long?>>()
        val released = AtomicInteger()
        val orphaned = AtomicInteger()
        val orphanPermits = Semaphore(0)
        val unavailable = ConcurrentLinkedQueue<AcquireUnavailableReason>()

        override fun refreshFinished(group: GroupId, result: RefreshResult, generation: Long?) {
            finished += result to generation
        }

        override fun leaseReleased(group: GroupId, lease: LeaseInfo, heldFor: Duration) {
            released.incrementAndGet()
        }

        override fun leaseOrphaned(group: GroupId, lease: LeaseInfo) {
            orphaned.incrementAndGet()
            orphanPermits.release()
        }

        override fun acquireUnavailable(group: GroupId, reason: AcquireUnavailableReason) {
            unavailable += reason
        }
    }
}
