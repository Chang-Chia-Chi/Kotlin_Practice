package infra.snapshotcache.core

import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.CopyOutResult
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.NotReadyException
import infra.snapshotcache.api.ShuttingDownException
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.testkit.AccountingFixture
import infra.snapshotcache.testkit.ConnectionTracker
import infra.snapshotcache.testkit.InMemoryGenerationStore
import infra.snapshotcache.testkit.ScriptedFailureException
import infra.snapshotcache.testkit.StoreOp
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.nio.file.Path
import java.sql.Connection
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * P3 tests for [DefaultSnapshotCache] (plan P3; spec 5.1 waitBudget and acquire atomicity,
 * spec 6.3 orphan safety net, spec 9.2/9.3 acquire-before-readiness rows, spec 10.2 step 1).
 *
 * Tests run against the frozen api plus the pinned P3 construction surface only; the fake
 * store fakes at the spi boundary. Acquire atomicity itself is proven at the registry level
 * by P1's AFTER_READ_CURRENT test; the facade-level publish+GC-mid-acquire interleaving
 * belongs to P5's six-case suite and is deliberately absent here.
 *
 * No sleeps. Joins, future gets and semaphore waits are bounds on broken implementations,
 * never sequencing. The one permitted nondeterminism is the Cleaner orphan test, bounded
 * by awaiting the orphan event while forcing GC.
 */
class DefaultSnapshotCacheTest {

    private val t0: Instant = Instant.parse("2026-01-01T00:00:00Z")
    private val leaseDeadline: Duration = Duration.ofMinutes(5)
    private val group = GroupId("orders")

    private val store = InMemoryGenerationStore()
    private val clock = MutableClock(t0)
    private val registry = GenerationRegistry(3, leaseDeadline, clock)
    private val events = RecordingCacheEvents()

    @RegisterExtension
    @JvmField
    val accounting = AccountingFixture(store).apply {
        currentGeneration = { registry.current() }
        refCounts = { registry.liveGenerations().associate { it.generation to it.refCount } }
    }

    private val config = SnapshotCacheConfig(
        storagePath = Path.of("unused-storage"),
        tempDirectory = Path.of("unused-temp"),
    )

    private val cache = DefaultSnapshotCache(config, mapOf(group to GroupRuntime(registry, store)), events, clock)

    /** Pinned staging path: drives the full build->promote->open->publish chain so the accounting equations hold. */
    private fun publishGen(
        dataAsOf: Instant = t0.minusSeconds(600),
        rowCounts: Map<String, Long> = mapOf("t_a" to 10L, "t_b" to 20L),
    ): Long {
        val gen = registry.beginBuild()
        store.createCandidate(gen).close()
        registry.beginPublish(gen)
        store.promote(gen)
        val opened = store.open(gen)
        registry.publish(gen, opened, GenerationInfo(gen, dataAsOf, clock.instant(), rowCounts))
        return gen
    }

    private fun refCountOf(gen: Long): Int? =
        registry.liveGenerations().firstOrNull { it.generation == gen }?.refCount

    // ------------------------------------------------------------------ construction / currentInfo

    @Test
    fun unknownGroup_everyEntryPointThrowsIllegalArgument() {
        val nope = GroupId("nope")
        val spec = CopyOutSpec("select 1", "tgt", ConnectionTracker().issue("caller target").connection)
        assertThatThrownBy { cache.acquire(nope) }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { cache.withSnapshot(nope) { throw AssertionError("block must not run") } }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { cache.copyOut(nope, spec) }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { cache.currentInfo(nope) }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun currentInfo_nullBeforeFirstPublish_thenReportsInfoWithoutTakingLease() {
        assertThat(cache.currentInfo(group)).describedAs("nothing published yet (spec 5.1, D24)").isNull()

        val dataAsOf = t0.minusSeconds(300)
        val rows = mapOf("t_a" to 7L, "t_b" to 8L)
        val gen = publishGen(dataAsOf = dataAsOf, rowCounts = rows)

        assertThat(cache.currentInfo(group)).isEqualTo(GenerationInfo(gen, dataAsOf, t0, rows))
        assertThat(refCountOf(gen)).describedAs("currentInfo must not take a lease").isEqualTo(0)
        assertThat(events.released).isEmpty()
        assertThat(events.acquireWaited).isEmpty()
        assertThat(events.unavailable).isEmpty()
    }

    // ------------------------------------------------------------------ acquire / close (steady state)

    @Test
    fun acquire_immediateWhenCurrentExists_bindsGenerationDataAsOfAndOwner() {
        val dataAsOf = t0.minusSeconds(120)
        val gen = publishGen(dataAsOf = dataAsOf)

        val snap = cache.acquire(group)
        assertThat(snap.generation).isEqualTo(gen)
        assertThat(snap.dataAsOf).isEqualTo(dataAsOf)
        assertThat(refCountOf(gen)).isEqualTo(1)
        val lease = registry.liveGenerations().single().leases.single()
        assertThat(lease.owner).describedAs("lease owner is the acquiring thread's name (pinned)")
            .isEqualTo(Thread.currentThread().name)
        assertThat(lease.acquiredAt).isEqualTo(t0)
        assertThat(lease.deadline).isEqualTo(t0.plus(leaseDeadline))

        snap.close()
        assertThat(refCountOf(gen)).isEqualTo(0)
        assertThat(events.acquireWaited).describedAs("no wait happened, so none may be recorded").isEmpty()
        assertThat(events.unavailable).isEmpty()
    }

    @Test
    fun snapshotClose_idempotent_releasesOnce_closesIssuedConnections() {
        val gen = publishGen()
        val snap = cache.acquire(group)
        val c1 = snap.connection()
        val c2 = snap.connection()
        assertThat(c1.isClosed).isFalse()

        snap.close()
        assertThat(c1.isClosed).describedAs("close() must close every connection the snapshot issued").isTrue()
        assertThat(c2.isClosed).isTrue()
        assertThat(refCountOf(gen)).isEqualTo(0)

        snap.close() // I6 through the api: idempotent, releases exactly once
        snap.close()
        assertThat(refCountOf(gen)).isEqualTo(0)
        assertThat(events.released).describedAs("leaseReleased fires once per lease, not per close call").hasSize(1)
        assertThat(events.orphaned).isEmpty()
        assertThat(store.tracker.unclosed()).isEmpty()
    }

    @Test
    fun leaseReleased_reportsHeldForViaInjectedClock() {
        publishGen()
        val snap = cache.acquire(group)
        clock.advance(Duration.ofSeconds(3))
        snap.close()

        val (lease, heldFor) = events.released.single()
        assertThat(heldFor).isEqualTo(Duration.ofSeconds(3))
        assertThat(lease.owner).isEqualTo(Thread.currentThread().name)
        assertThat(lease.acquiredAt).isEqualTo(t0)
        assertThat(lease.deadline).isEqualTo(t0.plus(leaseDeadline))
    }

    // ------------------------------------------------------------------ waitBudget (spec 5.1, 9.3, D21/D22)

    @Test
    fun waitBudgetZero_failsFastWithoutBlocking_underTwoThreadPool_thenUsableAfterPublish() {
        // Spec 17.8 scenario: two zero-budget acquires on a 2-thread scheduler pool must
        // both fail fast; the pool must not be exhausted by blocked acquires.
        val pool = Executors.newFixedThreadPool(2)
        try {
            val submitted = (1..2).map {
                pool.submit<Throwable?> { catchThrowable { cache.acquire(group, Duration.ZERO) } }
            }
            submitted.forEach { future ->
                val thrown = future.get(5, TimeUnit.SECONDS) // bound: a blocking acquire would time this out
                assertThat(thrown).isInstanceOf(NotReadyException::class.java)
                assertThat((thrown as NotReadyException).reason).isEqualTo(AcquireUnavailableReason.NOT_READY)
                assertThat(thrown.group).isEqualTo(group)
            }
            assertThat(pool.submit<String> { "free" }.get(5, TimeUnit.SECONDS))
                .describedAs("both pool threads must be free again").isEqualTo("free")
        } finally {
            pool.shutdownNow()
        }

        assertThatThrownBy { cache.withSnapshot(group, Duration.ZERO) { throw AssertionError("block must not run") } }
            .isInstanceOf(NotReadyException::class.java)
        assertThat(events.unavailable).containsExactly(
            AcquireUnavailableReason.NOT_READY,
            AcquireUnavailableReason.NOT_READY,
            AcquireUnavailableReason.NOT_READY,
        )

        // Spec 9.2 "No generation yet, waitBudget == 0": back to a usable state after publish.
        val gen = publishGen()
        cache.acquire(group, Duration.ZERO).use { assertThat(it.generation).isEqualTo(gen) }
    }

    @Test
    fun waitBudget_defaultsToConfigValue_perCall() {
        assertThat(cache.defaultWaitBudget).isEqualTo(Duration.ofSeconds(30))

        // A zero config default must flow through as the per-call default: NOT_READY, not TIMEOUT.
        val zeroDefault = DefaultSnapshotCache(
            config.copy(defaultWaitBudget = Duration.ZERO),
            mapOf(group to GroupRuntime(registry, store)),
            events,
            clock,
        )
        val thrown = catchThrowable { zeroDefault.acquire(group) }
        assertThat(thrown).isInstanceOf(NotReadyException::class.java)
        assertThat((thrown as NotReadyException).reason).isEqualTo(AcquireUnavailableReason.NOT_READY)
    }

    @Test
    fun positiveBudget_waiterReturnsOnPublish_recordsAcquireWaited() {
        val outcome = AtomicReference<Any?>()
        val waiter = Thread({
            outcome.set(runCatching { cache.acquire(group, Duration.ofSeconds(30)) }.getOrElse { it })
        }, "budget-waiter")
        waiter.isDaemon = true
        waiter.start()
        awaitParked(waiter)

        val gen = publishGen()

        waiter.join(5_000) // bound: publish must signal well before the 30s budget
        assertThat(waiter.isAlive).describedAs("publish must release the waiter at once").isFalse()
        val result = outcome.get()
        assertThat(result).describedAs("waited acquire must succeed once a generation publishes, got %s", result)
            .isInstanceOf(Snapshot::class.java)
        val snap = result as Snapshot
        assertThat(snap.generation).isEqualTo(gen)
        snap.close()

        assertThat(events.acquireWaited)
            .describedAs("an acquire that actually waited must record snapshot_acquire_waited_seconds").hasSize(1)
        assertThat(events.unavailable).isEmpty()
        assertThat(events.released.single().first.owner).isEqualTo("budget-waiter")
    }

    @Test
    fun positiveBudget_expiry_throwsTimeout_thenUsableAfterPublish() {
        val thrown = catchThrowable { cache.acquire(group, Duration.ofMillis(100)) }
        assertThat(thrown).isInstanceOf(NotReadyException::class.java)
        assertThat((thrown as NotReadyException).reason)
            .describedAs("budget expiry must be counted reason=timeout (spec 9.2)")
            .isEqualTo(AcquireUnavailableReason.TIMEOUT)
        assertThat(events.unavailable).containsExactly(AcquireUnavailableReason.TIMEOUT)

        // Spec 9.2 "No generation yet, waitBudget > 0": back to a usable state after publish.
        val gen = publishGen()
        cache.acquire(group).use { assertThat(it.generation).isEqualTo(gen) }
    }

    @Test
    fun positiveBudget_waitIsInterruptible_releasedPromptlyWithTimeout_interruptFlagReset() {
        val outcome = AtomicReference<Throwable?>()
        val flagSet = AtomicReference<Boolean>()
        val waiter = Thread({
            outcome.set(catchThrowable { cache.acquire(group, Duration.ofSeconds(30)) })
            flagSet.set(Thread.currentThread().isInterrupted)
        }, "interrupted-waiter")
        waiter.isDaemon = true
        waiter.start()
        awaitParked(waiter)

        waiter.interrupt() // registry NOT shutting down: the not-shutting-down branch

        waiter.join(5_000) // bound: an uninterruptible wait would serve out the 30s budget (spec 9.3)
        assertThat(waiter.isAlive).describedAs("interrupt must release the waiter promptly").isFalse()
        val thrown = outcome.get()
        assertThat(thrown).isInstanceOf(NotReadyException::class.java)
        assertThat((thrown as NotReadyException).reason).isEqualTo(AcquireUnavailableReason.TIMEOUT)
        assertThat(flagSet.get())
            .describedAs("the interrupt flag must be re-set inside the waiter before it exits").isTrue()
    }

    // ------------------------------------------------------------------ shutdown (spec 10.2 step 1)

    @Test
    fun shutdown_releasesParkedWaiterImmediately_withShuttingDownException() {
        val outcome = AtomicReference<Throwable?>()
        val waiter = Thread({
            outcome.set(catchThrowable { cache.acquire(group, Duration.ofSeconds(30)) })
        }, "shutdown-waiter")
        waiter.isDaemon = true
        waiter.start()
        awaitParked(waiter)

        registry.beginShutdown()

        waiter.join(5_000) // bound: release is instant, never serving out the 30s budget
        assertThat(waiter.isAlive).describedAs("shutdown must release the waiter at once").isFalse()
        assertThat(outcome.get()).isInstanceOf(ShuttingDownException::class.java)
        assertThat(events.unavailable).containsExactly(AcquireUnavailableReason.SHUTTING_DOWN)
    }

    @Test
    fun shutdown_refusesNewCallsOnEveryEntryPoint() {
        publishGen() // a published generation does not soften the refusal (spec 10.2 step 1)
        registry.beginShutdown()

        val spec = CopyOutSpec("select 1", "tgt", ConnectionTracker().issue("caller target").connection)
        assertThatThrownBy { cache.acquire(group) }.isInstanceOf(ShuttingDownException::class.java)
        assertThatThrownBy { cache.withSnapshot(group) { throw AssertionError("block must not run") } }
            .isInstanceOf(ShuttingDownException::class.java)
        assertThatThrownBy { cache.copyOut(group, spec) }.isInstanceOf(ShuttingDownException::class.java)
        assertThat(events.unavailable).containsExactly(
            AcquireUnavailableReason.SHUTTING_DOWN,
            AcquireUnavailableReason.SHUTTING_DOWN,
            AcquireUnavailableReason.SHUTTING_DOWN,
        )
    }

    // ------------------------------------------------------------------ withSnapshot (spec 5.1, D9)

    @Test
    fun withSnapshot_returnsBlockResult_releasesLeaseAndConnections() {
        val dataAsOf = t0.minusSeconds(60)
        val gen = publishGen(dataAsOf = dataAsOf)
        var conn: Connection? = null

        val result = cache.withSnapshot(group) { snap ->
            assertThat(snap.generation).isEqualTo(gen)
            assertThat(snap.dataAsOf).isEqualTo(dataAsOf)
            assertThat(refCountOf(gen)).isEqualTo(1)
            conn = snap.connection() // deliberately not closed by the block
            "block-result"
        }

        assertThat(result).isEqualTo("block-result")
        assertThat(refCountOf(gen)).describedAs("scope exit must release the lease").isEqualTo(0)
        assertThat(conn!!.isClosed).describedAs("scope exit must close snapshot-issued connections").isTrue()
        assertThat(events.released).hasSize(1)
    }

    @Test
    fun withSnapshot_releasesLeaseWhenBlockThrows_exceptionPropagates() {
        val gen = publishGen()
        val boom = IllegalStateException("consumer failure")

        val thrown = catchThrowable { cache.withSnapshot<Nothing>(group) { throw boom } }

        assertThat(thrown).describedAs("the block's exception must propagate unchanged").isSameAs(boom)
        assertThat(refCountOf(gen)).describedAs("exception path must release the lease").isEqualTo(0)
        assertThat(store.tracker.unclosed()).isEmpty()
        // fixture verifies the accounting equations at test end
    }

    // ------------------------------------------------------------------ copyOut (spec 5.1, 6.4)

    @Test
    fun copyOut_isAcquireCopyRelease_carriesGenerationDataAsOfAndRows() {
        val dataAsOf = t0.minusSeconds(45)
        val gen = publishGen(dataAsOf = dataAsOf)
        store.copyOutRows = 42
        val spec = CopyOutSpec("select * from t_unified", "tgt", ConnectionTracker().issue("caller target").connection)

        val result = cache.copyOut(group, spec)

        assertThat(result).isEqualTo(CopyOutResult(gen, dataAsOf, 42))
        val copyCalls = store.calls().filter { it.op == StoreOp.COPY_OUT }
        assertThat(copyCalls).describedAs("the copy must be delegated to GenerationStore.copyOut").hasSize(1)
        assertThat(copyCalls.single().gen).isEqualTo(gen)
        assertThat(copyCalls.single().detail).isEqualTo("tgt")
        assertThat(refCountOf(gen)).describedAs("the lease is released immediately after the copy").isEqualTo(0)
        assertThat(events.released).hasSize(1)
    }

    @Test
    fun copyOut_releasesLeaseWhenCopyThrows_thenUsableAgain() {
        val gen = publishGen()
        store.failOnNth(StoreOp.COPY_OUT, 1)
        val spec = CopyOutSpec("select 1", "tgt", ConnectionTracker().issue("caller target").connection)

        assertThatThrownBy { cache.copyOut(group, spec) }.isInstanceOf(ScriptedFailureException::class.java)
        assertThat(refCountOf(gen)).describedAs("a failed copy must still release the lease").isEqualTo(0)

        store.copyOutRows = 7
        assertThat(cache.copyOut(group, spec).rowsCopied).describedAs("usable again after the failure").isEqualTo(7)
        assertThat(refCountOf(gen)).isEqualTo(0)
    }

    // ------------------------------------------------------------------ orphan safety net (spec 6.3)

    @Test
    fun orphanedHandle_cleanerForceReleasesExactlyOnce_neverForClosedHandles() {
        val gen = publishGen()
        cache.acquire(group).close() // closed normally: its cleaner must be disarmed
        acquireAndDrop() // never closed: the Cleaner's job

        // The one permitted nondeterminism: bounded by awaiting the orphan event while forcing GC.
        var fired = false
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        while (!fired && System.nanoTime() < deadline) {
            System.gc()
            fired = events.orphanPermits.tryAcquire(200, TimeUnit.MILLISECONDS)
        }
        assertThat(fired).describedAs("the Cleaner must force-release the dropped handle").isTrue()

        // Bounded chance for a wrong second orphan (e.g. the closed handle) to surface.
        var second = false
        repeat(3) {
            if (!second) {
                System.gc()
                second = events.orphanPermits.tryAcquire(150, TimeUnit.MILLISECONDS)
            }
        }
        assertThat(second).describedAs("only the dropped handle may orphan; close() disarms the Cleaner").isFalse()
        assertThat(events.orphaned).describedAs("orphan counter incremented exactly once").hasSize(1)
        assertThat(events.released).describedAs("only the explicit close is a normal release").hasSize(1)
        assertThat(refCountOf(gen)).describedAs("the orphan release must return the refcount (I6)").isEqualTo(0)
    }

    /** Separate frame so the returned handle is unreachable the moment this returns. */
    private fun acquireAndDrop() {
        cache.acquire(group)
    }

    // ------------------------------------------------------------------ test helpers

    /**
     * Bounded wait until [thread] is parked in a wait (the acquire's condition await).
     * Establishes "the waiter is actually waiting" before publish/shutdown - a condition
     * check with a deadline, not sequencing by sleep.
     */
    private fun awaitParked(thread: Thread) {
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

    /** Deterministic advancing clock; no real waiting (spec 17.1). */
    private class MutableClock(@Volatile private var now: Instant) : Clock() {
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = now
        fun advance(by: Duration) {
            now = now.plus(by)
        }
    }

    /** Records the P3-relevant events; [orphanPermits] lets the orphan test await the Cleaner. */
    private class RecordingCacheEvents : CacheEvents {
        val acquireWaited = CopyOnWriteArrayList<Duration>()
        val unavailable = CopyOnWriteArrayList<AcquireUnavailableReason>()
        val released = CopyOnWriteArrayList<Pair<LeaseInfo, Duration>>()
        val orphaned = CopyOnWriteArrayList<LeaseInfo>()
        val orphanPermits = Semaphore(0)

        override fun acquireWaited(group: GroupId, waited: Duration) {
            acquireWaited += waited
        }

        override fun acquireUnavailable(group: GroupId, reason: AcquireUnavailableReason) {
            unavailable += reason
        }

        override fun leaseReleased(group: GroupId, lease: LeaseInfo, heldFor: Duration) {
            released += lease to heldFor
        }

        override fun leaseOrphaned(group: GroupId, lease: LeaseInfo) {
            orphaned += lease
            orphanPermits.release()
        }
    }
}
