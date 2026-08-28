package infra.snapshotcache.core

import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GenerationState
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.NoOpHooks
import infra.snapshotcache.spi.OpenGeneration
import java.time.Clock
import java.time.Duration
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Lifecycle of one generation (plan 2.5). GONE is terminal; the registry drops the record. */
internal enum class Lifecycle { BUILDING, OPENING, LIVE, RECLAIMING, GONE }

/**
 * Registry-level lease on one generation. P3 wraps it into the `api.Snapshot` handle.
 * [generation] never changes for the lifetime of the lease (I8).
 */
internal class RegistryLease(
    val generation: Long,
    val info: LeaseInfo,
    /** Attached generation this lease pins; set at publish, so a LIVE record always has one. */
    val opened: OpenGeneration,
    /** What the pinned generation contains; set at publish alongside [opened]. */
    val generationInfo: GenerationInfo,
) {
    /** Guarded by the owning registry's lock; makes [GenerationRegistry.release] idempotent. */
    var released = false
}

/**
 * The deep module: all mutable state - generation table, current pointer, refcounts,
 * leases, shutting-down flag - behind one lock. It never performs I/O and never calls
 * [infra.snapshotcache.spi.GenerationStore]; storage effects are decided here and
 * executed by the orchestrator outside the lock, with the [Lifecycle] transitional
 * states carrying invariants across the gap (plan 2.5).
 *
 * [HookRunner] calls always happen outside the lock: tests park on latches there,
 * and a latch under the lock would deadlock the suite.
 */
internal class GenerationRegistry(
    private val maxLive: Int,
    private val leaseDeadline: Duration,
    private val clock: Clock,
    private val hooks: HookRunner = NoOpHooks,
) {
    private val lock = ReentrantLock()
    private val published = lock.newCondition()

    private val records = sortedMapOf<Long, GenRecord>()
    private var nextGeneration = 0L
    private var currentGen: Long? = null
    private var shuttingDown = false
    private var roundInProgress = false
    private var consecutiveVerifyFailures = 0

    private class GenRecord(val generation: Long) {
        var state = Lifecycle.BUILDING
        var refCount = 0
        var fileBytes = 0L
        var opened: OpenGeneration? = null
        var info: GenerationInfo? = null
        val leases = mutableListOf<RegistryLease>()
    }

    // ---- build path (RefreshCycle, P4) ----

    /**
     * Overlap guard (spec 4.4): true begins the round; false means one is already running
     * and this trigger must be skipped. Registry state per plan 2.5 - the flag is mutable
     * state, so it lives under the one monitor.
     */
    fun tryBeginRound(): Boolean = lock.withLock {
        if (roundInProgress) {
            false
        } else {
            roundInProgress = true
            true
        }
    }

    /** Ends the round begun by a successful [tryBeginRound]; called in a finally. */
    fun endRound(): Unit = lock.withLock {
        roundInProgress = false
    }

    /** Increments the consecutive verify-failure counter and returns the new count (spec 8.5). */
    fun recordVerifyFailure(): Int = lock.withLock {
        ++consecutiveVerifyFailures
    }

    /** Resets the consecutive verify-failure counter on a successful publish (spec 8.5). */
    fun resetVerifyFailures(): Unit = lock.withLock {
        consecutiveVerifyFailures = 0
    }

    /** Allocates the next generation number (strictly increasing, I3) and registers it BUILDING. */
    fun beginBuild(): Long = lock.withLock {
        val gen = ++nextGeneration
        records[gen] = GenRecord(gen)
        gen
    }

    /**
     * BUILDING or OPENING -> GONE. Candidate file deletion happens outside, after this
     * call. OPENING is legal because a round can fail after [beginPublish] - promote,
     * attach or verify - and its cleanup uses the same edge (spec 9.2).
     */
    fun discardBuild(gen: Long): Unit = lock.withLock {
        val record = records.getValue(gen)
        check(record.state == Lifecycle.BUILDING || record.state == Lifecycle.OPENING) {
            "generation $gen is ${record.state}, expected BUILDING or OPENING"
        }
        record.state = Lifecycle.GONE
        records.remove(gen)
    }

    /** BUILDING -> OPENING. Promote + attach run outside, between this and [publish]. */
    fun beginPublish(gen: Long): Unit = lock.withLock {
        transition(gen, Lifecycle.BUILDING, Lifecycle.OPENING)
    }

    /**
     * OPENING -> LIVE with the attached [OpenGeneration] and its [GenerationInfo], which
     * [tryAcquire] copies onto every lease of [gen] and [currentInfo] reports. The
     * generation becomes current and every [awaitCurrent] waiter is signalled.
     *
     * [OpenGeneration.fileBytes] is captured before taking the lock - on the real adapter
     * it is a file stat, and no I/O runs under the lock (plan 2.5).
     */
    fun publish(gen: Long, opened: OpenGeneration, info: GenerationInfo) {
        val fileBytes = opened.fileBytes()
        lock.withLock {
            val record = transition(gen, Lifecycle.OPENING, Lifecycle.LIVE)
            record.fileBytes = fileBytes
            record.opened = opened
            record.info = info
            currentGen = gen
            published.signalAll()
        }
        hooks.at(Hook.AFTER_POINTER_SWAP)
    }

    // ---- consumer path ----

    /**
     * Returns a lease on the current generation, or null if none exists. Reading the
     * pointer and incrementing the refcount happen in one critical section (spec 5.1):
     * the pointer read before [Hook.AFTER_READ_CURRENT] is re-taken afterwards, so a
     * full publish + reclaim cycle interleaved at the hook still yields a LIVE
     * generation whose refcount is counted before any detach can be decided (I2).
     *
     * Also null once shutdown has begun, decided in the same critical section: a caller
     * that checked the flag and was then preempted must not still be granted a lease over
     * a store the drain has already reported clean (spec 10.2 step 1).
     */
    fun tryAcquire(owner: String): RegistryLease? {
        lock.withLock { currentGen }
        hooks.at(Hook.AFTER_READ_CURRENT)
        return lock.withLock {
            if (shuttingDown) return null
            val gen = currentGen ?: return null
            val record = records.getValue(gen)
            check(record.state == Lifecycle.LIVE) { "current generation $gen is ${record.state}, not LIVE" }
            // LIVE is reachable only through publish, which sets both.
            val opened = checkNotNull(record.opened) { "LIVE generation $gen carries no OpenGeneration" }
            val info = checkNotNull(record.info) { "LIVE generation $gen carries no GenerationInfo" }
            record.refCount++
            val acquiredAt = clock.instant()
            val lease = RegistryLease(
                gen,
                LeaseInfo(owner, acquiredAt, acquiredAt.plus(leaseDeadline)),
                opened,
                info,
            )
            record.leases += lease
            lease
        }
    }

    /**
     * Idempotent per lease instance: the refcount is decremented exactly once (I6).
     * Every effective release signals the condition so [awaitQuiescence] re-checks;
     * orphan releases route through here too, so they signal as well (spec 10.2 step 4).
     */
    fun release(lease: RegistryLease): Unit = lock.withLock {
        if (lease.released) return
        lease.released = true
        val record = records.getValue(lease.generation)
        record.refCount--
        record.leases.remove(lease)
        check(record.refCount >= 0) { "refcount of generation ${lease.generation} went negative" }
        published.signalAll()
    }

    /**
     * Waits interruptibly, bounded by [budget], until a current generation exists or
     * shutdown begins - signalled by [publish] and [beginShutdown], never polled
     * (spec 9.3). Returns true iff a current generation exists on exit.
     */
    fun awaitCurrent(budget: Duration): Boolean {
        lock.withLock {
            var remaining = budget.toNanos()
            while (currentGen == null && !shuttingDown && remaining > 0) {
                remaining = published.awaitNanos(remaining)
            }
            return currentGen != null
        }
    }

    // ---- K enforcement / GC ----

    /**
     * Non-null means live generations exceed K and refresh must pause (spec 6.1, I4):
     * the returned leases are the ones holding non-current generations alive. Never
     * throws - being over K is an explicit, reported state.
     */
    fun blockedByK(): List<LeaseInfo>? = lock.withLock {
        val live = records.values.filter { it.state == Lifecycle.LIVE }
        if (live.size <= maxLive) return null
        live.filter { it.generation != currentGen }.flatMap { record -> record.leases.map { it.info } }
    }

    /**
     * Marks every eligible generation - non-current, LIVE, refcount == 0 - RECLAIMING,
     * making it invisible to [tryAcquire] from then on. Detach + delete run outside.
     */
    fun beginReclaim(): List<Long> {
        val marked = lock.withLock {
            records.values
                .filter { it.state == Lifecycle.LIVE && it.generation != currentGen && it.refCount == 0 }
                .map { it.state = Lifecycle.RECLAIMING; it.generation }
        }
        hooks.at(Hook.BEFORE_DETACH)
        return marked
    }

    /** RECLAIMING -> GONE: detach + delete succeeded outside. */
    fun reclaimed(gen: Long): Unit = lock.withLock {
        remove(gen, Lifecycle.RECLAIMING)
    }

    /** RECLAIMING -> LIVE: DETACH failed (spec 9.2); the generation is retried next pass. */
    fun deferReclaim(gen: Long): Unit = lock.withLock {
        transition(gen, Lifecycle.RECLAIMING, Lifecycle.LIVE)
    }

    // ---- state / shutdown ----

    fun current(): Long? = lock.withLock { currentGen }

    /** [GenerationInfo] of the current generation; null before the first publish (spec 5.1, D24). */
    fun currentInfo(): GenerationInfo? = lock.withLock {
        currentGen?.let { records.getValue(it).info }
    }

    /** Snapshot of every registered generation, for the admin view (spec 5.3, 12.7). */
    fun liveGenerations(): List<GenerationState> = lock.withLock {
        records.values.map { record ->
            GenerationState(
                generation = record.generation,
                isCurrent = record.generation == currentGen,
                refCount = record.refCount,
                fileBytes = record.fileBytes,
                leases = record.leases.map { it.info },
            )
        }
    }

    /** Leases past their deadline at `clock.instant()`. Diagnostic only - nothing is reclaimed (spec 6.2, D8). */
    fun expiredLeases(): List<LeaseInfo> = lock.withLock {
        val now = clock.instant()
        records.values.flatMap { record -> record.leases.map { it.info } }.filter { it.deadline.isBefore(now) }
    }

    /** Sets the flag and releases every [awaitCurrent] waiter at once (spec 10.2 step 1). */
    fun beginShutdown(): Unit = lock.withLock {
        shuttingDown = true
        published.signalAll()
    }

    fun isShuttingDown(): Boolean = lock.withLock { shuttingDown }

    /**
     * Waits interruptibly, bounded by [budget], until zero leases are outstanding across
     * all generations - signalled by [release], never polled (spec 10.2 step 4). Returns
     * the leases still outstanding on exit: empty iff drained. A zero or negative budget
     * returns the current outstanding snapshot immediately.
     */
    fun awaitQuiescence(budget: Duration): List<LeaseInfo> = lock.withLock {
        var remaining = budget.toNanos()
        while (remaining > 0 && records.values.any { it.leases.isNotEmpty() }) {
            remaining = published.awaitNanos(remaining)
        }
        records.values.flatMap { record -> record.leases.map { it.info } }
    }

    // ---- internals (caller must hold the lock) ----

    private fun transition(gen: Long, from: Lifecycle, to: Lifecycle): GenRecord {
        val record = records.getValue(gen)
        check(record.state == from) { "generation $gen is ${record.state}, expected $from" }
        record.state = to
        return record
    }

    private fun remove(gen: Long, from: Lifecycle) {
        transition(gen, from, Lifecycle.GONE)
        records.remove(gen)
    }
}
