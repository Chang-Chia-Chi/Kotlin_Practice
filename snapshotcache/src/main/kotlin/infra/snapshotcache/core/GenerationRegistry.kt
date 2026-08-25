package infra.snapshotcache.core

import infra.snapshotcache.api.GenerationState
import infra.snapshotcache.api.Hook
import infra.snapshotcache.api.HookRunner
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.NoOpHooks
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

    private class GenRecord(val generation: Long) {
        var state = Lifecycle.BUILDING
        var refCount = 0
        var fileBytes = 0L
        val leases = mutableListOf<RegistryLease>()
    }

    // ---- build path (RefreshCycle, P4) ----

    /** Allocates the next generation number (strictly increasing, I3) and registers it BUILDING. */
    fun beginBuild(): Long = lock.withLock {
        val gen = ++nextGeneration
        records[gen] = GenRecord(gen)
        gen
    }

    /** BUILDING -> GONE. Candidate file deletion happens outside, after this call. */
    fun discardBuild(gen: Long): Unit = lock.withLock {
        remove(gen, Lifecycle.BUILDING)
    }

    /** BUILDING -> OPENING. Promote + attach run outside, between this and [publish]. */
    fun beginPublish(gen: Long): Unit = lock.withLock {
        transition(gen, Lifecycle.BUILDING, Lifecycle.OPENING)
    }

    /** OPENING -> LIVE; the generation becomes current and every [awaitCurrent] waiter is signalled. */
    fun publish(gen: Long, fileBytes: Long) {
        lock.withLock {
            val record = transition(gen, Lifecycle.OPENING, Lifecycle.LIVE)
            record.fileBytes = fileBytes
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
     */
    fun tryAcquire(owner: String): RegistryLease? {
        lock.withLock { currentGen }
        hooks.at(Hook.AFTER_READ_CURRENT)
        return lock.withLock {
            val gen = currentGen ?: return null
            val record = records.getValue(gen)
            check(record.state == Lifecycle.LIVE) { "current generation $gen is ${record.state}, not LIVE" }
            record.refCount++
            val acquiredAt = clock.instant()
            val lease = RegistryLease(gen, LeaseInfo(owner, acquiredAt, acquiredAt.plus(leaseDeadline)))
            record.leases += lease
            lease
        }
    }

    /** Idempotent per lease instance: the refcount is decremented exactly once (I6). */
    fun release(lease: RegistryLease): Unit = lock.withLock {
        if (lease.released) return
        lease.released = true
        val record = records.getValue(lease.generation)
        record.refCount--
        record.leases.remove(lease)
        check(record.refCount >= 0) { "refcount of generation ${lease.generation} went negative" }
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
