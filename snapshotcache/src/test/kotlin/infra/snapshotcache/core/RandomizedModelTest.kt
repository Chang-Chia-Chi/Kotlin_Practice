package infra.snapshotcache.core

import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.GenerationCheck
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.NotReadyException
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.api.VerifyResult
import infra.snapshotcache.testkit.AccountingFixture
import infra.snapshotcache.testkit.InMemoryGenerationStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Random
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * P6: the randomized model test.
 *
 * Each sequence drives the REAL integration stack - facade + cycle + registry + query-stub
 * over the recording fake (the P5 wiring pattern) - with randomly generated operations
 * from the FIXED op set: acquire / close / refresh-success / refresh-failure /
 * verify-failure / gc / orphan. A simple in-test model (current pointer, live set,
 * per-generation refcounts) is updated per op, and ALL of I1-I8 are checked in their
 * observable forms after EVERY step.
 *
 * Single-threaded by design: the model test sequences operations; interleaving coverage was
 * P5's job. Zero sleeps - the only bounded waits are the orphan-Cleaner await (the one
 * permitted nondeterminism, same as P5 case 4).
 *
 * Seed policy (FIXED): [SEED] is the fixed CI seed; sequence i runs from seed SEED+i
 * (SEED+ORPHAN_SEED_OFFSET+i for the orphan run). On any failure the header prints the
 * exact per-sequence seed and the FULL operation sequence, so the failure replays exactly.
 *
 * Orphan determinism (recorded choice per the phase payload): Cleaner-forced orphaning
 * costs a System.gc() await, so the bulk run excludes it; a dedicated smaller run includes
 * it with at least one guaranteed orphan per sequence. The op appears and is model-checked;
 * the bulk run stays within CI budget.
 *
 * Accounting: the store/registry pair lives per sequence, so a
 * class-level `@RegisterExtension` cannot reach it. Instead [endOfSequence] calls
 * [AccountingFixture.verify] explicitly at the end of EVERY sequence - the fixture's
 * documented alternative to extension registration, and strictly more frequent than
 * once per test.
 */
internal class RandomizedModelTest {

    private companion object {
        const val SEED = 20260826L
        const val ORPHAN_SEED_OFFSET = 1_000_000L
        const val MAIN_SEQUENCES = 5000
        const val MAIN_OPS = 40
        const val ORPHAN_SEQUENCES = 10
        const val ORPHAN_OPS = 20
        const val K = 3
    }

    @Test
    fun model_thousandsOfRandomSequences_allInvariantsAfterEveryStep() {
        repeat(MAIN_SEQUENCES) { i -> runSequence(SEED + i, i, MAIN_OPS, withOrphan = false) }
    }

    @Test
    fun model_orphanOpIncluded_dedicatedSmallerRun() {
        repeat(ORPHAN_SEQUENCES) { i ->
            runSequence(SEED + ORPHAN_SEED_OFFSET + i, i, ORPHAN_OPS, withOrphan = true)
        }
    }

    // ------------------------------------------------------------------ one sequence

    private fun runSequence(seed: Long, index: Int, ops: Int, withOrphan: Boolean) {
        val world = World()
        val model = Model()
        val pool = mutableListOf<Held>()
        val log = mutableListOf<String>()
        try {
            val rnd = Random(seed)
            var orphanRan = false
            repeat(ops) { step ->
                val op = pickOp(rnd, pool.isNotEmpty(), withOrphan && model.current != null)
                log += "[$step] ${describe(op)}"
                when (op) {
                    Op.REFRESH_SUCCESS -> opRefresh(world, model, log, expected = null)
                    Op.REFRESH_FAILURE -> opRefresh(world, model, log, expected = RefreshResult.SOURCE_ERROR)
                    Op.VERIFY_FAILURE -> opRefresh(world, model, log, expected = RefreshResult.VERIFY_FAILED)
                    Op.ACQUIRE -> opAcquire(world, model, pool, log)
                    Op.CLOSE -> opClose(rnd, model, pool, log)
                    Op.GC -> opGc(world, model)
                    Op.ORPHAN -> {
                        opOrphan(world, model)
                        orphanRan = true
                    }
                }
                checkStep(world, model, pool)
            }
            if (withOrphan && !orphanRan) {
                // Guarantee the op appears in the dedicated run even on an unlucky draw.
                if (model.current == null) {
                    log += "[extra] refresh-success (to enable the guaranteed orphan)"
                    opRefresh(world, model, log, expected = null)
                    checkStep(world, model, pool)
                }
                log += "[extra] orphan (guaranteed once per orphan sequence)"
                opOrphan(world, model)
                checkStep(world, model, pool)
            }
            endOfSequence(world, model, pool, log)
        } catch (failure: Throwable) {
            throw AssertionError(
                buildString {
                    appendLine("MODEL TEST FAILURE - fixed base seed=$SEED, sequence #$index, per-sequence seed=$seed")
                    appendLine("replay: runSequence(seed=$seed, ops=$ops, withOrphan=$withOrphan)")
                    appendLine("full operation sequence:")
                    log.forEach { appendLine("  $it") }
                    appendLine("cause: $failure")
                },
                failure,
            )
        }
    }

    // ------------------------------------------------------------------ ops

    /**
     * One refresh round. [expected] null means healthy (SUCCESS); SOURCE_ERROR scripts the
     * source to throw; VERIFY_FAILED scripts the caller check to fail. The model mirrors
     * the cycle's K guard exactly: over K it reclaims eligibles first, then blocks.
     */
    private fun opRefresh(world: World, model: Model, log: MutableList<String>, expected: RefreshResult?) {
        val blocked = model.guardBlocksAndReclaim()
        when (expected) {
            RefreshResult.SOURCE_ERROR ->
                world.source.behavior = { throw IllegalStateException("model: scripted source failure") }
            RefreshResult.VERIFY_FAILED -> world.failVerify.set(true)
            else -> {}
        }
        val outcome = try {
            world.cache.triggerRefresh(world.group)
        } finally {
            world.source.behavior = {}
            world.failVerify.set(false)
        }
        log += "      -> ${outcome.result} gen=${outcome.generation}"
        if (blocked) {
            assertThat(outcome.result)
                .describedAs("model predicts BLOCKED_BY_K (live > K with every non-current gen leased)")
                .isEqualTo(RefreshResult.BLOCKED_BY_K)
            return
        }
        if (expected != null) {
            // I7 in its observable form is the unchanged model state, asserted by checkStep.
            assertThat(outcome.result).describedAs("scripted failure round").isEqualTo(expected)
            return
        }
        assertThat(outcome.result)
            .describedAs("healthy round must publish (detail=%s)", outcome.detail)
            .isEqualTo(RefreshResult.SUCCESS)
        val gen = checkNotNull(outcome.generation) { "SUCCESS outcome must carry its generation" }
        assertThat(gen)
            .describedAs("I3: published generation numbers strictly increasing")
            .isGreaterThan(model.lastPublished)
        model.lastPublished = gen
        model.published += gen
        model.refCounts[gen] = 0
        model.current = gen
        model.reclaimEligible() // the cycle's post-publish GC
    }

    private fun opAcquire(world: World, model: Model, pool: MutableList<Held>, log: MutableList<String>) {
        val current = model.current
        if (current == null) {
            val thrown = catchThrowable { world.cache.acquire(world.group, Duration.ZERO) }
            log += "      -> NotReadyException (model knows nothing is published)"
            assertThat(thrown)
                .describedAs("acquire before any publish must fail fast (spec 9.2)")
                .isInstanceOf(NotReadyException::class.java)
            assertThat((thrown as NotReadyException).reason).isEqualTo(AcquireUnavailableReason.NOT_READY)
            return
        }
        val snap = world.cache.acquire(world.group, Duration.ZERO)
        log += "      -> handle on gen=${snap.generation}"
        assertThat(snap.generation).describedAs("acquire must hand out the current generation").isEqualTo(current)
        model.refCounts[current] = model.refCounts.getValue(current) + 1
        pool += Held(snap, current)
    }

    /** Closes a random held handle; picking an already-closed one is the double-close no-op case. */
    private fun opClose(rnd: Random, model: Model, pool: MutableList<Held>, log: MutableList<String>) {
        val held = pool[rnd.nextInt(pool.size)]
        val doubleClose = held.closed
        held.snap.close()
        log += "      -> close gen=${held.gen}${if (doubleClose) " (double close: must be a no-op)" else ""}"
        if (!held.closed) {
            held.closed = true
            model.refCounts[held.gen] = model.refCounts.getValue(held.gen) - 1
        }
        if (pool.size > 30) pool.removeAll { it.closed }
    }

    private fun opGc(world: World, model: Model) {
        world.cache.gc(world.group)
        model.reclaimEligible()
    }

    /**
     * Drops a handle without close and awaits the Cleaner's forced release:
     * bounded await on the orphan event while forcing GC - the one permitted
     * nondeterminism (the P3/P5 precedent). Net model change: released +1 orphan,
     * refcounts unchanged.
     */
    private fun opOrphan(world: World, model: Model) {
        checkNotNull(model.current) { "orphan op requires a published generation" }
        val before = world.events.orphaned.get()
        acquireAndDrop(world)
        var fired = false
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        while (!fired && System.nanoTime() < deadline) {
            System.gc()
            fired = world.events.orphanPermits.tryAcquire(100, TimeUnit.MILLISECONDS)
        }
        assertThat(fired).describedAs("the Cleaner must force-release the dropped handle").isTrue()
        assertThat(world.events.orphaned.get())
            .describedAs("orphan counter incremented by exactly 1")
            .isEqualTo(before + 1)
    }

    /** Separate frame so the acquired handle is unreachable the moment this returns. */
    private fun acquireAndDrop(world: World) {
        world.cache.acquire(world.group, Duration.ZERO)
    }

    // ------------------------------------------------------------------ invariant sweep

    /**
     * Model vs observables plus I1-I8 in their observable forms, after every op.
     * I3 (strict increase) and I7 (failure leaves the model untouched, which this sweep
     * then compares against reality) are enforced in [opRefresh]; I5 is "eventually
     * deleted", checked at sequence end after a final gc.
     */
    private fun checkStep(world: World, model: Model, pool: List<Held>) {
        val live = world.registry.liveGenerations()

        // I6: refcount never negative.
        live.forEach {
            assertThat(it.refCount)
                .describedAs("I6: refcount of generation %d never negative", it.generation)
                .isGreaterThanOrEqualTo(0)
        }

        // Model vs registry: live set, refcounts, current pointer.
        assertThat(live.map { it.generation }.toSet())
            .describedAs("model live set vs registry")
            .isEqualTo(model.refCounts.keys)
        live.forEach {
            assertThat(it.refCount)
                .describedAs("model refcount of generation %d", it.generation)
                .isEqualTo(model.refCounts.getValue(it.generation))
        }
        assertThat(world.registry.current()).describedAs("model current pointer").isEqualTo(model.current)
        assertThat(world.cache.currentInfo(world.group)?.generation)
            .describedAs("currentInfo mirrors the current pointer")
            .isEqualTo(model.current)

        // I1: current only ever points to a generation published after a passed verify.
        world.registry.current()?.let {
            assertThat(model.published)
                .describedAs("I1: current %d must be a verified, published generation", it)
                .contains(it)
        }

        // I2 + I7: every live (and so every leased) generation is opened, never closed or
        // deleted; nothing else - no candidate residue - is opened or on disk.
        assertThat(world.store.openedGenerations())
            .describedAs("I2: opened generations must be exactly the live set")
            .isEqualTo(model.refCounts.keys)
        assertThat(world.store.generationsOnDisk())
            .describedAs("I7: on-disk files must be exactly the live set (no candidate residue)")
            .isEqualTo(model.refCounts.keys)

        // I4: live <= K, except the explicit blocked state. A close can leave live > K with
        // a reclaimable generation until the next GC pass runs (I5 is "eventually"), so the
        // excess must be explained by leases after ONE corrective gc - the P5 sweep pattern.
        if (live.size > K && !live.filter { !it.isCurrent }.all { it.refCount > 0 }) {
            world.cache.gc(world.group)
            model.reclaimEligible()
            val after = world.registry.liveGenerations()
            assertThat(after.size <= K || after.filter { !it.isCurrent }.all { it.refCount > 0 })
                .describedAs("I4: live=%d > K=%d not explained by leases even after a GC pass", after.size, K)
                .isTrue()
            assertThat(after.map { it.generation }.toSet())
                .describedAs("model live set vs registry after the corrective GC pass")
                .isEqualTo(model.refCounts.keys)
        }

        // I8: every held handle still reports the generation it was acquired on.
        pool.forEach {
            assertThat(it.snap.generation)
                .describedAs("I8: handle acquired on generation %d must never move", it.gen)
                .isEqualTo(it.gen)
        }
    }

    /** I5: with every lease released, one final gc leaves exactly the current generation. */
    private fun endOfSequence(world: World, model: Model, pool: List<Held>, log: MutableList<String>) {
        log += "[end] close all handles + final gc (I5 + spec 17.3 accounting)"
        pool.forEach { held ->
            held.snap.close()
            if (!held.closed) {
                held.closed = true
                model.refCounts[held.gen] = model.refCounts.getValue(held.gen) - 1
            }
        }
        world.cache.gc(world.group)
        model.reclaimEligible()
        val survivors = world.registry.liveGenerations().map { it.generation }
        assertThat(survivors)
            .describedAs("I5: every non-current, unleased generation is eventually deleted")
            .isEqualTo(listOfNotNull(model.current))
        assertThat(world.store.openedGenerations()).isEqualTo(setOfNotNull(model.current))
        world.fixture.verify()
    }

    // ------------------------------------------------------------------ op generation

    private enum class Op(val weight: Int) {
        REFRESH_SUCCESS(4), ACQUIRE(4), CLOSE(4), REFRESH_FAILURE(2), VERIFY_FAILURE(2), GC(2), ORPHAN(1)
    }

    private fun pickOp(rnd: Random, closeable: Boolean, orphanable: Boolean): Op {
        val candidates = Op.entries.filter {
            when (it) {
                Op.CLOSE -> closeable
                Op.ORPHAN -> orphanable
                else -> true
            }
        }
        var roll = rnd.nextInt(candidates.sumOf { it.weight })
        for (op in candidates) {
            roll -= op.weight
            if (roll < 0) return op
        }
        return candidates.last()
    }

    private fun describe(op: Op): String = op.name.lowercase().replace('_', '-')

    // ------------------------------------------------------------------ model + world

    /** The model state: current pointer, live set with per-generation refcounts. */
    private class Model {
        var current: Long? = null
        var lastPublished = 0L
        val refCounts = mutableMapOf<Long, Int>()
        val published = mutableSetOf<Long>()

        /** The reclaim pass: every non-current generation with refcount 0 is deleted. */
        fun reclaimEligible() {
            refCounts.keys.filter { it != current && refCounts.getValue(it) == 0 }
                .forEach { refCounts.remove(it) }
        }

        /** Mirrors RefreshCycle.round()'s K guard: over K -> reclaim pass first, then re-check. */
        fun guardBlocksAndReclaim(): Boolean {
            if (refCounts.size <= K) return false
            reclaimEligible()
            return refCounts.size > K
        }
    }

    private class Held(val snap: Snapshot, val gen: Long) {
        var closed = false
    }

    private class ModelEvents : CacheEvents {
        val orphaned = AtomicInteger()
        val orphanPermits = Semaphore(0)

        override fun leaseOrphaned(group: GroupId, lease: LeaseInfo) {
            orphaned.incrementAndGet()
            orphanPermits.release()
        }
    }

    /** One fresh integration stack per sequence - the P5 wiring pattern over the P2/P4 kit. */
    private class World {
        val group = GroupId("orders")
        val store = InMemoryGenerationStore()
        val script = QueryScript().apply {
            tables["t_a"] = 10L
            tables["t_b"] = 20L
        }
        val stubStore = QueryStubGenerationStore(store, script)
        val clock = MutableTestClock(Instant.parse("2026-01-01T00:00:00Z"))
        val registry = GenerationRegistry(K, Duration.ofMinutes(5), clock)
        val events = ModelEvents()
        val source = ScriptedSource()
        val failVerify = AtomicBoolean(false)
        val config = SnapshotCacheConfig(
            storagePath = Path.of("unused-storage"),
            tempDirectory = Path.of("unused-temp"),
        )
        val cycle = RefreshCycle(
            group = group,
            registry = registry,
            store = stubStore,
            source = source,
            config = config,
            events = events,
            checks = listOf(
                GenerationCheck { _, _ ->
                    if (failVerify.get()) VerifyResult.Fail("model_rule", "scripted model failure") else VerifyResult.Pass
                },
            ),
            clock = clock,
        )
        val cache = DefaultSnapshotCache(config, mapOf(group to GroupRuntime(registry, stubStore, cycle)), events, clock)
        val fixture = AccountingFixture(store).apply {
            currentGeneration = { registry.current() }
            refCounts = { registry.liveGenerations().associate { it.generation to it.refCount } }
        }
    }
}
