package host

import infra.etl.task.Outcome
import infra.etl.task.TaskEvent
import infra.etl.task.TriggerResult
import infra.etl.task.WiringResult
import infra.snapshotcache.api.NotReadyException
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.ShuttingDownException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

private fun ComposedHost.wired(): infra.etl.task.TaskAdmin {
    val result = start()
    check(result is WiringResult.Wired) { "wiring failed: " + result }
    return result.admin
}

/** Blocks until the named run has an outcome, or fails. No sleep: latch + bounded spin. */
private fun ComposedHost.await(name: String, runId: String): infra.etl.task.TaskOutcome {
    check(listener.ended.await(120, TimeUnit.SECONDS)) { "task never ended" }
    val deadline = System.nanoTime() + 10_000_000_000L
    while (System.nanoTime() < deadline) {
        admin.run(name, runId)?.let { return it }
        Thread.onSpinWait()
    }
    error("run " + runId + " ended but never recorded an outcome")
}

private fun ComposedHost.runTask(name: String = "wip-summary"): infra.etl.task.TaskOutcome {
    listener.ended = java.util.concurrent.CountDownLatch(1)
    val accepted = admin.trigger(name, "tester")
    check(accepted is TriggerResult.Accepted) { "not accepted: " + accepted }
    return await(name, accepted.runId)
}

class ComposedHostTest {


    // ---------------- 0. the first wall a host hits: the default verify gate demands `id`

    @Test
    fun `a source whose tables have no id column can never publish a generation`(@TempDir root: Path) {
        ComposedHost(root, source = LotSource(500, withIdColumn = false)).use { host ->
            writeShapeD(host.taskDirectory)
            host.wired()

            val outcome = host.managed.admin.triggerRefresh(host.group)
            println("[VERIFY] triggerRefresh with no id column = " + outcome)
            println("[VERIFY] verifyFailed events = " + host.cacheEvents.verifyFailures)
            assertThat(outcome.result).isEqualTo(RefreshResult.VERIFY_FAILED)
            assertThat(host.cacheEvents.verifyFailures.map { it.first }).contains("key_unique")
            assertThat(host.managed.cache.currentInfo(host.group)).isNull()
        }
    }

    // ---------------------------------------------------------------- 1. end to end

    @Test
    fun `shape D task carries rows from a real generation into the target database`(@TempDir root: Path) {
        ComposedHost(root).use { host ->
            writeShapeD(host.taskDirectory)
            host.wired()
            assertThat(host.cron.names()).containsExactly("wip-summary")

            val refresh = host.managed.admin.triggerRefresh(host.group)
            assertThat(refresh.result).isEqualTo(RefreshResult.SUCCESS)
            assertThat(refresh.generation).isEqualTo(1L)

            val outcome = host.runTask()
            assertThat(outcome.failure).isNull()
            assertThat(outcome.outcome).isEqualTo(Outcome.SUCCEEDED)

            assertThat(host.reportRows()).containsExactly(
                Triple("F11", 250L, "93750.000"),
                Triple("F12", 250L, "94125.000"),
            )
            println("[E2E] report rows = " + host.reportRows())
        }
    }

    // ---------------------------------------------------------- 2. reclaimability (spec 8.6)

    @Test
    fun `the generation a cacheCopy read becomes reclaimable after the step`(@TempDir root: Path) {
        ComposedHost(root).use { host ->
            writeShapeD(host.taskDirectory)
            host.wired()
            host.managed.admin.triggerRefresh(host.group)
            val genFile = root.resolve("cache").resolve(GROUP).resolve("gen_0000000001.db")
            assertThat(Files.exists(genFile)).isTrue()

            host.runTask()

            // Unpinned the moment the step returned: copyOut releases its own lease.
            val live = host.managed.admin.liveGenerations(host.group)
            println("[RECLAIM] after copy, liveGenerations = " + live)
            assertThat(live.single().generation).isEqualTo(1L)
            assertThat(live.single().refCount).isZero()
            assertThat(live.single().leases).isEmpty()

            // Still current, so gc defers it - reclaimable means "nothing pins it", not "gone".
            assertThat(host.managed.admin.gc(host.group).reclaimed).isEmpty()

            // Publish a successor; generation 1 is then non-current and unpinned.
            assertThat(host.managed.admin.triggerRefresh(host.group).result).isEqualTo(RefreshResult.SUCCESS)
            val afterSecond = host.managed.admin.gc(host.group)
            println("[RECLAIM] gc after second refresh = " + afterSecond)
            println("[RECLAIM] reclaimed events = " + host.cacheEvents.reclaimed)

            assertThat(host.cacheEvents.reclaimed + afterSecond.reclaimed).contains(1L)
            assertThat(afterSecond.deferred).doesNotContain(1L)
            assertThat(Files.exists(genFile)).isFalse()
        }
    }


    // ------------------- 2b. the negative half: the copy really does pin the generation

    @Test
    fun `while the copy runs the generation is pinned and gc defers it`(@TempDir root: Path) {
        ComposedHost(root, source = LotSource(2000)).use { host ->
            writeShapeD(host.taskDirectory, copySql = SLOW_COPY)
            host.wired()
            host.managed.admin.triggerRefresh(host.group)

            listenerReset(host)
            val accepted = host.admin.trigger("wip-summary", "tester") as TriggerResult.Accepted
            assertThat(host.awaitLease()).isNotNull()

            // Publish a successor while gen 1 is pinned by the running cacheCopy step.
            host.managed.admin.triggerRefresh(host.group)
            val whilePinned = host.managed.admin.gc(host.group)
            val pinnedState = host.managed.admin.liveGenerations(host.group).single { it.generation == 1L }
            println("[PINNED] gc while the copy holds gen 1 = " + whilePinned)
            println("[PINNED] gen 1 state = " + pinnedState)
            assertThat(whilePinned.reclaimed).doesNotContain(1L)
            // The finding: GcOutcome says NOTHING about a lease-pinned generation. `deferred`
            // means "the reclaim I/O failed" (RefreshCycle.kt:255-262), and beginReclaim never
            // marks a leased generation at all (GenerationRegistry.kt:244-251). So an operator
            // asking gc() why disk is not being freed gets an empty answer either way.
            assertThat(whilePinned.deferred).isEmpty()
            assertThat(whilePinned.reclaimed).isEmpty()
            assertThat(pinnedState.refCount).isEqualTo(1)
            assertThat(pinnedState.leases).hasSize(1)

            host.await("wip-summary", accepted.runId)
            val afterCopy = host.managed.admin.gc(host.group)
            println("[PINNED] gc after the copy released = " + afterCopy)
            assertThat(afterCopy.reclaimed).contains(1L)
        }
    }

    // -------------- 5. how many consumer-side DuckDB instances does composition create?

    @Test
    fun `each concurrent task opens its own scratch DuckDB instance`(@TempDir root: Path) {
        ComposedHost(root, source = LotSource(2000)).use { host ->
            writeShapeD(host.taskDirectory, name = "task-a", cron = null, copySql = SLOW_COPY)
            writeShapeD(host.taskDirectory, name = "task-b", cron = null, copySql = SLOW_COPY)
            host.wired()
            host.managed.admin.triggerRefresh(host.group)

            val a = host.admin.trigger("task-a", "tester") as TriggerResult.Accepted
            val b = host.admin.trigger("task-b", "tester") as TriggerResult.Accepted

            var peak = emptyList<String>()
            val deadline = System.nanoTime() + 60_000_000_000L
            while (System.nanoTime() < deadline) {
                val dirs = Files.list(root.resolve("scratch")).use { s -> s.map { it.fileName.toString() }.toList() }
                if (dirs.size > peak.size) peak = dirs
                if (peak.size >= 2) break
                if (host.admin.run("task-a", a.runId) != null && host.admin.run("task-b", b.runId) != null) break
                Thread.onSpinWait()
            }
            println("[SCRATCH] concurrent scratch run directories = " + peak.size + " " + peak)
            println("[SCRATCH] EtlWiring scratchMemoryLimitMb default = 4096 MB, per instance")
            println("[SCRATCH] SnapshotCacheConfig.consumerMemoryLimit default = 1GB, for 'a single shared consumer instance' (cache spec 6.5)")
            assertThat(peak.size).isEqualTo(2)
            listOf("task-a" to a, "task-b" to b).forEach { (name, accepted) ->
                val deadlineB = System.nanoTime() + 120_000_000_000L
                while (host.admin.run(name, accepted.runId) == null && System.nanoTime() < deadlineB) Thread.onSpinWait()
            }
        }
    }

    // ------------------------------------------------- 3a. NotReady across the boundary

    @Test
    fun `triggering before any generation exists fails the step once with no retry`(@TempDir root: Path) {
        ComposedHost(root).use { host ->
            writeShapeD(host.taskDirectory)
            host.wired()

            val outcome = host.runTask()
            val errors = host.listener.stepErrors

            assertThat(outcome.outcome).isEqualTo(Outcome.FAILED)
            assertThat(outcome.failure).isInstanceOf(NotReadyException::class.java)
            println("[NOTREADY] failure = " + outcome.failure)
            println("[NOTREADY] step errors = " + errors.map { it.step.step + " attempt=" + it.attempt + " willRetry=" + it.willRetry })
            assertThat(errors).hasSize(1)
            assertThat(errors.single().attempt).isEqualTo(1)
            assertThat(errors.single().willRetry).isFalse()
            // Nothing reached the target.
            assertThat(host.reportRows()).isEmpty()
            assertThat(host.cacheEvents.unavailable).isNotEmpty()
        }
    }

    // ------------------------------------------------- 3b. close the cache mid-copy

    @Test
    fun `closing the cache mid-copy`(@TempDir root: Path) {
        ComposedHost(root, source = LotSource(2000)).use { host ->
            writeShapeD(host.taskDirectory, copySql = SLOW_COPY)
            host.wired()
            host.managed.admin.triggerRefresh(host.group)

            listenerReset(host)
            val accepted = host.admin.trigger("wip-summary", "tester") as TriggerResult.Accepted
            val leased = host.awaitLease()
            println("[CLOSE-MID] lease seen before close = " + leased)

            val closeStart = System.nanoTime()
            host.managed.close()
            val closeMs = (System.nanoTime() - closeStart) / 1_000_000
            println("[CLOSE-MID] managed.close() took " + closeMs + " ms")

            val outcome = host.await("wip-summary", accepted.runId)
            println("[CLOSE-MID] outcome = " + outcome.outcome + " failure = " + outcome.failure)

            // A second trigger after close must be refused by the cache, not by the ETL layer.
            listenerReset(host)
            val second = host.admin.trigger("wip-summary", "tester") as TriggerResult.Accepted
            val secondOutcome = host.await("wip-summary", second.runId)
            println("[CLOSE-MID] after close, second run failure = " + secondOutcome.failure)
            assertThat(secondOutcome.failure).isInstanceOf(ShuttingDownException::class.java)
        }
    }


    // -------------------------- 3b-ii. drain times out while a task is still copying

    @Test
    fun `closing the cache with a drain timeout shorter than the copy`() {
        // NOT @TempDir: the whole point of this test is that the generation file stays open,
        // and JUnit's TempDir cleanup fails the test with "Failed to delete temp directory".
        val root = Files.createTempDirectory("drain-timeout")
        val host = ComposedHost(root, source = LotSource(2000), leaseDrainTimeout = Duration.ofMillis(1))
        try {
            writeShapeD(host.taskDirectory, copySql = SLOW_COPY)
            host.wired()
            host.managed.admin.triggerRefresh(host.group)

            listenerReset(host)
            val accepted = host.admin.trigger("wip-summary", "tester") as TriggerResult.Accepted
            val leased = host.awaitLease()
            assertThat(leased).isNotNull()

            val closeStart = System.nanoTime()
            host.managed.close()
            println("[DRAIN-TIMEOUT] close() returned in " + ((System.nanoTime() - closeStart) / 1_000_000) + " ms while a lease was held: " + leased)

            val outcome = host.await("wip-summary", accepted.runId)
            println("[DRAIN-TIMEOUT] the copy that outlived close(): " + outcome.outcome + " failure = " + outcome.failure)

            val files = Files.list(root.resolve("cache").resolve(GROUP)).use { it.toList() }
            println("[DRAIN-TIMEOUT] generation files still on disk after close(): " + files.map { it.fileName.toString() })
            assertThat(files).isNotEmpty()
            // The store was left open on purpose (spec 10.2 step 5). On Windows that means the
            // file cannot be deleted for the rest of the process's life.
            val deleted = files.map { runCatching { Files.delete(it) }.isSuccess }
            println("[DRAIN-TIMEOUT] could the host delete them? " + files.map { it.fileName.toString() }.zip(deleted))
            assertThat(deleted).containsOnly(false)
        } finally {
            runCatching { host.close() }
        }
    }

    // ------------------------------------------------- 3c. reload while the cache refreshes

    @Test
    fun `reloading the ETL config while the cache refreshes`(@TempDir root: Path) {
        ComposedHost(root).use { host ->
            writeShapeD(host.taskDirectory)
            host.wired()
            host.managed.admin.triggerRefresh(host.group)

            val refreshing = Thread {
                repeat(4) { host.managed.admin.triggerRefresh(host.group) }
            }
            val reloads = CopyOnWriteArrayList<String>()
            val reloading = Thread {
                repeat(4) { i ->
                    writeShapeD(host.taskDirectory, cron = "0 */" + (i + 1) + " * * * ?")
                    reloads += (host.admin.reload(host.taskDirectory)?.toString() ?: "ok")
                }
            }
            refreshing.start(); reloading.start()
            refreshing.join(60_000); reloading.join(60_000)

            println("[RELOAD] reload reports = " + reloads)
            println("[RELOAD] refresh results = " + host.cacheEvents.refreshes)
            assertThat(reloads).containsOnly("ok")
            assertThat(host.cacheEvents.refreshes.map { it.first }).containsOnly(RefreshResult.SUCCESS)
            assertThat(host.cron.names()).containsExactly("wip-summary")

            val outcome = host.runTask()
            assertThat(outcome.outcome).isEqualTo(Outcome.SUCCEEDED)
        }
    }

    // ------------------------------------------------- 4. the owner / threading measurement

    @Test
    fun `what LeaseInfo owner actually says when the consumer is SimpleEtl`(@TempDir root: Path) {
        ComposedHost(root, source = LotSource(2000)).use { host ->
            writeShapeD(host.taskDirectory, copySql = SLOW_COPY)
            host.wired()
            host.managed.admin.triggerRefresh(host.group)

            listenerReset(host)
            val accepted = host.admin.trigger("wip-summary", "tester") as TriggerResult.Accepted
            val leaseWhileHeld = host.awaitLease()
            val outcome = host.await("wip-summary", accepted.runId)
            assertThat(outcome.outcome).isEqualTo(Outcome.SUCCEEDED)

            println("[OWNER] CacheAdmin.liveGenerations lease while held : " + leaseWhileHeld)
            println("[OWNER] CacheEvents.leaseReleased owners            : " + host.cacheEvents.leases.map { it.owner })
            println("[OWNER] step threads seen by TaskRunListener        : " + host.listener.stepThreads)
            println("[OWNER] kotlinx.coroutines.debug property = " + System.getProperty("kotlinx.coroutines.debug"))
            println("[OWNER] assertions enabled (-ea)?                 : " + javaClass.desiredAssertionStatus())
            println("[OWNER] CoroutineName in thread name?               : " + host.listener.stepThreads.any { it.second.contains("wip-summary") })

            assertThat(host.cacheEvents.leases).isNotEmpty()
        }
    }
}

private const val SLOW_COPY =
    "select t.i as id, 'L' || t.i as lot_id, cast(t.i as decimal(18,3)) as qty, 'F12' as site from range(1, 6000000) t(i)"

private fun listenerReset(host: ComposedHost) {
    host.listener.ended = java.util.concurrent.CountDownLatch(1)
}

/** Polls CacheAdmin until a lease is visible, which is the only way to read it while held. */
private fun ComposedHost.awaitLease(): infra.snapshotcache.api.LeaseInfo? {
    val deadline = System.nanoTime() + 60_000_000_000L
    while (System.nanoTime() < deadline) {
        managed.admin.liveGenerations(group).flatMap { it.leases }.firstOrNull()?.let { return it }
        if (listener.ended.count == 0L) return null
        Thread.onSpinWait()
    }
    return null
}

