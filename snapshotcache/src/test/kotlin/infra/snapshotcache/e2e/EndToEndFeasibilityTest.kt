package infra.snapshotcache.e2e

import com.sun.management.UnixOperatingSystemMXBean
import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.CacheEvents
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.api.GenerationSource
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.LeaseInfo
import infra.snapshotcache.api.NotReadyException
import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.ShuttingDownException
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.core.DefaultSnapshotCache
import infra.snapshotcache.core.GenerationRegistry
import infra.snapshotcache.core.GroupRuntime
import infra.snapshotcache.core.RefreshCycle
import infra.snapshotcache.duckdb.DuckDbGenerationStore
import infra.snapshotcache.spi.GenerationStore
import infra.snapshotcache.spi.OpenGeneration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.SimpleFormatter

/**
 * P8: the E2E feasibility test - the mandatory whole-chain proof on real
 * DuckDB 1.1.3. The full production stack - [DuckDbGenerationStore] (wrapped in a
 * thin recording spy for the step 8 accounting clause) + [GenerationRegistry] +
 * [RefreshCycle] + [DefaultSnapshotCache] - built over real files, real ATTACH, with a
 * [SyntheticSource] in place of the Oracle-backed one.
 *
 * One ordered scenario, shared state, the seven mandated steps plus the step 8
 * end-of-test resource assertions. Zero sleeps: every wait is a latch, a thread-state
 * poll, or a bounded join - bounds on broken implementations, never sequencing (the one
 * real-time bound is the deliberately short leaseDrainTimeout in step 7, whose held
 * lease is never released, so the outcome is deterministic).
 *
 * K is configured to 1: with a single held lease, steady live = held + current = 2, so
 * the blocked-by-K state is reachable at K=1 and unreachable at the default K=3
 * (GC reclaims every intermediate generation, so live never exceeds 2). The P5 suite
 * used K=1 for the same reason; the blocking semantics are identical at any K.
 *
 * What this proves: build -> verify -> publish -> serve -> block-at-K -> GC -> delete on
 * real DuckDB 1.1.3, plus A3, A4 (adapter guard, per the P7 finding), A7 and file-level
 * A1. What it deliberately does not prove: absence of slow memory leaks (deferred) and
 * performance at production scale (deferred).
 */
@Tag("e2e")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
internal class EndToEndFeasibilityTest {

    /**
     * Self-managed, not @TempDir: a non-static @TempDir field is re-injected with a
     * fresh directory per test METHOD even under PER_CLASS lifecycle, and its
     * per-method cleanup tries to delete the still-ATTACHed generation file mid-scenario
     * (locked on Windows). One directory for the whole ordered scenario, removed in
     * [tearDown] after the stores close.
     */
    private val tempRoot: Path = Files.createTempDirectory("snapshotcache-e2e")

    private val groupDir: Path get() = tempRoot.resolve("group")
    private val coldDir: Path get() = tempRoot.resolve("cold-group")
    private val spillDir: Path get() = tempRoot.resolve("spill")

    private val group = GroupId("e2e-orders")

    /**
     * Never published: the primary group always has a current generation after step 1,
     * so its acquires never wait - a second, never-published group is what makes the
     * "waiter already inside waitBudget" clause of the shutdown step reachable.
     */
    private val coldGroup = GroupId("e2e-cold")

    private val clock: Clock = Clock.systemUTC()
    private val events = RecordingEvents()
    private val defaultSource = SyntheticSource()

    @Volatile
    private var sourceBehavior: GenerationSource = defaultSource

    private lateinit var config: SnapshotCacheConfig
    private lateinit var realStore: DuckDbGenerationStore
    private lateinit var coldStore: DuckDbGenerationStore
    private lateinit var store: RecordingStoreSpy
    private lateinit var registry: GenerationRegistry
    private lateinit var coldRegistry: GenerationRegistry
    private lateinit var cache: DefaultSnapshotCache

    private lateinit var gen1Outcome: RefreshOutcome
    private lateinit var heldSnapshot: Snapshot
    private var fdBaseline: Long? = null

    @AfterAll
    fun tearDown() {
        if (::heldSnapshot.isInitialized) runCatching { heldSnapshot.close() }
        if (::realStore.isInitialized) realStore.close()
        if (::coldStore.isInitialized) coldStore.close()
        // Cleanup, not assertion: best-effort recursive delete once every DuckDB
        // instance has released its files.
        runCatching {
            Files.walk(tempRoot).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach { runCatching { Files.delete(it) } }
            }
        }
    }

    // ------------------------------------------------------------------------------ step 1

    @Test
    @Order(10)
    fun step1_dirtyStartup_wipesLeftovers_zeroBudgetFailsFast_parkedAcquireReleasedByFirstPublish() {
        // Leftovers from a "crashed pod": a promoted file, its WAL sibling, a half-built .tmp.
        Files.createDirectories(groupDir)
        Files.write(groupDir.resolve("gen_0000000042.db"), byteArrayOf(1, 2, 3))
        Files.write(groupDir.resolve("gen_0000000042.db.wal"), byteArrayOf(4))
        Files.write(groupDir.resolve("gen_0000000007.db.tmp"), byteArrayOf(5, 6))

        buildStack()

        // Startup wipe via the store primitives - listOnDisk + delete (P9 owns the
        // orchestration; the E2E performs the wipe itself).
        assertThat(store.listOnDisk()).containsExactly(7L, 42L)
        store.listOnDisk().forEach { store.delete(it) }
        assertThat(store.listOnDisk()).isEmpty()
        assertThat(Files.list(groupDir).use { it.toList() })
            .describedAs("the wipe must remove WAL siblings too")
            .isEmpty()

        // Nothing published yet: currentInfo is null - it is the readiness seam - and a
        // zero budget throws NotReadyException immediately.
        assertThat(cache.currentInfo(group)).isNull()
        val zeroBudget = catchThrowable { cache.acquire(group, Duration.ZERO) }
        assertThat(zeroBudget).isInstanceOf(NotReadyException::class.java)
        assertThat((zeroBudget as NotReadyException).reason).isEqualTo(AcquireUnavailableReason.NOT_READY)

        // Park-then-publish (P5 thread-state precedent): an acquire(30s) issued BEFORE
        // the first publish returns once gen 1 lands, not after serving out its budget.
        val acquired = AtomicReference<Any?>()
        val waiter = Thread({
            acquired.set(runCatching { cache.acquire(group, Duration.ofSeconds(30)) }.getOrElse { it })
        }, "e2e-cold-start-waiter")
        waiter.isDaemon = true
        waiter.start()
        awaitParked(waiter)

        gen1Outcome = cache.triggerRefresh(group)

        joinOrFail(waiter)
        val result = acquired.get()
        assertThat(result)
            .describedAs("the parked acquire must succeed once gen 1 publishes, got %s", result)
            .isInstanceOf(Snapshot::class.java)
        (result as Snapshot).use { snap -> assertThat(snap.generation).isEqualTo(1L) }
        assertThat(events.waited)
            .describedAs("snapshot_acquire_waited_seconds must be recorded (spec 9.3)")
            .isNotEmpty()
    }

    // ------------------------------------------------------------------------------ step 2

    @Test
    @Order(20)
    fun step2_firstGeneration_unionViewServed_readOnlyRejectsInsert_A3() {
        assertThat(gen1Outcome.result).isEqualTo(RefreshResult.SUCCESS)
        assertThat(gen1Outcome.generation).isEqualTo(1L)

        // Readiness flipped: currentInfo reports gen 1 with the verify gate's row counts
        // (the union view is not a BASE TABLE, so it is absent - P4b behavior).
        val info = checkNotNull(cache.currentInfo(group))
        assertThat(info.generation).isEqualTo(1L)
        assertThat(info.rowCounts).isEqualTo(mapOf("t_a" to 2_000L, "t_b" to 3_000L))

        cache.withSnapshot(group) { snap ->
            assertThat(snap.generation).isEqualTo(1L)
            assertThat(snap.dataAsOf).isEqualTo(info.dataAsOf)
            val connection = snap.connection()
            assertThat(queryLong(connection, "SELECT COUNT(*) FROM t_unified")).isEqualTo(5_000L)
            // The union view's shape: the source column distinguishes the two aligned tables.
            connection.createStatement().use { st ->
                st.executeQuery("SELECT source, COUNT(*) FROM t_unified GROUP BY source ORDER BY source").use { rs ->
                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getString(1)).isEqualTo("A")
                    assertThat(rs.getLong(2)).isEqualTo(2_000L)
                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getString(1)).isEqualTo("B")
                    assertThat(rs.getLong(2)).isEqualTo(3_000L)
                    assertThat(rs.next()).isFalse()
                }
            }
            assertThat(queryString(connection, "SELECT name FROM t_a WHERE id = 1")).isEqualTo("g1-a-1")

            // A3: the READ_ONLY attach rejects an INSERT through the handle's connection.
            assertThatThrownBy {
                connection.createStatement().use { it.execute("INSERT INTO t_a VALUES (99, 'nope', 0.0)") }
            }.isInstanceOf(SQLException::class.java)
            // The rejected write did not disturb reads on the same connection (P7 precedent).
            assertThat(queryLong(connection, "SELECT COUNT(*) FROM t_a")).isEqualTo(2_000L)
        }
    }

    // ------------------------------------------------------------------------------ step 3

    @Test
    @Order(30)
    fun step3_kEnforcement_blockedByK_currentKeepsServing_heldHandlePinned_I8_releaseAutoResumes() {
        val held = cache.acquire(group)
        assertThat(held.generation).isEqualTo(1L)

        // Refresh with gen 1 leased: gen 2 publishes, gen 1 cannot be reclaimed - live
        // generations reach 2, exceeding K=1.
        assertThat(cache.triggerRefresh(group).result).isEqualTo(RefreshResult.SUCCESS)
        assertThat(checkNotNull(cache.currentInfo(group)).generation).isEqualTo(2L)
        assertThat(store.listOnDisk()).containsExactly(1L, 2L)

        // The next refresh records BLOCKED_BY_K while current keeps serving (I4).
        val blocked = cache.triggerRefresh(group)
        assertThat(blocked.result).isEqualTo(RefreshResult.BLOCKED_BY_K)
        assertThat(events.finished).contains(RefreshResult.BLOCKED_BY_K to null)
        cache.withSnapshot(group) { snap ->
            assertThat(snap.generation).isEqualTo(2L)
            assertThat(queryString(snap.connection(), "SELECT name FROM t_a WHERE id = 1")).isEqualTo("g2-a-1")
        }

        // The held handle still queries gen 1 with unchanged results (I8) - provable by
        // content, since every generation stamps its own marker values.
        assertThat(held.generation).isEqualTo(1L)
        val heldConnection = held.connection()
        assertThat(queryLong(heldConnection, "SELECT COUNT(*) FROM t_unified")).isEqualTo(5_000L)
        assertThat(queryString(heldConnection, "SELECT name FROM t_a WHERE id = 1")).isEqualTo("g1-a-1")

        // Release -> GC reclaims gen 1, files on disk drop, refresh auto-resumes. The
        // handle's close also closes heldConnection (P3), so the adapter guard is clean.
        held.close()
        val resumed = cache.triggerRefresh(group)
        assertThat(resumed.result)
            .describedAs("refresh must auto-resume once the blocking lease releases (spec 6.1)")
            .isEqualTo(RefreshResult.SUCCESS)
        assertThat(resumed.generation).isEqualTo(3L)
        assertThat(events.reclaimed).contains(1L)
        assertThat(groupDir.resolve("gen_0000000001.db")).doesNotExist()
        assertThat(store.listOnDisk()).containsExactly(3L)
    }

    // ------------------------------------------------------------------------------ step 4

    @Test
    @Order(40)
    fun step4_detachInUse_adapterGuardDefersReclaim_fileGoneAfterConnectionCloses_A4_fileLevelA1() {
        val old = checkNotNull(cache.currentInfo(group)).generation

        // BINDING (the P7 A4 finding): the "raw connection" is staged through
        // OpenGeneration.connection() - a store-tracked connection outside any handle.
        // Engine DETACH does not reliably fail under an idle reader on 1.1.3, so this
        // verifies the ADAPTER guard, not engine behavior.
        val lease = checkNotNull(registry.tryAcquire("e2e-step4-raw")) { "current generation must be acquirable" }
        val raw = checkNotNull(lease.opened) { "published lease must carry its OpenGeneration" }.connection()
        registry.release(lease) // refcount back to 0: only the raw connection pins the generation

        // Publish the next generation, then trigger GC: reclamation must be deferred.
        assertThat(cache.triggerRefresh(group).result).isEqualTo(RefreshResult.SUCCESS)
        val deferredPass = cache.gc(group)
        assertThat(deferredPass.deferred)
            .describedAs("reclamation must be deferred while the raw connection is open (A4)")
            .containsExactly(old)
        assertThat(deferredPass.reclaimed).isEmpty()
        assertThat(store.listOnDisk()).contains(old)
        // The deferred detach left the reader untouched.
        assertThat(queryLong(raw, "SELECT COUNT(*) FROM t_unified")).isEqualTo(5_000L)

        // Close the connection, GC again: detached AND the file is gone (file-level A1).
        raw.close()
        val reclaimedPass = cache.gc(group)
        assertThat(reclaimedPass.reclaimed).containsExactly(old)
        assertThat(groupDir.resolve("gen_${old.toString().padStart(10, '0')}.db"))
            .describedAs("the generation file must be gone from disk (file-level A1)")
            .doesNotExist()
        assertThat(store.listOnDisk()).containsExactly(old + 1)
    }

    // -------------------------------------------------------------- steady-state rotations

    @Test
    @Order(45)
    fun steadyStateRotations_meetTheTwentyPlusRotationTarget_andSetThePostWarmupFdBaseline() {
        repeat(18) {
            assertThat(cache.triggerRefresh(group).result).isEqualTo(RefreshResult.SUCCESS)
            assertThat(store.listOnDisk())
                .describedAs("GC must keep exactly the current generation's file on disk")
                .hasSize(1)
        }
        // 22 full build->publish->reclaim rotations so far (steps 1-4 plus these 18).
        assertThat(checkNotNull(cache.currentInfo(group)).generation).isEqualTo(22L)

        // Post-warmup FD baseline: growth is judged only after warmup has absorbed driver
        // loading and JIT (Unix MXBean, P7 precedent - stays null and step 8 skips on Windows).
        fdBaseline = (ManagementFactory.getOperatingSystemMXBean() as? UnixOperatingSystemMXBean)
            ?.openFileDescriptorCount
    }

    // ------------------------------------------------------------------------------ step 5

    @Test
    @Order(50)
    fun step5_failurePaths_midBuildThrowLeavesCurrent_I7_zeroRowsRejectedByNonEmpty() {
        val current = checkNotNull(cache.currentInfo(group))

        // Round 1: the source throws mid-build, some rows already written.
        sourceBehavior = GenerationSource { ctx ->
            ctx.target.createStatement().use { st ->
                st.execute("CREATE TABLE t_a (id BIGINT)")
                st.execute("INSERT INTO t_a VALUES (1)")
            }
            throw RuntimeException("synthetic mid-build failure")
        }
        val sourceError = cache.triggerRefresh(group)
        assertThat(sourceError.result).isEqualTo(RefreshResult.SOURCE_ERROR)
        assertThat(sourceError.detail).contains("synthetic mid-build failure")
        assertNoTmpFiles()
        assertThat(checkNotNull(cache.currentInfo(group)).generation)
            .describedAs("I7: current unchanged after a failed refresh")
            .isEqualTo(current.generation)
        assertThat(store.listOnDisk()).containsExactly(current.generation)

        // Round 2: zero rows - the non-disableable non_empty gate rejects.
        sourceBehavior = SyntheticSource(rowsA = 0, rowsB = 0)
        val rejected = cache.triggerRefresh(group)
        assertThat(rejected.result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertThat(rejected.detail).contains("non_empty")
        assertThat(events.verifyFailures)
            .describedAs("the snapshot_verify_failed_total{rule} seam must have counted exactly this rejection")
            .hasSize(1)
        assertThat(events.verifyFailures.single().first).isEqualTo("non_empty")
        assertNoTmpFiles()
        assertThat(checkNotNull(cache.currentInfo(group)).generation).isEqualTo(current.generation)
        assertThat(store.listOnDisk()).containsExactly(current.generation)

        // Return to a usable state: the old generation still serves.
        cache.withSnapshot(group) { snap ->
            assertThat(queryLong(snap.connection(), "SELECT COUNT(*) FROM t_unified")).isEqualTo(5_000L)
        }
        sourceBehavior = defaultSource
    }

    // ------------------------------------------------------------------------------ step 6

    @Test
    @Order(60)
    fun step6_copyOut_intoSecondInstanceViaFileAttach_A7_lineageCorrect_leaseReleasedImmediately() {
        val info = checkNotNull(cache.currentInfo(group))
        val releasesBefore = events.released.size

        DriverManager.getConnection("jdbc:duckdb:").use { target ->
            val result = cache.copyOut(group, CopyOutSpec("SELECT id, name FROM t_a WHERE id <= 10", "copied_subset", target))
            // The result carries the correct lineage.
            assertThat(result.generation).isEqualTo(info.generation)
            assertThat(result.dataAsOf).isEqualTo(info.dataAsOf)
            assertThat(result.rowsCopied).isEqualTo(10L)
            // The rows really live in the SECOND instance (A7: cross-instance file ATTACH).
            target.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*), MIN(name) FROM copied_subset").use { rs ->
                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getLong(1)).isEqualTo(10L)
                    assertThat(rs.getString(2)).isEqualTo("g${info.generation}-a-1")
                }
            }
        }

        // The lease is released immediately after the copy.
        assertThat(events.released.size).isEqualTo(releasesBefore + 1)
        assertThat(registry.liveGenerations().map { it.refCount }).allMatch { it == 0 }
    }

    // ------------------------------------------------------------------------------ step 7

    @Test
    @Order(70)
    fun step7_gracefulShutdown_abortsInFlightBuild_releasesWaiterImmediately_drainTimeoutNamesHeldLease() {
        val currentBefore = checkNotNull(cache.currentInfo(group)).generation

        // A build mid-flight: the source parks on a latch (P5 pattern; cooperative
        // abort - interrupt delivery is P9 wiring).
        val buildEntered = CountDownLatch(1)
        val releaseBuild = CountDownLatch(1)
        sourceBehavior = GenerationSource {
            buildEntered.countDown()
            check(releaseBuild.await(30, TimeUnit.SECONDS)) { "shutdown never released the parked build" }
        }
        val refreshOutcome = AtomicReference<RefreshOutcome>()
        val refresher = Thread({ refreshOutcome.set(cache.triggerRefresh(group)) }, "e2e-shutdown-refresher")
        refresher.isDaemon = true
        refresher.start()
        check(buildEntered.await(10, TimeUnit.SECONDS)) { "the build never started" }
        assertThat(Files.list(groupDir).use { paths -> paths.anyMatch { it.fileName.toString().endsWith(".tmp") } })
            .describedAs("the in-flight candidate .tmp must exist mid-build")
            .isTrue()

        // A lease still held across shutdown; its owner is this thread's name.
        val ownerName = Thread.currentThread().name
        heldSnapshot = cache.acquire(group)

        // A waiter already inside waitBudget, on the never-published second group.
        val waiterOutcome = AtomicReference<Throwable?>()
        val waiter = Thread({
            waiterOutcome.set(catchThrowable { cache.acquire(coldGroup, Duration.ofSeconds(30)) })
        }, "e2e-cold-group-waiter")
        waiter.isDaemon = true
        waiter.start()
        awaitParked(waiter)

        val warnCapture = WarnCapture.install()
        val outstanding: List<LeaseInfo>
        val drainElapsed: Duration
        try {
            val started = System.nanoTime()
            outstanding = cache.shutdown()
            drainElapsed = Duration.ofNanos(System.nanoTime() - started)
        } finally {
            warnCapture.uninstall()
            releaseBuild.countDown()
        }

        // shutdown() returns the still-outstanding lease naming its owner, after the
        // short drain timeout (the held lease is never released, so the timeout outcome
        // is deterministic - a bound, not sequencing).
        assertThat(outstanding).hasSize(1)
        assertThat(outstanding.single().owner).isEqualTo(ownerName)
        assertThat(drainElapsed)
            .describedAs("shutdown must serve the full drain budget before giving up on the held lease")
            .isGreaterThanOrEqualTo(config.leaseDrainTimeout)
        assertThat(drainElapsed)
            .describedAs("bound on a broken implementation: the drain must not hang")
            .isLessThan(Duration.ofSeconds(10))
        assertThat(warnCapture.warnings)
            .describedAs("the drain timeout must WARN naming the outstanding lease owner, got %s", warnCapture.warnings)
            .anyMatch { ownerName in it }

        // The thread already inside waitBudget is released immediately, not after its
        // 30s budget (the 5s join bound proves it).
        joinOrFail(waiter)
        assertThat(waiterOutcome.get()).isInstanceOf(ShuttingDownException::class.java)

        // An acquire issued during shutdown throws ShuttingDownException at once.
        assertThat(catchThrowable { cache.acquire(group) }).isInstanceOf(ShuttingDownException::class.java)

        // The aborted build: candidate .tmp deleted, never promoted, current unchanged (I7).
        joinOrFail(refresher)
        assertThat(refreshOutcome.get().result).isEqualTo(RefreshResult.SHUTDOWN_ABORTED)
        assertNoTmpFiles()
        assertThat(checkNotNull(cache.currentInfo(group)).generation).isEqualTo(currentBefore)
        assertThat(store.listOnDisk()).containsExactly(currentBefore)
    }

    // ------------------------------------------------------------------------------ step 8

    @Test
    @Order(80)
    fun step8_endOfTest_filesMatchLiveGenerations_noTmpRemains_accountingEquationsHold() {
        // The lease that outlived the drain is released now (consumer-side cleanup), so
        // the end state is the steady one the resource assertions describe.
        heldSnapshot.close()

        val current = checkNotNull(registry.current())
        val live = registry.liveGenerations()
        assertThat(live.map { it.generation }).containsExactly(current)
        assertThat(live.single().refCount).isZero()
        assertThat(store.listOnDisk())
            .describedAs("files on disk must correspond exactly to live generations")
            .containsExactlyElementsOf(live.map { it.generation })
        assertNoTmpFiles()
        assertThat(coldStore.listOnDisk()).isEmpty()

        // Step 8's "if wrapped" clause: the real store IS wrapped with a recording spy,
        // so the accounting equations are asserted.
        store.verifyAccountingEquations(current, live.associate { it.generation to it.refCount })
    }

    @Test
    @Order(81)
    fun step8_endOfTest_fdCountEqualsPostWarmupBaseline() {
        val os = ManagementFactory.getOperatingSystemMXBean()
        assumeTrue(os is UnixOperatingSystemMXBean, "FD counting requires the Unix MXBean; skipped on Windows (P7 precedent)")
        val baseline = checkNotNull(fdBaseline) { "the baseline must have been captured after the warmup rotations" }
        val now = (os as UnixOperatingSystemMXBean).openFileDescriptorCount
        assertThat(now - baseline)
            .describedAs("open FD growth since the post-warmup baseline (baseline %d, now %d)", baseline, now)
            .isLessThanOrEqualTo(0)
    }

    // ------------------------------------------------------------------ wiring + helpers

    /** The production wiring of plan P9's shape, minus CDI: real store, registry, cycle, facade. */
    private fun buildStack() {
        config = SnapshotCacheConfig(
            storagePath = groupDir,
            tempDirectory = spillDir,
            // K=1 makes the blocked-by-K state reachable with one held lease (see class doc).
            maxLiveGenerations = 1,
            // Deliberately short: step 7's held lease is never released, so the drain
            // always times out - deterministic outcome, bound not sequencing.
            leaseDrainTimeout = Duration.ofMillis(200),
        )
        realStore = DuckDbGenerationStore(groupDir, spillDir, "500MB")
        store = RecordingStoreSpy(realStore)
        coldStore = DuckDbGenerationStore(coldDir, spillDir, "200MB")
        registry = GenerationRegistry(config.maxLiveGenerations, config.leaseDeadline, clock)
        coldRegistry = GenerationRegistry(config.maxLiveGenerations, config.leaseDeadline, clock)
        val cycle = RefreshCycle(
            group = group,
            registry = registry,
            store = store,
            source = GenerationSource { sourceBehavior.refresh(it) },
            config = config,
            events = events,
            clock = clock,
        )
        cache = DefaultSnapshotCache(
            config,
            mapOf(
                group to GroupRuntime(registry, store, cycle),
                coldGroup to GroupRuntime(coldRegistry, coldStore),
            ),
            events,
            clock,
        )
    }

    private fun assertNoTmpFiles() {
        val tmp = Files.list(groupDir).use { paths ->
            paths.filter { it.fileName.toString().endsWith(".tmp") }.toList()
        }
        assertThat(tmp).describedAs("no candidate .tmp may remain").isEmpty()
    }

    private fun queryLong(connection: Connection, sql: String): Long =
        connection.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                check(rs.next()) { "query returned no rows: $sql" }
                rs.getLong(1)
            }
        }

    private fun queryString(connection: Connection, sql: String): String =
        connection.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                check(rs.next()) { "query returned no rows: $sql" }
                rs.getString(1)
            }
        }

    /**
     * Bounded wait until [thread] is parked in a wait (the acquire's condition await) -
     * a thread-state precondition check with a deadline, never sequencing by sleep
     * (P3/P5 precedent).
     */
    private fun awaitParked(thread: Thread) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (true) {
            when (thread.state) {
                Thread.State.WAITING, Thread.State.TIMED_WAITING -> return
                Thread.State.TERMINATED -> throw AssertionError("waiter terminated before parking; acquire did not wait")
                else -> {
                    if (System.nanoTime() >= deadline) throw AssertionError("thread ${thread.name} never parked")
                    Thread.onSpinWait()
                }
            }
        }
    }

    /** Bounded join: a bound on broken implementations, never sequencing. */
    private fun joinOrFail(thread: Thread) {
        thread.join(10_000)
        assertThat(thread.isAlive).describedAs("thread %s must have finished", thread.name).isFalse()
    }
}

// ---------------------------------------------------------------------- support types

/** Records the events this scenario asserts on (the metric seams). */
private class RecordingEvents : CacheEvents {
    val finished = CopyOnWriteArrayList<Pair<RefreshResult, Long?>>()
    val waited = CopyOnWriteArrayList<Duration>()
    val unavailable = CopyOnWriteArrayList<AcquireUnavailableReason>()
    val verifyFailures = CopyOnWriteArrayList<Pair<String, String>>()
    val released = CopyOnWriteArrayList<LeaseInfo>()
    val reclaimed = CopyOnWriteArrayList<Long>()

    override fun refreshFinished(group: GroupId, result: RefreshResult, generation: Long?) {
        finished += result to generation
    }

    override fun acquireWaited(group: GroupId, waited: Duration) {
        this.waited += waited
    }

    override fun acquireUnavailable(group: GroupId, reason: AcquireUnavailableReason) {
        unavailable += reason
    }

    override fun verifyFailed(group: GroupId, rule: String, detail: String) {
        verifyFailures += rule to detail
    }

    override fun leaseReleased(group: GroupId, lease: LeaseInfo, heldFor: Duration) {
        released += lease
    }

    override fun generationReclaimed(group: GroupId, generation: Long) {
        reclaimed += generation
    }
}

/**
 * Step 8's optional clause: if the real storage is wrapped with a recording spy, the
 * accounting equations hold. A thin effect-recording decorator over the
 * real store. Effects, not attempts (P2 precedent): an operation that threw mutated
 * nothing and is not counted - which is exactly what keeps the equations honest across
 * step 4's deferred DETACH and step 5/7's abort paths.
 */
private class RecordingStoreSpy(private val real: GenerationStore) : GenerationStore {
    private val created = ConcurrentHashMap.newKeySet<Long>()
    private val promoted = ConcurrentHashMap.newKeySet<Long>()
    private val deleted = ConcurrentHashMap.newKeySet<Long>()
    private val opens = ConcurrentHashMap<Long, Int>()
    private val closes = ConcurrentHashMap<Long, Int>()

    override fun createCandidate(gen: Long) = real.createCandidate(gen).also { created += gen }

    override fun promote(gen: Long) {
        real.promote(gen)
        promoted += gen
    }

    override fun open(gen: Long) = real.open(gen).also { opens.merge(gen, 1, Int::plus) }

    override fun close(gen: Long) {
        real.close(gen)
        closes.merge(gen, 1, Int::plus)
    }

    override fun delete(gen: Long) {
        real.delete(gen)
        deleted += gen
    }

    override fun listOnDisk() = real.listOnDisk()

    override fun copyOut(opened: OpenGeneration, spec: CopyOutSpec) = real.copyOut(opened, spec)

    /** The four accounting equations, verbatim, against the registry's end-of-test facts. */
    fun verifyAccountingEquations(current: Long?, refCounts: Map<Long, Int>) {
        // Equation 1: count(createCandidate) == count(promote) + count(delete of candidates).
        // "Delete of candidates" = deletes of generations created but never promoted; the
        // step 1 wipe's deletes of pre-existing leftovers are not candidate deletes.
        val candidateDeletes = deleted.count { it in created && it !in promoted }
        assertThat(created.size)
            .describedAs("17.3 eq 1: createCandidate == promote + candidate deletes")
            .isEqualTo(promoted.size + candidateDeletes)

        val legitimatelyOpen = buildSet {
            current?.let { add(it) }
            addAll(refCounts.filterValues { it > 0 }.keys)
        }

        // Equation 2: per generation, count(open) == count(close), except still-live ones.
        for (gen in opens.keys + closes.keys) {
            if (gen in legitimatelyOpen) continue
            assertThat(opens[gen] ?: 0)
                .describedAs("17.3 eq 2: open == close for generation %d", gen)
                .isEqualTo(closes[gen] ?: 0)
        }

        // Equation 3: opened generations == { current } U { gens with refcount > 0 }.
        val stillOpen = (opens.keys + closes.keys)
            .filter { (opens[it] ?: 0) > (closes[it] ?: 0) }
            .toSet()
        assertThat(stillOpen)
            .describedAs("17.3 eq 3: opened == {current} U {refcount>0}")
            .isEqualTo(legitimatelyOpen)

        // Equation 4: generations on disk == opened generations.
        assertThat(real.listOnDisk().toSet())
            .describedAs("17.3 eq 4: on disk == opened")
            .isEqualTo(stillOpen)
    }
}

/**
 * Captures WARN-level log records at the JUL root. jboss-logging has no other provider
 * on this test classpath (no jboss-logmanager, log4j or slf4j binding), so it falls back
 * to java.util.logging - the same records the shutdown drain-timeout WARN travels
 * through.
 */
private class WarnCapture : Handler() {
    private val formatter = SimpleFormatter()
    val warnings = CopyOnWriteArrayList<String>()

    override fun publish(record: LogRecord) {
        if (record.level.intValue() >= Level.WARNING.intValue()) {
            warnings += runCatching { formatter.formatMessage(record) }.getOrElse { record.message ?: "" }
        }
    }

    override fun flush() {}
    override fun close() {}

    fun uninstall() {
        java.util.logging.Logger.getLogger("").removeHandler(this)
    }

    companion object {
        fun install(): WarnCapture = WarnCapture().also { java.util.logging.Logger.getLogger("").addHandler(it) }
    }
}
