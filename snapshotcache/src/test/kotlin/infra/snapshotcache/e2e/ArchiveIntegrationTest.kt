package infra.snapshotcache.e2e

import infra.snapshotarchive.ArchiveStatus
import infra.snapshotarchive.Archiver
import infra.snapshotarchive.Diff
import infra.snapshotarchive.DiffOp
import infra.snapshotarchive.EtlDiff
import infra.snapshotarchive.FallbackReason
import infra.snapshotarchive.ManifestDao
import infra.snapshotarchive.ManifestSchema
import infra.snapshotarchive.ObjectStore
import infra.snapshotarchive.RunOutcome
import infra.snapshotcache.api.BuildContext
import infra.snapshotcache.api.GenerationSource
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.SnapshotCacheConfig
import infra.snapshotcache.core.DefaultSnapshotCache
import infra.snapshotcache.core.GenerationRegistry
import infra.snapshotcache.core.GroupRuntime
import infra.snapshotcache.core.RefreshCycle
import infra.snapshotcache.duckdb.DuckDbGenerationStore
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * The one place the real framework and the real archive layer meet.
 *
 * Every test under `infra.snapshotarchive` fakes [infra.snapshotcache.api.SnapshotCache] with
 * a hand-written stand-in over a DuckDB file. That is the right call there - a crash matrix
 * has no business booting a generation registry - but it means the fake has no refcounts, no
 * generation table and no K ceiling, so an entire class of assumption goes unchecked: what
 * the archive layer believes about a *real* lease.
 *
 * This suite closes that. Real [DefaultSnapshotCache] over a real [DuckDbGenerationStore],
 * real Oracle manifest, real MinIO objects, real Parquet. Nothing here is faked except the
 * source of rows, which is a test-controlled generator precisely so the diff's answers can be
 * asserted exactly rather than approximately.
 *
 * The most valuable assertion is `an archiver's lease blocks reclaim, not publishing`: spec
 * 18.6 item 2 concluded a lease held across an export is a non-issue against the spec 6.1
 * K ceiling, and that conclusion was an argument from a 40 ms measurement, never a test.
 */
@Testcontainers
class ArchiveIntegrationTest {

    /**
     * One group per test. The Oracle manifest outlives the class, and D31's monotonicity
     * guard is keyed on `(group, data_as_of)` - so tests sharing a group id would have the
     * guard refuse a later test's checkpoint because an earlier test had already published a
     * newer one. Found by exactly that failure on the first run, which is the guard working.
     */
    private val group = GroupId("orders${GROUPS.incrementAndGet()}")
    private val clock = MovingClock(Instant.parse("2026-08-29T10:00:00Z"))
    private val source = MutableSource()

    private lateinit var store: DuckDbGenerationStore
    private lateinit var registry: GenerationRegistry
    private lateinit var cache: DefaultSnapshotCache
    private lateinit var manifest: ManifestDao
    private lateinit var objects: ObjectStore
    private lateinit var archiver: Archiver
    private lateinit var diff: EtlDiff
    private lateinit var tempRoot: Path

    @BeforeEach
    fun wireTheRealStack(@TempDir dir: Path) {
        val config = SnapshotCacheConfig(
            storagePath = dir.resolve("generations"),
            tempDirectory = dir.resolve("spill"),
            maxLiveGenerations = K,
            servingMemoryLimit = "500MB",
        )
        store = DuckDbGenerationStore(config.storagePath, config.tempDirectory, config.servingMemoryLimit)
        registry = GenerationRegistry(config.maxLiveGenerations, config.leaseDeadline, clock)
        val cycle = RefreshCycle(
            group = group,
            registry = registry,
            store = store,
            source = GenerationSource { source.refresh(it) },
            config = config,
            clock = clock,
        )
        cache = DefaultSnapshotCache(config, mapOf(group to GroupRuntime(registry, store, cycle)), clock = clock)

        tempRoot = dir
        manifest = ManifestDao(jdbi, bucket = BUCKET, clock = clock)
        objects = ObjectStore(minioClient, BUCKET)
        archiver = Archiver(
            cache = cache,
            manifest = manifest,
            objects = objects,
            tables = mapOf(group to listOf("t_a")),
            tempRoot = dir.resolve("archive"),
        )
        diff = EtlDiff(
            cache = cache,
            manifest = manifest,
            objects = objects,
            primaryKeys = mapOf(group to mapOf("t_a" to listOf("id"))),
            downloadRoot = dir.resolve("download"),
        )
    }

    @AfterEach
    fun shutDown() {
        runCatching { diff.close() }
        runCatching { archiver.close() }
        // `shutdown()` returns the leases still outstanding at the drain deadline. Discarding
        // it would throw away exactly the evidence the lease tests are about, so it is
        // asserted: every test here is expected to leave nothing leaked.
        val outstanding = runCatching { cache.shutdown() }.getOrDefault(emptyList())
        runCatching { store.close() }
        assertThat(outstanding).describedAs("no lease may outlive the test that took it").isEmpty()
    }

    /**
     * The headline: a checkpoint taken off a real generation, and a diff against a later real
     * generation that names exactly the rows that moved. Every layer is the production one.
     */
    @Test
    fun `a checkpoint of one real generation diffs exactly against the next`() {
        source.rows = mutableMapOf(1L to "alpha", 2L to "beta", 3L to "gamma")
        refresh()
        assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.PUBLISHED)
        val v1 = manifest.newestComplete(group)!!.version

        source.rows[2] = "beta-changed"
        source.rows.remove(3)
        source.rows[4] = "delta"
        // Under the default 5-minute leaseDeadline; a bigger jump would silently age any
        // open lease past its deadline and stop testing the healthy path.
        clock.advance(Duration.ofMinutes(1))
        refresh()

        val changes = diff.withDiff(group, v1) { d ->
            assertThat(d).isInstanceOf(Diff.Incremental::class.java)
            (d as Diff.Incremental).changes("t_a")
        }

        assertThat(changes.map { it.key.single() as Long to it.op }).containsExactlyInAnyOrder(
            2L to DiffOp.U,
            3L to DiffOp.D,
            4L to DiffOp.I,
        )
        assertThat(changes.single { it.op == DiffOp.U }.changedColumns).containsExactly("name")
    }

    /**
     * I4 (spec 6.1): a held lease pins its generation, and the K ceiling bites one round
     * later than is comfortable to assume.
     *
     * This is deliberately NOT "spec 18.6 item 2 is now tested". Item 2's conclusion - that a
     * lease held across an export is a non-issue - rests entirely on exports taking ~40 ms.
     * This test parks the archiver indefinitely, which removes exactly that property, so what
     * it shows is the opposite and more useful thing: if an archiver ever did hold a lease for
     * a long time, refresh WOULD stall. Item 2 is safe because of duration, not because of
     * structure, and that distinction is worth a test rather than a sentence.
     *
     * `blockedByK` is evaluated before a generation is allocated and returns null while
     * `live.size <= maxLive`, so the first refresh after the lease is taken still publishes.
     * It is the round after that which is refused.
     */
    @Test
    fun `I4_a held lease pins its generation and stalls refresh one round later`() {
        source.rows = mutableMapOf(1L to "alpha")
        refresh()

        val parked = CountDownLatch(1)
        val released = CountDownLatch(1)
        val slowArchiver = Archiver(
            cache = cache,
            manifest = manifest,
            objects = objects,
            tables = mapOf(group to listOf("t_a")),
            tempRoot = tempRoot.resolve("parked"),
            steps = { _, _ ->
                if (parked.count > 0) {
                    parked.countDown()
                    check(released.await(30, TimeUnit.SECONDS)) { "the test never released the archiver" }
                }
            },
        )
        try {
            val run = slowArchiver.submit(group)
            check(parked.await(30, TimeUnit.SECONDS)) { "the archiver never reached a step hook" }

            // One LIVE generation, pinned by the archiver's lease. 1 <= K, so this publishes.
            source.rows[2] = "beta"
            clock.advance(Duration.ofMinutes(1))
            assertThat(cache.triggerRefresh(group).generation)
                .describedAs("the first refresh after a lease is taken is not blocked")
                .isNotNull()

            // Now two are live and the pinned one cannot be reclaimed, so K=1 is exceeded.
            assertThat(registry.liveGenerations()).hasSize(2)
            assertThat(registry.liveGenerations().count { it.refCount > 0 })
                .describedAs("exactly the archiver's generation is pinned")
                .isEqualTo(1)

            source.rows[3] = "gamma"
            clock.advance(Duration.ofMinutes(1))
            val blocked = cache.triggerRefresh(group)
            assertThat(blocked.result)
                .describedAs("spec 6.1: refresh pauses rather than going stale silently")
                .isEqualTo(RefreshResult.BLOCKED_BY_K)
            assertThat(blocked.generation).isNull()

            released.countDown()
            run.get(60, TimeUnit.SECONDS)
        } finally {
            released.countDown()
            slowArchiver.close()
        }

        // Lease released, so the pin is gone and refresh recovers on its own.
        assertThat(registry.liveGenerations().sumOf { it.refCount }).isZero()
        source.rows[4] = "delta"
        clock.advance(Duration.ofMinutes(1))
        assertThat(cache.triggerRefresh(group).generation)
            .describedAs("releasing the lease unblocks the ceiling without intervention")
            .isNotNull()
    }

    /**
     * D31's monotonicity guard against real timestamps. `data_as_of` comes from the framework's
     * injected clock via the generation it leased, so archiving twice without an intervening
     * refresh offers the same instant - which spec 18.3 step 2 requires be refused.
     */
    @Test
    fun `archiving the same generation twice is refused rather than published`() {
        source.rows = mutableMapOf(1L to "alpha")
        refresh()
        assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.PUBLISHED)

        assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.SKIPPED_NOT_NEWER)

        assertThat(manifest.expired(group, FAR_FUTURE).filter { it.status == ArchiveStatus.COMPLETE })
            .describedAs("the refused run must not have left a second COMPLETE version")
            .hasSize(1)
    }

    /** A consumer with no recorded watermark full-compares; nothing about that is an error. */
    @Test
    fun `a consumer that has never run gets the full-compare signal`() {
        source.rows = mutableMapOf(1L to "alpha")
        refresh()

        val reason = diff.withDiff(group, watermark = null) { d ->
            assertThat(d).isInstanceOf(Diff.FullCompare::class.java)
            (d as Diff.FullCompare).reason
        }

        assertThat(reason).isEqualTo(FallbackReason.ABSENT)
    }

    /**
     * D35's long-running-job race, which is the only shape that separates the recorded
     * watermark from "the latest checkpoint now".
     *
     * A checkpoint published *while this run holds its lease* describes state the run never
     * read. Spec 18.4: the watermark "is a recorded version, never 'the latest checkpoint
     * now' (taken after the last run; using it silently under-reports)". So v2 is created
     * inside the diff block, and the watermark handed back must still be v1.
     *
     * Asserted this way because the single-checkpoint version of this test passes under either
     * rule - it cannot tell the `data_as_of <= T` predicate from its absence.
     */
    @Test
    fun `a checkpoint published mid-run never becomes this run's watermark`() {
        source.rows = mutableMapOf(1L to "alpha")
        refresh()
        assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.PUBLISHED)
        val v1 = manifest.newestComplete(group)!!.version

        val next = diff.withDiff(group, v1) { d ->
            // Mid-run: a newer generation is published and archived. The lease is on the old one.
            source.rows[2] = "beta"
            clock.advance(Duration.ofMinutes(1))
            refresh()
            assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.PUBLISHED)
            val v2 = manifest.newestComplete(group)!!.version
            assertThat(v2).describedAs("a genuinely newer checkpoint exists").isGreaterThan(v1)

            d.nextWatermark()
        }

        assertThat(next)
            .describedAs("v2 describes data this run never read, so it must not become its baseline")
            .isEqualTo(v1)
    }

    private fun refresh() {
        val outcome = cache.triggerRefresh(group)
        check(outcome.generation != null) { "refresh did not publish: $outcome" }
    }

    /** Rows the next refresh will write. Mutating it is how a test creates a change to find. */
    private class MutableSource {
        var rows: MutableMap<Long, String> = mutableMapOf()

        fun refresh(ctx: BuildContext) {
            ctx.target.createStatement().use { statement ->
                statement.execute("CREATE TABLE t_a (id BIGINT NOT NULL, name VARCHAR NOT NULL)")
                for ((id, name) in rows) {
                    statement.execute("INSERT INTO t_a VALUES ($id, '${name.replace("'", "''")}')")
                }
            }
        }
    }

    /** `dataAsOf` is `clock.instant()` inside the refresh, so moving this moves the snapshot's moment. */
    private class MovingClock(private var now: Instant) : Clock() {
        override fun getZone() = ZoneOffset.UTC
        override fun withZone(zone: java.time.ZoneId): Clock = this
        override fun instant(): Instant = now
        fun advance(by: Duration) { now = now.plus(by) }
    }

    companion object {

        /** K=1 makes the lease-versus-publish question in the second test unavoidable. */
        private const val K = 1
        private const val BUCKET = "snapshot-archive"
        private val FAR_FUTURE: Instant = Instant.parse("2099-01-01T00:00:00Z")
        private val GROUPS = AtomicLong()

        @Container
        @JvmStatic
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:slim-faststart")

        @Container
        @JvmStatic
        val minio: MinIOContainer = MinIOContainer("minio/minio:RELEASE.2024-10-02T17-50-41Z")

        private lateinit var jdbi: Jdbi
        private lateinit var minioClient: MinioClient

        @BeforeAll
        @JvmStatic
        fun createBackingServices() {
            jdbi = Jdbi.create(oracle.jdbcUrl, oracle.username, oracle.password)
            jdbi.useHandle<RuntimeException> { handle -> ManifestSchema.DDL.forEach { handle.execute(it) } }
            minioClient = MinioClient.builder()
                .endpoint(minio.s3URL)
                .credentials(minio.userName, minio.password)
                .build()
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build())
        }
    }
}
