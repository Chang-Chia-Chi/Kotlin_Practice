package infra.snapshotarchive

import infra.snapshotcache.api.GroupId
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * P12 acceptance: the spec 18.3 publish protocol, end to end.
 *
 * Real Oracle for the manifest, because every safety property here is about what survives a
 * crash and the conditional transitions that decide it - a compatibility mode would prove
 * the protocol works against a compatibility mode. Real DuckDB behind the snapshot, because
 * the export is a real `COPY ... TO parquet` off a READ_ONLY-attached generation file.
 * A fake object store, because the crash matrix and the interleavings have no business
 * waiting on a container; [ObjectStoreTest] covers what the fake stands in for.
 *
 * Zero sleeps. Every wait is a latch, a barrier, or a bounded join, and every bound is a
 * bound on a broken implementation rather than on sequencing (spec 17.4).
 */
@Testcontainers
class ArchiverTest {

    private val group = GroupId("g${GROUPS.incrementAndGet()}")
    private val cache = FileBackedCache(database)
    // put + sizeOf only, as this suite's own fake used to be: the archiver uploads and
    // verifies, and must never reach get or delete. Reclaiming is ticket 04's.
    private val store = RecordingObjectStore()
    private val tempRoot: Path = Files.createTempDirectory(scratch, "archiver-")
    private val warnings = LogCapture.install()

    @AfterEach
    fun removeWarnCapture() {
        warnings.uninstall()
    }

    @Test
    fun `a run publishes a COMPLETE version whose objects match the recorded inventory exactly`() {
        archiver().use { archiver ->
            assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.PUBLISHED)
        }

        val row = onlyRow()
        assertThat(row.status).isEqualTo(ArchiveStatus.COMPLETE)
        assertThat(row.dataAsOf).isEqualTo(cache.dataAsOf)
        assertThat(row.generation).isEqualTo(cache.generation)

        val inventory = Inventory.decode(row.inventory)
        assertThat(inventory.map { it.table }).containsExactlyInAnyOrder("t_a", "t_b")
        assertThat(inventory.single { it.table == "t_a" }.rowCount).isEqualTo(A_ROWS)
        assertThat(inventory.single { it.table == "t_b" }.rowCount).isEqualTo(B_ROWS)

        // "Match exactly" is a set equality in both directions: nothing in the bucket that the
        // inventory does not name, nothing named that is not in the bucket at the right size.
        assertThat(store.stored.keys).containsExactlyInAnyOrderElementsOf(keysOf(row))
        inventory.forEach { obj ->
            assertThat(store.sizeOf(keyOf(row, obj))).isEqualTo(obj.bytes)
            assertThat(obj.bytes).isPositive()
            assertThat(obj.checksum).hasSize(64)
        }
    }

    /**
     * D33's ordering, asserted where it actually matters. The store reports, at the instant
     * of each upload, what the manifest said about this group - so a reordering that put an
     * object in the bucket first would be caught even though the finished state looks the same.
     */
    @Test
    fun `no object is uploaded before its covering PENDING row is committed`() {
        val statusAtUpload = CopyOnWriteArrayList<Pair<String, ArchiveStatus?>>()
        store.beforePut = { key -> statusAtUpload += key to rows().singleOrNull()?.status }

        archiver().use { it.runOnce(group) }

        val row = onlyRow()
        assertThat(statusAtUpload).hasSize(2)
        assertThat(statusAtUpload.map { it.second }).containsOnly(ArchiveStatus.PENDING)
        assertThat(statusAtUpload.map { it.first }).containsExactlyInAnyOrderElementsOf(keysOf(row))
    }

    /** Spec 18.3 step 2 / D31: equal is a regression too, and the skip is loud. */
    @Test
    fun `a run whose data_as_of is not newer than the newest COMPLETE skips and alerts`() {
        archiver().use { archiver ->
            assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.PUBLISHED)
            val published = onlyRow()
            store.stored.clear()

            assertThat(archiver.runOnce(group)).isEqualTo(RunOutcome.SKIPPED_NOT_NEWER)

            assertThat(rows().map { it.version }).containsExactly(published.version)
            assertThat(store.stored).isEmpty()
        }
        assertThat(warnings.messages).anyMatch { it.contains("ALERT") && it.contains(group.value) }
    }

    /**
     * The crash matrix. One injected failure at every step boundary of spec 18.3, each
     * checked against the one property the whole ordering exists to buy: whatever is in the
     * bucket is covered by a manifest row, and a COMPLETE row is one a reader can trust.
     *
     * A crash at [ArchiveStep.BEFORE_EACH_UPLOAD] fires on the second object, so the
     * genuinely partial state - a PENDING row with some of its objects present - is one of
     * the cases actually exercised rather than one argued about.
     */
    @ParameterizedTest
    @EnumSource(ArchiveStep::class)
    fun `a crash at any step leaves no row, or a PENDING row, or a COMPLETE row that is true`(
        crashAt: ArchiveStep,
    ) {
        val passages = AtomicInteger()
        val crashOn = if (crashAt == ArchiveStep.BEFORE_EACH_UPLOAD) 2 else 1
        val archiver = archiver(steps = { _, step ->
            if (step == crashAt && passages.incrementAndGet() == crashOn) {
                throw IllegalStateException("injected crash at $step")
            }
        })

        val thrown = archiver.use { running -> catchThrowable { running.runOnce(group) } }

        // AFTER_COMPLETE is past the last decision, so the run reports success and the crash
        // surfaces on the way out; every earlier one aborts the run.
        assertThat(thrown).isInstanceOf(IllegalStateException::class.java)
        assertConverged()
        // The lease is scoped to the run on every exit path, crash included.
        assertThat(cache.liveLeases).hasValue(0)
        assertThat(tempRoot).doesNotExist()
    }

    /** Spec 18.2: a run that finds its group busy skips and logs; it never queues. */
    @Test
    fun `the same group never runs twice at once`() {
        val parked = CountDownLatch(1)
        val proceed = CountDownLatch(1)
        val second = AtomicReference<RunOutcome>()

        archiver(steps = { _, step ->
            if (step == ArchiveStep.AFTER_EXPORT) {
                parked.countDown()
                check(proceed.await(10, TimeUnit.SECONDS)) { "the parked run was never released" }
            }
        }).use { archiver ->
            val first = Thread { archiver.runOnce(group) }.apply { start() }
            check(parked.await(10, TimeUnit.SECONDS)) { "the first run never reached AFTER_EXPORT" }

            second.set(archiver.runOnce(group))

            proceed.countDown()
            first.join(10_000)
        }

        assertThat(second.get()).isEqualTo(RunOutcome.SKIPPED_BUSY)
        assertThat(rows()).hasSize(1)
    }

    /**
     * Cross-group parallelism, proved rather than observed: both runs have to be inside the
     * export step at the same moment or the barrier never trips and the test fails loudly.
     */
    @Test
    fun `different groups run in parallel`() {
        val other = GroupId("g${GROUPS.incrementAndGet()}")
        val bothInFlight = CyclicBarrier(2)

        archiver(
            groups = listOf(group, other),
            steps = { _, step ->
                if (step == ArchiveStep.AFTER_EXPORT) bothInFlight.await(10, TimeUnit.SECONDS)
            },
        ).use { archiver ->
            val a = archiver.submit(group)
            val b = archiver.submit(other)
            a.get(30, TimeUnit.SECONDS)
            b.get(30, TimeUnit.SECONDS)
        }

        assertThat(onlyRow().status).isEqualTo(ArchiveStatus.COMPLETE)
        assertThat(rows(other).single().status).isEqualTo(ArchiveStatus.COMPLETE)
    }

    /**
     * Plan P12's per-table clause, proved the same way: the two exports have to overlap or
     * the barrier never trips. Serialising them would leave the first task waiting for a
     * partner that has not been submitted yet.
     */
    @Test
    fun `per-table export tasks run in parallel within a run`() {
        val bothExporting = CyclicBarrier(2)
        cache.onConnection = { bothExporting.await(10, TimeUnit.SECONDS) }

        archiver().use { assertThat(it.runOnce(group)).isEqualTo(RunOutcome.PUBLISHED) }

        assertThat(onlyRow().status).isEqualTo(ArchiveStatus.COMPLETE)
    }

    /**
     * Spec 18.3's shutdown clause, all of it. The run is parked mid-upload on the archiver's
     * own pool, so `close` interrupts it for real; what has to come back is the lease, an
     * empty temp root, and - deliberately - a PENDING row nobody touched.
     */
    @Test
    fun `shutdown mid-upload releases the lease, deletes temp files, and leaves the PENDING row alone`() {
        val uploading = CountDownLatch(1)
        val neverReleased = CountDownLatch(1)
        val archiver = archiver(steps = { _, step ->
            if (step == ArchiveStep.BEFORE_EACH_UPLOAD) {
                uploading.countDown()
                neverReleased.await() // interruptible: this is what shutdown breaks
            }
        })

        archiver.submit(group)
        check(uploading.await(10, TimeUnit.SECONDS)) { "the run never reached the upload step" }
        assertThat(cache.liveLeases).hasValue(1)

        archiver.close()

        assertThat(cache.liveLeases).hasValue(0)
        assertThat(tempRoot).doesNotExist()
        // The watchdog of ticket 04 owns this row. Resolving it here would build a second
        // recovery path that only clean exits ever exercise.
        assertThat(onlyRow().status).isEqualTo(ArchiveStatus.PENDING)
    }

    /** The scheduler is four lines, but four lines nothing else would notice were missing. */
    @Test
    fun `start schedules a run for every configured group`() {
        val other = GroupId("g${GROUPS.incrementAndGet()}")
        val seen = ConcurrentHashMap.newKeySet<GroupId>()
        val ran = CountDownLatch(2)

        archiver(
            groups = listOf(group, other),
            // Counted once per group: a group's later scheduled runs are refused by the D31
            // guard anyway, but a latch that any two passages could satisfy would not be
            // asserting what the test claims.
            steps = { runGroup, step ->
                if (step == ArchiveStep.AFTER_COMPLETE && seen.add(runGroup)) ran.countDown()
            },
        ).use { archiver ->
            archiver.start(Duration.ofMillis(20))
            check(ran.await(30, TimeUnit.SECONDS)) { "the scheduler never ran both groups" }
        }

        assertThat(seen).containsExactlyInAnyOrder(group, other)
    }

    // --- fixture ------------------------------------------------------------------------

    private fun archiver(
        groups: List<GroupId> = listOf(group),
        steps: (GroupId, ArchiveStep) -> Unit = { _, _ -> },
    ): Archiver = Archiver(
        cache = cache,
        manifest = dao,
        objects = store,
        tables = groups.associateWith { listOf("t_a", "t_b") },
        tempRoot = tempRoot,
        drainBudget = Duration.ofSeconds(10),
        steps = steps,
    )

    private fun rows(of: GroupId = group): List<ManifestEntry> = dao.expired(of, FAR_FUTURE)

    private fun onlyRow(): ManifestEntry = rows().single()

    private fun keyOf(row: ManifestEntry, obj: ArchivedObject): String =
        row.uriPrefix.removePrefix("$BUCKET/") + obj.objectKey

    private fun keysOf(row: ManifestEntry): List<String> =
        Inventory.decode(row.inventory).map { keyOf(row, it) }

    /**
     * The invariant behind every row of the crash matrix (D33): no object without a covering
     * manifest row, and no COMPLETE row whose inventory the bucket cannot honour.
     */
    private fun assertConverged() {
        val rows = rows()
        if (rows.isEmpty()) {
            assertThat(store.stored.keys).isEmpty()
            return
        }
        val row = rows.single()
        val inventory = Inventory.decode(row.inventory)
        val covered = inventory.map { keyOf(row, it) }
        assertThat(store.stored.keys).isSubsetOf(covered)
        if (row.status == ArchiveStatus.COMPLETE) {
            assertThat(store.stored.keys).containsExactlyInAnyOrderElementsOf(covered)
            inventory.forEach { assertThat(store.sizeOf(keyOf(row, it))).isEqualTo(it.bytes) }
        }
    }

    companion object {

        private const val BUCKET = "test-bucket"
        private const val A_ROWS = 2_000L
        private const val B_ROWS = 300L
        private val T0: Instant = Instant.parse("2026-08-29T10:00:00Z")
        private val FAR_FUTURE: Instant = Instant.parse("2999-01-01T00:00:00Z")
        private val GROUPS = AtomicLong()

        @Container
        @JvmStatic
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:slim-faststart")

        private lateinit var dao: ManifestDao
        private lateinit var scratch: Path
        private lateinit var database: Path

        @BeforeAll
        @JvmStatic
        fun createFixtures() {
            val jdbi = Jdbi.create(oracle.jdbcUrl, oracle.username, oracle.password)
            jdbi.useHandle<RuntimeException> { handle -> ManifestSchema.DDL.forEach { handle.execute(it) } }
            dao = ManifestDao(jdbi, bucket = BUCKET, clock = Clock.fixed(T0, ZoneOffset.UTC))

            scratch = Files.createTempDirectory("archiver-test")
            database = scratch.resolve("generation.db")
            DriverManager.getConnection("jdbc:duckdb:$database").use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE t_a AS SELECT i AS id, 'row-' || i AS name FROM range($A_ROWS) t(i)")
                    statement.execute("CREATE TABLE t_b AS SELECT i AS id FROM range($B_ROWS) t(i)")
                }
            }
        }
    }
}
