package infra.snapshotarchive

import infra.snapshotcache.api.GroupId
import org.assertj.core.api.Assertions.assertThat
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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * P13 acceptance: convergence and retention.
 *
 * Real Oracle, because every property here rests on the conditional transitions of ticket 02
 * and on what a `UPDATE ... WHERE status = ?` reports when it matches nothing - a
 * compatibility mode would prove the protocol against a compatibility mode. A fake object
 * store, because none of this is about MinIO's wire behaviour; [ObjectStoreTest] covers what
 * the fake stands in for, `delete` included. Real DuckDB behind the two tests that drive the
 * real [Archiver], because those replay ticket 03's crash matrix and must crash the real run.
 *
 * Time is the injected [Clock] throughout: the DAO stamps rows at a fixed instant and each
 * pass runs at whatever later instant the test needs. Zero sleeps, and no test waits for a
 * timeout to elapse - it simply happens later.
 */
@Testcontainers
class ArchiveMaintenanceTest {

    private val group = GroupId("m${GROUPS.incrementAndGet()}")
    // The purge deletes; nothing here downloads. Matches the surface this suite's own fake
    // overrode before consolidation.
    private val store = RecordingObjectStore(
        setOf(RecordingObjectStore.Op.PUT, RecordingObjectStore.Op.SIZE_OF, RecordingObjectStore.Op.DELETE),
    )
    private val cache = FileBackedCache(database)
    private val tempRoot: Path = Files.createTempDirectory(scratch, "maintenance-")
    private val warnings = LogCapture.install()

    @AfterEach
    fun removeAlertCapture() {
        warnings.uninstall()
    }

    // --- watchdog -----------------------------------------------------------------------

    /** The crashed-after-uploading case: the run did everything but the flip, so the flip happens. */
    @Test
    fun `a PENDING row past the timeout whose objects all landed is completed`() {
        val entry = pending(T0)

        maintenance().watchdog(group)

        assertThat(find(entry).status).isEqualTo(ArchiveStatus.COMPLETE)
        assertThat(store.stored.keys).containsExactlyInAnyOrderElementsOf(keysOf(entry))
    }

    /**
     * The crashed-mid-upload case. One object of two is missing, so the version can never be
     * honoured and saying so is the only useful answer: consumers fall back to a full compare
     * (D34), which is why FAILED is a normal outcome rather than an error.
     */
    @Test
    fun `a PENDING row past the timeout with a missing object is failed, loudly`() {
        val entry = pending(T0, upload = listOf("t_a"))

        maintenance().watchdog(group)

        assertThat(find(entry).status).isEqualTo(ArchiveStatus.FAILED)
        assertThat(warnings.messages).anyMatch { it.contains("ALERT") && it.contains("t_b.parquet") }
    }

    /**
     * The timeout is the whole point of the timeout: below it, a PENDING row is an upload in
     * progress, not wreckage. Failing one would throw away a checkpoint that was seconds from
     * being published.
     */
    @Test
    fun `a PENDING row younger than the timeout is left alone`() {
        val entry = pending(T0, upload = emptyList())

        maintenance(now = T0.plus(Duration.ofMinutes(5))).watchdog(group)

        assertThat(find(entry).status).isEqualTo(ArchiveStatus.PENDING)
    }

    /**
     * The race the ticket-02 conditional transitions exist for, driven for real: a run parked
     * between its last upload and its flip, with the watchdog ruling on the row in the gap.
     *
     * Exactly one side moves the row. The watchdog wins here because it goes first, and the
     * uploader is told it changed nothing rather than assuming it won - and the verdict is
     * the same either way, because by then every object is in the bucket.
     */
    @Test
    fun `the watchdog and the uploader resolve their race to exactly one winner`() {
        val uploaded = CountDownLatch(1)
        val proceed = CountDownLatch(1)
        val outcome = AtomicReference<RunOutcome>()

        archiver(steps = { _, step ->
            if (step == ArchiveStep.AFTER_UPLOAD) {
                uploaded.countDown()
                check(proceed.await(10, TimeUnit.SECONDS)) { "the parked run was never released" }
            }
        }).use { archiver ->
            val run = Thread { outcome.set(archiver.runOnce(group)) }.apply { start() }
            check(uploaded.await(30, TimeUnit.SECONDS)) { "the run never reached AFTER_UPLOAD" }

            maintenance().watchdog(group)
            assertThat(onlyRow().status).isEqualTo(ArchiveStatus.COMPLETE)

            proceed.countDown()
            run.join(30_000)
        }

        assertThat(outcome.get()).isEqualTo(RunOutcome.LOST_RACE)
        assertThat(onlyRow().status).isEqualTo(ArchiveStatus.COMPLETE)
    }

    /**
     * Ticket 03's crash matrix, run on through the maintenance passes. Each injected crash
     * has to reach a terminal state - resolved, or reclaimed and gone - and the pass has to be
     * idempotent, so the second one is asserted to change nothing.
     *
     * The invariant checked after every pass is D33's, extended over the purge: nothing in the
     * bucket that no manifest row covers. That is the case the design calls impossible, and
     * this is where it is asserted rather than swept for.
     */
    @ParameterizedTest
    @EnumSource(ArchiveStep::class)
    fun `every crash in the archiver's matrix converges within two passes`(crashAt: ArchiveStep) {
        val passages = AtomicInteger()
        val crashOn = if (crashAt == ArchiveStep.BEFORE_EACH_UPLOAD) 2 else 1
        archiver(steps = { _, step ->
            if (step == crashAt && passages.incrementAndGet() == crashOn) {
                throw IllegalStateException("injected crash at $step")
            }
        }).use { archiver -> runCatching { archiver.runOnce(group) } }

        val maintenance = maintenance()
        maintenance.sweep()
        assertThat(warnings.failures).isEmpty()
        assertThat(rows().map { it.status }).doesNotContain(ArchiveStatus.PENDING)
        assertNoDanglingObjects()

        val afterFirst = rows()
        val objectsAfterFirst = store.stored.toMap()
        maintenance.sweep()
        assertThat(warnings.failures).isEmpty()
        assertThat(rows()).isEqualTo(afterFirst)
        assertThat(store.stored).isEqualTo(objectsAfterFirst)
        assertNoDanglingObjects()
    }

    // --- purge --------------------------------------------------------------------------

    /**
     * The reclaim order, observed where it matters rather than inferred from the end state.
     * At the moment each object is deleted its row must still be there, and must already have
     * stopped being COMPLETE - so a crash anywhere in the middle leaves objects a row still
     * covers, and never a trusted row the bucket can no longer honour.
     */
    @Test
    fun `purge marks, then deletes objects, then deletes the row - for expired and FAILED alike`() {
        val expired = completed(T0.minus(Duration.ofHours(72)))
        val newest = completed(T0.minus(Duration.ofHours(71)))
        val failed = failed(T0.minus(Duration.ofHours(70)))
        val seen = CopyOnWriteArrayList<Pair<String, ArchiveStatus?>>()
        store.beforeDelete = { key -> seen += key to rows().firstOrNull { key in keysOf(it) }?.status }

        maintenance().purge(group)

        assertThat(seen.map { it.first })
            .containsExactlyInAnyOrderElementsOf(keysOf(expired) + keysOf(failed))
        assertThat(seen.map { it.second }).containsOnly(ArchiveStatus.FAILED)
        assertThat(rows().map { it.version }).containsExactly(newest.version)
        assertThat(store.stored.keys).containsExactlyInAnyOrderElementsOf(keysOf(newest))
    }

    /**
     * D34, unconditionally. Every version in the window has expired - which is exactly what a
     * broken archiver looks like from here - and the newest COMPLETE one still survives with
     * its objects, because the alternative is that the last good baseline evaporates at the
     * moment nobody is publishing a replacement.
     */
    @Test
    fun `keep-newest-COMPLETE survives a window in which every version is expired`() {
        val old = completed(T0.minus(Duration.ofDays(30)))
        val older = completed(T0.minus(Duration.ofDays(20)))
        val newest = completed(T0.minus(Duration.ofDays(10)))

        maintenance().purge(group)

        assertThat(rows().map { it.version }).containsExactly(newest.version)
        assertThat(store.stored.keys).containsExactlyInAnyOrderElementsOf(keysOf(newest))
        assertThat(store.stored.keys).doesNotContainAnyElementsOf(keysOf(old) + keysOf(older))
    }

    /**
     * The one case where reclaiming promptly would be wrong. A version FAILED more recently
     * than the watchdog timeout may still have the uploader that lost the race writing into
     * it; deleting its objects now would let those later writes land behind a row that no
     * longer exists, which is the dangling object the whole ordering is built to prevent.
     * Waiting out one timeout - by definition longer than an upload can take - closes it.
     */
    @Test
    fun `a version FAILED more recently than the timeout is left for a later pass`() {
        val fresh = failed(T0.minus(Duration.ofHours(70)))

        maintenance(now = T0.plus(Duration.ofMinutes(5))).purge(group)
        assertThat(rows().map { it.version }).containsExactly(fresh.version)
        assertThat(store.stored.keys).containsExactlyInAnyOrderElementsOf(keysOf(fresh))

        maintenance(now = T0.plus(Duration.ofMinutes(20))).purge(group)
        assertThat(rows()).isEmpty()
        assertThat(store.stored).isEmpty()
    }

    // --- staleness ----------------------------------------------------------------------

    /** Spec 18.5's operational alert. Nothing is wrong with the diffs; something is wrong with us. */
    @Test
    fun `the staleness alert fires when the newest COMPLETE checkpoint is too old`() {
        val maintenance = maintenance(now = T0)

        assertThat(maintenance.staleness(group)).isNull()
        assertThat(warnings.messages).anyMatch { it.contains("ALERT") && it.contains("no COMPLETE") }

        warnings.messages.clear()
        completed(T0.minus(Duration.ofHours(4)))
        assertThat(maintenance.staleness(group)).isEqualTo(Duration.ofHours(4))
        assertThat(warnings.messages).anyMatch { it.contains("ALERT") && it.contains(group.value) }
    }

    @Test
    fun `the staleness alert stays quiet while the archiver is keeping up`() {
        completed(T0.minus(Duration.ofHours(1)))

        assertThat(maintenance(now = T0).staleness(group)).isEqualTo(Duration.ofHours(1))

        assertThat(warnings.messages).noneMatch { it.contains("ALERT") }
    }

    // --- fixture ------------------------------------------------------------------------

    private fun maintenance(
        now: Instant = T0.plus(Duration.ofHours(1)),
        retention: Duration = Duration.ofHours(48),
        staleness: Duration = Duration.ofHours(3),
    ): ArchiveMaintenance = ArchiveMaintenance(
        manifest = dao,
        objects = store,
        groups = listOf(group),
        clock = Clock.fixed(now, ZoneOffset.UTC),
        retention = retention,
        stalenessThreshold = staleness,
    )

    private fun archiver(steps: (GroupId, ArchiveStep) -> Unit): Archiver = Archiver(
        cache = cache,
        manifest = dao,
        objects = store,
        tables = mapOf(group to listOf("t_a", "t_b")),
        tempRoot = tempRoot,
        drainBudget = Duration.ofSeconds(10),
        steps = steps,
    )

    /**
     * A PENDING version with a synthetic inventory, and by default every object in the bucket
     * at the size the inventory records. The DAO's clock is fixed, so the row's age is
     * whatever the pass's clock says it is.
     */
    private fun pending(
        dataAsOf: Instant,
        tables: List<String> = listOf("t_a", "t_b"),
        upload: List<String> = tables,
    ): ManifestEntry {
        val inventory = tables.map { ArchivedObject(it, "$it.parquet", BYTES.toLong(), CHECKSUM, 10) }
        val entry = dao.insertPending(group, dataAsOf, Inventory.encode(inventory), generation = 1)
        upload.forEach { store.seed(keyOf(entry, "$it.parquet"), BYTES) }
        return entry
    }

    private fun completed(dataAsOf: Instant): ManifestEntry =
        pending(dataAsOf).also { dao.markComplete(group, it.version) }

    private fun failed(dataAsOf: Instant): ManifestEntry =
        pending(dataAsOf).also { dao.markFailed(group, it.version) }

    private fun rows(): List<ManifestEntry> = dao.expired(group, FAR_FUTURE)

    private fun onlyRow(): ManifestEntry = rows().single()

    private fun find(entry: ManifestEntry): ManifestEntry =
        requireNotNull(dao.find(group, entry.version)) { "version ${entry.version} was deleted" }

    private fun keyOf(entry: ManifestEntry, objectKey: String): String =
        entry.uriPrefix.removePrefix("$BUCKET/") + objectKey

    private fun keysOf(entry: ManifestEntry): List<String> =
        Inventory.decode(entry.inventory).map { keyOf(entry, it.objectKey) }

    /**
     * D33's guarantee, asserted rather than swept for: every object in the bucket is covered
     * by a manifest row that names it, and a COMPLETE row's inventory is one the bucket can
     * still honour.
     */
    private fun assertNoDanglingObjects() {
        val rows = rows()
        assertThat(store.stored.keys).isSubsetOf(rows.flatMap { keysOf(it) })
        rows.filter { it.status == ArchiveStatus.COMPLETE }.forEach { row ->
            assertThat(store.stored.keys).containsAll(keysOf(row))
        }
    }

    companion object {

        private const val BUCKET = "test-bucket"
        // Int, because the only two consumers are an Int (`RecordingObjectStore.seed`) and a
        // Long (`ArchivedObject.bytes`), and only one of those conversions is lossless.
        private const val BYTES = 4_096
        private val CHECKSUM = "0".repeat(64)
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

            scratch = Files.createTempDirectory("maintenance-test")
            database = scratch.resolve("generation.db")
            DriverManager.getConnection("jdbc:duckdb:$database").use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE t_a AS SELECT i AS id FROM range(200) t(i)")
                    statement.execute("CREATE TABLE t_b AS SELECT i AS id FROM range(100) t(i)")
                }
            }
        }
    }
}
