package infra.snapshotarchive

import infra.snapshotcache.api.CopyOutResult
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCache
import io.minio.MinioClient
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

/**
 * P14 acceptance: the spec 18.4 ETL protocol.
 *
 * Real Oracle for the manifest, because the watermark predicate and the purged-baseline
 * fallback are the DAO's SQL and ticket 04's purge, not a mock's opinion of them. Real DuckDB
 * for both halves of the join - a checkpoint written by the real [Archiver] as real Parquet,
 * read back through `read_parquet` against a real READ_ONLY attach. A fake object store,
 * because nothing here is about MinIO's wire behaviour.
 *
 * Zero sleeps. The one concurrency claim - parallel downloads - is a barrier that never trips
 * if the downloads were serialized.
 */
@Testcontainers
class EtlDiffTest {

    private val group = GroupId("d${GROUPS.incrementAndGet()}")
    private val store = BytesObjectStore()
    private val work: Path = Files.createTempDirectory(scratch, "diff-")
    private val database: Path = work.resolve("generation.db")
    private val cache = DiffCache(database)
    private val archiver = Archiver(
        cache = cache,
        manifest = dao,
        objects = store,
        tables = mapOf(group to TABLES),
        tempRoot = work.resolve("archive"),
        drainBudget = Duration.ofSeconds(10),
    )
    private val helper = EtlDiff(
        cache = cache,
        manifest = dao,
        objects = store,
        primaryKeys = mapOf(group to KEYS),
        downloadRoot = work.resolve("download"),
    )

    @BeforeEach
    fun createGeneration() {
        write(
            "CREATE TABLE t_a (id INTEGER, name VARCHAR, balance INTEGER)",
            "CREATE TABLE t_b (id INTEGER, label VARCHAR)",
            "INSERT INTO t_b VALUES (1, 'x')",
        )
    }

    @AfterEach
    fun stop() {
        archiver.close()
        helper.close()
    }

    // --- the diff itself ----------------------------------------------------------------

    /**
     * The E2E the ticket is for: a checkpoint published off one generation, edits applied to
     * the next, and the exact I/U/D that separates them.
     *
     * `changed_columns` is asserted per row rather than counted, because it is the part an
     * ETL writes into its target: an update that names the wrong column writes the right row
     * with the wrong content, which no row-count assertion would catch.
     */
    @Test
    fun `the diff yields exactly the inserted, updated and deleted rows with their changed columns`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
        val v1 = publish()

        write(
            "INSERT INTO t_a VALUES (4, 'd', 40)",
            "UPDATE t_a SET balance = 25 WHERE id = 2",
            "DELETE FROM t_a WHERE id = 3",
        )

        helper.withDiff(group, v1) { diff ->
            assertThat(diff).isInstanceOf(Diff.Incremental::class.java)
            assertThat((diff as Diff.Incremental).baselineVersion).isEqualTo(v1)
            assertThat(diff.changes("t_a")).containsExactlyInAnyOrder(
                ChangedRow(
                    key = listOf(4),
                    op = DiffOp.I,
                    changedColumns = listOf("name", "balance"),
                    values = mapOf("id" to 4, "name" to "d", "balance" to 40),
                ),
                ChangedRow(
                    key = listOf(2),
                    op = DiffOp.U,
                    changedColumns = listOf("balance"),
                    values = mapOf("id" to 2, "name" to "b", "balance" to 25),
                ),
                ChangedRow(key = listOf(3), op = DiffOp.D, changedColumns = emptyList(), values = emptyMap()),
            )
            // An untouched table is not a source of noise: the join finds nothing to say.
            assertThat(diff.changes("t_b")).isEmpty()
        }
    }

    /**
     * Plan P14's parallel clause, proved rather than observed. Both downloads have to be in
     * flight at once or the barrier never trips and the test fails loudly; serializing them
     * would leave the first waiting for a partner that was never submitted.
     */
    @Test
    fun `per-table checkpoint downloads run in parallel`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()
        val bothDownloading = CyclicBarrier(TABLES.size)
        store.beforeGet = { bothDownloading.await(10, TimeUnit.SECONDS) }

        helper.withDiff(group, v1) { diff ->
            assertThat((diff as Diff.Incremental).changes("t_a")).isEmpty()
        }
    }

    /**
     * The lease is the reason this is a scoped block rather than a function returning rows.
     * It has to be held for the whole diff - otherwise the live side of the comparison could
     * be reclaimed underneath it - and it has to come back on the path where the ETL's own
     * `apply` throws, which is the path a leak would hide in.
     */
    @Test
    fun `the snapshot lease is held for the whole diff and released on every exit path`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()

        helper.withDiff(group, v1) { diff ->
            assertThat(cache.liveLeases).hasValue(1)
            assertThat((diff as Diff.Incremental).changes("t_a")).isEmpty()
        }
        assertThat(cache.liveLeases).hasValue(0)

        assertThatThrownBy {
            helper.withDiff(group, v1) { throw IllegalStateException("the ETL's own apply failed") }
        }.hasMessage("the ETL's own apply failed")
        assertThat(cache.liveLeases).hasValue(0)
        assertThat(work.resolve("download")).isEmptyDirectory()
    }

    // --- the fallback -------------------------------------------------------------------

    /** A brand new ETL. Nothing is wrong; it has simply never recorded anything. */
    @Test
    fun `an absent watermark returns a full-compare signal`() {
        publish()

        helper.withDiff(group, watermark = null) { diff ->
            assertThat(diff).isInstanceOf(Diff.FullCompare::class.java)
            assertThat((diff as Diff.FullCompare).reason).isEqualTo(FallbackReason.ABSENT)
            // The signal is worth nothing without the lease: the caller anti-joins against this.
            assertThat(diff.snapshot.generation).isEqualTo(cache.generation)
        }
    }

    /**
     * The job ran slower than retention, driven through ticket 04's real purge rather than by
     * deleting a row by hand. This is the case that lets retention stay a fixed window
     * instead of a consumer registration scheme, so it has to be a return value.
     */
    @Test
    fun `a purged watermark returns a full-compare signal`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()
        write("INSERT INTO t_a VALUES (2, 'b', 20)")
        publish()

        ArchiveMaintenance(
            manifest = dao,
            objects = store,
            groups = listOf(group),
            clock = Clock.fixed(T0.plus(Duration.ofDays(30)), ZoneOffset.UTC),
        ).use { it.purge(group) }
        assertThat(dao.find(group.value, v1)).isNull()

        helper.withDiff(group, v1) { diff ->
            assertThat((diff as Diff.FullCompare).reason).isEqualTo(FallbackReason.PURGED)
        }
    }

    /**
     * A version that exists but is not COMPLETE - here one the purge has marked on its way to
     * reclaiming it. Its objects may already be half gone, so trusting it would be the one
     * way this layer could hand an ETL a baseline that is not a snapshot of anything.
     */
    @Test
    fun `a watermark whose version is not COMPLETE returns a full-compare signal`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()
        assertThat(dao.retire(group.value, v1)).isTrue()

        helper.withDiff(group, v1) { diff ->
            assertThat((diff as Diff.FullCompare).reason).isEqualTo(FallbackReason.NOT_COMPLETE)
        }
    }

    // --- the watermark ------------------------------------------------------------------

    /**
     * D35's predicate, at its boundaries: equal `data_as_of` counts, later does not, and a
     * group with nothing COMPLETE at or before the snapshot's moment yields null - which the
     * next run reads back as [FallbackReason.ABSENT] and full-compares.
     */
    @Test
    fun `the next watermark is the newest COMPLETE version at or before the snapshot's dataAsOf`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()
        val atV1 = cache.dataAsOf
        write("INSERT INTO t_a VALUES (2, 'b', 20)")
        val v2 = publish()

        assertThat(watermarkAt(cache.dataAsOf)).isEqualTo(v2)
        assertThat(watermarkAt(atV1)).isEqualTo(v1)
        assertThat(watermarkAt(atV1.minusMillis(1))).isNull()
    }

    /**
     * The long-running-job race, which is the whole reason the predicate carries `<= T`.
     *
     * A checkpoint lands while this run is still working. It describes state the run never
     * read, so adopting it would silently drop everything that changed in the gap on the next
     * run. The predicate is evaluated against the leased snapshot's moment, so it cannot be
     * selected however late the ETL asks - and asking late is exactly what spec 18.4 step 4
     * says it does, at commit time.
     */
    @Test
    fun `a checkpoint published mid-run is never selected as the new watermark`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()

        helper.withDiff(group, v1) { diff ->
            val leasedAt = diff.snapshot.dataAsOf
            // A newer generation is published while the diff holds its lease. No write to the
            // file: the archiver simply exports the same state again under a later stamp.
            cache.dataAsOf = leasedAt.plus(Duration.ofHours(1))
            val v2 = publish()

            assertThat(dao.find(group.value, v2)?.status).isEqualTo(ArchiveStatus.COMPLETE)
            assertThat(diff.nextWatermark()).isEqualTo(v1)
        }
    }

    /**
     * D24/D35: the helper computes the watermark and hands it back, and the ETL commits it
     * with its own output. A watermark this layer wrote would be one that could outlive a
     * rolled-back run, so the manifest has to come out of a diff exactly as it went in.
     */
    @Test
    fun `the helper returns the computed watermark and writes nothing itself`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()
        write("UPDATE t_a SET balance = 11 WHERE id = 1")
        val before = rows()

        helper.withDiff(group, v1) { diff ->
            assertThat((diff as Diff.Incremental).changes("t_a")).hasSize(1)
            assertThat(diff.nextWatermark()).isEqualTo(v1)
        }

        assertThat(rows()).isEqualTo(before)
    }

    // --- never under-reports ------------------------------------------------------------

    /**
     * The property the whole design exists for, over twelve rounds of pseudo-random edits on
     * a fixed seed.
     *
     * The model is what the consumer's target holds: everything it applied on its last run.
     * After every diff the assertion is that every key whose live value differs from that
     * target was reported - never a subset. What the run additionally reports is counted, not
     * failed: an older baseline over-reports by design (D25, D32), and a run that reported
     * nothing extra across twelve rounds would mean the baseline was tracking the live
     * snapshot rather than the recorded watermark.
     *
     * The generator never returns a column to a value it held before - balances only
     * increase, ids are never reused. That is not decoration; see
     * `a value that returns to its baseline inside one archive interval is not reported`
     * below for what it is avoiding and why the exclusion is honest rather than convenient.
     */
    @Test
    fun `every injected change appears in the diff of the next run - the helper never under-reports`() {
        val random = Random(20260829)
        val live = (1..20).associateWith { it }.toMutableMap()
        load(live)
        publish()
        var applied = live.toMap()
        var watermark: Long? = null
        var nextId = 21
        var overReported = 0
        var incrementalRuns = 0
        var fallbackRuns = 0

        repeat(12) {
            repeat(1 + random.nextInt(4)) {
                when (random.nextInt(3)) {
                    0 -> { live[nextId] = nextId; nextId++ }
                    1 -> live.keys.toList().randomOrNull(random)?.let { live[it] = live.getValue(it) + 1 }
                    else -> live.keys.toList().randomOrNull(random)?.let { live.remove(it) }
                }
            }
            load(live)
            if (random.nextBoolean()) publish()

            helper.withDiff(group, watermark) { diff ->
                val owed = (applied.keys + live.keys).filter { applied[it] != live[it] }
                when (diff) {
                    is Diff.FullCompare -> fallbackRuns++
                    is Diff.Incremental -> {
                        val reported = diff.changes("t_a").map { it.key.single() as Int }.toSet()
                        assertThat(reported).containsAll(owed)
                        overReported += (reported - owed.toSet()).size
                        incrementalRuns++
                    }
                }
                applied = live.toMap()
                watermark = diff.nextWatermark()
            }
        }

        assertThat(incrementalRuns).isGreaterThan(0)
        assertThat(fallbackRuns).isEqualTo(1) // only the first run, which has no watermark yet
        assertThat(overReported).isGreaterThan(0)
    }

    /**
     * The residual risk in single-baseline checkpoint diffing, pinned so it is a known
     * boundary rather than a surprise. **This is not a defect in the helper**, which reports
     * exactly what separates the baseline from the live snapshot; it is the one shape where
     * "an older baseline can only over-report" does not hold.
     *
     * The consumer applies 200 on the first run. The value then returns to its baseline of
     * 100 before any new checkpoint is published, so the second run - still on the same
     * recorded watermark, because nothing newer exists at or before its snapshot - finds
     * nothing to say, and the consumer's target keeps 200. Recovering needs a checkpoint
     * taken while the value was 200, which the archive cadence does not promise.
     *
     * Recorded as an open item in spec 18.6; see the ticket-05 progress entry.
     */
    @Test
    fun `a value that returns to its baseline inside one archive interval is not reported`() {
        write("INSERT INTO t_a VALUES (1, 'a', 100)")
        val v1 = publish()

        write("UPDATE t_a SET balance = 200 WHERE id = 1")
        helper.withDiff(group, v1) { diff ->
            assertThat((diff as Diff.Incremental).changes("t_a").single().op).isEqualTo(DiffOp.U)
            assertThat(diff.nextWatermark()).isEqualTo(v1)
        }

        write("UPDATE t_a SET balance = 100 WHERE id = 1")
        helper.withDiff(group, v1) { diff ->
            assertThat((diff as Diff.Incremental).changes("t_a")).isEmpty()
        }
    }

    /**
     * The one thing the helper refuses to answer. Comparing only the columns the two shapes
     * share would silently miss every change in the ones they do not, which is the
     * under-report the design makes impossible everywhere else - so a table that changed
     * shape since its checkpoint fails loudly and the consumer full-compares instead.
     */
    @Test
    fun `a table whose shape changed since the checkpoint fails loudly instead of comparing what it can`() {
        write("INSERT INTO t_a VALUES (1, 'a', 10)")
        val v1 = publish()
        write("ALTER TABLE t_a ADD COLUMN tier VARCHAR")

        helper.withDiff(group, v1) { diff ->
            assertThatThrownBy { (diff as Diff.Incremental).changes("t_a") }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("different shape")
        }
    }

    // --- fixture ------------------------------------------------------------------------

    /**
     * Applies DDL/DML to the generation file and advances the snapshot's `data_as_of`, which
     * is what a refresh cycle does: new state, new moment. Nothing may hold a READ_ONLY attach
     * while this runs.
     */
    private fun write(vararg sql: String) {
        DriverManager.getConnection("jdbc:duckdb:$database").use { connection ->
            connection.createStatement().use { statement -> sql.forEach { statement.execute(it) } }
        }
        cache.dataAsOf = cache.dataAsOf.plus(Duration.ofMinutes(10))
        cache.generation++
    }

    private fun load(rows: Map<Int, Int>) = write(
        "DELETE FROM t_a",
        "INSERT INTO t_a VALUES " + rows.entries.joinToString(", ") { "(${it.key}, 'n${it.key}', ${it.value})" },
    )

    private fun publish(): Long {
        check(archiver.runOnce(group) == RunOutcome.PUBLISHED) { "the checkpoint was not published" }
        return requireNotNull(dao.newestComplete(group.value)).version
    }

    /** The watermark a run leasing a snapshot stamped [at] would be handed. */
    private fun watermarkAt(at: Instant): Long? {
        val restore = cache.dataAsOf
        cache.dataAsOf = at
        return try {
            helper.withDiff(group, watermark = null) { it.nextWatermark() }
        } finally {
            cache.dataAsOf = restore
        }
    }

    private fun rows(): List<ManifestEntry> = dao.expired(group.value, FAR_FUTURE)

    companion object {

        private const val BUCKET = "test-bucket"
        private val TABLES = listOf("t_a", "t_b")
        private val KEYS = mapOf("t_a" to listOf("id"), "t_b" to listOf("id"))
        private val T0: Instant = Instant.parse("2026-08-29T10:00:00Z")
        private val FAR_FUTURE: Instant = Instant.parse("2999-01-01T00:00:00Z")
        private val GROUPS = AtomicLong()

        @Container
        @JvmStatic
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:slim-faststart")

        private lateinit var dao: ManifestDao
        private lateinit var scratch: Path

        @BeforeAll
        @JvmStatic
        fun createFixtures() {
            val jdbi = Jdbi.create(oracle.jdbcUrl, oracle.username, oracle.password)
            jdbi.useHandle<RuntimeException> { handle -> ManifestSchema.DDL.forEach { handle.execute(it) } }
            dao = ManifestDao(jdbi, bucket = BUCKET, clock = Clock.fixed(T0, ZoneOffset.UTC))
            scratch = Files.createTempDirectory("etl-diff-test")
        }
    }
}

/**
 * A [SnapshotCache] over one real DuckDB generation file, attached READ_ONLY as the serving
 * store attaches it. `dataAsOf` moves because the test moves it: the diff's whole correctness
 * argument is about which moment the lease was taken at.
 */
private class DiffCache(private val file: Path) : SnapshotCache {

    override val defaultWaitBudget: Duration = Duration.ofSeconds(5)

    @Volatile
    var dataAsOf: Instant = Instant.parse("2026-08-29T10:00:00Z")

    @Volatile
    var generation: Long = 1

    val liveLeases = AtomicInteger()

    override fun <T> withSnapshot(group: GroupId, waitBudget: Duration, block: (Snapshot) -> T): T {
        liveLeases.incrementAndGet()
        return DiffSnapshot(file, dataAsOf, generation) { liveLeases.decrementAndGet() }.use(block)
    }

    override fun copyOut(group: GroupId, spec: CopyOutSpec, waitBudget: Duration): CopyOutResult =
        throw NotImplementedError("the diff joins on the lease's own connection; nothing is copied out")

    override fun acquire(group: GroupId, waitBudget: Duration): Snapshot =
        throw NotImplementedError("withDiff scopes the lease so it cannot outlive the diff")

    override fun currentInfo(group: GroupId): GenerationInfo? =
        throw NotImplementedError("the helper reads generation and dataAsOf off the lease it holds")
}

private class DiffSnapshot(
    private val file: Path,
    override val dataAsOf: Instant,
    override val generation: Long,
    private val onRelease: () -> Unit,
) : Snapshot {

    private val issued = CopyOnWriteArrayList<Connection>()

    override fun connection(): Connection {
        val connection = DriverManager.getConnection("jdbc:duckdb:")
        connection.createStatement().use { statement ->
            statement.execute("ATTACH '${file.toAbsolutePath()}' AS g (READ_ONLY)")
            statement.execute("USE g")
        }
        issued += connection
        return connection
    }

    override fun close() {
        issued.forEach { runCatching { it.close() } }
        onRelease()
    }
}

/**
 * In-memory object store holding real bytes, because this suite round-trips real Parquet
 * through it. [beforeGet] is where a test observes downloads overlapping.
 */
private class BytesObjectStore : ObjectStore(unusedClient(), "test-bucket") {

    val stored = ConcurrentHashMap<String, ByteArray>()

    @Volatile
    var beforeGet: ((String) -> Unit)? = null

    override fun put(key: String, file: Path) {
        stored[key] = Files.readAllBytes(file)
    }

    override fun sizeOf(key: String): Long? = stored[key]?.size?.toLong()

    override fun delete(key: String) {
        stored.remove(key)
    }

    override fun get(key: String, file: Path) {
        beforeGet?.invoke(key)
        Files.write(file, requireNotNull(stored[key]) { "no object at '$key'" })
    }

    private companion object {

        /** Never dialled: every method is overridden. Building one opens no socket. */
        fun unusedClient(): MinioClient = MinioClient.builder()
            .endpoint("http://127.0.0.1:1")
            .credentials("unused", "unused")
            .build()
    }
}
