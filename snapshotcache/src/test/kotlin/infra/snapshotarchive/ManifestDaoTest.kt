package infra.snapshotarchive

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.Callable
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * P11 acceptance (plan 3c): the manifest is a durable contract, so it is tested against a
 * real Oracle rather than a stand-in.
 *
 * That choice is load-bearing for three of the assertions below. Sequence allocation, the
 * row count a conditional UPDATE reports when it matches nothing, and CLOB round-tripping
 * are all things a compatibility mode approximates and Oracle defines; a green suite against
 * H2 would prove the DAO works on H2. One container for the whole class - the image is
 * multi-GB - and each test uses its own group id, so no test depends on another's rows or on
 * the shared sequence's absolute values.
 */
@Testcontainers
class ManifestDaoTest {

    @Test
    fun `insert allocates a version and round-trips every column`() {
        val group = group()
        val inventory = """[{"table":"t_a","object_key":"t_a.parquet","bytes":14180166,"checksum":"ab12","row_count":1000000}]"""

        val entry = dao.insertPending(group, T0, inventory, generation = 7)

        assertThat(entry.version).isPositive()
        assertThat(entry.status).isEqualTo(ArchiveStatus.PENDING)
        assertThat(entry.uriPrefix).isEqualTo("test-bucket/snapshots/$group/v${entry.version}/")

        val stored = dao.find(group, entry.version)
        assertThat(stored).isEqualTo(entry)
        // The CLOB in particular: a driver that truncated it would still pass a row-count check.
        assertThat(stored!!.inventory).isEqualTo(inventory)
        assertThat(stored.generation).isEqualTo(7)
    }

    /** D31: the sequence is the only source of versions, and it never hands the same one twice. */
    @Test
    fun `versions are unique across groups`() {
        val a = dao.insertPending(group(), T0, "[]", generation = 1)
        val b = dao.insertPending(group(), T0, "[]", generation = 1)

        assertThat(a.version).isNotEqualTo(b.version)
    }

    @Test
    fun `pending becomes complete exactly once`() {
        val group = group()
        val entry = dao.insertPending(group, T0, "[]", generation = 1)

        assertThat(dao.markComplete(group, entry.version)).isTrue()
        // Not PENDING any more, so the second attempt moves nothing and says so.
        assertThat(dao.markComplete(group, entry.version)).isFalse()
        assertThat(dao.find(group, entry.version)!!.status).isEqualTo(ArchiveStatus.COMPLETE)
    }

    /**
     * The uploader-versus-watchdog race of D33, which has no second line of defence: both
     * paths issue the same conditional UPDATE, so the loser must learn it changed nothing
     * rather than assume it won.
     */
    @Test
    fun `a completed version can no longer be failed`() {
        val group = group()
        val entry = dao.insertPending(group, T0, "[]", generation = 1)
        dao.markComplete(group, entry.version)

        assertThat(dao.markFailed(group, entry.version)).isFalse()
        assertThat(dao.find(group, entry.version)!!.status).isEqualTo(ArchiveStatus.COMPLETE)
    }

    @Test
    fun `a transition on a row that does not exist reports false rather than throwing`() {
        assertThat(dao.markComplete(group(), version = 999_999_999)).isFalse()
    }

    /**
     * Two writers, one row, released together by a barrier: exactly one wins. This is the
     * property ticket 04's watchdog is built on, so it is asserted against real Oracle
     * locking rather than argued from the SQL.
     */
    @Test
    fun `two concurrent writers race one transition and exactly one wins`() {
        val group = group()
        val entry = dao.insertPending(group, T0, "[]", generation = 1)
        val barrier = CyclicBarrier(2)
        val pool = Executors.newFixedThreadPool(2)

        val attempt = Callable {
            barrier.await(30, TimeUnit.SECONDS)
            dao.markComplete(group, entry.version)
        }
        val results = try {
            pool.invokeAll(listOf(attempt, attempt)).map { it.get(60, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }

        assertThat(results).containsExactlyInAnyOrder(true, false)
    }

    @Test
    fun `the monotonicity guard rejects a data_as_of regression`() {
        val group = group()
        val first = dao.insertPending(group, T0, "[]", generation = 1)
        dao.markComplete(group, first.version)

        assertThatThrownBy { dao.insertPending(group, T0.minusSeconds(60), "[]", generation = 2) }
            .isInstanceOf(DataAsOfRegression::class.java)
            .hasMessageContaining(group)
        // Equal is also a regression: spec 18.3 step 2 says strictly greater.
        assertThatThrownBy { dao.insertPending(group, T0, "[]", generation = 2) }
            .isInstanceOf(DataAsOfRegression::class.java)
    }

    /** A PENDING predecessor is not a baseline, so it must not gate the next publish. */
    @Test
    fun `the monotonicity guard ignores non-complete predecessors`() {
        val group = group()
        dao.insertPending(group, T0, "[]", generation = 1)

        assertThat(dao.insertPending(group, T0.minusSeconds(60), "[]", generation = 2).version).isPositive()
    }

    @Test
    fun `watermark includes a checkpoint taken exactly at the boundary`() {
        val group = group()
        val entry = dao.insertPending(group, T0, "[]", generation = 1)
        dao.markComplete(group, entry.version)

        // `data_as_of <= T` (D35), so an equal instant is eligible - the ETL did process it.
        assertThat(dao.watermark(group, T0)).isEqualTo(entry.version)
    }

    @Test
    fun `watermark is null when the group has no complete versions`() {
        val group = group()
        dao.insertPending(group, T0, "[]", generation = 1)

        assertThat(dao.watermark(group, T0.plusSeconds(3600))).isNull()
    }

    /**
     * The long-running-job race of D35: every COMPLETE checkpoint is newer than the moment
     * the ETL actually read, so none of them may become its baseline.
     */
    @Test
    fun `watermark is null when every complete version is newer than the instant asked for`() {
        val group = group()
        val entry = dao.insertPending(group, T0, "[]", generation = 1)
        dao.markComplete(group, entry.version)

        assertThat(dao.watermark(group, T0.minusSeconds(1))).isNull()
    }

    @Test
    fun `watermark picks the newest eligible version and ignores later ones`() {
        val group = group()
        val old = dao.insertPending(group, T0, "[]", generation = 1)
        dao.markComplete(group, old.version)
        val mid = dao.insertPending(group, T0.plusSeconds(60), "[]", generation = 2)
        dao.markComplete(group, mid.version)
        val future = dao.insertPending(group, T0.plusSeconds(600), "[]", generation = 3)
        dao.markComplete(group, future.version)

        assertThat(dao.watermark(group, T0.plusSeconds(120))).isEqualTo(mid.version)
    }

    @Test
    fun `newest complete ignores pending and failed rows`() {
        val group = group()
        val complete = dao.insertPending(group, T0, "[]", generation = 1)
        dao.markComplete(group, complete.version)
        val failed = dao.insertPending(group, T0.plusSeconds(60), "[]", generation = 2)
        dao.markFailed(group, failed.version)
        dao.insertPending(group, T0.plusSeconds(120), "[]", generation = 3)

        assertThat(dao.newestComplete(group)!!.version).isEqualTo(complete.version)
    }

    @Test
    fun `newest complete is null for a group that has never published`() {
        assertThat(dao.newestComplete(group())).isNull()
    }

    /**
     * The raw retention query: aged by `data_as_of`, every status, no keep-newest rule.
     * Applying D34's policy is ticket 04's job and deliberately does not live here.
     */
    @Test
    fun `expired returns versions older than the cutoff, oldest first, whatever their status`() {
        val group = group()
        val oldest = dao.insertPending(group, T0, "[]", generation = 1)
        dao.markComplete(group, oldest.version)
        val older = dao.insertPending(group, T0.plusSeconds(60), "[]", generation = 2)
        dao.markFailed(group, older.version)
        val kept = dao.insertPending(group, T0.plusSeconds(3600), "[]", generation = 3)

        val expired = dao.expired(group, T0.plusSeconds(120))

        assertThat(expired.map { it.version }).containsExactly(oldest.version, older.version)
        assertThat(expired.map { it.version }).doesNotContain(kept.version)
    }

    private fun group(): String = "g${GROUPS.incrementAndGet()}"

    companion object {

        /** Whole seconds: Oracle's TIMESTAMP keeps microseconds, so nanos would not round-trip. */
        private val T0: Instant = Instant.parse("2026-08-29T10:00:00Z")

        private val GROUPS = AtomicLong()

        @Container
        @JvmStatic
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:slim-faststart")

        private lateinit var dao: ManifestDao

        @BeforeAll
        @JvmStatic
        fun createSchema() {
            val jdbi = Jdbi.create(oracle.jdbcUrl, oracle.username, oracle.password)
            jdbi.useHandle<RuntimeException> { handle ->
                ManifestSchema.DDL.forEach { handle.execute(it) }
            }
            dao = ManifestDao(jdbi, bucket = "test-bucket", clock = Clock.fixed(T0, ZoneOffset.UTC))
        }
    }
}
