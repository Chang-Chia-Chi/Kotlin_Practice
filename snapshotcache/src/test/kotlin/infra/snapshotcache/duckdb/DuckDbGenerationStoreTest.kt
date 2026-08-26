package infra.snapshotcache.duckdb

import com.sun.management.UnixOperatingSystemMXBean
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.spi.OpenGeneration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.streams.toList

/**
 * Adapter-level integration tests on real DuckDB 1.1.3 (plan P7 acceptance): A3, A4,
 * file-gone-after-reclaim, and no-leak evidence over 20 small rotations. The FD-count
 * assertion runs on Linux CI only (the MXBean is Unix-only); every other assertion runs
 * everywhere, backed by the store's own tracked-connection count.
 */
class DuckDbGenerationStoreTest {

    @TempDir
    lateinit var groupDir: Path

    @TempDir
    lateinit var tempDir: Path

    private lateinit var store: DuckDbGenerationStore

    @BeforeEach
    fun openStore() {
        store = DuckDbGenerationStore(groupDir, tempDir, "500MB")
    }

    @AfterEach
    fun closeStore() {
        store.close()
    }

    @Test
    fun candidateLifecycle_tmpFileWithSettings_promoteRenamesAtomically_openServesRows() {
        val tmp = groupDir.resolve("gen_0000000001.db.tmp")
        val final = groupDir.resolve("gen_0000000001.db")

        val candidate = store.createCandidate(1)
        assertThat(tmp).exists()
        candidate.connection().createStatement().use { st ->
            st.executeQuery("SELECT current_setting('temp_directory')").use { rs ->
                rs.next()
                assertThat(rs.getString(1)).isEqualTo(tempDir.toString())
            }
            st.executeQuery("SELECT current_setting('memory_limit')").use { rs ->
                rs.next()
                // 500MB = 476.8 MiB in DuckDB's display units (probe-verified on 1.1.3).
                assertThat(rs.getString(1)).startsWith("476.8")
            }
            st.execute("CREATE TABLE t_a (id INTEGER, v VARCHAR)")
            st.execute("INSERT INTO t_a VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        }
        candidate.close()

        store.promote(1)
        assertThat(tmp).doesNotExist()
        assertThat(final).exists()

        val opened = store.open(1)
        assertThat(opened.fileBytes()).isGreaterThan(0)
        opened.connection().use { connection ->
            assertThat(count(connection, "t_a")).isEqualTo(3)
        }
        store.close(1)
        store.delete(1)
    }

    @Test
    fun A3_readOnlyAttach_rejectsInsertThroughOpenGenerationConnection() {
        val opened = buildAndOpen(1)
        opened.connection().use { connection ->
            assertThatThrownBy {
                connection.createStatement().use { it.execute("INSERT INTO t_a VALUES (99, 'nope')") }
            }.isInstanceOf(SQLException::class.java)
            // The rejected write did not disturb reads on the same connection.
            assertThat(count(connection, "t_a")).isEqualTo(3)
        }
        store.close(1)
        store.delete(1)
    }

    @Test
    fun A4_closeWhileConnectionInUse_throws_thenSucceeds_andFileIsGoneAfterDelete() {
        val opened = buildAndOpen(1)
        val connection = opened.connection()

        assertThatThrownBy { store.close(1) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("still has")
        // The deferred DETACH left the reader untouched (spec 9.2: defer to next GC pass).
        assertThat(count(connection, "t_a")).isEqualTo(3)

        connection.close()
        assertThatCode { store.close(1) }.doesNotThrowAnyException()
        store.delete(1)
        assertThat(groupDir.resolve("gen_0000000001.db")).doesNotExist()
        assertThat(store.listOnDisk()).isEmpty()
    }

    @Test
    fun candidateClose_isIdempotent_andNeverThrows() {
        val candidate = store.createCandidate(1)
        candidate.connection().createStatement().use { it.execute("CREATE TABLE t (id INTEGER)") }
        assertThatCode {
            candidate.close()
            candidate.close()
        }.doesNotThrowAnyException()

        // Even a candidate whose connection was already closed underneath it stays silent.
        val broken = store.createCandidate(2)
        broken.connection().close()
        assertThatCode { broken.close() }.doesNotThrowAnyException()
        store.delete(1)
        store.delete(2)
    }

    @Test
    fun listOnDisk_reportsPromotedAndLeftoverTmp_deleteRemovesBothForms() {
        // Gen 7 crashes mid-build: candidate closed but never promoted, .tmp left behind.
        store.createCandidate(7).close()
        // Gen 8 completes normally.
        store.createCandidate(8).also { candidate ->
            candidate.connection().createStatement().use { it.execute("CREATE TABLE t (id INTEGER)") }
            candidate.close()
        }
        store.promote(8)

        assertThat(store.listOnDisk()).containsExactly(7L, 8L)

        store.delete(7)
        store.delete(8)
        assertThat(store.listOnDisk()).isEmpty()
        val leftovers = Files.list(groupDir).use { entries -> entries.toList() }
        assertThat(leftovers).isEmpty()
    }

    @Test
    fun copyOut_copiesFileToFileIntoTargetInstance_andRestoresTargetState() {
        val opened = buildAndOpen(1)
        DriverManager.getConnection("jdbc:duckdb:").use { target ->
            val rows = store.copyOut(
                opened,
                CopyOutSpec("SELECT id, v FROM t_a WHERE id <= 2", "copied", target),
            )
            assertThat(rows).isEqualTo(2)
            target.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM copied").use { rs ->
                    rs.next()
                    assertThat(rs.getLong(1)).isEqualTo(2)
                }
                // Default database restored and the generation detached from the target.
                st.executeQuery("SELECT current_database()").use { rs ->
                    rs.next()
                    assertThat(rs.getString(1)).isEqualTo("memory")
                }
                st.executeQuery("SELECT database_name FROM duckdb_databases()").use { rs ->
                    val databases = mutableListOf<String>()
                    while (rs.next()) databases += rs.getString(1)
                    assertThat(databases).noneMatch { it.startsWith("copyout_") }
                }
            }
        }
        store.close(1)
        store.delete(1)
    }

    @Test
    fun twentyRotations_leaveNoFiles_andNoOpenTrackedConnections() {
        assertThat(store.openIssuedConnections()).isZero()
        repeat(20) { round -> rotate(round + 1L) }
        assertThat(store.openIssuedConnections()).isZero()
        assertThat(store.listOnDisk()).isEmpty()
        val leftovers = Files.list(groupDir).use { entries -> entries.toList() }
        assertThat(leftovers).isEmpty()
    }

    @Test
    fun servingThreads_capsTheServingInstanceThreadPool() {
        DuckDbGenerationStore(groupDir.resolve("threads"), tempDir, "500MB", servingThreads = 2).use { capped ->
            capped.createCandidate(1).also { candidate ->
                candidate.connection().createStatement().use { it.execute("CREATE TABLE t (id INTEGER)") }
                candidate.close()
            }
            capped.promote(1)
            capped.open(1).connection().use { connection ->
                connection.createStatement().use { st ->
                    st.executeQuery("SELECT current_setting('threads')").use { rs ->
                        rs.next()
                        assertThat(rs.getLong(1)).isEqualTo(2)
                    }
                }
            }
        }
    }

    @Test
    fun abortShapedRounds_deleteWithoutClose_doNotGrowConnectionTracking() {
        // Abort paths (create/source/promote failure) reach delete without ever opening
        // the generation, so no close(gen) runs to drop the tracking entry.
        repeat(20) { round ->
            val gen = round + 1L
            store.createCandidate(gen).close()
            store.delete(gen)
        }
        assertThat(store.trackedGenerations()).isZero()
        assertThat(store.openIssuedConnections()).isZero()
        assertThat(store.listOnDisk()).isEmpty()
    }

    @Test
    fun twentyRotations_returnFdCountToBaseline() {
        val os = ManagementFactory.getOperatingSystemMXBean()
        assumeTrue(os is UnixOperatingSystemMXBean, "FD counting requires the Unix MXBean; skipped on Windows")
        os as UnixOperatingSystemMXBean

        // Warmup absorbs driver loading, JIT and buffer-pool growth (spec 17.6 methodology).
        rotate(1)
        rotate(2)
        val baseline = os.openFileDescriptorCount

        repeat(20) { round -> rotate(round + 3L) }
        val after = os.openFileDescriptorCount
        assertThat(after - baseline)
            .describedAs("open FD growth over 20 rotations (baseline %d, after %d)", baseline, after)
            .isLessThanOrEqualTo(0)
    }

    /** One full generation lifecycle: build -> promote -> open -> read -> detach -> delete. */
    private fun rotate(gen: Long) {
        val opened = buildAndOpen(gen)
        opened.connection().use { connection ->
            assertThat(count(connection, "t_a")).isEqualTo(3)
        }
        store.close(gen)
        store.delete(gen)
    }

    private fun buildAndOpen(gen: Long): OpenGeneration {
        store.createCandidate(gen).use { candidate ->
            candidate.connection().createStatement().use { st ->
                st.execute("CREATE TABLE t_a (id INTEGER, v VARCHAR)")
                st.execute("INSERT INTO t_a VALUES (1, 'one'), (2, 'two'), (3, 'three')")
            }
        }
        store.promote(gen)
        return store.open(gen)
    }

    private fun count(connection: Connection, table: String): Long =
        connection.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM $table").use { rs ->
                rs.next()
                rs.getLong(1)
            }
        }
}
