package infra.snapshotarchive

import infra.snapshotcache.duckdb.DuckDbGenerationStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Ticket 01, spec 18.6 items 1-2: can DuckDB 1.1.3 run `COPY (SELECT ...) TO '<f>.parquet'`
 * on a connection whose current database is a READ_ONLY attached generation file?
 *
 * The question is load-bearing rather than academic. If yes, the archiver exports straight
 * from the serving instance under the lease it already holds. If no, every export has to
 * stage through the public `copyOut` into the shared consumer instance (D16) first, which
 * doubles the I/O and lengthens the lease hold. Spec 18.3 could not be written until this
 * was settled, so it was settled empirically against the pinned version: it works, and the
 * staging fallback is not needed. These tests are what stop that answer going stale.
 *
 * The connection under test is the real one: [DuckDbGenerationStore.open] duplicates its
 * in-memory serving connection and `USE`s the READ_ONLY attached generation, which is
 * exactly what a consumer gets from `Snapshot.connection()`.
 */
class ParquetExportSpikeTest {

    @Test
    fun `exports a table from a read-only attached snapshot connection`(@TempDir dir: Path) {
        withSnapshotConnection(dir, rows = 5_000) { connection ->
            val target = dir.resolve("t_a.parquet")

            val rows = exportTable(connection, "t_a", target)

            assertThat(rows).isEqualTo(5_000)
            assertThat(Files.size(target)).isGreaterThan(0)
            assertThat(readBack(target)).isEqualTo(5_000)
        }
    }

    /**
     * Exporting must not have bought write access as a side effect. The A3 guarantee is
     * what lets the archiver run against the live serving instance at all, so it is
     * asserted here rather than assumed from the attach flag.
     */
    @Test
    fun `export does not make the attached generation writable`(@TempDir dir: Path) {
        withSnapshotConnection(dir, rows = 10) { connection ->
            exportTable(connection, "t_a", dir.resolve("t_a.parquet"))

            val statement = connection.createStatement()
            val write = statement.use {
                runCatching { it.execute("INSERT INTO t_a VALUES (1, 'x', 1.0)") }
            }

            // Asserted on the rejection itself, not merely on failure: "something threw"
            // would stay green if the connection were closed, the table renamed, or the
            // column types drifted - and this test is the whole evidence behind spec 18.6
            // item 1's claim that the archiver may run against the live serving instance.
            assertThat(write.exceptionOrNull())
                .hasMessageContaining("read-only")
                .hasMessageContaining("INSERT")
        }
    }

    /**
     * Spec 18.6 item 2: checkpoint bytes and export duration at ~1M rows, the numbers that
     * size retention storage and settle whether a lease held across an export interacts
     * badly with the K-generation ceiling.
     *
     * Measured 2026-08-29 on the pinned 1.1.3: 1M rows produced 14.2 MB in ~40-50 ms, so
     * the lease-hold question answers itself. Only the size is asserted: it is deterministic
     * and it is the number ticket 04 sizes retention against. Duration is printed, never
     * asserted - a wall-clock bound on a contended CI runner fails the build without
     * telling anyone what regressed, and this suite takes no timing dependencies.
     */
    @Test
    fun `measures checkpoint size and export duration at one million rows`(@TempDir dir: Path) {
        withSnapshotConnection(dir, rows = 1_000_000) { connection ->
            val target = dir.resolve("t_a.parquet")

            val startedAt = System.nanoTime()
            val rows = exportTable(connection, "t_a", target)
            val elapsedMs = (System.nanoTime() - startedAt) / 1_000_000

            val bytes = Files.size(target)
            println("spike 18.6: $rows rows -> $bytes bytes in ${elapsedMs}ms")

            assertThat(rows).isEqualTo(1_000_000)
            assertThat(readBack(target)).isEqualTo(1_000_000)
            assertThat(bytes).isLessThan(64L * 1024 * 1024)
        }
    }

    /**
     * Builds one real generation of [rows] rows, promotes it, opens it, and hands [block]
     * the same READ_ONLY-attached duplicate a consumer would hold.
     */
    private fun withSnapshotConnection(dir: Path, rows: Long, block: (Connection) -> Unit) {
        DuckDbGenerationStore(
            directory = dir.resolve("generations"),
            tempDirectory = dir.resolve("tmp"),
            memoryLimit = "1GB",
        ).use { store ->
            store.createCandidate(GENERATION).use { candidate ->
                candidate.connection().createStatement().use { statement ->
                    statement.execute(
                        """
                        CREATE TABLE t_a AS
                          SELECT i AS id, 'row-' || i AS name, i * 0.5 AS amount
                          FROM range($rows) t(i)
                        """.trimIndent(),
                    )
                }
            }
            store.promote(GENERATION)
            val opened = store.open(GENERATION)
            opened.connection().use(block)
            store.close(GENERATION)
        }
    }

    /** Reads the export back through a separate instance: proves it is real, portable Parquet. */
    private fun readBack(target: Path): Long =
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                val path = target.toAbsolutePath().toString().replace(java.io.File.separatorChar, '/')
                statement.executeQuery("SELECT COUNT(*) FROM read_parquet('$path')").use { rows ->
                    check(rows.next()) { "read_parquet returned no rows for $target" }
                    rows.getLong(1)
                }
            }
        }

    private companion object {
        const val GENERATION = 1L
    }
}
