package infra.snapshotcache.duckdb

import infra.snapshotcache.spi.ident
import infra.snapshotcache.spi.literal
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Spec 18.6 items 1-2, the M3 archive layer's opening spike: can DuckDB 1.1.3 run
 * `COPY (SELECT ...) TO '<f>.parquet'` on a connection whose current database is a
 * READ_ONLY attached generation file?
 *
 * The question gated the whole archiver design. If yes, an export streams straight from the
 * serving instance under the lease it already holds. If no, every export has to stage
 * through the public `copyOut` into the shared consumer instance (D16) first, doubling the
 * I/O and lengthening the lease hold. It was settled empirically against the pinned
 * version: it works, and the staging fallback is not needed.
 *
 * This test is the executable half of that answer - the prose half is spec 18.6. It lives
 * beside the other DuckDB adapter tests because what it pins is a DuckDB capability, not
 * archive policy; the archiver that consumes it is built in M3 ticket 03, which owns where
 * the production export function lands.
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
     * Spec 18.6 item 2: checkpoint bytes at ~1M rows, the number that sizes retention
     * storage and settles whether a lease held across an export interacts badly with the
     * K-generation ceiling.
     *
     * Measured 2026-08-29 on the pinned 1.1.3: 1M rows produced 14,180,166 bytes - the same
     * count on all three runs - in ~40 ms, so the lease-hold question answers itself. Only
     * the size is asserted, and only as a ceiling: the exact byte count is reproducible but
     * pinning it would fail on any future DuckDB Parquet encoding change, which is not what
     * this test is for. Duration is printed, never asserted - a wall-clock bound on a
     * contended CI runner fails the build without telling anyone what regressed.
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
     * The statement under test, plus the row count an archiver would record in its inventory.
     *
     * The count comes from `COUNT(*)` rather than COPY's update count. 1.1.3 does report
     * one, but an empty table and a driver that stopped classifying COPY as DML both return
     * 0 and nothing downstream could tell them apart - and that value would be committed
     * into a manifest row that the watchdog later verifies a real object against.
     */
    private fun exportTable(connection: Connection, table: String, target: Path): Long =
        connection.createStatement().use { statement ->
            statement.execute(
                "COPY (SELECT * FROM ${ident(table)}) TO '${literal(target.toAbsolutePath().toString())}' (FORMAT PARQUET)",
            )
            statement.executeQuery("SELECT COUNT(*) FROM ${ident(table)}").use { rows ->
                check(rows.next()) { "row count query returned nothing for table $table" }
                rows.getLong(1)
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
                statement.executeQuery("SELECT COUNT(*) FROM read_parquet('${literal(target.toAbsolutePath().toString())}')")
                    .use { rows ->
                        check(rows.next()) { "read_parquet returned no rows for $target" }
                        rows.getLong(1)
                    }
            }
        }

    private companion object {
        const val GENERATION = 1L
    }
}
