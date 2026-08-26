package infra.simpleetl

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * The pipe's own behaviour, with a DuckDB source and a DuckDB target, so the whole class runs in
 * milliseconds and no container is involved. The Oracle-only items - the fetch size against a
 * driver that honours it, the million-row stream, and the shared read transaction - are in
 * [RowPipeOracleTest].
 *
 * Source and target are two independent DuckDB instances, not two tables in one. A pipe holds
 * its source result set open while the target creates and writes a table, and a shared instance
 * would fold two unrelated transactions into one story about what the pipe did.
 */
class RowPipeTest {

    private val sourceDb = Pipe.openDuck()
    private val targetDb = Pipe.openDuck()
    private val recording = RecordingConnections { sourceDb.duplicate() }
    private val jdbi: Jdbi = Jdbi.create(recording)

    @AfterEach
    fun closeConnections() {
        sourceDb.close()
        targetDb.close()
    }

    private fun source(sql: String, parameters: Map<String, Any?> = emptyMap()) =
        JdbcSource(jdbi = jdbi, sql = sql, parameters = parameters)

    private fun target(table: String, createTable: CreateTable = CreateTable.AUTO) =
        DuckDbTableWriter(targetDb, table, createTable, Pipe.STEP)

    /**
     * The happy path, and the chunk arithmetic with it: 175 rows at a chunk size of 50 is three
     * full chunks and a remainder of 25. The [ProbeWriter] in front of the real writer is what
     * makes the chunk boundaries observable; the real writer behind it is what proves the values
     * landed. A pipe that read everything and wrote once would pass a row count and fail here.
     */
    @Test
    fun `a pipe streams the source into the target one chunk at a time`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)
        val probe = ProbeWriter(target("wip_stg"))

        val result = RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src order by lot_id"),
            target = probe,
            step = Pipe.STEP,
            chunkSize = 50,
        ).run()

        assertThat(result.rowsRead).isEqualTo(175L)
        assertThat(result.rowsWritten).isEqualTo(175L)
        assertThat(probe.chunkSizes).containsExactly(50, 50, 50, 25)
        assertThat(probe.opens).isEqualTo(1)
        assertThat(probe.closes).isEqualTo(1)
        assertThat(Pipe.rowCount(targetDb, "wip_stg")).isEqualTo(175L)
        assertThat(Pipe.longs(targetDb, "select lot_id from wip_stg order by lot_id"))
            .isEqualTo((0L until 175L).toList())
        assertThat(Pipe.strings(targetDb, "select lot_code from wip_stg order by lot_id").take(3))
            .containsExactly("L0", "L1", "L2")
    }

    /** A chunk size larger than the source is one short chunk, not one empty chunk plus one. */
    @Test
    fun `a source smaller than one chunk is written as a single chunk`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 7)
        val probe = ProbeWriter(target("wip_stg"))

        val result = RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src"),
            target = probe,
            step = Pipe.STEP,
            chunkSize = 5000,
        ).run()

        assertThat(result).isEqualTo(PipeResult(7L, 7L))
        assertThat(probe.chunkSizes).containsExactly(7)
    }

    /**
     * An empty source still opens the target, which under AUTO means the table exists and is
     * empty afterwards. That is the difference between "no rows" and "the step did nothing", and
     * a later step referencing the dataset depends on it.
     */
    @Test
    fun `an empty source creates the target and reports no rows`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 0)
        val probe = ProbeWriter(target("wip_stg"))

        val result = RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src"),
            target = probe,
            step = Pipe.STEP,
            chunkSize = 50,
        ).run()

        assertThat(result).isEqualTo(PipeResult(0L, 0L))
        assertThat(probe.opens).isEqualTo(1)
        assertThat(probe.closes).isEqualTo(1)
        assertThat(probe.chunkSizes).describedAs("an empty source writes no chunk at all").isEmpty()
        assertThat(Pipe.tableExists(targetDb, "wip_stg")).isTrue()
        assertThat(Pipe.rowCount(targetDb, "wip_stg")).isZero()
        recording.assertStreamed("empty source")
    }

    /**
     * Done-when item 4, first half. The dropped rows must be absent from the target and counted
     * in `rowsRead` but not in `rowsWritten`; a pipe that wrote them anyway, or that reported
     * `rowsRead == rowsWritten`, fails here.
     */
    @Test
    fun `a transform returning null drops the row`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 10)

        val result = RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src order by lot_id"),
            target = target("wip_stg"),
            step = Pipe.STEP,
            chunkSize = 4,
            transform = RowTransform { row -> if (row.long("lot_id")!! % 2L == 0L) row else null },
        ).run()

        assertThat(result.rowsRead).isEqualTo(10L)
        assertThat(result.rowsWritten).isEqualTo(5L)
        assertThat(Pipe.longs(targetDb, "select lot_id from wip_stg order by lot_id"))
            .containsExactly(0L, 2L, 4L, 6L, 8L)
    }

    /** The degenerate end of the same rule: every row dropped is a legal, empty, successful run. */
    @Test
    fun `a transform dropping every row writes nothing and still succeeds`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 10)
        val probe = ProbeWriter(target("wip_stg"))

        val result = RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src"),
            target = probe,
            step = Pipe.STEP,
            chunkSize = 4,
            transform = RowTransform { null },
        ).run()

        assertThat(result.rowsRead).isEqualTo(10L)
        assertThat(result.rowsWritten).isZero()
        assertThat(Pipe.rowCount(targetDb, "wip_stg")).isZero()
        assertThat(probe.closes).isEqualTo(1)
    }

    /**
     * Done-when item 4, second half. The target is `REQUIRED` and already declares `row_hash`,
     * because `RowPipe`'s frozen constructor has no `transform.addColumns` channel (spec 9.1
     * puts that in the YAML layer) and source metadata cannot describe a column the transform
     * invents. Under AUTO the generated DDL comes from source metadata, so the added column
     * would have nowhere to land.
     *
     * The assertion is on the values, not on the column list: `row_hash` reaching the table as
     * a column of nulls would satisfy a column-name check and lose every computed value.
     */
    @Test
    fun `a transform adding a column lands that column in the target`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 6)
        Pipe.exec(
            targetDb,
            "create table wip_stg (lot_id BIGINT, lot_code VARCHAR, qty DECIMAL(18,3), " +
                "site VARCHAR, row_hash VARCHAR)",
        )

        val result = RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src order by lot_id"),
            target = target("wip_stg", CreateTable.REQUIRED),
            step = Pipe.STEP,
            chunkSize = 4,
            transform = RowTransform { row -> row.with("row_hash", "h-" + row.string("lot_code")) },
        ).run()

        assertThat(result).isEqualTo(PipeResult(6L, 6L))
        assertThat(Pipe.strings(targetDb, "select row_hash from wip_stg order by lot_id"))
            .containsExactly("h-L0", "h-L1", "h-L2", "h-L3", "h-L4", "h-L5")
    }

    /** `JdbcSource.parameters` binds by name, so the filter runs in the database, not in the JVM. */
    @Test
    fun `bound parameters filter the source query`() {
        Pipe.createSourceTable(sourceDb, "f12_src", rows = 4, site = "F12")
        Pipe.createSourceTable(sourceDb, "f14_src", rows = 6, site = "F14")
        Pipe.exec(sourceDb, "create table wip_src as select * from f12_src union all select * from f14_src")

        val result = RowPipe(
            source = source(
                "select lot_id, lot_code, qty, site from wip_src where site = :site",
                mapOf("site" to "F14"),
            ),
            target = target("wip_stg"),
            step = Pipe.STEP,
            chunkSize = 50,
        ).run()

        assertThat(result.rowsRead).describedAs("the unbound site must never reach the JVM").isEqualTo(6L)
        assertThat(Pipe.strings(targetDb, "select distinct site from wip_stg")).containsExactly("F14")
    }

    /**
     * Done-when item 2, the half that does not need Oracle: the pipe asks the source statement
     * for a fetch size equal to the chunk size. Measured on the pinned jar, duckdb_jdbc 1.1.3
     * accepts `setFetchSize` and goes on reporting 2048, so the assertion is on the request and
     * not on the reading. [RowPipeOracleTest] asserts the reading, on the driver whose default
     * of 10 is the reason the item exists.
     */
    @Test
    fun `the source statement is asked for a fetch size equal to the chunk size`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 30)

        RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src"),
            target = target("wip_stg"),
            step = Pipe.STEP,
            chunkSize = 137,
        ).run()

        assertThat(recording.fetchSizesRequested)
            .describedAs("every fetch size the pipe asked the source statement for")
            .containsOnly(137)
    }

    /**
     * The `Jdbi` convenience form owns one Handle for the run: one connection, opened and closed,
     * however many chunks it takes. A pipe that opened a connection per chunk would still pass
     * every row-count assertion in this class.
     */
    @Test
    fun `the Jdbi form opens one source connection for the whole run and closes it`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 175)

        RowPipe(
            source = source("select lot_id, lot_code, qty, site from wip_src"),
            target = target("wip_stg"),
            step = Pipe.STEP,
            chunkSize = 50,
        ).run()

        assertThat(recording.connectionsOpened.get()).isEqualTo(1)
        recording.assertStreamed("Jdbi form")
    }

    /** A chunk size below one has no meaning and would loop or write nothing; it is rejected. */
    @Test
    fun `a chunk size below one is rejected`() {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 1)

        val thrown = catchThrowable {
            RowPipe(
                source = source("select lot_id, lot_code, qty, site from wip_src"),
                target = target("wip_stg"),
                step = Pipe.STEP,
                chunkSize = 0,
            ).run()
        }

        assertThat(thrown).isInstanceOf(IllegalArgumentException::class.java)
    }

    /**
     * Done-when item 6, first property. The snapshot cache hands a `GenerationSource` a write
     * `Connection` to a candidate generation file that it opened itself, expects several tables
     * to be populated through it, and then goes on to verify and promote that file through the
     * same connection. So the pipe borrows the connection and must not close it.
     *
     * A file-mode connection, opened exactly as the cache opens one, rather than
     * `jdbc:duckdb:` - the seam is worth testing against the connection kind it will really get.
     * The final CREATE TABLE is the usability half: `isClosed` alone would still pass on a
     * connection whose instance the pipe had shut down underneath it.
     */
    @Test
    fun `a pipe populates a caller supplied file connection and leaves it open and usable`(@TempDir dir: Path) {
        Pipe.createSourceTable(sourceDb, "wip_src", rows = 40)
        val file = dir.resolve("generation-7.duckdb").toString().replace('\\', '/')
        val generation = Pipe.openDuck("jdbc:duckdb:$file")

        try {
            listOf("wip", "lot").forEach { table ->
                RowPipe(
                    source = source("select lot_id, lot_code, qty, site from wip_src"),
                    target = DuckDbTableWriter(generation, table, CreateTable.AUTO, Pipe.STEP),
                    step = Pipe.STEP,
                    chunkSize = 16,
                ).run()
            }

            assertThat(generation.isClosed).describedAs("the pipe borrowed this connection").isFalse()
            assertThat(Pipe.rowCount(generation, "wip")).isEqualTo(40L)
            assertThat(Pipe.rowCount(generation, "lot")).isEqualTo(40L)
            // Still writable, which is what the cache does next: it verifies and promotes the file.
            Pipe.exec(generation, "create table verify as select count(*) as n from wip")
            assertThat(Pipe.longs(generation, "select n from verify")).containsExactly(40L)
        } finally {
            generation.close()
        }
    }
}
