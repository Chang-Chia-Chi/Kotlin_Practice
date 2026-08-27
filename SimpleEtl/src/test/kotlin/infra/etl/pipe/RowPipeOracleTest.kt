package infra.etl.pipe

import infra.etl.Pipe
import infra.etl.ProbeWriter
import infra.etl.RecordingConnections
import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DuckDbTableWriter
import infra.etl.jdbc.JdbcTableWriter
import infra.etl.pipe.JdbcSource
import infra.etl.pipe.PipeResult
import infra.etl.pipe.RowPipe
import infra.etl.pipe.RowTransform
import java.sql.Connection
import java.sql.DriverManager
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer

/**
 * The three P3 items that only a real Oracle can answer: the fetch size against a driver that
 * honours it and defaults to 10, the million-row stream, and one read transaction shared by two
 * pipes. One container for the whole class - the image is multi-GB.
 *
 * Rows are generated with `connect by level <= :rows` rather than inserted, so a million-row
 * source costs one statement and no fixture INSERT loop. `lot_id` is CAST to a declared
 * `NUMBER(18)`: an uncast expression reports precision 0, which AUTO DDL rejects at writer open
 * (spec 4.4), and Oracle folds every integer type into NUMBER, so it arrives as DECIMAL and is
 * read with `Row.decimal`, never `Row.long`.
 */
@Testcontainers
class RowPipeOracleTest {

    companion object {

        /** Two columns, so a row is wide enough to be worth measuring and narrow enough to be quick. */
        private const val GENERATED = """
            select cast(level as number(18))    as lot_id,
                   cast('L' || level as varchar2(40)) as lot_code
            from dual connect by level <= :rows
        """

        @Container
        @JvmStatic
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:slim-faststart")

        private lateinit var connection: Connection

        private fun connect(): Connection =
            DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)

        @BeforeAll
        @JvmStatic
        fun openFixtureConnection() {
            connection = connect()
        }

        @AfterAll
        @JvmStatic
        fun closeFixtureConnection() = connection.close()

        // exec / count used to be declared here, byte for byte the same as Pipe.exec and
        // Pipe.rowCount, which this class already imports for its DuckDB connections. Both take a
        // plain java.sql.Connection, so the Oracle one needed no copy of its own (review finding
        // L8). Deleting them touched no assertion.
    }

    private val recording = RecordingConnections { connect() }
    private val jdbi: Jdbi = Jdbi.create(recording)

    /**
     * Done-when item 2. Oracle's default fetch size is 10, which is unusable at this row count
     * (spec 5.2 step 1), and this is the driver that actually reports what was set - duckdb_jdbc
     * 1.1.3 accepts `setFetchSize` and keeps reporting 2048, which is why [RowPipeTest] asserts
     * the request and this asserts the reading.
     *
     * The default is measured here rather than hard-coded into the assertion alone, so that the
     * test still means something if a future ojdbc changes it: the pipe's fetch size must equal
     * the chunk size and must not be whatever the driver would have used.
     */
    @Test
    fun `the source statement's fetch size is the chunk size, not the Oracle default`() {
        val driverDefault = connection.prepareStatement("select 1 from dual").use { it.fetchSize }
        assertEquals(10, driverDefault) { "ojdbc's own default fetch size" }

        Pipe.openDuck().use { duck ->
            RowPipe(
                source = JdbcSource(jdbi, GENERATED, mapOf("rows" to 1000)),
                target = DuckDbTableWriter(duck, "wip_stg", CreateTable.AUTO, Pipe.STEP),
                step = Pipe.STEP,
                chunkSize = 2500,
            ).run()

            assertEquals(1000L, Pipe.rowCount(duck, "wip_stg"))
        }

        assertAll(
            {
                assertEquals(setOf(2500), recording.fetchSizesAtExecute.toSet()) {
                    "fetch size the source statement reported when it was executed; " +
                        "was ${recording.fetchSizesAtExecute}"
                }
            },
            {
                assertFalse(driverDefault in recording.fetchSizesAtExecute) {
                    "the driver default $driverDefault was used; was ${recording.fetchSizesAtExecute}"
                }
            },
        )
        recording.assertStreamed("fetch size")
    }

    /**
     * Done-when item 1: a million rows stream, and the heap does not grow with the row count.
     *
     * Method. The same pipe shape is run twice, at 100,000 and at 1,000,000 rows, with the same
     * chunk size, and each run measures **live heap** - `System.gc()` repeated, then
     * `totalMemory - freeMemory` - at the moment its **last** row is inside the transform. At
     * that moment a pipe that accumulates is holding every row it has read; a pipe that streams
     * is holding at most one chunk, which is identical in the two runs. The assertion is on the
     * difference between the two readings, so a constant baseline - the JVM, the driver's fetch
     * buffer, DuckDB's JNI layer - cancels out, and only growth that tracks the row count is
     * left. 900,000 retained Rows would be several hundred MB; the bound is 64 MB.
     *
     * Absence of an OutOfMemoryError is deliberately not the test: the surefire heap is large
     * enough to hold a million of these rows, so an accumulating pipe would pass that and fail
     * this.
     *
     * The peak chunk size is asserted alongside, as the structural half of the same claim: no
     * chunk ever exceeded `chunkSize`, whatever the heap readings say.
     *
     * Cost: about a minute of Oracle reads on top of the container start. The row count is the
     * plan's, not a convenient smaller one.
     */
    @Test
    fun `one million rows stream from Oracle into DuckDB without the heap growing with the row count`() {
        val small = streamed(rows = 100_000, chunkSize = 10_000)
        val large = streamed(rows = 1_000_000, chunkSize = 10_000)

        assertAll(
            { assertEquals(PipeResult(100_000L, 100_000L), small.result) },
            { assertEquals(PipeResult(1_000_000L, 1_000_000L), large.result) },
            { assertEquals(1_000_000L, large.rowsInTarget) },
            { assertEquals(10_000, large.largestChunk) { "no chunk exceeded the chunk size" } },
            {
                assertTrue(small.liveHeapAtLastRow > 0L) {
                    "the baseline reading must be a real one; was ${small.liveHeapAtLastRow}"
                }
            },
        )

        val growth = large.liveHeapAtLastRow - small.liveHeapAtLastRow
        assertTrue(growth < 64L * 1024 * 1024) {
            "live heap at the last row grew by $growth bytes for 900000 more rows " +
                "(100000: ${small.liveHeapAtLastRow}, 1000000: ${large.liveHeapAtLastRow})"
        }
    }

    /**
     * Done-when item 6, second property. The snapshot cache reads a group of tables into one
     * generation and needs them internally consistent: read in separate transactions, the union
     * can show a row twice or not at all, intermittently, and irreproducibly afterwards.
     *
     * So a `JdbcSource` over a caller-supplied `Handle` must borrow it - two pipes, one
     * transaction - and the only assertion that proves it is a committed write from a second
     * connection landing *between* the two pipes and staying invisible to the second one.
     *
     * **The isolation level is not incidental.** Oracle's default is READ COMMITTED, which gives
     * statement-level read consistency: each query takes a fresh snapshot even inside one
     * transaction, so at the default this test would pass just as happily against a pipe that
     * opened its own connection per run, and would be asserting nothing. Transaction-level read
     * consistency on Oracle requires SERIALIZABLE (or a READ ONLY transaction), which is what
     * the caller sets here, and what a caller sharing a source transaction must set for real.
     * The `count` at the end is the vacuity guard: it proves the third row really was committed
     * and really is visible outside the shared transaction.
     */
    @Test
    fun `two pipes over one borrowed Handle read from a single source transaction`() {
        Pipe.exec(
            connection,
            "create table gen_src (lot_id number(18) not null, lot_code varchar2(20) not null)",
            "insert into gen_src (lot_id, lot_code) values (1, 'L1')",
            "insert into gen_src (lot_id, lot_code) values (2, 'L2')",
        )
        val sql = "select lot_id, lot_code from gen_src order by lot_id"

        Pipe.openDuck().use { generation ->
            val handle = jdbi.open()
            handle.connection.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE
            handle.begin()
            try {
                val first = RowPipe(
                    source = JdbcSource(handle, sql),
                    target = DuckDbTableWriter(generation, "wip", CreateTable.AUTO, Pipe.STEP),
                    step = Pipe.STEP,
                    chunkSize = 50,
                ).run()

                // A different connection, committed (ojdbc's autoCommit is on), between the pipes.
                connect().use { other ->
                    Pipe.exec(other, "insert into gen_src (lot_id, lot_code) values (3, 'L3')")
                }

                val second = RowPipe(
                    source = JdbcSource(handle, sql),
                    target = DuckDbTableWriter(generation, "lot", CreateTable.AUTO, Pipe.STEP),
                    step = Pipe.STEP,
                    chunkSize = 50,
                ).run()

                assertAll(
                    { assertEquals(2L, first.rowsWritten) },
                    {
                        assertEquals(2L, second.rowsWritten) {
                            "the second pipe reads the transaction's snapshot, not the table"
                        }
                    },
                    { assertEquals(2L, Pipe.rowCount(generation, "wip")) },
                    { assertEquals(2L, Pipe.rowCount(generation, "lot")) },
                    {
                        assertEquals(3L, Pipe.rowCount(connection, "gen_src")) {
                            "outside the shared transaction the third row is committed and visible"
                        }
                    },
                    { assertFalse(handle.isClosed) { "the pipe borrows the Handle and must not close it" } },
                    {
                        assertTrue(handle.isInTransaction) {
                            "the pipe neither commits nor rolls back a borrowed transaction"
                        }
                    },
                )
            } finally {
                handle.rollback()
                handle.close()
            }
        }
    }

    /**
     * Done-when item 3 on the other target kind. [RowPipeCommitTest] proves the chunk boundary
     * for DuckDB, where the appender's flush is the commit; a JDBC target commits because the
     * writer's own Handle runs under ojdbc's autoCommit, and nothing measured that until here.
     * Same shape as the DuckDB timeline - 175 rows, chunks of 50 - observed from the class
     * fixture connection, which is a different Oracle session from the one the writer inserts on
     * and therefore sees committed rows only.
     */
    @Test
    fun `each chunk is committed to an Oracle target at its chunk boundary`() {
        Pipe.exec(connection, "create table wip_tgt (lot_id number(18), lot_code varchar2(40))")
        val visibleAtRow = ArrayList<Long>()

        val result = RowPipe(
            source = JdbcSource(jdbi, GENERATED, mapOf("rows" to 175)),
            target = JdbcTableWriter(Jdbi.create { connect() }, "wip_tgt", Pipe.STEP),
            step = Pipe.STEP,
            chunkSize = 50,
            transform = RowTransform { row ->
                visibleAtRow.add(Pipe.rowCount(connection, "wip_tgt"))
                row
            },
        ).run()

        assertAll(
            { assertEquals(PipeResult(175L, 175L), result) },
            {
                assertEquals(List(175) { k -> (k / 50) * 50L }, visibleAtRow) {
                    "rows committed to Oracle and visible to another session while row k was in the transform"
                }
            },
            { assertEquals(175L, Pipe.rowCount(connection, "wip_tgt")) },
        )
    }

    private class Streamed(
        val result: PipeResult,
        val rowsInTarget: Long,
        val liveHeapAtLastRow: Long,
        val largestChunk: Int,
    )

    private fun streamed(rows: Int, chunkSize: Int): Streamed = Pipe.openDuck().use { duck ->
        val probe = ProbeWriter(DuckDbTableWriter(duck, "wip_stg", CreateTable.AUTO, Pipe.STEP))
        var seen = 0L
        var liveHeap = 0L
        val result = RowPipe(
            source = JdbcSource(jdbi, GENERATED, mapOf("rows" to rows)),
            target = probe,
            step = Pipe.STEP,
            chunkSize = chunkSize,
            transform = RowTransform { row ->
                seen++
                if (seen == rows.toLong()) liveHeap = Pipe.liveHeapBytes()
                row
            },
        ).run()
        Streamed(result, Pipe.rowCount(duck, "wip_stg"), liveHeap, probe.chunkSizes.max())
    }
}
