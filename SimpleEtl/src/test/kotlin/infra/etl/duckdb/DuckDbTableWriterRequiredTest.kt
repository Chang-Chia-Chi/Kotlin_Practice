package infra.etl.duckdb

import infra.etl.Scratch
import infra.etl.Scratch.STEP
import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DuckDbTableWriter
import java.math.BigDecimal
import java.time.LocalDateTime
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

/**
 * `createTable: REQUIRED` against a pre-existing DuckDB table (validation rule 15, P2 done-when 3,
 * 5 and 7).
 *
 * The append is positional; the column order comes from the catalog and never from the caller.
 * Everything here exists to make a misalignment loud.
 */
class DuckDbTableWriterRequiredTest {

    private val connection = Scratch.open()

    @AfterEach
    fun closeConnection() = connection.close()

    private fun writer(table: String) = DuckDbTableWriter(connection, table, CreateTable.REQUIRED, STEP)

    /**
     * Done-when 3, and the heart of the positional-appender risk. Two targets hold the same five
     * columns in different orders, the source hands them in a third order, and the chunk is the
     * same Rows both times.
     *
     * The two VARCHAR columns are what make this bite: swapping a VARCHAR with a DECIMAL throws,
     * but swapping two VARCHARs is silent, lands the wrong text in both, and a row-count
     * assertion would not notice.
     */
    @Test
    fun `values land by column name whatever order the target table declares`() {
        Scratch.exec(
            connection,
            "create table in_order (lot_code VARCHAR, note VARCHAR, qty DECIMAL(18,3), upd_ts TIMESTAMP, lot_id BIGINT)",
            "create table reordered (lot_id BIGINT, upd_ts TIMESTAMP, note VARCHAR, qty DECIMAL(18,3), lot_code VARCHAR)",
        )
        val source = Scratch.read(
            connection,
            """
            select CAST(1.500 AS DECIMAL(18,3))    as qty,
                   CAST('epsilon' AS VARCHAR)      as note,
                   CAST(7 AS BIGINT)               as lot_id,
                   CAST('alpha' AS VARCHAR)        as lot_code,
                   TIMESTAMP '2024-01-02 03:04:05' as upd_ts
            """,
        )

        listOf("in_order", "reordered").forEach { table ->
            writer(table).use {
                it.open(source.columns)
                assertEquals(1, it.write(source.rows))
            }
        }

        listOf("in_order", "reordered").forEach { table ->
            val row = Scratch.read(connection, "select lot_code, note, qty, upd_ts, lot_id from $table").rows.single()
            assertAll(
                { assertEquals("alpha", row.string("lot_code")) { "lot_code in $table" } },
                { assertEquals("epsilon", row.string("note")) { "note in $table" } },
                {
                    val qty = row.decimal("qty")
                    assertTrue(qty != null && qty.compareTo(BigDecimal("1.500")) == 0) {
                        "qty in $table must compare equal to 1.500 whatever its scale, was $qty"
                    }
                },
                {
                    assertEquals(LocalDateTime.parse("2024-01-02T03:04:05"), row.dateTime("upd_ts")) {
                        "upd_ts in $table"
                    }
                },
                { assertEquals(7L, row.long("lot_id")) { "lot_id in $table" } },
            )
        }
    }

    /**
     * Done-when 5, inverted by P0's S3 ruling. The pre-P0 rule rejected a nullable BIGINT; S3
     * measured appendBigDecimal into BIGINT as exact at scale 0, with overflow beyond Long
     * throwing loudly, so amended rule 15 permits it. The safety argument depends on the value
     * coming from Row.long(), which is why the round trip is asserted and not just the open.
     */
    @Test
    fun `REQUIRED accepts a nullable BIGINT column and round trips both a value and a null through it`() {
        Scratch.exec(connection, "create table wip_stg (lot_id BIGINT, lot_code VARCHAR)")
        val nullability = Scratch.nullability(connection, "wip_stg")
        assertEquals(true, nullability["lot_id"]) { "target nullability was $nullability" }
        val source = Scratch.read(connection, "select CAST(7 AS BIGINT) as lot_id, CAST('L1' AS VARCHAR) as lot_code")
        val nullRow = Scratch.read(connection, "select CAST(NULL AS BIGINT) as lot_id, CAST('L2' AS VARCHAR) as lot_code")

        writer("wip_stg").use {
            it.open(source.columns)
            assertEquals(2, it.write(source.rows + nullRow.rows))
        }

        val back = Scratch.read(connection, "select lot_id, lot_code from wip_stg order by lot_code").rows
        assertAll(
            { assertEquals(7L, back[0].long("lot_id")) },
            // Sentinel guard through the UNTYPED accessor, deliberately. The original wording here
            // described an AssertJ overload trap that no longer exists after the JUnit 5 migration;
            // the guard itself still earns its place. `Row.get` returns Any?, so this compares
            // boxed and a returned 0L would fail it, while the two assertNulls below establish
            // that what came back is genuinely null rather than merely not-zero.
            { assertNotEquals(0L, back[1]["lot_id"]) },
            { assertNull(back[1]["lot_id"]) { "was ${back[1]["lot_id"]}" } },
            { assertNull(back[1].long("lot_id")) { "was ${back[1].long("lot_id")}" } },
        )
    }

    /**
     * Rule 15 rejects a nullable column outside VARCHAR, DECIMAL, TIMESTAMP, BIGINT, and rejects
     * DATE whether nullable or not because the silent truncation does not depend on
     * nullability. BLOB has no byte[] overload at all. The check is on the target's declared
     * nullability, not the source's - the source columns here are always reported nullable by
     * duckdb_jdbc.
     */
    @ParameterizedTest
    @MethodSource("rejectedTargetColumns")
    fun `REQUIRED rejects a target column with no write path, at open`(declaration: String, expression: String) {
        Scratch.exec(connection, "create table wip_stg (risky $declaration)")
        val source = Scratch.read(connection, "select $expression as risky")

        val thrown = assertThrows<Throwable> { writer("wip_stg").use { it.open(source.columns) } }

        assertAll(
            { assertFalse(thrown is NullPointerException) { "expected a diagnostic, not: $thrown" } },
            { assertTrue(thrown.message?.contains(STEP) == true) { "message was: ${thrown.message}" } },
            { assertTrue(thrown.message?.contains("risky") == true) { "message was: ${thrown.message}" } },
            { assertEquals(0L, Scratch.rowCount(connection, "wip_stg")) },
        )
    }

    /**
     * The other half of rule 15, so the test above cannot pass by rejecting everything. A NOT
     * NULL DOUBLE or BOOLEAN target is legal and takes the primitive append path - the only place
     * in a DuckDB-only test where that path is reachable, since AUTO never generates either type
     * from a source duckdb_jdbc reports as nullable.
     */
    @Test
    fun `REQUIRED accepts NOT NULL DOUBLE and BOOLEAN target columns and writes them`() {
        Scratch.exec(connection, "create table wip_stg (ratio DOUBLE NOT NULL, is_active BOOLEAN NOT NULL)")
        val source = Scratch.read(connection, "select CAST(2.5 AS DOUBLE) as ratio, CAST(true AS BOOLEAN) as is_active")

        writer("wip_stg").use {
            it.open(source.columns)
            assertEquals(1, it.write(source.rows))
        }

        val row = Scratch.read(connection, "select ratio, is_active from wip_stg").rows.single()
        assertAll(
            { assertEquals(2.5, row.double("ratio")) },
            { assertTrue(row.bool("is_active") == true) { "is_active was ${row.bool("is_active")}" } },
        )
    }

    /**
     * The target catalog read must filter on the exact table name. In JDBC, DatabaseMetaData
     * patterns treat `_` as a single-character wildcard, so getColumns(null, null, "wip_stg", null)
     * also returns the columns of `wipXstg` on this driver. Merging two tables columns produces a
     * column list the target does not have, and since the append is positional every value after
     * the first extra column lands one place to the left - silently.
     *
     * The decoy is deliberately one column wide and differently named: if its `weird` column is
     * merged in, `lot_code` and `note` shift and the assertions below fail on content, not on a
     * count.
     */
    @Test
    fun `a table whose name matches the target as a JDBC wildcard pattern is not merged into the column list`() {
        Scratch.exec(
            connection,
            "create table wip_stg (lot_id BIGINT, lot_code VARCHAR, note VARCHAR)",
            "create table wipXstg (weird VARCHAR)",
        )
        val source = Scratch.read(
            connection,
            "select CAST(7 AS BIGINT) as lot_id, CAST('alpha' AS VARCHAR) as lot_code, CAST('epsilon' AS VARCHAR) as note",
        )

        writer("wip_stg").use {
            it.open(source.columns)
            assertEquals(1, it.write(source.rows))
        }

        val row = Scratch.read(connection, "select lot_id, lot_code, note from wip_stg").rows.single()
        assertAll(
            { assertEquals(7L, row.long("lot_id")) },
            { assertEquals("alpha", row.string("lot_code")) },
            { assertEquals("epsilon", row.string("note")) },
            { assertEquals(0L, Scratch.rowCount(connection, "wipXstg")) { "the decoy table" } },
        )
    }

    @Test
    fun `REQUIRED fails naming the step and the table when the table does not exist`() {
        val source = Scratch.read(connection, "select CAST('L1' AS VARCHAR) as lot_code")

        val thrown = assertThrows<Throwable> { writer("no_such_table").use { it.open(source.columns) } }

        assertAll(
            { assertTrue(thrown.message?.contains(STEP) == true) { "message was: ${thrown.message}" } },
            {
                assertTrue(thrown.message?.contains("no_such_table") == true) {
                    "message was: ${thrown.message}"
                }
            },
        )
    }

    /** A Row key with no matching target column is an error, not a silently dropped value. */
    @Test
    fun `a source column with no matching target column is an error and nothing is written`() {
        Scratch.exec(connection, "create table wip_stg (lot_code VARCHAR)")
        val source = Scratch.read(
            connection,
            "select CAST('L1' AS VARCHAR) as lot_code, CAST('x' AS VARCHAR) as not_in_target",
        )

        val thrown = assertThrows<Throwable> {
            writer("wip_stg").use {
                it.open(source.columns)
                it.write(source.rows)
            }
        }

        assertAll(
            { assertTrue(thrown.message?.contains(STEP) == true) { "message was: ${thrown.message}" } },
            {
                assertTrue(thrown.message?.contains("not_in_target") == true) {
                    "message was: ${thrown.message}"
                }
            },
            { assertEquals(0L, Scratch.rowCount(connection, "wip_stg")) },
        )
    }

    /** A NOT NULL target column with no matching Row key is an error naming that column. */
    @Test
    fun `a NOT NULL target column with no matching source column is an error and nothing is written`() {
        Scratch.exec(connection, "create table wip_stg (lot_code VARCHAR, lot_id BIGINT NOT NULL)")
        val source = Scratch.read(connection, "select CAST('L1' AS VARCHAR) as lot_code")

        val thrown = assertThrows<Throwable> {
            writer("wip_stg").use {
                it.open(source.columns)
                it.write(source.rows)
            }
        }

        assertAll(
            { assertTrue(thrown.message?.contains(STEP) == true) { "message was: ${thrown.message}" } },
            { assertTrue(thrown.message?.contains("lot_id") == true) { "message was: ${thrown.message}" } },
            { assertEquals(0L, Scratch.rowCount(connection, "wip_stg")) },
        )
    }

    /**
     * Done-when 7 for the DuckDB target. duckdb_jdbc 1.1.3 offers nothing to count - DuckDBConnection
     * is public final and DuckDBAppender has no interface, so neither can be doubled - so the
     * closure is proved by the state the writer must leave behind after a failure it did not
     * expect: the caller's connection still open and usable, close() safe to call again, and a
     * fresh writer able to take the same table and land data in it.
     *
     * The failure is injected mid-write, so the exception path is what does the proving.
     */
    @Test
    fun `a failure mid write leaves the connection open and the table writable by a new writer`() {
        Scratch.exec(connection, "create table wip_stg (lot_id BIGINT NOT NULL, lot_code VARCHAR)")
        val good = Scratch.read(connection, "select CAST(7 AS BIGINT) as lot_id, CAST('L1' AS VARCHAR) as lot_code")
        val bad = Scratch.read(connection, "select CAST(NULL AS BIGINT) as lot_id, CAST('L2' AS VARCHAR) as lot_code")
        val failed = writer("wip_stg")

        val thrown = assertThrows<Throwable> {
            failed.open(good.columns)
            failed.write(good.rows + bad.rows)
        }

        // A null reaching a NOT NULL column is reported, never appended as a placeholder.
        assertAll(
            { assertFalse(thrown is NullPointerException) { "expected a diagnostic, not: $thrown" } },
            { assertTrue(thrown.message?.contains(STEP) == true) { "message was: ${thrown.message}" } },
            { assertTrue(thrown.message?.contains("lot_id") == true) { "message was: ${thrown.message}" } },
        )
        failed.close()
        failed.close()
        assertFalse(connection.isClosed) { "the writer does not own the caller's connection" }
        // What the failed chunk leaves behind, measured rather than assumed. The writer rejects
        // the bad row before it calls beginRow for it, so the good row ahead of it was already
        // complete and close() flushed it: one row of a two-row chunk is committed. P0's claim
        // that "a failed attempt keeps its partial rows because close() on the exception path
        // flushes what it had" therefore holds here, at row granularity, and the discard case
        // only arises when a row is abandoned half-appended - which this writer never produces.
        //
        // Asserting the count alone would not distinguish which row survived, so both are pinned.
        // P4 accounts for scratch space on this answer and a retry re-reads the same source rows.
        assertAll(
            { assertEquals(1L, Scratch.rowCount(connection, "wip_stg")) },
            {
                assertEquals(
                    7L,
                    Scratch.read(connection, "select lot_id from wip_stg").rows.single().long("lot_id"),
                ) { "the row completed before the failure, and only that row" }
            },
        )

        val recovery = Scratch.read(connection, "select CAST(9 AS BIGINT) as lot_id, CAST('L9' AS VARCHAR) as lot_code")
        writer("wip_stg").use {
            it.open(recovery.columns)
            assertEquals(1, it.write(recovery.rows))
        }
        val recovered = Scratch.read(connection, "select lot_code from wip_stg where lot_id = 9").rows
        assertEquals(1, recovered.size) { "was $recovered" }
    }

    /**
     * The other retention shape, and the companion to the test above. There the writer rejects the
     * bad row before beginRow, so the row ahead of it is complete when close() flushes and one row
     * survives. Here the value is one the framework cannot know is bad - it is in range for
     * CanonicalType.DECIMAL and only DuckDB knows the target column is too narrow - so the driver
     * throws part way through the second row and leaves it half-appended. close() then discards
     * the whole unflushed buffer, the completed first row included, and nothing survives.
     *
     * Measured on 1.1.3: a complete row plus a part-appended row leaves 0, a complete row plus a
     * bare beginRow() leaves 1, so it is the partial append that poisons the buffer and not the
     * open row. The two tests together are what P4 budgets scratch space from: a failed attempt
     * costs between zero and one chunk of rows, depending on where the failure landed.
     */
    @Test
    fun `a value the driver rejects inside an append discards the rows already completed in that chunk`() {
        Scratch.exec(connection, "create table wip_stg (lot_id BIGINT, qty DECIMAL(18,3))")
        val fits = Scratch.read(
            connection,
            "select CAST(7 AS BIGINT) as lot_id, CAST(1.500 AS DECIMAL(23,3)) as qty",
        )
        val tooWide = Scratch.read(
            connection,
            "select CAST(9 AS BIGINT) as lot_id, CAST(99999999999999999999.999 AS DECIMAL(23,3)) as qty",
        )
        val writer = writer("wip_stg")

        // Split deliberately. open must succeed: a value in range for CanonicalType.DECIMAL and
        // out of range for the target column is not something the framework can detect, and the
        // throw has to come from inside the append for this to be shape B at all. Verified on the
        // real driver - duckdb_jdbc_appender_append_decimal raises it from within write(). If a
        // later change rejects the width at open, this fails here rather than passing vacuously.
        writer.open(fits.columns)
        val thrown = assertThrows<Throwable> { writer.write(fits.rows + tooWide.rows) }

        assertNotNull(thrown) { "the driver must reject a value too wide for the target column" }
        writer.close()
        assertAll(
            {
                assertEquals(0L, Scratch.rowCount(connection, "wip_stg")) {
                    "the completed first row goes with the buffer"
                }
            },
            { assertFalse(connection.isClosed) { "the writer does not own the caller's connection" } },
        )
    }

    @Test
    fun `closing a writer that was never opened is safe`() {
        writer("wip_stg").close()

        assertFalse(connection.isClosed) { "the writer does not own the caller's connection" }
    }

    companion object {

        @JvmStatic
        fun rejectedTargetColumns(): List<Arguments> = listOf(
            Arguments.of("DOUBLE", "CAST(2.5 AS DOUBLE)"),
            Arguments.of("BOOLEAN", "CAST(true AS BOOLEAN)"),
            Arguments.of("DATE", "DATE '2024-01-02'"),
            Arguments.of("DATE NOT NULL", "DATE '2024-01-02'"),
            Arguments.of("BLOB", "from_hex('AABB')"),
        )
    }
}
