package infra.etl.pipe

import infra.etl.Duck
import infra.etl.pipe.Row
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

/**
 * Row semantics of spec 4.2 and 4.5, exercised through Rows produced by RowMapper
 * (Row's constructor is internal, so it is not part of the P1 surface).
 */
class RowTest {

    /** Deliberately mixed-case labels: 4.5 normalises every key to lower case on read. */
    private val fixture = """
        select CAST(7 AS BIGINT)                as LOT_ID,
               'L1'                             as Lot_Code,
               CAST(1.500 AS DECIMAL(18,3))     as qty,
               CAST(NULL AS VARCHAR)            as note,
               CAST(NULL AS DECIMAL(18,3))      as scrap_qty,
               TIMESTAMP '2024-01-02 03:04:05'  as upd_ts,
               true                             as active
    """

    private fun row(step: String = "map-lots"): Row = Duck.row(fixture, step)

    @Test
    fun `columns are lower case and in source order`() {
        assertEquals(
            listOf("lot_id", "lot_code", "qty", "note", "scrap_qty", "upd_ts", "active"),
            row().columns.toList(),
        )
    }

    @Test
    fun `get returns null both for an absent key and for a SQL NULL`() {
        val row = row()
        assertAll(
            { assertNull(row["note"]) { "a SQL NULL must read back as null, was ${row["note"]}" } },
            {
                assertNull(row["no_such_column"]) {
                    "an absent key must read back as null, was ${row["no_such_column"]}"
                }
            },
            { assertEquals("L1", row["lot_code"]) },
        )
    }

    @Test
    fun `contains distinguishes an absent key from a SQL NULL`() {
        val row = row()
        assertAll(
            { assertTrue(row.contains("note")) { "contains(note) was false; columns were ${row.columns}" } },
            { assertTrue(row.contains("scrap_qty")) { "contains(scrap_qty) was false; columns were ${row.columns}" } },
            {
                assertFalse(row.contains("no_such_column")) {
                    "contains(no_such_column) was true; columns were ${row.columns}"
                }
            },
        )
    }

    @Test
    fun `typed accessors return the canonical value`() {
        val row = row()
        assertAll(
            { assertEquals(7L, row.long("lot_id")) },
            { assertEquals("L1", row.string("lot_code")) },
            {
                assertTrue(row.decimal("qty")?.compareTo(BigDecimal("1.500")) == 0) {
                    "expected 1.500 by comparison, was ${row.decimal("qty")}"
                }
            },
            { assertEquals(LocalDateTime.of(2024, 1, 2, 3, 4, 5), row.dateTime("upd_ts")) },
            { assertTrue(row.bool("active") == true) { "active was ${row.bool("active")}" } },
        )
    }

    @Test
    fun `typed accessors return null for a SQL NULL, never a placeholder`() {
        val row = row()
        assertAll(
            { assertNull(row.string("note")) { "was ${row.string("note")}" } },
            { assertNull(row.decimal("scrap_qty")) { "was ${row.decimal("scrap_qty")}" } },
        )
    }

    @Test
    fun `a typed accessor for the wrong type reports step, column, actual and requested type`() {
        val thrown = assertThrows<Throwable> { row(step = "map-lots").long("lot_code") }

        assertAll(
            { assertFalse(thrown is ClassCastException) { "expected a diagnostic error, was $thrown" } },
            {
                val message = thrown.message!!.lowercase()
                assertTrue(listOf("map-lots", "lot_code", "string", "long").all { it in message }) {
                    "the message must name the step, the column, the actual and the requested type; " +
                        "was: ${thrown.message}"
                }
            },
        )
    }

    @Test
    fun `a wrong-type accessor error also names the requested type for the temporal accessors`() {
        val thrown = assertThrows<Throwable> { row().dateTime("qty") }

        assertAll(
            { assertFalse(thrown is ClassCastException) { "expected a diagnostic error, was $thrown" } },
            {
                assertTrue(listOf("qty", "decimal", "datetime").all { it in thrown.message!!.lowercase() }) {
                    "the message must name the column, the actual and the requested type; was: ${thrown.message}"
                }
            },
        )
    }

    @Test
    fun `the row is still usable after a wrong-type accessor error`() {
        val row = row()
        assertThrows<Throwable> { row.instant("lot_code") }

        assertAll(
            { assertEquals("L1", row.string("lot_code")) },
            { assertEquals(7L, row.long("lot_id")) },
            { assertTrue("lot_code" in row.columns) { "columns were ${row.columns}" } },
        )
    }

    @Test
    fun `with adds a column and leaves the original row untouched`() {
        val row = row()
        val enriched = row.with("row_hash", "abc")

        assertAll(
            { assertEquals("abc", enriched.string("row_hash")) },
            { assertTrue("row_hash" in enriched.columns) { "enriched columns were ${enriched.columns}" } },
            { assertFalse("row_hash" in row.columns) { "the original row's columns were ${row.columns}" } },
            { assertNull(row["row_hash"]) { "the original row gained a value: ${row["row_hash"]}" } },
        )
    }

    @Test
    fun `with replaces an existing value and leaves the original row untouched`() {
        val row = row()
        val replaced = row.with("lot_code", "L2")

        assertAll(
            { assertEquals("L2", replaced.string("lot_code")) },
            { assertEquals("L1", row.string("lot_code")) },
            { assertEquals(row.columns, replaced.columns) },
        )
    }

    @Test
    fun `with accepts null and the column stays present`() {
        val cleared = row().with("lot_code", null)

        assertAll(
            { assertTrue(cleared.contains("lot_code")) { "columns were ${cleared.columns}" } },
            { assertNull(cleared["lot_code"]) { "was ${cleared["lot_code"]}" } },
        )
    }

    @Test
    fun `without removes a column and leaves the original row untouched`() {
        val row = row()
        val trimmed = row.without("lot_code")

        assertAll(
            { assertFalse(trimmed.contains("lot_code")) { "trimmed columns were ${trimmed.columns}" } },
            { assertNull(trimmed["lot_code"]) { "was ${trimmed["lot_code"]}" } },
            { assertTrue(row.contains("lot_code")) { "the original row's columns were ${row.columns}" } },
            {
                assertEquals(
                    listOf("lot_id", "qty", "note", "scrap_qty", "upd_ts", "active"),
                    trimmed.columns.toList(),
                )
            },
        )
    }

    @Test
    fun `a transform chain of with and without composes`() {
        val transformed = row().with("row_hash", "abc").without("note").with("row_hash", "def")

        assertAll(
            { assertEquals("def", transformed.string("row_hash")) },
            { assertFalse(transformed.contains("note")) { "columns were ${transformed.columns}" } },
            { assertDoesNotThrow { transformed.long("lot_id") } },
        )
    }

    /**
     * The step label is only observable through an accessor error, so it is only a copy
     * away from being silently dropped. A transform returns row.with(...), and P3 hands
     * that copy to the writer: if the label did not survive the copy, the first diagnostic
     * anyone sees at 03:00 would name no step.
     */
    @Test
    fun `the step survives with and without into the copied row`() {
        val transformed = row(step = "map-lots").with("row_hash", "abc").without("note")

        val message = assertThrows<Throwable> { transformed.long("row_hash") }.message!!.lowercase()

        assertTrue(listOf("map-lots", "row_hash", "string", "long").all { it in message }) {
            "the copied row's error must still name the step, the column and both types; was: $message"
        }
    }

    /**
     * 4.2 calls Row immutable, and P3 will hand the same Row to a transform and then to a
     * writer. A typed accessor that memoised its conversion into a shared field would pass
     * every single-threaded test above and fail here. No sleeps: the pool is drained by
     * awaiting the futures.
     */
    @Test
    fun `a Row is safe to read from several threads at once`() {
        val row = row()
        val pool = Executors.newFixedThreadPool(8)
        try {
            val results = (1..64).map {
                pool.submit<List<Any?>> {
                    listOf(row.long("lot_id"), row.string("lot_code"), row.decimal("qty"), row.columns.toList())
                }
            }.map { it.get(30, TimeUnit.SECONDS) }

            assertAll(
                {
                    assertTrue(results.all { it == results.first() }) {
                        "concurrent reads disagreed; distinct results were ${results.distinct()}"
                    }
                },
                { assertEquals(7L, results.first()[0]) },
            )
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `the instant accessor returns the canonical instant`() {
        val row = Duck.row("select TIMESTAMPTZ '2024-01-02 03:04:05+00' as event_ts")

        assertEquals(Instant.parse("2024-01-02T03:04:05Z"), row.instant("event_ts"))
    }
}
