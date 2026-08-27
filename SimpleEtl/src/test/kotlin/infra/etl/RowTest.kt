package infra.etl

import infra.etl.pipe.Row
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

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
        assertThat(row().columns)
            .containsExactly("lot_id", "lot_code", "qty", "note", "scrap_qty", "upd_ts", "active")
    }

    @Test
    fun `get returns null both for an absent key and for a SQL NULL`() {
        val row = row()
        assertThat(row["note"]).isNull()
        assertThat(row["no_such_column"]).isNull()
        assertThat(row["lot_code"]).isEqualTo("L1")
    }

    @Test
    fun `contains distinguishes an absent key from a SQL NULL`() {
        val row = row()
        assertThat(row.contains("note")).isTrue()
        assertThat(row.contains("scrap_qty")).isTrue()
        assertThat(row.contains("no_such_column")).isFalse()
    }

    @Test
    fun `typed accessors return the canonical value`() {
        val row = row()
        assertThat(row.long("lot_id")).isEqualTo(7L)
        assertThat(row.string("lot_code")).isEqualTo("L1")
        assertThat(row.decimal("qty")).isEqualByComparingTo(BigDecimal("1.500"))
        assertThat(row.dateTime("upd_ts")).isEqualTo(LocalDateTime.of(2024, 1, 2, 3, 4, 5))
        assertThat(row.bool("active")).isTrue()
    }

    @Test
    fun `typed accessors return null for a SQL NULL, never a placeholder`() {
        val row = row()
        assertThat(row.string("note")).isNull()
        assertThat(row.decimal("scrap_qty")).isNull()
    }

    @Test
    fun `a typed accessor for the wrong type reports step, column, actual and requested type`() {
        val thrown = catchThrowable { row(step = "map-lots").long("lot_code") }

        assertThat(thrown).isNotInstanceOf(ClassCastException::class.java)
        assertThat(thrown.message!!.lowercase()).contains("map-lots", "lot_code", "string", "long")
    }

    @Test
    fun `a wrong-type accessor error also names the requested type for the temporal accessors`() {
        val thrown = catchThrowable { row().dateTime("qty") }

        assertThat(thrown).isNotInstanceOf(ClassCastException::class.java)
        assertThat(thrown.message!!.lowercase()).contains("qty", "decimal", "datetime")
    }

    @Test
    fun `the row is still usable after a wrong-type accessor error`() {
        val row = row()
        assertThat(catchThrowable { row.instant("lot_code") }).isNotNull()

        assertThat(row.string("lot_code")).isEqualTo("L1")
        assertThat(row.long("lot_id")).isEqualTo(7L)
        assertThat(row.columns).contains("lot_code")
    }

    @Test
    fun `with adds a column and leaves the original row untouched`() {
        val row = row()
        val enriched = row.with("row_hash", "abc")

        assertThat(enriched.string("row_hash")).isEqualTo("abc")
        assertThat(enriched.columns).contains("row_hash")
        assertThat(row.columns).doesNotContain("row_hash")
        assertThat(row["row_hash"]).isNull()
    }

    @Test
    fun `with replaces an existing value and leaves the original row untouched`() {
        val row = row()
        val replaced = row.with("lot_code", "L2")

        assertThat(replaced.string("lot_code")).isEqualTo("L2")
        assertThat(row.string("lot_code")).isEqualTo("L1")
        assertThat(replaced.columns).isEqualTo(row.columns)
    }

    @Test
    fun `with accepts null and the column stays present`() {
        val cleared = row().with("lot_code", null)

        assertThat(cleared.contains("lot_code")).isTrue()
        assertThat(cleared["lot_code"]).isNull()
    }

    @Test
    fun `without removes a column and leaves the original row untouched`() {
        val row = row()
        val trimmed = row.without("lot_code")

        assertThat(trimmed.contains("lot_code")).isFalse()
        assertThat(trimmed["lot_code"]).isNull()
        assertThat(row.contains("lot_code")).isTrue()
        assertThat(trimmed.columns).containsExactly("lot_id", "qty", "note", "scrap_qty", "upd_ts", "active")
    }

    @Test
    fun `a transform chain of with and without composes`() {
        val transformed = row().with("row_hash", "abc").without("note").with("row_hash", "def")

        assertThat(transformed.string("row_hash")).isEqualTo("def")
        assertThat(transformed.contains("note")).isFalse()
        assertThatCode { transformed.long("lot_id") }.doesNotThrowAnyException()
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

        val message = catchThrowable { transformed.long("row_hash") }.message!!.lowercase()

        assertThat(message).contains("map-lots", "row_hash", "string", "long")
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

            assertThat(results).allMatch { it == results.first() }
            assertThat(results.first()[0]).isEqualTo(7L)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `the instant accessor returns the canonical instant`() {
        val row = Duck.row("select TIMESTAMPTZ '2024-01-02 03:04:05+00' as event_ts")

        assertThat(row.instant("event_ts")).isEqualTo(Instant.parse("2024-01-02T03:04:05Z"))
    }
}
