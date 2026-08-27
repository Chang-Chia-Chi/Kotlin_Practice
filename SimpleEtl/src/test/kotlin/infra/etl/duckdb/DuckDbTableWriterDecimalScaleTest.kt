package infra.etl.duckdb

import infra.etl.Scratch
import infra.etl.Scratch.STEP
import java.math.BigDecimal
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows

/**
 * Review finding H1: under `createTable: REQUIRED` the writer compared only the canonical type of
 * a DECIMAL pair and never its scale, so a source wider than the target was accepted at [open] and
 * rounded away by `appendBigDecimal` on every row - the silent-rounding class the AUTO path's
 * `ddlType` is written to refuse (spec 4.4, spike S3).
 *
 * The accepted cases are what stop this being a test that a stricter writer passes by rejecting
 * everything: an equal scale and a *wider* target both still load, and a computed expression -
 * which reports no usable scale at all - is left exactly where it was.
 */
class DuckDbTableWriterDecimalScaleTest {

    private val connection = Scratch.open()

    @AfterEach
    fun closeConnection() = connection.close()

    private fun write(target: String, sourceSql: String) {
        Scratch.exec(connection, "create table wip_req (qty $target)")
        val source = Scratch.read(connection, sourceSql)
        DuckDbTableWriter(connection, "wip_req", CreateTable.REQUIRED, STEP).use {
            it.open(source.columns)
            it.write(source.rows)
        }
    }

    @Test
    fun aSourceScaleWiderThanTheTargetIsRejectedAtOpenRatherThanRoundedAtWrite() {
        val failure = assertThrows<IllegalArgumentException> {
            write("DECIMAL(18,3)", "select cast(1.2345678901 as DECIMAL(38,10)) as qty")
        }

        assertAll(
            { assertTrue(failure.message!!.contains(STEP)) { "the error must name the step: ${failure.message}" } },
            { assertTrue(failure.message!!.contains("qty")) { "and the column: ${failure.message}" } },
            {
                assertTrue(failure.message!!.contains("10") && failure.message!!.contains("3")) {
                    "and both scales, or an author cannot tell which end to change: ${failure.message}"
                }
            },
        )
    }

    @Test
    fun anEqualScaleAndAWiderTargetBothStillWrite() {
        write("DECIMAL(18,3)", "select cast(1.234 as DECIMAL(18,3)) as qty")
        assertEquals(1L, Scratch.rowCount(connection, "wip_req"))
    }

    @Test
    fun aTargetWiderThanTheSourceIsNotRounding() {
        write("DECIMAL(38,10)", "select cast(1.234 as DECIMAL(18,3)) as qty")

        val written = Scratch.read(connection, "select qty from wip_req").rows.single().decimal("qty")
        assertEquals(BigDecimal("1.2340000000"), written)
    }
}
