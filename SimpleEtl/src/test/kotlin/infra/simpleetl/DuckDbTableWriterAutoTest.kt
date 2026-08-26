package infra.simpleetl

import infra.simpleetl.Scratch.STEP
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.math.BigDecimal
import java.time.LocalDateTime

/**
 * `createTable: AUTO` against a DuckDB source (spec 4.6, plan P2 done-when 1, 2 and 4).
 *
 * Everything a DuckDB result set reports is nullable - duckdb_jdbc 1.1.3 says columnNullable for
 * NOT NULL columns too - so this file can only exercise the nullable half of the DDL rule. The
 * NOT NULL half needs a source that reports nullability truthfully and lives in
 * [WriterOracleTest]. Splitting them is the point: a DuckDB-only AUTO DDL suite would pass while
 * asserting nothing at all about "NOT NULL columns keep their natural mapping".
 */
class DuckDbTableWriterAutoTest {

    private val connection = Scratch.open()

    @AfterEach
    fun closeConnection() = connection.close()

    /** One column per canonical type that has a null-accepting appender method (4.6). */
    private val values = """
        select CAST('L1'  AS VARCHAR)          as lot_code,
               CAST(7     AS BIGINT)           as lot_id,
               CAST(1.500 AS DECIMAL(18,3))    as qty,
               TIMESTAMP '2024-01-02 03:04:05' as upd_ts
    """

    private val nulls = """
        select CAST(NULL AS VARCHAR)       as lot_code,
               CAST(NULL AS BIGINT)        as lot_id,
               CAST(NULL AS DECIMAL(18,3)) as qty,
               CAST(NULL AS TIMESTAMP)     as upd_ts
    """

    private fun writer(table: String) = DuckDbTableWriter(connection, table, CreateTable.AUTO, STEP)

    /**
     * Done-when 1, nullable half. Every generated type must be one a null can reach:
     * append(String), appendBigDecimal, or appendLocalDateTime. DECIMAL keeps the source's
     * precision and scale - `CanonicalType.DECIMAL.duckDbType` is the bare string "DECIMAL",
     * which DuckDB silently resolves to DECIMAL(18,3), so emitting it unqualified truncates.
     */
    @Test
    fun `AUTO DDL creates every nullable source column as a null accepting type`() {
        val source = Scratch.read(connection, values)
        assertThat(source.columns.map { it.nullable })
            .describedAs("duckdb_jdbc reports every column nullable; if this changes the split with the Oracle suite must be revisited")
            .containsOnly(true)

        writer("wip_stg").use { it.open(source.columns) }

        assertThat(Scratch.declaredTypes(connection, "wip_stg")).containsExactly(
            "lot_code" to "VARCHAR",
            // BIGINT joined VARCHAR/DECIMAL/TIMESTAMP when S3 showed appendBigDecimal is exact
            // at scale 0 and the value comes from Row.long(). See P0's ruling on rule 15.
            "lot_id" to "BIGINT",
            "qty" to "DECIMAL(18,3)",
            "upd_ts" to "TIMESTAMP",
        )
    }

    /** The truncation P1 flagged: a source wider than DECIMAL(18,3) must not land as DECIMAL(18,3). */
    @Test
    fun `AUTO DDL carries the source precision and scale onto the target DECIMAL column`() {
        val source = Scratch.read(connection, "select CAST(1.0 AS DECIMAL(38,10)) as big_qty")

        writer("wip_stg").use { it.open(source.columns) }

        assertThat(Scratch.declaredTypes(connection, "wip_stg")).containsExactly("big_qty" to "DECIMAL(38,10)")
    }

    /**
     * The accepted half of the DECIMAL width guard, so the rejection test below cannot pass by
     * rejecting every width. Both ends of `1 <= p <= 38` and `0 <= s <= p` must survive: (1,0) is
     * the narrowest declarable column and (38,38) the widest scale DuckDB accepts.
     *
     * ColumnMeta is public with precision and scale defaulting to 0 (spec 11.1), so a width is
     * fed straight into open. No source query can produce these boundaries on demand, and going
     * through one would test the driver rather than the guard.
     */
    @ParameterizedTest
    @MethodSource("acceptedDecimalWidths")
    fun `AUTO DDL accepts a DECIMAL width at the edge of what DuckDB can declare`(precision: Int, scale: Int) {
        val columns = listOf(decimal(precision, scale))

        writer("wip_stg").use { it.open(columns) }

        assertThat(Scratch.declaredTypes(connection, "wip_stg"))
            .containsExactly("total" to "DECIMAL($precision,$scale)")
    }

    /**
     * The rejection half, and the headline contract change of this phase. An unconstrained Oracle
     * NUMBER reports (0,-127), a FLOAT reports (126,-127), and a computed expression can report a
     * scale wider than its precision; none of those is a width DuckDB can declare, and guessing
     * one is how a value silently changes shape. The empty information_schema is what proves the
     * guard fires before CREATE TABLE rather than after it.
     */
    @ParameterizedTest
    @MethodSource("rejectedDecimalWidths")
    fun `AUTO DDL rejects a DECIMAL width it cannot declare, at open and before the table exists`(
        precision: Int,
        scale: Int,
    ) {
        val columns = listOf(decimal(precision, scale))

        val thrown = catchThrowable { writer("wip_stg").use { it.open(columns) } }

        assertThat(thrown)
            .isNotInstanceOf(NullPointerException::class.java)
            .hasMessageContaining(STEP)
            .hasMessageContaining("total")
        assertThat(Scratch.declaredTypes(connection, "wip_stg")).isEmpty()
    }

    private fun decimal(precision: Int, scale: Int) =
        ColumnMeta("total", CanonicalType.DECIMAL, nullable = true, precision = precision, scale = scale)

    /**
     * Done-when 2. A placeholder would satisfy "the column exists and holds something", so the
     * two placeholders that could plausibly be written are excluded before null is asserted.
     */
    @Test
    fun `a null in every nullable canonical type round trips as null, not as an empty string or a zero`() {
        val source = Scratch.read(connection, values)
        val nullRow = Scratch.read(connection, nulls)

        writer("wip_stg").use {
            it.open(source.columns)
            assertThat(it.write(source.rows + nullRow.rows)).isEqualTo(2)
        }

        val back = Scratch.read(
            connection,
            "select lot_code, lot_id, qty, upd_ts from wip_stg order by lot_id nulls last",
        ).rows
        assertThat(back).hasSize(2)

        assertThat(back[0].string("lot_code")).isEqualTo("L1")
        assertThat(back[0].long("lot_id")).isEqualTo(7L)
        assertThat(back[0].decimal("qty")).isEqualByComparingTo(BigDecimal("1.500"))
        assertThat(back[0].dateTime("upd_ts")).isEqualTo(LocalDateTime.parse("2024-01-02T03:04:05"))

        assertThat(back[1].string("lot_code")).isNotEqualTo("").isNull()
        // Row.long() is Long?, which Kotlin resolves to AssertJ's *primitive* LongAssert, whose
        // isNotEqualTo asserts non-null before it compares - so on a correct null it fails with
        // "Expecting actual not to be null" and the sentinel is never checked at all. The untyped
        // accessor is Any? and gets the Object assertion, which does compare.
        assertThat(back[1]["lot_id"]).isNotEqualTo(0L).isNull()
        assertThat(back[1].long("lot_id")).isNull()
        assertThat(back[1].decimal("qty")).isNotEqualTo(BigDecimal.ZERO).isNull()
        assertThat(back[1].dateTime("upd_ts")).isNull()
        // The column is present and holds SQL NULL, not absent and not a sentinel string.
        assertThat(back[1].contains("upd_ts")).isTrue()
        assertThat(back[1]["upd_ts"]).isNotEqualTo("").isNull()
    }

    /**
     * Done-when 4. "At open time, not mid-chunk" is asserted by the absence of the table: a
     * writer that discovered the BLOB while appending would have created it first.
     */
    @Test
    fun `a BLOB column is rejected at open, before the table exists and before any row is written`() {
        val source = Scratch.read(
            connection,
            "select CAST('L1' AS VARCHAR) as lot_code, from_hex('AABB') as payload",
        )
        val writer = writer("wip_stg")

        val thrown = catchThrowable { writer.open(source.columns) }

        assertThat(thrown)
            .isNotInstanceOf(ClassCastException::class.java)
            .isNotInstanceOf(NullPointerException::class.java)
            .hasMessageContaining(STEP)
            .hasMessageContaining("payload")
        assertThat(thrown.message!!.uppercase()).containsAnyOf("BLOB", "RAW", "BYTE")
        assertThat(Scratch.declaredTypes(connection, "wip_stg")).isEmpty()

        // Return to a usable state: close after a failed open is safe and the caller's
        // connection - which the writer does not own - is still open.
        writer.close()
        assertThat(connection.isClosed).isFalse()
        assertThat(Scratch.read(connection, "select 1 as ok").rows).hasSize(1)
    }

    /**
     * The gap this phase found. A nullable column must be created as a type a null can reach,
     * and the writer sources the value with the accessor matching that type (4.6's dispatch).
     * For BOOLEAN, DOUBLE, DATE and INSTANT no such pair exists: BOOLEAN and DOUBLE have only
     * primitive append overloads, DATE truncates silently and is rejected outright by rule 15,
     * INSTANT has no branch in 4.6 at all, and routing any of them through VARCHAR, DECIMAL or
     * TIMESTAMP would make Row.string / Row.decimal / Row.dateTime throw on the value's real
     * type. Since duckdb_jdbc calls every column nullable, all four are reachable from any
     * scratch-to-scratch pipe.
     *
     * Rejecting at open is the only outcome consistent with the rest of the spec - the framework
     * never guesses, and 4.6 refuses encoding tricks - but the spec does not say so, so this
     * test is the lead's to adjudicate.
     */
    @ParameterizedTest
    @ValueSource(
        strings = [
            "CAST(2.5 AS DOUBLE)",
            "CAST(true AS BOOLEAN)",
            "DATE '2024-01-02'",
            "TIMESTAMPTZ '2024-01-02 03:04:05+02'",
        ],
    )
    fun `a nullable source column with no null accepting write path is rejected at open`(expression: String) {
        val source = Scratch.read(connection, "select $expression as risky")

        val thrown = catchThrowable { writer("wip_stg").use { it.open(source.columns) } }

        assertThat(thrown)
            .isNotInstanceOf(NullPointerException::class.java)
            .hasMessageContaining(STEP)
            .hasMessageContaining("risky")
        assertThat(Scratch.declaredTypes(connection, "wip_stg")).isEmpty()
    }

    companion object {

        @JvmStatic
        fun acceptedDecimalWidths(): List<Arguments> = listOf(
            Arguments.of(38, 38),
            Arguments.of(1, 0),
        )

        @JvmStatic
        fun rejectedDecimalWidths(): List<Arguments> = listOf(
            Arguments.of(0, -127),
            Arguments.of(39, 0),
            Arguments.of(126, -127),
            Arguments.of(5, 7),
        )
    }
}
