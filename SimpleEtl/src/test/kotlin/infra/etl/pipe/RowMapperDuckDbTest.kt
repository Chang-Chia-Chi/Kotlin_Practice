package infra.etl.pipe

import infra.etl.Duck
import infra.etl.pipe.CanonicalType
import infra.etl.pipe.Row
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows

/**
 * The JDBC-to-canonical type mapping against a real duckdb_jdbc 1.1.3 result set - the half of
 * the table that Oracle DDL cannot produce (INTEGER / BIGINT / SMALLINT, DuckDB DATE, BOOLEAN)
 * plus every other row, so the DuckDB read seam stands on its own for P3's scratch-to-Oracle
 * pipes.
 *
 * Fixtures are SELECT-only: nothing is written, per P1's "not in scope".
 */
class RowMapperDuckDbTest {

    private val allTypes = """
        select CAST(7      AS BIGINT)              as c_bigint,
               CAST(7      AS INTEGER)             as c_integer,
               CAST(7      AS SMALLINT)            as c_smallint,
               CAST(1.500  AS DECIMAL(18,3))       as c_decimal,
               CAST(2.5    AS DOUBLE)              as c_double,
               CAST(1.5    AS FLOAT)               as c_float,
               CAST('L1'   AS VARCHAR)             as c_varchar,
               CAST(true   AS BOOLEAN)             as c_boolean,
               DATE '2024-01-02'                   as c_date,
               TIMESTAMP '2024-01-02 03:04:05'     as c_timestamp,
               TIMESTAMPTZ '2024-01-02 03:04:05+02' as c_timestamptz,
               from_hex('AABB')                    as c_blob
    """

    private val allNulls = """
        select CAST(NULL AS BIGINT)                   as c_bigint,
               CAST(NULL AS INTEGER)                  as c_integer,
               CAST(NULL AS SMALLINT)                 as c_smallint,
               CAST(NULL AS DECIMAL(18,3))            as c_decimal,
               CAST(NULL AS DOUBLE)                   as c_double,
               CAST(NULL AS FLOAT)                    as c_float,
               CAST(NULL AS VARCHAR)                  as c_varchar,
               CAST(NULL AS BOOLEAN)                  as c_boolean,
               CAST(NULL AS DATE)                     as c_date,
               CAST(NULL AS TIMESTAMP)                as c_timestamp,
               CAST(NULL AS TIMESTAMP WITH TIME ZONE) as c_timestamptz,
               CAST(NULL AS BLOB)                     as c_blob
    """

    @Test
    fun `column metadata carries the canonical type of every 4 3 mapping`() {
        val read = Duck.read(allTypes, "duck-read")

        assertEquals(
            listOf(
                "c_bigint" to CanonicalType.LONG,
                "c_integer" to CanonicalType.LONG,
                "c_smallint" to CanonicalType.LONG,
                "c_decimal" to CanonicalType.DECIMAL,
                "c_double" to CanonicalType.DOUBLE,
                "c_float" to CanonicalType.DOUBLE,
                "c_varchar" to CanonicalType.STRING,
                "c_boolean" to CanonicalType.BOOLEAN,
                "c_date" to CanonicalType.DATE,
                "c_timestamp" to CanonicalType.DATETIME,
                "c_timestamptz" to CanonicalType.INSTANT,
                "c_blob" to CanonicalType.BYTES,
            ),
            read.columns.map { it.name to it.type },
        )
    }

    @Test
    fun `values are the canonical Kotlin types of spec 4 1`() {
        val row = Duck.read(allTypes, "duck-read").row

        assertAll(
            { assertEquals(7L, row["c_bigint"]) },
            { assertEquals(7L, row["c_integer"]) },
            { assertEquals(7L, row["c_smallint"]) },
            { assertInstanceOf(BigDecimal::class.java, row["c_decimal"]) },
            { assertEquals(2.5, row["c_double"]) },
            // DuckDB hands a FLOAT column back as java.lang.Float; 4.3 says the canonical type is Double.
            { assertEquals(1.5, row["c_float"]) },
            { assertEquals("L1", row["c_varchar"]) },
            { assertEquals(true, row["c_boolean"]) },
            { assertEquals(LocalDate.of(2024, 1, 2), row["c_date"]) },
            { assertEquals(LocalDateTime.of(2024, 1, 2, 3, 4, 5), row["c_timestamp"]) },
            { assertEquals(Instant.parse("2024-01-02T01:04:05Z"), row["c_timestamptz"]) },
            { assertArrayEquals(byteArrayOf(0xAA.toByte(), 0xBB.toByte()), row["c_blob"] as ByteArray) },
        )
    }

    @Test
    fun `typed accessors agree with the canonical values`() {
        val row = Duck.read(allTypes, "duck-read").row

        assertAll(
            { assertEquals(7L, row.long("c_integer")) },
            {
                assertTrue(row.decimal("c_decimal")?.compareTo(BigDecimal("1.5")) == 0) {
                    "expected 1.5 by comparison, was ${row.decimal("c_decimal")}"
                }
            },
            { assertEquals(1.5, row.double("c_float")) },
            { assertEquals("L1", row.string("c_varchar")) },
            { assertTrue(row.bool("c_boolean") == true) { "c_boolean was ${row.bool("c_boolean")}" } },
            { assertEquals(LocalDate.of(2024, 1, 2), row.date("c_date")) },
            { assertEquals(LocalDateTime.of(2024, 1, 2, 3, 4, 5), row.dateTime("c_timestamp")) },
            { assertEquals(Instant.parse("2024-01-02T01:04:05Z"), row.instant("c_timestamptz")) },
            { assertArrayEquals(byteArrayOf(0xAA.toByte(), 0xBB.toByte()), row.bytes("c_blob")) },
        )
    }

    @Test
    fun `SQL NULL in every canonical type reads back as null and the column stays present`() {
        val read = Duck.read(allNulls, "duck-read")
        val row = read.row

        assertEquals(12, read.columns.size) { "columns were ${read.columns.map { it.name }}" }
        read.columns.forEach { column ->
            assertNull(row[column.name]) { "get(${column.name}) was ${row[column.name]}" }
            assertTrue(row.contains(column.name)) { "contains(${column.name}) was false" }
        }
        assertAll(
            { assertNull(row.string("c_varchar")) { "was ${row.string("c_varchar")}" } },
            { assertNull(row.long("c_bigint")) { "was ${row.long("c_bigint")}" } },
            { assertNull(row.decimal("c_decimal")) { "was ${row.decimal("c_decimal")}" } },
            { assertNull(row.double("c_double")) { "was ${row.double("c_double")}" } },
            { assertNull(row.bool("c_boolean")) { "was ${row.bool("c_boolean")}" } },
            { assertNull(row.date("c_date")) { "was ${row.date("c_date")}" } },
            { assertNull(row.dateTime("c_timestamp")) { "was ${row.dateTime("c_timestamp")}" } },
            { assertNull(row.instant("c_timestamptz")) { "was ${row.instant("c_timestamptz")}" } },
            { assertNull(row.bytes("c_blob")) { "was ${row.bytes("c_blob")}" } },
        )
    }

    @Test
    fun `a null value is never substituted with an empty string or a sentinel`() {
        val row = Duck.row("select CAST(NULL AS VARCHAR) as note, CAST(NULL AS DECIMAL(18,3)) as qty")

        assertAll(
            { assertNotEquals("", row["note"]) },
            { assertNotEquals(BigDecimal.ZERO, row["qty"]) },
            { assertNull(row["note"]) { "was ${row["note"]}" } },
            { assertNull(row["qty"]) { "was ${row["qty"]}" } },
        )
    }

    @Test
    fun `lower case DuckDB identifiers survive unchanged and upper case aliases are folded`() {
        val row = Duck.row("select 1 as LOT_ID, 2 as lot_qty, 3 as \"MixedCase\"")

        assertEquals(listOf("lot_id", "lot_qty", "mixedcase"), row.columns.toList())
    }

    @Test
    fun `an unsupported DuckDB column type is an error naming the step and the column`() {
        val thrown = assertThrows<Throwable> { Duck.read("select CAST(1 AS HUGEINT) as big_id", "load-wip") }

        assertAll(
            { assertFalse(thrown is ClassCastException) { "expected a diagnostic error, was $thrown" } },
            {
                assertTrue(listOf("load-wip", "big_id").all { it in thrown.message!!.lowercase() }) {
                    "the message must name the step and the column; was: ${thrown.message}"
                }
            },
        )
    }

    /**
     * 4.5 folds both labels to the same key, so a silently collapsed Row would carry one
     * column where the result set had two - and P2's positional appender would then write
     * the wrong value into the wrong column with nothing in the log.
     */
    @Test
    fun `duplicate Row keys are a diagnostic error, not a silently collapsed Row`() {
        val thrown = assertThrows<Throwable> { Duck.read("select 1 as qty, 2 as QTY", "load-wip") }

        assertTrue(listOf("load-wip", "qty").all { it in thrown.message!!.lowercase() }) {
            "the message must name the step and the duplicated key; was: ${thrown.message}"
        }
    }

    @Test
    fun `a mapper stays usable for the rows after the first`() {
        val read = Duck.read(
            "select * from (values (1, 'a'), (2, NULL), (3, 'c')) as t(lot_id, lot_code)",
            "duck-read",
        )

        assertAll(
            { assertEquals(3, read.rows.size) { "rows were ${read.rows.size}" } },
            { assertEquals(listOf(1L, 2L, 3L), read.rows.map { it.long("lot_id") }) },
            { assertEquals(listOf("a", null, "c"), read.rows.map { it.string("lot_code") }) },
        )
    }

    /**
     * duckdb_jdbc 1.1.3 reports columnNullable for every column, NOT NULL included, so the
     * DuckDB side of ColumnMeta.nullable is always true. Nullability coverage that means
     * anything has to come from Oracle - see RowMapperOracleTest.
     */
    @Test
    fun `DuckDB reports every column as nullable`() {
        assertTrue(Duck.read(allTypes, "duck-read").columns.all { it.nullable }) {
            "columns duckdb_jdbc did not report as nullable: " +
                "${Duck.read(allTypes, "duck-read").columns.filterNot { it.nullable }.map { it.name }}"
        }
    }
}
