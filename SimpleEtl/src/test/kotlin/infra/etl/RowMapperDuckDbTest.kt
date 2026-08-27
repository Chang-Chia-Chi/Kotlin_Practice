package infra.etl

import infra.etl.pipe.CanonicalType
import infra.etl.pipe.Row
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

/**
 * Spec 4.3 against a real duckdb_jdbc 1.1.3 result set - the half of the mapping table that
 * Oracle DDL cannot produce (INTEGER / BIGINT / SMALLINT, DuckDB DATE, BOOLEAN) plus every
 * other row, so the DuckDB read seam stands on its own for P3's scratch-to-Oracle pipes.
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

        assertThat(read.columns.map { it.name to it.type }).containsExactly(
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
        )
    }

    @Test
    fun `values are the canonical Kotlin types of spec 4 1`() {
        val row = Duck.read(allTypes, "duck-read").row

        assertThat(row["c_bigint"]).isEqualTo(7L)
        assertThat(row["c_integer"]).isEqualTo(7L)
        assertThat(row["c_smallint"]).isEqualTo(7L)
        assertThat(row["c_decimal"]).isInstanceOf(BigDecimal::class.java)
        assertThat(row["c_double"]).isEqualTo(2.5)
        // DuckDB hands a FLOAT column back as java.lang.Float; 4.3 says the canonical type is Double.
        assertThat(row["c_float"]).isEqualTo(1.5)
        assertThat(row["c_varchar"]).isEqualTo("L1")
        assertThat(row["c_boolean"]).isEqualTo(true)
        assertThat(row["c_date"]).isEqualTo(LocalDate.of(2024, 1, 2))
        assertThat(row["c_timestamp"]).isEqualTo(LocalDateTime.of(2024, 1, 2, 3, 4, 5))
        assertThat(row["c_timestamptz"]).isEqualTo(Instant.parse("2024-01-02T01:04:05Z"))
        assertThat(row["c_blob"] as ByteArray).containsExactly(0xAA.toByte(), 0xBB.toByte())
    }

    @Test
    fun `typed accessors agree with the canonical values`() {
        val row = Duck.read(allTypes, "duck-read").row

        assertThat(row.long("c_integer")).isEqualTo(7L)
        assertThat(row.decimal("c_decimal")).isEqualByComparingTo(BigDecimal("1.5"))
        assertThat(row.double("c_float")).isEqualTo(1.5)
        assertThat(row.string("c_varchar")).isEqualTo("L1")
        assertThat(row.bool("c_boolean")).isTrue()
        assertThat(row.date("c_date")).isEqualTo(LocalDate.of(2024, 1, 2))
        assertThat(row.dateTime("c_timestamp")).isEqualTo(LocalDateTime.of(2024, 1, 2, 3, 4, 5))
        assertThat(row.instant("c_timestamptz")).isEqualTo(Instant.parse("2024-01-02T01:04:05Z"))
        assertThat(row.bytes("c_blob")).containsExactly(0xAA.toByte(), 0xBB.toByte())
    }

    @Test
    fun `SQL NULL in every canonical type reads back as null and the column stays present`() {
        val read = Duck.read(allNulls, "duck-read")
        val row = read.row

        assertThat(read.columns).hasSize(12)
        read.columns.forEach { column ->
            assertThat(row[column.name]).describedAs("get(%s)", column.name).isNull()
            assertThat(row.contains(column.name)).describedAs("contains(%s)", column.name).isTrue()
        }
        assertThat(row.string("c_varchar")).isNull()
        assertThat(row.long("c_bigint")).isNull()
        assertThat(row.decimal("c_decimal")).isNull()
        assertThat(row.double("c_double")).isNull()
        assertThat(row.bool("c_boolean")).isNull()
        assertThat(row.date("c_date")).isNull()
        assertThat(row.dateTime("c_timestamp")).isNull()
        assertThat(row.instant("c_timestamptz")).isNull()
        assertThat(row.bytes("c_blob")).isNull()
    }

    @Test
    fun `a null value is never substituted with an empty string or a sentinel`() {
        val row = Duck.row("select CAST(NULL AS VARCHAR) as note, CAST(NULL AS DECIMAL(18,3)) as qty")

        assertThat(row["note"]).isNotEqualTo("")
        assertThat(row["qty"]).isNotEqualTo(BigDecimal.ZERO)
        assertThat(row["note"]).isNull()
        assertThat(row["qty"]).isNull()
    }

    @Test
    fun `lower case DuckDB identifiers survive unchanged and upper case aliases are folded`() {
        val row = Duck.row("select 1 as LOT_ID, 2 as lot_qty, 3 as \"MixedCase\"")

        assertThat(row.columns).containsExactly("lot_id", "lot_qty", "mixedcase")
    }

    @Test
    fun `an unsupported DuckDB column type is an error naming the step and the column`() {
        val thrown = catchThrowable { Duck.read("select CAST(1 AS HUGEINT) as big_id", "load-wip") }

        assertThat(thrown).isNotInstanceOf(ClassCastException::class.java)
        assertThat(thrown.message!!.lowercase()).contains("load-wip", "big_id")
    }

    /**
     * 4.5 folds both labels to the same key, so a silently collapsed Row would carry one
     * column where the result set had two - and P2's positional appender would then write
     * the wrong value into the wrong column with nothing in the log.
     */
    @Test
    fun `duplicate Row keys are a diagnostic error, not a silently collapsed Row`() {
        val thrown = catchThrowable { Duck.read("select 1 as qty, 2 as QTY", "load-wip") }

        assertThat(thrown).isNotNull()
        assertThat(thrown.message!!.lowercase()).contains("load-wip", "qty")
    }

    @Test
    fun `a mapper stays usable for the rows after the first`() {
        val read = Duck.read(
            "select * from (values (1, 'a'), (2, NULL), (3, 'c')) as t(lot_id, lot_code)",
            "duck-read",
        )

        assertThat(read.rows).hasSize(3)
        assertThat(read.rows.map { it.long("lot_id") }).containsExactly(1L, 2L, 3L)
        assertThat(read.rows.map { it.string("lot_code") }).containsExactly("a", null, "c")
    }

    /**
     * duckdb_jdbc 1.1.3 reports columnNullable for every column, NOT NULL included, so the
     * DuckDB side of ColumnMeta.nullable is always true. Nullability coverage that means
     * anything has to come from Oracle - see RowMapperOracleTest.
     */
    @Test
    fun `DuckDB reports every column as nullable`() {
        assertThat(Duck.read(allTypes, "duck-read").columns).allMatch { it.nullable }
    }
}
