package infra.simpleetl

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.time.LocalDateTime

/**
 * Spec 4.3 against a real Oracle result set. One container for the whole class: the image is
 * multi-GB and the first pull dominates the run.
 *
 * What this file establishes that DuckDB cannot: Oracle's own type quirks. INTEGER, SMALLINT
 * and FLOAT are synonyms for NUMBER and arrive as Types.NUMERIC, BINARY_DOUBLE and
 * TIMESTAMP WITH TIME ZONE arrive as Oracle-private type codes outside java.sql.Types, an
 * Oracle DATE arrives as Types.TIMESTAMP and carries a time component (P0's ruling), and
 * ColumnMeta.nullable is only meaningful here because duckdb_jdbc reports every column as
 * nullable.
 */
@Testcontainers
class RowMapperOracleTest {

    companion object {

        @Container
        @JvmStatic
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:slim-faststart")

        private lateinit var connection: Connection

        @BeforeAll
        @JvmStatic
        fun createFixture() {
            connection = DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                    create table wip_rows (
                        lot_id      NUMBER(18)               not null,
                        qty         NUMBER(18,3),
                        n_integer   INTEGER,
                        n_smallint  SMALLINT,
                        n_float     FLOAT,
                        bin_double  BINARY_DOUBLE,
                        lot_code    VARCHAR2(20)             not null,
                        grade       CHAR(3),
                        note        NVARCHAR2(20),
                        descr       CLOB,
                        upd_dt      DATE,
                        upd_ts      TIMESTAMP,
                        event_tstz  TIMESTAMP WITH TIME ZONE,
                        fingerprint RAW(4),
                        payload     BLOB,
                        is_active   BOOLEAN
                    )
                    """
                )
                // Column-listed, not positional: 4.4 exists because a DDL change must not be
                // able to misalign values, and a fixture is not exempt from its own spec.
                statement.execute(
                    """
                    insert into wip_rows (
                        lot_id, qty, n_integer, n_smallint, n_float, bin_double, lot_code,
                        grade, note, descr, upd_dt, upd_ts, event_tstz, fingerprint, payload,
                        is_active
                    ) values (
                        7, 1.5, 11, 12, 2.5, 3.5, 'L1', 'AAA', 'note-1', 'a clob value',
                        TO_DATE('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS'),
                        TIMESTAMP '2024-01-02 03:04:05.123',
                        TIMESTAMP '2024-01-02 03:04:05 +02:00',
                        HEXTORAW('AABBCCDD'),
                        HEXTORAW('EEFF'),
                        TRUE
                    )
                    """
                )
                statement.execute("insert into wip_rows (lot_id, lot_code) values (8, 'L2')")
            }
            // No commit(): ojdbc opens the connection with autoCommit on and ORA-17273s an
            // explicit commit.
        }

        @AfterAll
        @JvmStatic
        fun closeFixture() {
            // Guarded: an @AfterAll that throws on an uninitialised lateinit buries whatever
            // actually failed in @BeforeAll.
            if (::connection.isInitialized) connection.close()
        }
    }

    private class Read(val columns: List<ColumnMeta>, val rows: List<Row>) {
        val row: Row get() = rows.first()
    }

    private fun read(sql: String, step: String = "load-wip"): Read =
        connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { rs ->
                val mapper = RowMapper(rs.metaData, step)
                val rows = ArrayList<Row>()
                while (rs.next()) rows.add(mapper.map(rs))
                Read(mapper.columns, rows)
            }
        }

    private fun firstRow() = read("select * from wip_rows where lot_id = 7").row

    @Test
    fun `column metadata carries the canonical type of every 4 3 mapping`() {
        val columns = read("select * from wip_rows").columns

        assertThat(columns.map { it.name to it.type }).containsExactly(
            // NUMBER, NUMERIC, DECIMAL -> BigDecimal. Oracle folds INTEGER, SMALLINT and
            // FLOAT into NUMBER, so all five of these are DECIMAL, not LONG or DOUBLE.
            "lot_id" to CanonicalType.DECIMAL,
            "qty" to CanonicalType.DECIMAL,
            "n_integer" to CanonicalType.DECIMAL,
            "n_smallint" to CanonicalType.DECIMAL,
            "n_float" to CanonicalType.DECIMAL,
            "bin_double" to CanonicalType.DOUBLE,
            "lot_code" to CanonicalType.STRING,
            "grade" to CanonicalType.STRING,
            "note" to CanonicalType.STRING,
            "descr" to CanonicalType.STRING,
            "upd_dt" to CanonicalType.DATETIME,
            "upd_ts" to CanonicalType.DATETIME,
            "event_tstz" to CanonicalType.INSTANT,
            "fingerprint" to CanonicalType.BYTES,
            "payload" to CanonicalType.BYTES,
            // Oracle 23 has a native BOOLEAN, and 4.3 now carries the row. If ojdbc11
            // reports anything other than Types.BOOLEAN here, that is worth knowing now.
            "is_active" to CanonicalType.BOOLEAN,
        )
    }

    @Test
    fun `values are the canonical Kotlin types of spec 4 1`() {
        val row = firstRow()

        assertThat(row.decimal("lot_id")).isEqualByComparingTo(BigDecimal("7"))
        assertThat(row.decimal("qty")).isEqualByComparingTo(BigDecimal("1.5"))
        assertThat(row.decimal("n_integer")).isEqualByComparingTo(BigDecimal("11"))
        assertThat(row.decimal("n_float")).isEqualByComparingTo(BigDecimal("2.5"))
        assertThat(row.double("bin_double")).isEqualTo(3.5)
        assertThat(row.string("lot_code")).isEqualTo("L1")
        assertThat(row.string("grade")).isEqualTo("AAA")
        assertThat(row.string("note")).isEqualTo("note-1")
        // A CLOB arrives as oracle.sql.CLOB and must be materialised as a String.
        assertThat(row.string("descr")).isEqualTo("a clob value")
        assertThat(row.dateTime("upd_ts")).isEqualTo(LocalDateTime.parse("2024-01-02T03:04:05.123"))
        // TIMESTAMP WITH TIME ZONE is absolute: +02:00 local is 01:04:05Z.
        assertThat(row.instant("event_tstz")).isEqualTo(Instant.parse("2024-01-02T01:04:05Z"))
        assertThat(row.bytes("fingerprint"))
            .containsExactly(0xAA.toByte(), 0xBB.toByte(), 0xCC.toByte(), 0xDD.toByte())
        assertThat(row.bytes("payload")).containsExactly(0xEE.toByte(), 0xFF.toByte())
        assertThat(row.bool("is_active")).isTrue()
        assertThat(row["is_active"]).isEqualTo(true)
    }

    /**
     * P0's finding, and the reason DATE is rejected as a DuckDB write target (4.6): an Oracle
     * DATE is not a calendar date. If the time component is lost here, every downstream rule
     * that depends on it is arguing about nothing.
     */
    @Test
    fun `an Oracle DATE keeps its time component and maps to LocalDateTime`() {
        val row = firstRow()

        assertThat(row["upd_dt"]).isInstanceOf(LocalDateTime::class.java)
        assertThat(row.dateTime("upd_dt")).isEqualTo(LocalDateTime.of(2024, 1, 2, 3, 4, 5))
    }

    @Test
    fun `SQL NULL in every nullable column reads back as null and the column stays present`() {
        val read = read("select * from wip_rows where lot_id = 8")
        val row = read.row

        read.columns.filter { it.name !in setOf("lot_id", "lot_code") }.forEach { column ->
            assertThat(row[column.name]).describedAs("get(%s)", column.name).isNull()
            assertThat(row.contains(column.name)).describedAs("contains(%s)", column.name).isTrue()
        }
        assertThat(row.decimal("qty")).isNull()
        assertThat(row.string("descr")).isNull()
        assertThat(row.dateTime("upd_dt")).isNull()
        assertThat(row.instant("event_tstz")).isNull()
        assertThat(row.bytes("payload")).isNull()
        assertThat(row.bool("is_active")).isNull()
        assertThat(row.string("note")).isNotEqualTo("")
        assertThat(row.contains("no_such_column")).isFalse()
    }

    @Test
    fun `nullability comes from the Oracle catalog`() {
        val columns = read("select * from wip_rows").associateColumns()

        assertThat(columns.getValue("lot_id").nullable).isFalse()
        assertThat(columns.getValue("lot_code").nullable).isFalse()
        assertThat(columns.getValue("qty").nullable).isTrue()
        assertThat(columns.getValue("upd_dt").nullable).isTrue()
    }

    private fun Read.associateColumns() = columns.associateBy { it.name }

    @Test
    fun `upper case Oracle identifiers are folded to lower case Row keys`() {
        val row = read("select LOT_ID, LOT_CODE from wip_rows where lot_id = 7").row

        assertThat(row.columns).containsExactly("lot_id", "lot_code")
        assertThat(row.string("lot_code")).isEqualTo("L1")
    }

    /** Done-when 3: the two engines must be indistinguishable from the Row's point of view. */
    @Test
    fun `Oracle and DuckDB produce the same Row keys for the same query shape`() {
        val fromOracle = read("select lot_id, lot_code, qty from wip_rows where lot_id = 7").row
        val fromDuckDb = Duck.row(
            "select CAST(7 AS BIGINT) as LOT_ID, 'L1' as lot_code, CAST(1.5 AS DECIMAL(18,3)) as Qty"
        )

        assertThat(fromOracle.columns).isEqualTo(fromDuckDb.columns)
        assertThat(fromOracle.columns).containsExactly("lot_id", "lot_code", "qty")
    }

    @Test
    fun `an unsupported Oracle column type is an error naming the step and the column`() {
        val thrown = catchThrowable {
            read("select INTERVAL '1 2:3:4' DAY TO SECOND as ivl_col from dual", "load-wip")
        }

        assertThat(thrown).isNotInstanceOf(ClassCastException::class.java)
        assertThat(thrown.message!!.lowercase()).contains("load-wip", "ivl_col")
    }

    @Test
    fun `the connection stays usable after an unsupported column type is rejected`() {
        assertThat(catchThrowable { read("select INTERVAL '1' DAY as ivl_col from dual") }).isNotNull()

        val row = read("select lot_code from wip_rows where lot_id = 7").row

        assertThat(row.string("lot_code")).isEqualTo("L1")
    }
}
