package infra.simpleetl

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import java.sql.Types

/**
 * Spec 4.3 as a unit table, plus the DuckDB side of the enum.
 *
 * The (sqlType, typeName) pairs below are not invented: every one was observed on a real
 * result set from ojdbc11 23.5 against oracle-free 23, or from duckdb_jdbc 1.1.3. The
 * negative ints are Oracle's own extensions and are not java.sql.Types constants, which is
 * why fromJdbc cannot be a when-over-Types alone.
 */
class CanonicalTypeTest {

    companion object {

        @JvmStatic
        fun supported(): List<Arguments> = listOf(
            // NUMBER, NUMERIC, DECIMAL -> BigDecimal.  Oracle reports NUMBER for NUMBER,
            // NUMBER(p,s), INTEGER, SMALLINT and FLOAT alike, all as Types.NUMERIC.
            Arguments.of(Types.NUMERIC, "NUMBER", CanonicalType.DECIMAL),
            Arguments.of(Types.DECIMAL, "DECIMAL(18,3)", CanonicalType.DECIMAL),
            Arguments.of(Types.NUMERIC, "NUMERIC", CanonicalType.DECIMAL),

            // INTEGER, BIGINT, SMALLINT -> Long.  Reachable from DuckDB, never from Oracle DDL.
            Arguments.of(Types.BIGINT, "BIGINT", CanonicalType.LONG),
            Arguments.of(Types.INTEGER, "INTEGER", CanonicalType.LONG),
            Arguments.of(Types.SMALLINT, "SMALLINT", CanonicalType.LONG),

            // FLOAT, DOUBLE, BINARY_DOUBLE -> Double.  101 is Oracle's BINARY_DOUBLE.
            Arguments.of(Types.DOUBLE, "DOUBLE", CanonicalType.DOUBLE),
            Arguments.of(Types.FLOAT, "FLOAT", CanonicalType.DOUBLE),
            Arguments.of(101, "BINARY_DOUBLE", CanonicalType.DOUBLE),

            // VARCHAR2, CHAR, NVARCHAR2, CLOB -> String
            Arguments.of(Types.VARCHAR, "VARCHAR2", CanonicalType.STRING),
            Arguments.of(Types.VARCHAR, "VARCHAR", CanonicalType.STRING),
            Arguments.of(Types.CHAR, "CHAR", CanonicalType.STRING),
            Arguments.of(Types.NVARCHAR, "NVARCHAR2", CanonicalType.STRING),
            Arguments.of(Types.CLOB, "CLOB", CanonicalType.STRING),

            // DATE, TIMESTAMP -> LocalDateTime.  ojdbc reports an Oracle DATE column as
            // Types.TIMESTAMP with typeName DATE, which is why the time component survives.
            Arguments.of(Types.TIMESTAMP, "DATE", CanonicalType.DATETIME),
            Arguments.of(Types.TIMESTAMP, "TIMESTAMP", CanonicalType.DATETIME),

            // Types.DATE is a DuckDB DATE column; Oracle never produces it. See Assumptions.
            Arguments.of(Types.DATE, "DATE", CanonicalType.DATE),

            // TIMESTAMP WITH TIME ZONE -> Instant.  -101 is Oracle's, 2014 is DuckDB's.
            Arguments.of(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE", CanonicalType.INSTANT),
            Arguments.of(-101, "TIMESTAMP WITH TIME ZONE", CanonicalType.INSTANT),

            // RAW, BLOB -> ByteArray
            Arguments.of(Types.VARBINARY, "RAW", CanonicalType.BYTES),
            Arguments.of(Types.BLOB, "BLOB", CanonicalType.BYTES),

            // Not in the 4.3 table, but both engines produce it and the enum has it.
            Arguments.of(Types.BOOLEAN, "BOOLEAN", CanonicalType.BOOLEAN),
        )

        @JvmStatic
        fun unsupported(): List<Arguments> = listOf(
            Arguments.of(-104, "INTERVALDS"),                 // Oracle INTERVAL DAY TO SECOND
            Arguments.of(-103, "INTERVALYM"),                 // Oracle INTERVAL YEAR TO MONTH
            Arguments.of(Types.JAVA_OBJECT, "HUGEINT"),       // DuckDB HUGEINT
            Arguments.of(Types.STRUCT, "MY_OBJECT_TYPE"),
            Arguments.of(Types.ARRAY, "ARRAY"),
            Arguments.of(Types.OTHER, "SDO_GEOMETRY"),
        )
    }

    @ParameterizedTest(name = "{1} ({0}) -> {2}")
    @MethodSource("supported")
    fun `fromJdbc maps every type in spec 4 3`(sqlType: Int, typeName: String, expected: CanonicalType) {
        assertThat(CanonicalType.fromJdbc(sqlType, typeName)).isEqualTo(expected)
    }

    @ParameterizedTest(name = "{1} ({0}) is an error")
    @MethodSource("unsupported")
    fun `fromJdbc rejects anything else and names the type`(sqlType: Int, typeName: String) {
        val thrown = catchThrowable { CanonicalType.fromJdbc(sqlType, typeName) }

        assertThat(thrown).isNotNull().isNotInstanceOf(ClassCastException::class.java)
        assertThat(thrown.message!!.lowercase()).contains(typeName.lowercase())
    }

    /**
     * duckDbType is asserted against DuckDB itself rather than against a hard-coded string:
     * the natural mapping is what a real DuckDB column of that type reads back as. This is
     * the property P2's AUTO DDL depends on - one mapping table in both directions (4.4).
     */
    @ParameterizedTest
    @EnumSource(CanonicalType::class)
    fun `duckDbType is valid DDL and round-trips back to the same canonical type`(type: CanonicalType) {
        val columns = Duck.read("select CAST(NULL AS ${type.duckDbType}) as c", "ddl-round-trip").columns

        assertThat(columns.single().type).isEqualTo(type)
    }

    @Test
    fun `every canonical type declares a duckDb type`() {
        assertThat(CanonicalType.entries.map { it.duckDbType }).doesNotContainNull().noneMatch { it.isBlank() }
    }
}
