package infra.etl.pipe

import infra.etl.Duck
import infra.etl.pipe.RowMapper
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Types
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito

/**
 * The cases that no database will produce on demand: unsupported column types the drivers
 * here never emit, and the metadata-read-once property. Mockito is used strictly at the
 * ResultSetMetaData / ResultSet interface boundary; nothing internal to RowMapper is mocked.
 */
class RowMapperErrorTest {

    private fun metaData(vararg columns: Triple<String, Int, String>): ResultSetMetaData {
        val md = Mockito.mock(ResultSetMetaData::class.java)
        Mockito.`when`(md.columnCount).thenReturn(columns.size)
        columns.forEachIndexed { index, (name, sqlType, typeName) ->
            val i = index + 1
            Mockito.`when`(md.getColumnLabel(i)).thenReturn(name)
            Mockito.`when`(md.getColumnName(i)).thenReturn(name)
            Mockito.`when`(md.getColumnType(i)).thenReturn(sqlType)
            Mockito.`when`(md.getColumnTypeName(i)).thenReturn(typeName)
            Mockito.`when`(md.isNullable(i)).thenReturn(ResultSetMetaData.columnNullable)
        }
        return md
    }

    private val emptyResultSet: ResultSet get() = Mockito.mock(ResultSet::class.java)

    @ParameterizedTest(name = "{1} is rejected")
    // CanonicalTypeTest.unsupported() owns the full table; these two only prove that
    // RowMapper wraps the rejection with the step and the column.
    @CsvSource(
        "-104, INTERVALDS",
        "2002, MY_OBJECT_TYPE",
    )
    fun `an unsupported column type names the step, the column and the type`(sqlType: Int, typeName: String) {
        val md = metaData(Triple("ODD_COLUMN", sqlType, typeName))

        val thrown = assertThrows<Throwable> { RowMapper(md, "load-wip").map(emptyResultSet) }

        assertFalse(thrown is ClassCastException) { "expected a diagnostic error, was $thrown" }
        val message = thrown.message!!.lowercase()
        assertTrue(listOf("load-wip", "odd_column", typeName.lowercase()).all { it in message }) {
            "the message must name the step, the column and the type; was: $message"
        }
    }

    @Test
    fun `the error names the offending column, not the first one`() {
        val md = metaData(
            Triple("LOT_ID", Types.BIGINT, "BIGINT"),
            Triple("LOT_CODE", Types.VARCHAR, "VARCHAR2"),
            Triple("GEO_SHAPE", Types.STRUCT, "SDO_GEOMETRY"),
            Triple("QTY", Types.NUMERIC, "NUMBER"),
        )

        val message = assertThrows<Throwable> { RowMapper(md, "load-wip").map(emptyResultSet) }.message!!.lowercase()

        assertAll(
            { assertTrue("geo_shape" in message) { "the message did not name the offending column; was: $message" } },
            { assertFalse("lot_code" in message) { "the message named an innocent column; was: $message" } },
        )
    }

    @Test
    fun `a mapper over supported metadata still works after another mapper was rejected`() {
        val bad = metaData(Triple("GEO_SHAPE", Types.STRUCT, "SDO_GEOMETRY"))
        assertThrows<Throwable> { RowMapper(bad, "load-wip").map(emptyResultSet) }

        val read = Duck.read("select 1 as lot_id, 'L1' as lot_code", "load-wip")

        assertAll(
            { assertEquals(listOf("lot_id", "lot_code"), read.columns.map { it.name }) },
            { assertEquals("L1", read.row.string("lot_code")) },
        )
    }

    @Test
    fun `column metadata is read once, not once per row`() {
        Duck.withResultSet("select * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(lot_id, lot_code)") { rs ->
            val md = Mockito.spy(rs.metaData)
            val mapper = RowMapper(md, "load-wip")
            assertEquals(2, mapper.columns.size) { "columns were ${mapper.columns.map { it.name }}" }

            Mockito.clearInvocations(md)
            var rows = 0
            while (rs.next()) {
                mapper.map(rs)
                rows++
            }

            assertEquals(3, rows)
            Mockito.verifyNoMoreInteractions(md)
        }
    }

    @Test
    fun `nullability comes from the driver, and an unknown nullability is treated as nullable`() {
        val md = Mockito.mock(ResultSetMetaData::class.java)
        Mockito.`when`(md.columnCount).thenReturn(3)
        listOf(
            Triple(1, "REQUIRED_COL", ResultSetMetaData.columnNoNulls),
            Triple(2, "OPTIONAL_COL", ResultSetMetaData.columnNullable),
            Triple(3, "UNKNOWN_COL", ResultSetMetaData.columnNullableUnknown),
        ).forEach { (i, name, nullability) ->
            Mockito.`when`(md.getColumnLabel(i)).thenReturn(name)
            Mockito.`when`(md.getColumnName(i)).thenReturn(name)
            Mockito.`when`(md.getColumnType(i)).thenReturn(Types.VARCHAR)
            Mockito.`when`(md.getColumnTypeName(i)).thenReturn("VARCHAR2")
            Mockito.`when`(md.isNullable(i)).thenReturn(nullability)
        }

        val columns = RowMapper(md, "load-wip").columns

        assertEquals(
            listOf(
                "required_col" to false,
                "optional_col" to true,
                // Unknown must fall to nullable: 4.6 picks the null-accepting DuckDB type from
                // this flag, and guessing NOT NULL would produce a column that cannot take a null.
                "unknown_col" to true,
            ),
            columns.map { it.name to it.nullable },
        )
    }
}
