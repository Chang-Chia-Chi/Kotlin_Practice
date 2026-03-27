package com.workflow.queryexporter

import com.workflow.queryexporter.core.QueryExecutor
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class QueryExecutorTest {

    private val executor = QueryExecutor()

    private fun mockDataSource(
        columns: List<String>,
        rows: List<List<Any?>>,
    ): DataSource {
        val meta = mock<ResultSetMetaData>()
        whenever(meta.columnCount).thenReturn(columns.size)
        columns.forEachIndexed { i, col ->
            whenever(meta.getColumnLabel(i + 1)).thenReturn(col)
        }

        val rs = mock<ResultSet>()
        whenever(rs.metaData).thenReturn(meta)
        var rowIndex = -1
        whenever(rs.next()).thenAnswer {
            rowIndex++
            rowIndex < rows.size
        }
        for ((ri, row) in rows.withIndex()) {
            for ((ci, value) in row.withIndex()) {
                whenever(rs.getObject(ci + 1)).thenAnswer { invocation ->
                    if (rowIndex == ri) value else null
                }
            }
        }
        // For single-row or multi-row: use answer based on current rowIndex
        if (rows.isNotEmpty()) {
            for (ci in columns.indices) {
                whenever(rs.getObject(ci + 1)).thenAnswer {
                    if (rowIndex in rows.indices) rows[rowIndex][ci] else null
                }
            }
        }

        val stmt = mock<PreparedStatement>()
        whenever(stmt.executeQuery()).thenReturn(rs)
        val conn = mock<Connection>()
        whenever(conn.prepareStatement("SELECT 1")).thenReturn(stmt)
        val ds = mock<DataSource>()
        whenever(ds.connection).thenReturn(conn)
        return ds
    }

    @Nested
    inner class ResultMapping {

        @Test
        fun `single row result is mapped correctly`() {
            val ds = mockDataSource(
                columns = listOf("NAME", "VALUE"),
                rows = listOf(listOf("test", 42)),
            )

            val result = executor.execute(ds, "SELECT 1")

            assertEquals(1, result.size)
            assertEquals("test", result[0]["name"])
            assertEquals(42, result[0]["value"])
        }

        @Test
        fun `multiple columns mapped with lowercase labels`() {
            val ds = mockDataSource(
                columns = listOf("UPPER_CASE", "MixedCase"),
                rows = listOf(listOf("a", "b")),
            )

            val result = executor.execute(ds, "SELECT 1")

            assertEquals(1, result.size)
            assertTrue(result[0].containsKey("upper_case"))
            assertTrue(result[0].containsKey("mixedcase"))
        }

        @Test
        fun `multiple rows are all returned`() {
            val ds = mockDataSource(
                columns = listOf("ID"),
                rows = listOf(listOf(1), listOf(2), listOf(3)),
            )

            val result = executor.execute(ds, "SELECT 1")

            assertEquals(3, result.size)
            assertEquals(1, result[0]["id"])
            assertEquals(2, result[1]["id"])
            assertEquals(3, result[2]["id"])
        }

        @Test
        fun `null values are preserved in result map`() {
            val ds = mockDataSource(
                columns = listOf("COL1", "COL2"),
                rows = listOf(listOf("value", null)),
            )

            val result = executor.execute(ds, "SELECT 1")

            assertEquals(1, result.size)
            assertEquals("value", result[0]["col1"])
            assertNull(result[0]["col2"])
        }

        @Test
        fun `empty result set returns empty list`() {
            val ds = mockDataSource(
                columns = listOf("ID"),
                rows = emptyList(),
            )

            val result = executor.execute(ds, "SELECT 1")

            assertTrue(result.isEmpty())
        }
    }
}
