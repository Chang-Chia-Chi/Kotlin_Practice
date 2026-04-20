package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.DispatchDecision
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DuckDbParquetFormatterTest {

    private val formatter = DuckDbParquetFormatter()

    private fun sampleDecisions(): List<DispatchDecision> = listOf(
        DispatchDecision(
            dispatchOrder = 1,
            productId = "PROD-001",
            sourceBomId = "BOM-A",
            qty = 5,
            targetSiteId = "SITE-X",
            targetBomId = "TBOM-1",
            siteGap = BigDecimal("10.50"),
            bomGap = BigDecimal("3.25"),
        ),
        DispatchDecision(
            dispatchOrder = 2,
            productId = "PROD-002",
            sourceBomId = "BOM-B",
            qty = 3,
            targetSiteId = "SITE-Y",
            targetBomId = null,
            siteGap = BigDecimal("-2.00"),
            bomGap = null,
        ),
        DispatchDecision(
            dispatchOrder = 3,
            productId = "PROD-003",
            sourceBomId = "BOM-A",
            qty = 10,
            targetSiteId = "SITE-X",
            targetBomId = "TBOM-2",
            siteGap = BigDecimal("0.00"),
            bomGap = BigDecimal("7.80"),
        ),
    )

    @Test
    fun `format produces valid parquet with correct row count and values`() = runTest {
        val decisions = sampleDecisions()

        val parquetBytes = formatter.format(decisions)

        assertTrue(parquetBytes.isNotEmpty(), "Parquet output should not be empty")

        val rows = readParquetViaDuckDb(parquetBytes)
        assertEquals(3, rows.size)
        assertEquals("PROD-001", rows[0]["product_id"])
        assertEquals(5, rows[0]["qty"])
        assertEquals("SITE-X", rows[0]["target_site_id"])
        assertEquals("TBOM-1", rows[0]["target_bom_id"])
        assertEquals(BigDecimal("10.50"), rows[0]["site_gap"])
        assertEquals(BigDecimal("3.25"), rows[0]["bom_gap"])

        // Nullable fields
        assertEquals(null, rows[1]["target_bom_id"])
        assertEquals(null, rows[1]["bom_gap"])
    }

    @Test
    fun `format with empty decision list produces valid parquet with zero rows`() = runTest {
        val parquetBytes = formatter.format(emptyList())

        assertTrue(parquetBytes.isNotEmpty(), "Parquet should have schema even with 0 rows")

        val rows = readParquetViaDuckDb(parquetBytes)
        assertEquals(0, rows.size)
    }

    @Test
    fun `format with fresh connection per invocation has no state leakage`() = runTest {
        val decisions1 = listOf(
            DispatchDecision(1, "P1", "B1", 1, "S1", null, BigDecimal.ONE, null),
        )
        val decisions2 = listOf(
            DispatchDecision(1, "P2", "B2", 2, "S2", "T2", BigDecimal.TEN, BigDecimal("5.0")),
            DispatchDecision(2, "P3", "B3", 3, "S3", null, BigDecimal.ZERO, null),
        )

        val bytes1 = formatter.format(decisions1)
        val bytes2 = formatter.format(decisions2)

        val rows1 = readParquetViaDuckDb(bytes1)
        val rows2 = readParquetViaDuckDb(bytes2)

        assertEquals(1, rows1.size)
        assertEquals("P1", rows1[0]["product_id"])

        assertEquals(2, rows2.size)
        assertEquals("P2", rows2[0]["product_id"])
        assertEquals("P3", rows2[1]["product_id"])
    }

    @Test
    fun `format produces correct column schema`() = runTest {
        val parquetBytes = formatter.format(sampleDecisions())

        val columns = readParquetColumns(parquetBytes)

        assertEquals(
            setOf(
                "dispatch_order", "product_id", "source_bom_id", "qty",
                "target_site_id", "target_bom_id", "site_gap", "bom_gap",
            ),
            columns.keys,
        )
        assertEquals("INTEGER", columns["dispatch_order"])
        assertEquals("VARCHAR", columns["product_id"])
        assertEquals("VARCHAR", columns["source_bom_id"])
        assertEquals("INTEGER", columns["qty"])
        assertEquals("VARCHAR", columns["target_site_id"])
        assertEquals("VARCHAR", columns["target_bom_id"])
        assertEquals("DECIMAL", columns["site_gap"])
        assertEquals("DECIMAL", columns["bom_gap"])
    }

    /**
     * Reads parquet bytes back via a fresh DuckDB connection to verify content.
     * Writes bytes to a temp file, then queries with DuckDB's read_parquet.
     */
    private fun readParquetViaDuckDb(parquetBytes: ByteArray): List<Map<String, Any?>> {
        val tmpFile = kotlin.io.path.createTempFile(prefix = "test-parquet-", suffix = ".parquet")
        try {
            tmpFile.toFile().writeBytes(parquetBytes)
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    val rs = stmt.executeQuery(
                        "SELECT * FROM read_parquet('${tmpFile.toString().replace("\\", "/")}')",
                    )
                    val meta = rs.metaData
                    val rows = mutableListOf<Map<String, Any?>>()
                    while (rs.next()) {
                        val row = mutableMapOf<String, Any?>()
                        for (i in 1..meta.columnCount) {
                            val colName = meta.getColumnName(i)
                            val value = rs.getObject(i)
                            row[colName] = when (value) {
                                is java.math.BigDecimal -> value
                                is Number -> value.toInt()
                                else -> value
                            }
                        }
                        rows.add(row)
                    }
                    return rows
                }
            }
        } finally {
            tmpFile.toFile().delete()
        }
    }

    /**
     * Reads column names and types from parquet metadata.
     */
    private fun readParquetColumns(parquetBytes: ByteArray): Map<String, String> {
        val tmpFile = kotlin.io.path.createTempFile(prefix = "test-schema-", suffix = ".parquet")
        try {
            tmpFile.toFile().writeBytes(parquetBytes)
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    val rs = stmt.executeQuery(
                        "DESCRIBE SELECT * FROM read_parquet('${tmpFile.toString().replace("\\", "/")}')",
                    )
                    val columns = mutableMapOf<String, String>()
                    while (rs.next()) {
                        val name = rs.getString("column_name")
                        val type = rs.getString("column_type")
                            .replace(Regex("\\(.*\\)"), "") // strip precision e.g. DECIMAL(18,3) → DECIMAL
                        columns[name] = type
                    }
                    return columns
                }
            }
        } finally {
            tmpFile.toFile().delete()
        }
    }
}
