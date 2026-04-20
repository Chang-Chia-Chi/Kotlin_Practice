# Phase 1: DuckDB ParquetFormatter Implementation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `NoOpParquetFormatter` with a real `DuckDbParquetFormatter` that converts `List<DispatchDecision>` to Parquet bytes using DuckDB, plus comprehensive unit tests.

**Architecture:** Fresh in-memory DuckDB connection per invocation (avoids memory leak). Data inserted via JDBC PreparedStatement, exported via `COPY ... TO ... (FORMAT PARQUET)`. Temp file cleaned up in finally block.

**Tech Stack:** DuckDB JDBC (`org.duckdb:duckdb_jdbc`), Kotlin, JUnit 5, kotlinx-coroutines-test

---

### Task 1: Add DuckDB Maven Dependency

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: Add duckdb_jdbc dependency to pom.xml**

Add after the `<!-- AWS S3 -->` section (around line 186):

```xml
        <!-- DuckDB (in-memory Parquet conversion) -->
        <dependency>
            <groupId>org.duckdb</groupId>
            <artifactId>duckdb_jdbc</artifactId>
            <version>1.2.1</version>
        </dependency>
```

- [ ] **Step 2: Verify dependency resolves**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn dependency:resolve -pl WorkFlow -Dinclude=org.duckdb:duckdb_jdbc`
Expected: `BUILD SUCCESS`

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "build: add DuckDB JDBC dependency for Parquet conversion"
```

---

### Task 2: Write DuckDbParquetFormatter Unit Tests

**Files:**
- Create: `src/test/kotlin/dispatch/adapter/storage/DuckDbParquetFormatterTest.kt`

- [ ] **Step 1: Write the test class with all test cases**

```kotlin
package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.DispatchDecision
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.math.BigDecimal
import java.nio.file.Path
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
```

- [ ] **Step 2: Run the test to verify it fails (class not found)**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DuckDbParquetFormatterTest" -Dsurefire.failIfNoSpecifiedTests=false`
Expected: Compilation failure — `DuckDbParquetFormatter` class does not exist yet.

- [ ] **Step 3: Commit the test**

```bash
git add src/test/kotlin/dispatch/adapter/storage/DuckDbParquetFormatterTest.kt
git commit -m "test: add DuckDbParquetFormatter unit tests (red)"
```

---

### Task 3: Implement DuckDbParquetFormatter

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/storage/DuckDbParquetFormatter.kt`
- Modify: `src/main/kotlin/dispatch/adapter/storage/NoOpParquetFormatter.kt` (delete)

- [ ] **Step 1: Create the DuckDbParquetFormatter implementation**

```kotlin
package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.sql.DriverManager
import java.sql.Types

@ApplicationScoped
class DuckDbParquetFormatter : ParquetFormatter {

    private val log = LoggerFactory.getLogger(DuckDbParquetFormatter::class.java)

    override fun format(decisions: List<DispatchDecision>): ByteArray {
        val tmpFile = Files.createTempFile("dispatch-parquet-", ".parquet")
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        """
                        CREATE TABLE dispatch_decision (
                            dispatch_order INTEGER NOT NULL,
                            product_id VARCHAR NOT NULL,
                            source_bom_id VARCHAR NOT NULL,
                            qty INTEGER NOT NULL,
                            target_site_id VARCHAR NOT NULL,
                            target_bom_id VARCHAR,
                            site_gap DECIMAL(18,2) NOT NULL,
                            bom_gap DECIMAL(18,2)
                        )
                        """.trimIndent(),
                    )
                }

                conn.prepareStatement(
                    """
                    INSERT INTO dispatch_decision VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """.trimIndent(),
                ).use { ps ->
                    for (d in decisions) {
                        ps.setInt(1, d.dispatchOrder)
                        ps.setString(2, d.productId)
                        ps.setString(3, d.sourceBomId)
                        ps.setInt(4, d.qty)
                        ps.setString(5, d.targetSiteId)
                        if (d.targetBomId != null) ps.setString(6, d.targetBomId) else ps.setNull(6, Types.VARCHAR)
                        ps.setBigDecimal(7, d.siteGap)
                        if (d.bomGap != null) ps.setBigDecimal(8, d.bomGap) else ps.setNull(8, Types.DECIMAL)
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }

                val parquetPath = tmpFile.toString().replace("\\", "/")
                conn.createStatement().use { stmt ->
                    stmt.execute("COPY dispatch_decision TO '$parquetPath' (FORMAT PARQUET)")
                }
            }

            log.debug("Formatted {} decisions to Parquet ({} bytes)", decisions.size, Files.size(tmpFile))
            return Files.readAllBytes(tmpFile)
        } finally {
            Files.deleteIfExists(tmpFile)
        }
    }
}
```

- [ ] **Step 2: Delete NoOpParquetFormatter**

Delete `src/main/kotlin/dispatch/adapter/storage/NoOpParquetFormatter.kt` entirely.

- [ ] **Step 3: Run the tests to verify they pass**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow -Dtest="DuckDbParquetFormatterTest"`
Expected: All 4 tests PASS.

- [ ] **Step 4: Run the full test suite to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS. Any test that referenced `NoOpParquetFormatter` directly will fail — fix by updating to `DuckDbParquetFormatter` or removing the reference.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/storage/DuckDbParquetFormatter.kt
git rm src/main/kotlin/dispatch/adapter/storage/NoOpParquetFormatter.kt
git add src/test/kotlin/dispatch/adapter/storage/DuckDbParquetFormatterTest.kt
git commit -m "feat: replace NoOpParquetFormatter with DuckDbParquetFormatter"
```
