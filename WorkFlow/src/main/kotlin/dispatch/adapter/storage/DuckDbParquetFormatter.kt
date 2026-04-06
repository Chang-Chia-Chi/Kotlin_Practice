package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.DispatchDecision
import com.workflow.dispatch.usecase.port.outbound.storage.ParquetFormatter
import jakarta.enterprise.context.ApplicationScoped
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.sql.DriverManager
import java.sql.Types

/**
 * Converts a list of [DispatchDecision] into Parquet bytes using an in-memory DuckDB instance.
 *
 * Each invocation creates a fresh DuckDB connection (no shared state, no memory leak).
 * Data is inserted via JDBC PreparedStatement, then exported with `COPY ... TO ... (FORMAT PARQUET)`.
 * The temporary Parquet file is cleaned up in a finally block.
 */
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
