package com.example.gauntlet.infrastructure

import org.duckdb.DuckDBConnection
import org.jdbi.v3.core.ConnectionFactory
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.sqlobject.SqlObjectPlugin
import org.jdbi.v3.sqlobject.kotlin.KotlinSqlObjectPlugin
import java.sql.Connection
import java.sql.DriverManager

/**
 * DuckDB（OLAP）連線工廠。
 *
 * 兩個關鍵：
 *  1. memory_limit 與 threads 一定要設。DuckDB 的記憶體是 C++ native heap，
 *     JVM 的 -Xmx 管不到它，不設上限就等著被 OS OOM Killer 砍掉整個 pod。
 *  2. DuckDB 的 in-memory 資料庫是「一條連線一個 DB」。要多條連線共用同一份資料，
 *     必須留住 root connection 再 duplicate()，不能各自 DriverManager.getConnection()。
 */
class DuckDbJdbiProvider private constructor(
    private val root: DuckDBConnection,
    private val memoryLimit: String,
    private val threads: Int,
) : AutoCloseable {

    val jdbi: Jdbi = Jdbi.create(ConnectionFactory { newConnection() })
        .installPlugin(SqlObjectPlugin())
        .installPlugin(KotlinPlugin())
        .installPlugin(KotlinSqlObjectPlugin())

    private fun newConnection(): Connection =
        root.duplicate().also { applySettings(it) }

    private fun applySettings(connection: Connection) {
        connection.createStatement().use { statement ->
            statement.execute("SET memory_limit='$memoryLimit'")
            statement.execute("SET threads=$threads")
        }
    }

    fun migrate() {
        jdbi.useHandle<Exception> { handle ->
            handle.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_order_summary (
                    summary_date        DATE    PRIMARY KEY,
                    order_count         INTEGER NOT NULL,
                    total_amount_cents  BIGINT  NOT NULL,
                    max_amount_cents    BIGINT  NOT NULL,
                    avg_amount_cents    BIGINT  NOT NULL
                )
                """.trimIndent(),
            )
        }
    }

    override fun close() {
        root.close()
    }

    companion object {
        const val DEFAULT_MEMORY_LIMIT: String = "1GB"
        const val DEFAULT_THREADS: Int = 2

        /**
         * @param url "jdbc:duckdb:" 為純記憶體；"jdbc:duckdb:/path/to.db" 為檔案。
         */
        fun create(
            url: String = "jdbc:duckdb:",
            memoryLimit: String = DEFAULT_MEMORY_LIMIT,
            threads: Int = DEFAULT_THREADS,
        ): DuckDbJdbiProvider {
            val root = DriverManager.getConnection(url) as DuckDBConnection
            return DuckDbJdbiProvider(root, memoryLimit, threads)
        }
    }
}
