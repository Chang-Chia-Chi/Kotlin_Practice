package com.example.gauntlet.infrastructure

import org.jdbi.v3.core.ConnectionFactory
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.sqlobject.SqlObjectPlugin
import org.jdbi.v3.sqlobject.kotlin.KotlinSqlObjectPlugin
import org.sqlite.SQLiteDataSource
import java.nio.file.Path
import java.sql.Connection

/**
 * SQLite（OLTP）連線工廠。
 *
 * 每一條新連線都會先跑 PRAGMA，理由：
 *  - journal_mode=WAL 是資料庫層級的持久設定，但每條連線都跑一次最省事也最不會錯。
 *  - busy_timeout 是 per-connection 的，不跑就沒有，併發寫入直接 SQLITE_BUSY 炸掉。
 *
 * 注意：WAL 需要真實檔案，:memory: 不支援。測試也一律用暫存檔。
 */
object SqliteJdbiProvider {

    const val BUSY_TIMEOUT_MS: Int = 5_000

    fun create(dbFile: Path): Jdbi {
        val dataSource = SQLiteDataSource().apply {
            url = "jdbc:sqlite:${dbFile.toAbsolutePath()}"
        }
        val factory = ConnectionFactory {
            dataSource.connection.also { applyPragmas(it) }
        }
        return Jdbi.create(factory)
            .installPlugin(SqlObjectPlugin())
            .installPlugin(KotlinPlugin())
            .installPlugin(KotlinSqlObjectPlugin())
    }

    private fun applyPragmas(connection: Connection) {
        connection.createStatement().use { statement ->
            statement.execute("PRAGMA journal_mode=WAL")
            statement.execute("PRAGMA busy_timeout=$BUSY_TIMEOUT_MS")
            statement.execute("PRAGMA foreign_keys=ON")
            statement.execute("PRAGMA synchronous=NORMAL")
        }
    }

    fun migrate(jdbi: Jdbi) {
        jdbi.useHandle<Exception> { handle ->
            handle.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id            TEXT    PRIMARY KEY,
                    customer_id   TEXT    NOT NULL,
                    amount_cents  INTEGER NOT NULL,
                    order_date    TEXT    NOT NULL
                )
                """.trimIndent(),
            )
            handle.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)")
        }
    }
}
