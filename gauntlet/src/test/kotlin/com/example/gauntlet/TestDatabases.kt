package com.example.gauntlet

import com.example.gauntlet.infrastructure.DuckDbJdbiProvider
import com.example.gauntlet.infrastructure.SqliteJdbiProvider
import org.jdbi.v3.core.Jdbi
import java.nio.file.Files
import java.nio.file.Path

/**
 * 測試一律使用真實資料庫。
 * SQLite 走暫存檔（WAL 不支援 :memory:），DuckDB 走純記憶體 + duplicate()。
 */
object TestDatabases {

    fun newSqliteDir(): Path = Files.createTempDirectory("gauntlet-sqlite")

    fun newSqlite(dir: Path, name: String = "test.db"): Jdbi {
        val jdbi = SqliteJdbiProvider.create(dir.resolve(name))
        SqliteJdbiProvider.migrate(jdbi)
        return jdbi
    }

    fun newDuckDb(): DuckDbJdbiProvider =
        DuckDbJdbiProvider.create().also { it.migrate() }

    fun deleteRecursively(dir: Path) {
        if (!Files.exists(dir)) return
        Files.walk(dir).use { stream ->
            stream.toList().sortedDescending().forEach { Files.deleteIfExists(it) }
        }
    }
}
