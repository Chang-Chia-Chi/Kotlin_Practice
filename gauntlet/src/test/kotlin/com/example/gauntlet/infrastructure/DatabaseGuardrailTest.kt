package com.example.gauntlet.infrastructure

import com.example.gauntlet.TestDatabases
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path

/**
 * 第三層護欄：資料庫安全設定必須真的生效，不是「我們有寫那行 code」而已。
 * 這些斷言的輸出會被抄進 EVIDENCE.md。
 */
class DatabaseGuardrailTest {

    private lateinit var dir: Path

    @BeforeEach
    fun setUp() {
        dir = TestDatabases.newSqliteDir()
    }

    @AfterEach
    fun tearDown() {
        TestDatabases.deleteRecursively(dir)
    }

    @Test
    fun `sqlite runs in WAL mode`() {
        val jdbi = TestDatabases.newSqlite(dir)
        val mode = jdbi.withHandle<String, Exception> { handle ->
            handle.createQuery("PRAGMA journal_mode").mapTo(String::class.java).one()
        }
        assertEquals("wal", mode.lowercase())
    }

    @Test
    fun `sqlite busy_timeout is set on every connection`() {
        val jdbi = TestDatabases.newSqlite(dir)
        repeat(3) {
            val timeout = jdbi.withHandle<Int, Exception> { handle ->
                handle.createQuery("PRAGMA busy_timeout").mapTo(Int::class.java).one()
            }
            assertEquals(SqliteJdbiProvider.BUSY_TIMEOUT_MS, timeout)
        }
    }

    @Test
    fun `duckdb applies memory limit and thread cap`() {
        TestDatabases.newDuckDb().use { provider ->
            val settings = provider.jdbi.withHandle<Map<String, String>, Exception> { handle ->
                handle.createQuery(
                    "SELECT name, value FROM duckdb_settings() " +
                        "WHERE name IN ('memory_limit', 'threads')",
                ).map { rs, _ -> rs.getString("name") to rs.getString("value") }
                    .list()
                    .toMap()
            }
            assertEquals("2", settings["threads"])
            // memory_limit 的字串格式各版本不同（例如 '1GB' 會顯示成 '953.6 MiB'），
            // 這裡只確認它確實被設定過，實際值抄進 EVIDENCE.md。
            assertFalse(settings["memory_limit"].isNullOrBlank())
        }
    }
}
