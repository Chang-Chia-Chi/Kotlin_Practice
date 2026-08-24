package com.example.gauntlet.infrastructure

import com.example.gauntlet.TestDatabases
import com.example.gauntlet.application.NewOrderCommand
import com.example.gauntlet.application.ProcessOrderUseCase
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * 敵意測試：專門打併發與極限值。
 * 這裡如果拿掉 WAL 或 busy_timeout，測試會以 SQLITE_BUSY 失敗。
 */
class HostileInputSpec : StringSpec({

    val day = LocalDate.of(2026, 8, 24)

    "四條執行緒同時寫入 SQLite，全部成功且不重不漏" {
        val dir = TestDatabases.newSqliteDir()
        try {
            val useCase = ProcessOrderUseCase(SqliteOrderRepository(TestDatabases.newSqlite(dir)))
            val threads = 4
            val perThread = 25
            val pool = Executors.newFixedThreadPool(threads)
            val tasks = (0 until threads).map { t ->
                Callable {
                    (0 until perThread).count { i ->
                        useCase.execute(
                            NewOrderCommand("T$t-$i", "CUST-$t", 100L + i, day),
                        ).isRight()
                    }
                }
            }
            val succeeded = pool.invokeAll(tasks).sumOf { it.get() }
            pool.shutdown()
            pool.awaitTermination(30, TimeUnit.SECONDS) shouldBe true

            succeeded shouldBe threads * perThread
        } finally {
            TestDatabases.deleteRecursively(dir)
        }
    }

    "同一個 id 被兩條執行緒搶著寫，只有一條會成功" {
        val dir = TestDatabases.newSqliteDir()
        try {
            val useCase = ProcessOrderUseCase(SqliteOrderRepository(TestDatabases.newSqlite(dir)))
            val pool = Executors.newFixedThreadPool(2)
            val tasks = (0 until 2).map {
                Callable { useCase.execute(NewOrderCommand("RACE-1", "CUST-X", 500L, day)).isRight() }
            }
            val wins = pool.invokeAll(tasks).count { it.get() }
            pool.shutdown()
            pool.awaitTermination(30, TimeUnit.SECONDS) shouldBe true

            // 應用層擋一次、SQLite PRIMARY KEY 擋一次，兩層都在。
            wins shouldBe 1
        } finally {
            TestDatabases.deleteRecursively(dir)
        }
    }

    "超長字串在 domain 就被擋下，不會變成 SQL 參數" {
        val dir = TestDatabases.newSqliteDir()
        try {
            val useCase = ProcessOrderUseCase(SqliteOrderRepository(TestDatabases.newSqlite(dir)))
            val huge = "X".repeat(1_000_000)
            useCase.execute(NewOrderCommand(huge, huge, Long.MAX_VALUE, day)).isLeft() shouldBe true
        } finally {
            TestDatabases.deleteRecursively(dir)
        }
    }

    "SQL injection 樣式的 id 只會被當成字面值" {
        val dir = TestDatabases.newSqliteDir()
        try {
            val jdbi = TestDatabases.newSqlite(dir)
            val useCase = ProcessOrderUseCase(SqliteOrderRepository(jdbi))
            val nasty = "x'; DROP TABLE orders; --"
            useCase.execute(NewOrderCommand(nasty, "CUST-1", 100L, day)).isRight() shouldBe true

            val stillThere = jdbi.withHandle<Int, Exception> { handle ->
                handle.createQuery("SELECT count(*) FROM orders").mapTo(Int::class.java).one()
            }
            stillThere shouldBe 1
        } finally {
            TestDatabases.deleteRecursively(dir)
        }
    }
})
