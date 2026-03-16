package com.mapreduce.leader

import com.mapreduce.TestH2Factory
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Concrete subclass that exposes protected methods for testing.
 */
private class TestRepository(jdbi: Jdbi) : FencedRepository(jdbi) {
    fun testRequireEpoch(): Long = requireEpoch()
    fun testOptionalEpoch(): Long? = optionalEpoch()
    fun testAssertFenced(rowsAffected: Int, epoch: Long) = assertFenced(rowsAffected, epoch)

    fun testFencedUpdate(sql: String, bind: (org.jdbi.v3.core.statement.Update, Long) -> Unit): Int =
        fencedUpdate(sql, bind)
}

class FencedRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var repo: TestRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        repo = TestRepository(jdbi)
    }

    @AfterEach
    fun cleanup() {
        FencingTokenHolder.clear()
    }

    @Test
    fun `requireEpoch() throws when no FencingTokenHolder set`() {
        assertThrows<IllegalStateException> {
            repo.testRequireEpoch()
        }
    }

    @Test
    fun `requireEpoch() returns epoch when set`() {
        FencingTokenHolder.set(42L)
        assertEquals(42L, repo.testRequireEpoch())
    }

    @Test
    fun `optionalEpoch() returns null when not set`() {
        assertNull(repo.testOptionalEpoch())
    }

    @Test
    fun `assertFenced succeeds with rows greater than 0`() {
        // Should not throw
        repo.testAssertFenced(1, 10L)
        repo.testAssertFenced(5, 10L)
    }

    @Test
    fun `assertFenced throws StaleEpochException with rows = 0`() {
        val ex = assertThrows<StaleEpochException> {
            repo.testAssertFenced(0, 99L)
        }
        assertEquals(99L, ex.epoch)
    }

    @Test
    fun `fencedUpdate executes SQL and returns row count`() {
        // Insert a row to update
        seedTask("task-1", epoch = 5L)

        FencingTokenHolder.set(10L)

        val rows = repo.testFencedUpdate(
            "UPDATE task SET last_epoch = :epoch WHERE task_id = :id AND last_epoch <= :epoch"
        ) { update, epoch ->
            update.bind("epoch", epoch)
            update.bind("id", "task-1")
        }

        assertEquals(1, rows)
    }

    @Test
    fun `fencedUpdate throws StaleEpochException when 0 rows affected`() {
        // Insert a row with a higher epoch so the fence rejects the update
        seedTask("task-1", epoch = 100L)

        FencingTokenHolder.set(5L)

        val ex = assertThrows<StaleEpochException> {
            repo.testFencedUpdate(
                "UPDATE task SET last_epoch = :epoch WHERE task_id = :id AND last_epoch <= :epoch"
            ) { update, epoch ->
                update.bind("epoch", epoch)
                update.bind("id", "task-1")
            }
        }
        assertEquals(5L, ex.epoch)
    }

    @Test
    fun `fencedUpdate within withToken works end-to-end`() {
        seedTask("task-1", epoch = 0L)

        val rows = FencingTokenHolder.withToken(10L) {
            repo.testFencedUpdate(
                "UPDATE task SET last_epoch = :epoch WHERE task_id = :id AND last_epoch <= :epoch"
            ) { update, epoch ->
                update.bind("epoch", epoch)
                update.bind("id", "task-1")
            }
        }

        assertEquals(1, rows)
        assertNull(FencingTokenHolder.get())

        // Verify the epoch was actually written
        val storedEpoch = jdbi.withHandle<Long, Exception> { h ->
            h.createQuery("SELECT last_epoch FROM task WHERE task_id = :id")
                .bind("id", "task-1")
                .mapTo(Long::class.java)
                .one()
        }
        assertEquals(10L, storedEpoch)
    }

    private fun seedTask(taskId: String, epoch: Long) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, status, last_epoch)
                VALUES (:id, 'test-handler', 'default', 'PENDING', :epoch)
                """.trimIndent()
            )
                .bind("id", taskId)
                .bind("epoch", epoch)
                .execute()
        }
    }
}
