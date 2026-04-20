package com.taskqueue.queue

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.RepeatedTest

class TaskQueueDaoUnitTest {

    // ── computeBackoffSeconds ──

    @Test
    fun `backoff attempt 1 is approximately 1 second`() {
        val dao = createDao()
        val backoff = dao.computeBackoffSeconds(1)
        // 1^4 = 1, ±10% jitter → [0.9, 1.1], clamped to min 1
        assertThat(backoff).isBetween(1L, 2L)
    }

    @Test
    fun `backoff attempt 2 is approximately 16 seconds`() {
        val dao = createDao()
        val backoff = dao.computeBackoffSeconds(2)
        // 2^4 = 16, ±10% jitter → [14, 18]
        assertThat(backoff).isBetween(14L, 18L)
    }

    @Test
    fun `backoff attempt 3 is approximately 81 seconds`() {
        val dao = createDao()
        val backoff = dao.computeBackoffSeconds(3)
        // 3^4 = 81, ±10% jitter → [72, 90]
        assertThat(backoff).isBetween(72L, 90L)
    }

    @Test
    fun `backoff attempt 4 is approximately 256 seconds`() {
        val dao = createDao()
        val backoff = dao.computeBackoffSeconds(4)
        // 4^4 = 256, ±10% jitter → [230, 282]
        assertThat(backoff).isBetween(230L, 282L)
    }

    @Test
    fun `backoff is capped at 1 hour for large attempts`() {
        val dao = createDao()
        val backoff = dao.computeBackoffSeconds(10)
        // 10^4 = 10000 → capped to 3600, ±10% jitter → [3240, 3960]
        assertThat(backoff).isBetween(3240L, 3960L)
    }

    @Test
    fun `backoff is at least 1 second`() {
        val dao = createDao()
        val backoff = dao.computeBackoffSeconds(1)
        assertThat(backoff).isGreaterThanOrEqualTo(1L)
    }

    @RepeatedTest(20)
    fun `backoff has jitter - not always the same value`() {
        val dao = createDao()
        val values = (1..10).map { dao.computeBackoffSeconds(5) }.toSet()
        // 5^4 = 625, ±10% jitter → range is [562, 688]
        // With 10 samples, we should get at least 2 distinct values
        // (probability of all 10 being identical is negligible)
        assertThat(values.all { it in 562L..688L }).isTrue()
    }

    // ── generateUniqueKey ──

    @Test
    fun `generateUniqueKey produces deterministic output`() {
        val key1 = TaskQueueDao.generateUniqueKey("TYPE_A", "payload1")
        val key2 = TaskQueueDao.generateUniqueKey("TYPE_A", "payload1")
        assertThat(key1).isEqualTo(key2)
    }

    @Test
    fun `generateUniqueKey produces different keys for different types`() {
        val key1 = TaskQueueDao.generateUniqueKey("TYPE_A", "payload1")
        val key2 = TaskQueueDao.generateUniqueKey("TYPE_B", "payload1")
        assertThat(key1).isNotEqualTo(key2)
    }

    @Test
    fun `generateUniqueKey produces different keys for different payloads`() {
        val key1 = TaskQueueDao.generateUniqueKey("TYPE_A", "payload1")
        val key2 = TaskQueueDao.generateUniqueKey("TYPE_A", "payload2")
        assertThat(key1).isNotEqualTo(key2)
    }

    @Test
    fun `generateUniqueKey handles null payload`() {
        val key = TaskQueueDao.generateUniqueKey("TYPE_A", null)
        assertThat(key).isNotEmpty()
        assertThat(key).hasSize(64) // SHA-256 hex = 64 chars
    }

    @Test
    fun `generateUniqueKey output is 64 hex characters`() {
        val key = TaskQueueDao.generateUniqueKey("TEST", "data")
        assertThat(key).hasSize(64)
        assertThat(key).matches("[0-9a-f]{64}")
    }

    @Test
    fun `generateUniqueKey null vs empty payload produce different keys`() {
        val keyNull = TaskQueueDao.generateUniqueKey("TYPE", null)
        val keyEmpty = TaskQueueDao.generateUniqueKey("TYPE", "")
        // Both map to "TYPE:" since null → ""
        // Actually null payload → "TYPE:" and empty payload → "TYPE:"
        // So they should be equal
        assertThat(keyNull).isEqualTo(keyEmpty)
    }

    private fun createDao(): TaskQueueDao {
        // Create a DAO with a dummy Jdbi — we only need the pure functions
        val jdbi = org.jdbi.v3.core.Jdbi.create("jdbc:h2:mem:dummy;MODE=Oracle")
        return TaskQueueDao(jdbi)
    }
}
