package com.taskqueue.queue

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class TaskContextTest {

    private fun context(
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = null,
    ) = TaskContext(
        taskId = 1L,
        parentTaskId = null,
        taskType = "TEST",
        payload = null,
        priority = 5,
        retryCount = retryCount,
        maxRetries = maxRetries,
        deadlineAt = deadlineAt,
        scheduledAt = null,
        createdAt = Instant.now(),
    )

    // ── isExpired ──

    @Test
    fun `isExpired returns true when deadline is in the past`() {
        val ctx = context(deadlineAt = Instant.now().minus(1, ChronoUnit.HOURS))
        assertThat(ctx.isExpired()).isTrue()
    }

    @Test
    fun `isExpired returns false when deadline is in the future`() {
        val ctx = context(deadlineAt = Instant.now().plus(1, ChronoUnit.HOURS))
        assertThat(ctx.isExpired()).isFalse()
    }

    @Test
    fun `isExpired returns false when no deadline`() {
        val ctx = context(deadlineAt = null)
        assertThat(ctx.isExpired()).isFalse()
    }

    // ── hasRetriesLeft ──

    @Test
    fun `hasRetriesLeft returns true when retries remaining`() {
        val ctx = context(retryCount = 0, maxRetries = 3)
        assertThat(ctx.hasRetriesLeft()).isTrue()
    }

    @Test
    fun `hasRetriesLeft returns false when retries exhausted`() {
        val ctx = context(retryCount = 2, maxRetries = 3)
        assertThat(ctx.hasRetriesLeft()).isFalse()
    }

    @Test
    fun `hasRetriesLeft boundary - one retry left`() {
        val ctx = context(retryCount = 1, maxRetries = 3)
        assertThat(ctx.hasRetriesLeft()).isTrue()
    }

    @Test
    fun `hasRetriesLeft with zero maxRetries`() {
        val ctx = context(retryCount = 0, maxRetries = 0)
        assertThat(ctx.hasRetriesLeft()).isFalse()
    }

    // ── data class properties ──

    @Test
    fun `scheduledAt is stored correctly`() {
        val scheduledAt = Instant.now().plus(5, ChronoUnit.MINUTES)
        val ctx = context().copy(scheduledAt = scheduledAt)
        assertThat(ctx.scheduledAt).isEqualTo(scheduledAt)
    }

    @Test
    fun `parentTaskId can be null for root tasks`() {
        val ctx = context()
        assertThat(ctx.parentTaskId).isNull()
    }

    @Test
    fun `parentTaskId can be set for child tasks`() {
        val ctx = context().copy(parentTaskId = 42L)
        assertThat(ctx.parentTaskId).isEqualTo(42L)
    }
}
