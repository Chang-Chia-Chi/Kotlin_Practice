package com.taskqueue.handlers

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class RefreshUsersProducerJobTest {

    private val job = RefreshUsersProducerJob()

    @Test
    fun `produces a single REFRESH_PREMIUM_USERS task`() {
        val tasks = job.produce()

        assertThat(tasks).hasSize(1)
        assertThat(tasks[0].taskType).isEqualTo("REFRESH_PREMIUM_USERS")
        assertThat(tasks[0].priority).isEqualTo(3)
    }

    @Test
    fun `produced task has a deadline in the future`() {
        val before = Instant.now()
        val tasks = job.produce()
        val after = Instant.now()

        assertThat(tasks[0].deadlineAt).isNotNull()
        assertThat(tasks[0].deadlineAt).isAfter(before)
        // Deadline should be roughly 6 hours from now
        assertThat(tasks[0].deadlineAt).isBefore(after.plusSeconds(6 * 3600 + 10))
    }

    @Test
    fun `produced task has a uniqueKey for deduplication`() {
        val tasks = job.produce()

        assertThat(tasks[0].uniqueKey).isNotNull()
        assertThat(tasks[0].uniqueKey).isNotEmpty()
    }

    @Test
    fun `uniqueKey is deterministic for same taskType and payload`() {
        val tasks1 = job.produce()
        val tasks2 = job.produce()

        assertThat(tasks1[0].uniqueKey).isEqualTo(tasks2[0].uniqueKey)
    }

    @Test
    fun `job name is set`() {
        assertThat(job.name).isEqualTo("refresh-premium-users")
    }
}
