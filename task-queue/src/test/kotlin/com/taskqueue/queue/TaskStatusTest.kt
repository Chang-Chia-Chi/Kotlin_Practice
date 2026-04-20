package com.taskqueue.queue

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TaskStatusTest {

    @Test
    fun `TERMINAL set contains exactly DONE, CANCELLED, DISCARDED, EXPIRED`() {
        assertThat(TaskStatus.TERMINAL).containsExactlyInAnyOrder(
            TaskStatus.DONE,
            TaskStatus.CANCELLED,
            TaskStatus.DISCARDED,
            TaskStatus.EXPIRED,
        )
    }

    @Test
    fun `all 8 states exist`() {
        assertThat(TaskStatus.entries).hasSize(8)
        assertThat(TaskStatus.entries.map { it.name }).containsExactlyInAnyOrder(
            "PENDING", "SCHEDULED", "PROCESSING", "RETRYABLE",
            "DONE", "CANCELLED", "DISCARDED", "EXPIRED",
        )
    }

    @Test
    fun `PROCESSING is not terminal`() {
        assertThat(TaskStatus.TERMINAL).doesNotContain(TaskStatus.PROCESSING)
    }
}
