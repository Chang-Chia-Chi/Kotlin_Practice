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
    fun `CLAIMABLE set contains only PENDING`() {
        assertThat(TaskStatus.CLAIMABLE).containsExactly(TaskStatus.PENDING)
    }

    @Test
    fun `PROMOTABLE set contains RETRYABLE and SCHEDULED`() {
        assertThat(TaskStatus.PROMOTABLE).containsExactlyInAnyOrder(
            TaskStatus.RETRYABLE,
            TaskStatus.SCHEDULED,
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
    fun `TERMINAL and CLAIMABLE are disjoint`() {
        assertThat(TaskStatus.TERMINAL.intersect(TaskStatus.CLAIMABLE)).isEmpty()
    }

    @Test
    fun `PROMOTABLE and TERMINAL are disjoint`() {
        assertThat(TaskStatus.PROMOTABLE.intersect(TaskStatus.TERMINAL)).isEmpty()
    }

    @Test
    fun `PROCESSING is neither terminal, claimable, nor promotable`() {
        assertThat(TaskStatus.TERMINAL).doesNotContain(TaskStatus.PROCESSING)
        assertThat(TaskStatus.CLAIMABLE).doesNotContain(TaskStatus.PROCESSING)
        assertThat(TaskStatus.PROMOTABLE).doesNotContain(TaskStatus.PROCESSING)
    }
}
