package com.taskqueue.queue

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class TaskResultTest {

    @Test
    fun `Success is a singleton`() {
        val a = TaskResult.Success
        val b = TaskResult.Success
        assertThat(a).isSameAs(b)
    }

    @Test
    fun `Snooze holds duration`() {
        val duration = Duration.ofMinutes(30)
        val snooze = TaskResult.Snooze(duration)
        assertThat(snooze.duration).isEqualTo(duration)
    }

    @Test
    fun `Cancel holds reason`() {
        val cancel = TaskResult.Cancel("user requested")
        assertThat(cancel.reason).isEqualTo("user requested")
    }

    @Test
    fun `all result types are distinct subtypes of TaskResult`() {
        val results: List<TaskResult> = listOf(
            TaskResult.Success,
            TaskResult.Snooze(Duration.ofSeconds(10)),
            TaskResult.Cancel("test"),
        )
        assertThat(results).hasSize(3)
        assertThat(results[0]).isInstanceOf(TaskResult.Success::class.java)
        assertThat(results[1]).isInstanceOf(TaskResult.Snooze::class.java)
        assertThat(results[2]).isInstanceOf(TaskResult.Cancel::class.java)
    }

    @Test
    fun `when expression is exhaustive on TaskResult`() {
        val results = listOf(
            TaskResult.Success,
            TaskResult.Snooze(Duration.ofSeconds(1)),
            TaskResult.Cancel("x"),
        )
        for (result in results) {
            val label = when (result) {
                is TaskResult.Success -> "success"
                is TaskResult.Snooze -> "snooze"
                is TaskResult.Cancel -> "cancel"
            }
            assertThat(label).isNotEmpty()
        }
    }
}
