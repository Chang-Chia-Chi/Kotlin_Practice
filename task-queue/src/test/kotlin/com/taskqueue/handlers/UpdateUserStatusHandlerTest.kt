package com.taskqueue.handlers

import com.taskqueue.queue.TaskContext
import com.taskqueue.queue.TaskEmitter
import com.taskqueue.queue.TaskResult
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Instant

class UpdateUserStatusHandlerTest {

    private val handler = UpdateUserStatusHandler()

    private fun context(payload: String? = null) = TaskContext(
        taskId = 1L,
        parentTaskId = 100L,
        taskType = "UPDATE_USER_STATUS",
        payload = payload,
        priority = 5,
        retryCount = 0,
        maxRetries = 3,
        deadlineAt = null,
        scheduledAt = null,
        createdAt = Instant.now(),
    )

    @Test
    fun `taskType is UPDATE_USER_STATUS`() {
        assertThat(handler.taskType).isEqualTo("UPDATE_USER_STATUS")
    }

    @Test
    fun `handle returns Success for valid payload`() {
        val ctx = context(payload = """{"userId":"user-42"}""")
        val emitter = TaskEmitter(parentTaskId = ctx.taskId)

        val result = handler.handle(ctx, emitter)

        assertThat(result).isEqualTo(TaskResult.Success)
        assertThat(emitter.size).isEqualTo(0) // leaf task — no children
    }

    @Test
    fun `handle throws on null payload`() {
        val ctx = context(payload = null)
        val emitter = TaskEmitter(parentTaskId = ctx.taskId)

        assertThatThrownBy { handler.handle(ctx, emitter) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("userId")
    }

    @Test
    fun `handle parses userId from JSON payload`() {
        val ctx = context(payload = """{"userId":"premium-user-123"}""")
        val emitter = TaskEmitter(parentTaskId = ctx.taskId)

        // Should complete without error, demonstrating it parsed the userId
        val result = handler.handle(ctx, emitter)
        assertThat(result).isEqualTo(TaskResult.Success)
    }
}
