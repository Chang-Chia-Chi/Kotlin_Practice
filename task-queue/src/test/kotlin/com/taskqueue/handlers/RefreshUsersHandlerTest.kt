package com.taskqueue.handlers

import com.taskqueue.queue.TaskContext
import com.taskqueue.queue.TaskEmitter
import com.taskqueue.queue.TaskResult
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Test
import java.time.Instant

class RefreshUsersHandlerTest {

    private val jdbi = mockk<Jdbi>()
    private val handler = RefreshUsersHandler(jdbi)

    private fun context() = TaskContext(
        taskId = 1L,
        parentTaskId = null,
        taskType = "REFRESH_PREMIUM_USERS",
        payload = null,
        priority = 3,
        retryCount = 0,
        maxRetries = 3,
        deadlineAt = null,
        scheduledAt = null,
        createdAt = Instant.now(),
    )

    @Test
    fun `taskType is REFRESH_PREMIUM_USERS`() {
        assertThat(handler.taskType).isEqualTo("REFRESH_PREMIUM_USERS")
    }

    @Test
    fun `handle emits one child per user and returns Success`() {
        val users = listOf("user-1", "user-2", "user-3")
        every { jdbi.withHandle<List<String>, Exception>(any()) } returns users

        val emitter = TaskEmitter(parentTaskId = 1L)
        val result = handler.handle(context(), emitter)

        assertThat(result).isEqualTo(TaskResult.Success)
        val children = emitter.drain()
        assertThat(children).hasSize(3)
        assertThat(children.map { it.taskType }).containsOnly("UPDATE_USER_STATUS")
        assertThat(children.map { it.payload }).allMatch { it!!.contains("userId") }
    }

    @Test
    fun `handle emits no children when no premium users`() {
        every { jdbi.withHandle<List<String>, Exception>(any()) } returns emptyList()

        val emitter = TaskEmitter(parentTaskId = 1L)
        val result = handler.handle(context(), emitter)

        assertThat(result).isEqualTo(TaskResult.Success)
        assertThat(emitter.size).isEqualTo(0)
    }
}
