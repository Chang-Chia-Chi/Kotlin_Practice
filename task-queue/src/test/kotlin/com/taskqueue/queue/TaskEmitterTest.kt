package com.taskqueue.queue

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class TaskEmitterTest {

    @Test
    fun `emit adds a single task with defaults`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        emitter.emit(taskType = "CHILD_A")

        val tasks = emitter.drain()
        assertThat(tasks).hasSize(1)
        assertThat(tasks[0].taskType).isEqualTo("CHILD_A")
        assertThat(tasks[0].payload).isNull()
        assertThat(tasks[0].priority).isEqualTo(5)
        assertThat(tasks[0].deadlineAt).isNull()
        assertThat(tasks[0].uniqueKey).isNull()
    }

    @Test
    fun `emit with all parameters`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        val deadline = Instant.now().plus(1, ChronoUnit.HOURS)
        emitter.emit(
            taskType = "CHILD_B",
            payload = """{"key":"val"}""",
            priority = 2,
            deadlineAt = deadline,
        )

        val tasks = emitter.drain()
        assertThat(tasks).hasSize(1)
        assertThat(tasks[0].taskType).isEqualTo("CHILD_B")
        assertThat(tasks[0].payload).isEqualTo("""{"key":"val"}""")
        assertThat(tasks[0].priority).isEqualTo(2)
        assertThat(tasks[0].deadlineAt).isEqualTo(deadline)
    }

    @Test
    fun `emitAll adds multiple tasks from payloads`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        emitter.emitAll(
            taskType = "BATCH",
            payloads = listOf("a", "b", "c"),
            priority = 3,
        )

        val tasks = emitter.drain()
        assertThat(tasks).hasSize(3)
        assertThat(tasks.map { it.payload }).containsExactly("a", "b", "c")
        assertThat(tasks.map { it.taskType }).containsOnly("BATCH")
        assertThat(tasks.map { it.priority }).containsOnly(3)
    }

    @Test
    fun `drain returns accumulated tasks and clears buffer`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        emitter.emit(taskType = "A")
        emitter.emit(taskType = "B")

        val first = emitter.drain()
        assertThat(first).hasSize(2)

        val second = emitter.drain()
        assertThat(second).isEmpty()
    }

    @Test
    fun `size reflects accumulated count`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        assertThat(emitter.size).isEqualTo(0)

        emitter.emit(taskType = "X")
        assertThat(emitter.size).isEqualTo(1)

        emitter.emitAll(taskType = "Y", payloads = listOf("1", "2"))
        assertThat(emitter.size).isEqualTo(3)
    }

    @Test
    fun `size is zero after drain`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        emitter.emit(taskType = "X")
        emitter.drain()
        assertThat(emitter.size).isEqualTo(0)
    }

    @Test
    fun `emitAll with empty payloads list adds nothing`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        emitter.emitAll(taskType = "EMPTY", payloads = emptyList())
        assertThat(emitter.size).isEqualTo(0)
        assertThat(emitter.drain()).isEmpty()
    }

    @Test
    fun `emit and emitAll can be mixed`() {
        val emitter = TaskEmitter(parentTaskId = 1L)
        emitter.emit(taskType = "SINGLE")
        emitter.emitAll(taskType = "BATCH", payloads = listOf("a", "b"))
        emitter.emit(taskType = "SINGLE2")

        val tasks = emitter.drain()
        assertThat(tasks).hasSize(4)
        assertThat(tasks.map { it.taskType }).containsExactly("SINGLE", "BATCH", "BATCH", "SINGLE2")
    }
}
