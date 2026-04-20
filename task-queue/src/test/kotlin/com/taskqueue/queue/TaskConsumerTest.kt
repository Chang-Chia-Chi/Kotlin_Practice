package com.taskqueue.queue

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class TaskConsumerTest {

    private lateinit var dao: TaskQueueDao
    private lateinit var registry: TaskHandlerRegistry
    private lateinit var consumer: TaskConsumer

    @BeforeEach
    fun setUp() {
        dao = mockk(relaxed = true)
        registry = mockk()
        consumer = TaskConsumer(
            dao = dao,
            registry = registry,
            batchSize = 10,
            concurrency = 5,
            heartbeatIntervalSeconds = 3600, // effectively disabled for unit tests
        )
    }

    private fun taskContext(
        taskId: Long = 1L,
        taskType: String = "TEST",
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = null,
        payload: String? = null,
    ) = TaskContext(
        taskId = taskId,
        parentTaskId = null,
        taskType = taskType,
        payload = payload,
        priority = 5,
        retryCount = retryCount,
        maxRetries = maxRetries,
        deadlineAt = deadlineAt,
        scheduledAt = null,
        createdAt = Instant.now(),
    )

    // ── poll basic behavior ──

    @Test
    fun `poll does nothing when no tasks are claimed`() {
        every { dao.claimBatch(any()) } returns emptyList()

        consumer.poll()

        verify(exactly = 1) { dao.claimBatch(10) }
        verify(exactly = 0) { registry.getHandler(any()) }
    }

    @Test
    fun `poll handles claim exception gracefully`() {
        every { dao.claimBatch(any()) } throws RuntimeException("DB down")

        consumer.poll() // should not throw

        verify(exactly = 0) { registry.getHandler(any()) }
    }

    // ── Success path ──

    @Test
    fun `poll processes tasks and marks them DONE on Success`() {
        val task = taskContext(taskId = 42L, taskType = "DO_WORK")
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("DO_WORK") } returns handler
        every { handler.handle(any(), any()) } returns TaskResult.Success
        every { dao.completeWithChildren(42L, any()) } returns true

        consumer.poll()

        verify { handler.handle(task, any()) }
        verify { dao.completeWithChildren(42L, match { it.isEmpty() }) }
    }

    @Test
    fun `poll inserts children on Success with emitted tasks`() {
        val task = taskContext(taskId = 10L, taskType = "FAN_OUT")
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("FAN_OUT") } returns handler
        every { handler.handle(any(), any()) } answers {
            val emitter = secondArg<TaskEmitter>()
            emitter.emit(taskType = "CHILD", payload = "data")
            TaskResult.Success
        }
        every { dao.completeWithChildren(10L, any()) } returns true

        consumer.poll()

        verify { dao.completeWithChildren(10L, match { it.size == 1 && it[0].taskType == "CHILD" }) }
    }

    // ── Snooze path ──

    @Test
    fun `poll marks task SNOOZED on Snooze result`() {
        val task = taskContext(taskId = 20L, taskType = "DEFERRED")
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("DEFERRED") } returns handler
        every { handler.handle(any(), any()) } returns TaskResult.Snooze(Duration.ofMinutes(30))
        every { dao.markSnoozed(20L, 1800L) } returns true

        consumer.poll()

        verify { dao.markSnoozed(20L, 1800L) }
        verify(exactly = 0) { dao.completeWithChildren(any(), any()) }
    }

    // ── Cancel path ──

    @Test
    fun `poll marks task CANCELLED on Cancel result`() {
        val task = taskContext(taskId = 30L, taskType = "CANCEL_ME")
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("CANCEL_ME") } returns handler
        every { handler.handle(any(), any()) } returns TaskResult.Cancel("no longer needed")
        every { dao.markCancelled(30L, "no longer needed") } returns true

        consumer.poll()

        verify { dao.markCancelled(30L, "no longer needed") }
    }

    // ── Expired task ──

    @Test
    fun `poll marks expired task without invoking handler`() {
        val task = taskContext(
            taskId = 40L,
            deadlineAt = Instant.now().minus(1, ChronoUnit.HOURS),
        )

        every { dao.claimBatch(any()) } returns listOf(task)
        every { dao.markExpired(40L) } returns true

        consumer.poll()

        verify { dao.markExpired(40L) }
        verify(exactly = 0) { registry.getHandler(any()) }
    }

    // ── Unknown handler ──

    @Test
    fun `poll discards task with unknown handler`() {
        val task = taskContext(taskId = 50L, taskType = "UNKNOWN")

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("UNKNOWN") } returns null
        every { dao.markDiscarded(50L, any(), any()) } returns true

        consumer.poll()

        verify { dao.markDiscarded(50L, match { it!!.contains("No handler") }, 0) }
        verify(exactly = 0) { dao.completeWithChildren(any(), any()) }
    }

    // ── Retry path ──

    @Test
    fun `handler exception triggers retry when retries remain`() {
        val task = taskContext(taskId = 60L, taskType = "FLAKY", retryCount = 0, maxRetries = 3)
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("FLAKY") } returns handler
        every { handler.handle(any(), any()) } throws RuntimeException("transient error")
        every { dao.markRetryable(60L, any(), 0) } returns true

        consumer.poll()

        verify { dao.markRetryable(60L, match { it!!.contains("transient error") }, 0) }
    }

    @Test
    fun `handler exception triggers discard when retries exhausted`() {
        val task = taskContext(taskId = 70L, taskType = "DOOMED", retryCount = 2, maxRetries = 3)
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("DOOMED") } returns handler
        every { handler.handle(any(), any()) } throws RuntimeException("fatal error")
        every { dao.markDiscarded(70L, any(), 2) } returns true

        consumer.poll()

        verify { dao.markDiscarded(70L, match { it!!.contains("fatal error") }, 2) }
    }

    // ── Post-handler persistence failure ──

    @Test
    fun `post-handler persistence failure triggers retry`() {
        val task = taskContext(taskId = 80L, taskType = "INSERT_FAIL", retryCount = 0, maxRetries = 3)
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("INSERT_FAIL") } returns handler
        every { handler.handle(any(), any()) } answers {
            val emitter = secondArg<TaskEmitter>()
            emitter.emit(taskType = "CHILD", payload = "data")
            TaskResult.Success
        }
        every { dao.completeWithChildren(any(), any()) } throws RuntimeException("DB write failed")
        every { dao.markRetryable(80L, any(), 0) } returns true

        consumer.poll()

        verify { dao.markRetryable(80L, any(), 0) }
    }

    // ── Concurrent processing ──

    @Test
    fun `poll processes multiple tasks concurrently`() {
        val tasks = (1L..5L).map { taskContext(taskId = it, taskType = "CONCURRENT") }
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns tasks
        every { registry.getHandler("CONCURRENT") } returns handler
        every { handler.handle(any(), any()) } returns TaskResult.Success
        every { dao.completeWithChildren(any(), any()) } returns true

        consumer.poll()

        for (id in 1L..5L) {
            verify { dao.completeWithChildren(id, any()) }
        }
    }

    // ── Error message format ──

    @Test
    fun `error message includes exception class name and message`() {
        val task = taskContext(taskId = 90L, taskType = "ERR", retryCount = 0, maxRetries = 3)
        val handler = mockk<TaskHandler>()

        every { dao.claimBatch(any()) } returns listOf(task)
        every { registry.getHandler("ERR") } returns handler
        every { handler.handle(any(), any()) } throws IllegalArgumentException("bad input")
        every { dao.markRetryable(90L, any(), 0) } returns true

        consumer.poll()

        verify {
            dao.markRetryable(
                90L,
                match { it!!.contains("IllegalArgumentException") && it.contains("bad input") },
                0,
            )
        }
    }
}
