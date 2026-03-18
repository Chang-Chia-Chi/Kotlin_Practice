package com.mapreduce.queue.worker

import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.TaskPipeline
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.GroupTaskResolution
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

class TaskDispatcherTest {

    private lateinit var taskRepository: TaskRepository
    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var pipeline: TaskPipeline
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var dispatcher: TaskDispatcher

    @BeforeEach
    fun setUp() {
        taskRepository = mock<TaskRepository>()
        taskGroupRepository = mock<TaskGroupRepository>()
        handlerRegistry = mock<HandlerRegistry>()
        pipeline = mock<TaskPipeline>()
        shutdownCoordinator = mock<ShutdownCoordinator>()

        dispatcher = TaskDispatcher(
            taskRepository, taskGroupRepository, handlerRegistry,
            pipeline, shutdownCoordinator,
        )
    }

    // ── execute: no handler ───────────────────────────────────────

    @Test
    fun `execute with no handler dead-letters task`() = runTest {
        val task = testTask()
        whenever(handlerRegistry.resolve("test.handler")).thenReturn(null)

        dispatcher.execute(task)

        verify(taskRepository).deadLetter(eq("task-1"), any())
    }

    // ── execute: Success ──────────────────────────────────────────

    @Test
    fun `execute Success completes task via group path`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Success("done"))
        whenever(taskGroupRepository.resolveGroupTask(any(), any(), anyOrNull(), any(), anyOrNull(), anyOrNull()))
            .thenReturn(GroupTaskResolution(updated = true, barrierMet = false))

        dispatcher.execute(task)

        verify(taskGroupRepository).resolveGroupTask(eq("task-1"), eq("group-1"), eq("gen-1"), eq(false), anyOrNull(), anyOrNull())
    }

    @Test
    fun `execute Success without groupId completes via taskRepository`() = runTest {
        val task = testTask(groupId = null)
        stubPipeline(task, TaskResult.Success("done"))

        dispatcher.execute(task)

        verify(taskRepository).complete("task-1", "gen-1")
    }

    // ── execute: Failure ──────────────────────────────────────────

    @Test
    fun `execute Failure fails task`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Failure("boom"))
        whenever(taskRepository.fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1"))
    }

    // ── execute: DeadLetter ───────────────────────────────────────

    @Test
    fun `execute DeadLetter dead-letters task`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.DeadLetter("poison pill"))

        dispatcher.execute(task)

        verify(taskRepository).deadLetter("task-1", "poison pill")
    }

    // ── execute: Retry(consumeRetry=true) ─────────────────────────

    @Test
    fun `execute Retry with consumeRetry true fails task`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(5), reason = "transient", consumeRetry = true)
        stubPipeline(task, retry)
        whenever(taskRepository.fail(eq("task-1"), eq("transient"), eq(Duration.ofSeconds(5)), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail("task-1", "transient", Duration.ofSeconds(5), "gen-1")
    }

    // ── execute: Retry(consumeRetry=false) ────────────────────────

    @Test
    fun `execute Retry with consumeRetry false requeues`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(2), reason = "cb-requeue", consumeRetry = false)
        stubPipeline(task, retry)

        dispatcher.execute(task)

        verify(taskRepository).requeue("task-1", Duration.ofSeconds(2), "gen-1")
    }

    // ── helpers ───────────────────────────────────────────────────

    private fun testTask(
        taskId: String = "task-1",
        handler: String = "test.handler",
        queue: String = "default",
        groupId: String? = "group-1",
    ) = Task(
        taskId = taskId,
        handler = handler,
        queue = queue,
        payload = "{}",
        groupId = groupId,
        metadata = null,
        retryCount = 0,
        maxRetries = 3,
        claimedAt = Instant.now(),
        executionGeneration = "gen-1",
        createdAt = Instant.now(),
    )

    private fun stubPipeline(task: Task, result: TaskResult) {
        val handler = mock<com.mapreduce.queue.spi.TaskHandler>()
        whenever(handler.handlerName).thenReturn(task.handler)
        whenever(handlerRegistry.resolve(task.handler)).thenReturn(handler)
        kotlinx.coroutines.runBlocking {
            whenever(pipeline.execute(any(), any())).thenReturn(result)
        }
    }
}
