package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskCompleted
import com.mapreduce.event.TaskDeadLettered
import com.mapreduce.observability.AutoscalingMetrics
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.HandlerPipelineBuilder
import com.mapreduce.queue.pipeline.TaskExecutionContext
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import jakarta.enterprise.event.Event
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

class TaskDispatcherTest {

    private lateinit var config: FrameworkConfig
    private lateinit var workerConfig: FrameworkConfig.WorkerConfig
    private lateinit var taskRepository: TaskRepository
    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var pipelineBuilder: HandlerPipelineBuilder
    private lateinit var circuitBreaker: PodCircuitBreaker
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var autoscalingMetrics: AutoscalingMetrics
    private lateinit var deadLetterEvent: Event<TaskDeadLettered>
    private lateinit var taskCompletedEvent: Event<TaskCompleted>
    private lateinit var dispatcher: TaskDispatcher

    @BeforeEach
    fun setUp() {
        config = mock<FrameworkConfig>()
        workerConfig = mock<FrameworkConfig.WorkerConfig>()
        whenever(config.worker()).thenReturn(workerConfig)
        whenever(workerConfig.id()).thenReturn("worker-1")
        whenever(workerConfig.queues()).thenReturn(listOf("default", "mr"))

        taskRepository = mock<TaskRepository>()
        handlerRegistry = mock<HandlerRegistry>()
        pipelineBuilder = mock<HandlerPipelineBuilder>()
        circuitBreaker = mock<PodCircuitBreaker>()
        shutdownCoordinator = mock<ShutdownCoordinator>()
        autoscalingMetrics = mock<AutoscalingMetrics>()

        deadLetterEvent = mock<Event<TaskDeadLettered>>()
        taskCompletedEvent = mock<Event<TaskCompleted>>()

        dispatcher = TaskDispatcher(
            config, taskRepository, handlerRegistry, pipelineBuilder,
            circuitBreaker, shutdownCoordinator, autoscalingMetrics,
            deadLetterEvent, taskCompletedEvent,
        )
    }

    // ── claimTask ─────────────────────────────────────────────────

    @Test
    fun `claimTask delegates to repository with worker id and queues`() {
        dispatcher.claimTask()

        verify(taskRepository).claim("worker-1", listOf("default", "mr"))
    }

    // ── execute: no handler ───────────────────────────────────────

    @Test
    fun `execute with no handler dead-letters task`() = runTest {
        val task = testTask()
        whenever(handlerRegistry.resolve("test.handler")).thenReturn(null)

        dispatcher.execute(task)

        verify(taskRepository).deadLetter(eq("task-1"), any())
        verify(deadLetterEvent).fireAsync(any())
    }

    // ── execute: Success ──────────────────────────────────────────

    @Test
    fun `execute Success completes task and records CB success`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Success("done"))

        dispatcher.execute(task)

        verify(taskRepository).complete("task-1", "gen-1")
        verify(circuitBreaker).recordSuccess()
        verify(circuitBreaker, never()).recordFailure()
        verify(taskCompletedEvent).fireAsync(any())
    }

    // ── execute: Failure ──────────────────────────────────────────

    @Test
    fun `execute Failure fails task and records CB failure`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Failure("boom"))
        whenever(taskRepository.fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1"))
        verify(circuitBreaker).recordFailure()
        verify(circuitBreaker, never()).recordSuccess()
    }

    @Test
    fun `execute Failure that dead-letters fires dead-letter event`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Failure("permanent"))
        whenever(taskRepository.fail(eq("task-1"), eq("permanent"), anyOrNull(), eq("gen-1")))
            .thenReturn(true) // retries exhausted

        dispatcher.execute(task)

        verify(deadLetterEvent).fireAsync(any())
        verify(circuitBreaker).recordFailure()
    }

    // ── execute: DeadLetter ───────────────────────────────────────

    @Test
    fun `execute DeadLetter dead-letters task and records CB failure`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.DeadLetter("poison pill"))

        dispatcher.execute(task)

        verify(taskRepository).deadLetter("task-1", "poison pill")
        verify(circuitBreaker).recordFailure()
        verify(deadLetterEvent).fireAsync(any())
    }

    // ── execute: Retry(consumeRetry=true) ─────────────────────────

    @Test
    fun `execute Retry with consumeRetry true fails task and records CB failure`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(5), reason = "transient", consumeRetry = true)
        stubPipeline(task, retry)
        whenever(taskRepository.fail(eq("task-1"), eq("transient"), eq(Duration.ofSeconds(5)), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail("task-1", "transient", Duration.ofSeconds(5), "gen-1")
        verify(circuitBreaker).recordFailure()
    }

    @Test
    fun `execute Retry consumeRetry true that exhausts retries fires dead-letter event`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(reason = "transient", consumeRetry = true)
        stubPipeline(task, retry)
        whenever(taskRepository.fail(eq("task-1"), eq("transient"), anyOrNull(), eq("gen-1")))
            .thenReturn(true) // dead-lettered

        dispatcher.execute(task)

        verify(deadLetterEvent).fireAsync(any())
    }

    // ── execute: Retry(consumeRetry=false) ────────────────────────

    @Test
    fun `execute Retry with consumeRetry false requeues without CB recording`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(2), reason = "cb-requeue", consumeRetry = false)
        stubPipeline(task, retry)

        dispatcher.execute(task)

        verify(taskRepository).requeue("task-1", Duration.ofSeconds(2), "gen-1")
        verify(circuitBreaker, never()).recordFailure()
        verify(circuitBreaker, never()).recordSuccess()
    }

    // ── execute: pipeline exception ───────────────────────────────

    @Test
    fun `pipeline exception caught and treated as Failure`() = runTest {
        val task = testTask()
        stubPipelineThrows(task, RuntimeException("middleware bug"))
        whenever(taskRepository.fail(eq("task-1"), any(), anyOrNull(), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail(eq("task-1"), any(), anyOrNull(), eq("gen-1"))
        verify(circuitBreaker).recordFailure()
    }

    // ── helpers ───────────────────────────────────────────────────

    private fun testTask(
        taskId: String = "task-1",
        handler: String = "test.handler",
        queue: String = "default",
    ) = Task(
        taskId = taskId,
        handler = handler,
        queue = queue,
        payload = "{}",
        groupId = "group-1",
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

        val chain: suspend (TaskExecutionContext) -> TaskResult = { result }
        whenever(pipelineBuilder.chainFor(handler)).thenReturn(chain)
    }

    private fun stubPipelineThrows(task: Task, exception: Exception) {
        val handler = mock<com.mapreduce.queue.spi.TaskHandler>()
        whenever(handler.handlerName).thenReturn(task.handler)
        whenever(handlerRegistry.resolve(task.handler)).thenReturn(handler)

        val chain: suspend (TaskExecutionContext) -> TaskResult = { throw exception }
        whenever(pipelineBuilder.chainFor(handler)).thenReturn(chain)
    }
}
