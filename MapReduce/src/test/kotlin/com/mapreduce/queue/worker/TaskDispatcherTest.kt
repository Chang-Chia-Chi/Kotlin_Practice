package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.ErrorClassifierMiddleware
import com.mapreduce.queue.pipeline.MetricsMiddleware
import com.mapreduce.queue.pipeline.TaskExecutionContext
import com.mapreduce.queue.pipeline.TimeoutMiddleware
import com.mapreduce.queue.pipeline.TracingMiddleware
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
    private lateinit var metricsMiddleware: MetricsMiddleware
    private lateinit var tracingMiddleware: TracingMiddleware
    private lateinit var timeoutMiddleware: TimeoutMiddleware
    private lateinit var errorClassifierMiddleware: ErrorClassifierMiddleware
    private lateinit var circuitBreaker: PodCircuitBreaker
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
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
        circuitBreaker = mock<PodCircuitBreaker>()
        shutdownCoordinator = mock<ShutdownCoordinator>()
        meterRegistry = SimpleMeterRegistry()

        // Middleware mocks that pass through to next
        metricsMiddleware = mock<MetricsMiddleware>()
        tracingMiddleware = mock<TracingMiddleware>()
        timeoutMiddleware = mock<TimeoutMiddleware>()
        errorClassifierMiddleware = mock<ErrorClassifierMiddleware>()

        dispatcher = TaskDispatcher(
            config, taskRepository, handlerRegistry,
            metricsMiddleware, tracingMiddleware, timeoutMiddleware, errorClassifierMiddleware,
            circuitBreaker, shutdownCoordinator, meterRegistry,
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
    }

    // ── execute: Success ──────────────────────────────────────────

    @Test
    fun `execute Success completes task and records CB success`() = runTest {
        val task = testTask()
        stubPassthroughMiddleware(task, TaskResult.Success("done"))

        dispatcher.execute(task)

        verify(taskRepository).complete("task-1", "gen-1")
        verify(circuitBreaker).recordSuccess()
        verify(circuitBreaker, never()).recordFailure()
    }

    // ── execute: Failure ──────────────────────────────────────────

    @Test
    fun `execute Failure fails task and records CB failure`() = runTest {
        val task = testTask()
        stubPassthroughMiddleware(task, TaskResult.Failure("boom"))
        whenever(taskRepository.fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1"))
        verify(circuitBreaker).recordFailure()
        verify(circuitBreaker, never()).recordSuccess()
    }

    // ── execute: DeadLetter ───────────────────────────────────────

    @Test
    fun `execute DeadLetter dead-letters task and records CB failure`() = runTest {
        val task = testTask()
        stubPassthroughMiddleware(task, TaskResult.DeadLetter("poison pill"))

        dispatcher.execute(task)

        verify(taskRepository).deadLetter("task-1", "poison pill")
        verify(circuitBreaker).recordFailure()
    }

    // ── execute: Retry(consumeRetry=true) ─────────────────────────

    @Test
    fun `execute Retry with consumeRetry true fails task and records CB failure`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(5), reason = "transient", consumeRetry = true)
        stubPassthroughMiddleware(task, retry)
        whenever(taskRepository.fail(eq("task-1"), eq("transient"), eq(Duration.ofSeconds(5)), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail("task-1", "transient", Duration.ofSeconds(5), "gen-1")
        verify(circuitBreaker).recordFailure()
    }

    // ── execute: Retry(consumeRetry=false) ────────────────────────

    @Test
    fun `execute Retry with consumeRetry false requeues without CB recording`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(2), reason = "cb-requeue", consumeRetry = false)
        stubPassthroughMiddleware(task, retry)

        dispatcher.execute(task)

        verify(taskRepository).requeue("task-1", Duration.ofSeconds(2), "gen-1")
        verify(circuitBreaker, never()).recordFailure()
        verify(circuitBreaker, never()).recordSuccess()
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

    /**
     * Stubs all 4 middleware mocks to pass through to the innermost handler,
     * and stubs the handler registry to return a handler that produces [result].
     */
    private fun stubPassthroughMiddleware(task: Task, result: TaskResult) {
        val handler = mock<com.mapreduce.queue.spi.TaskHandler>()
        whenever(handler.handlerName).thenReturn(task.handler)
        whenever(handlerRegistry.resolve(task.handler)).thenReturn(handler)

        // Each middleware just calls next
        kotlinx.coroutines.runBlocking {
            whenever(metricsMiddleware.invoke(any(), any())).thenAnswer { inv ->
                val ctx = inv.getArgument<TaskExecutionContext>(0)
                val next = inv.getArgument<suspend (TaskExecutionContext) -> TaskResult>(1)
                kotlinx.coroutines.runBlocking { next(ctx) }
            }
            whenever(tracingMiddleware.invoke(any(), any())).thenAnswer { inv ->
                val ctx = inv.getArgument<TaskExecutionContext>(0)
                val next = inv.getArgument<suspend (TaskExecutionContext) -> TaskResult>(1)
                kotlinx.coroutines.runBlocking { next(ctx) }
            }
            whenever(timeoutMiddleware.invoke(any(), any())).thenAnswer { inv ->
                val ctx = inv.getArgument<TaskExecutionContext>(0)
                val next = inv.getArgument<suspend (TaskExecutionContext) -> TaskResult>(1)
                kotlinx.coroutines.runBlocking { next(ctx) }
            }
            whenever(errorClassifierMiddleware.invoke(any(), any())).thenAnswer { inv ->
                val ctx = inv.getArgument<TaskExecutionContext>(0)
                val next = inv.getArgument<suspend (TaskExecutionContext) -> TaskResult>(1)
                kotlinx.coroutines.runBlocking { next(ctx) }
            }
        }

        kotlinx.coroutines.runBlocking {
            whenever(handler.handle(any())).thenReturn(result)
        }
    }
}
