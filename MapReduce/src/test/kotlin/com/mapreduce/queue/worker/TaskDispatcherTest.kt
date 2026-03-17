package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.Middleware
import com.mapreduce.queue.pipeline.TaskExecutionContext
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.GroupTaskResolution
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.Tracer
import jakarta.enterprise.inject.Instance
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
    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var circuitBreaker: PodCircuitBreaker
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var tracer: Tracer
    private lateinit var dispatcher: TaskDispatcher

    /** A passthrough middleware that just calls next. */
    private class PassthroughMiddleware(override val order: Int) : Middleware {
        override suspend fun invoke(
            context: TaskExecutionContext,
            next: suspend (TaskExecutionContext) -> TaskResult,
        ): TaskResult = next(context)
    }

    @BeforeEach
    fun setUp() {
        config = mock<FrameworkConfig>()
        workerConfig = mock<FrameworkConfig.WorkerConfig>()
        whenever(config.worker()).thenReturn(workerConfig)
        whenever(workerConfig.id()).thenReturn("worker-1")
        whenever(workerConfig.queues()).thenReturn(listOf("default", "mr"))

        taskRepository = mock<TaskRepository>()
        taskGroupRepository = mock<TaskGroupRepository>()
        handlerRegistry = mock<HandlerRegistry>()
        circuitBreaker = mock<PodCircuitBreaker>()
        shutdownCoordinator = mock<ShutdownCoordinator>()
        meterRegistry = SimpleMeterRegistry()

        // Tracer → no-op span chain
        tracer = mock<Tracer>()
        val spanBuilder = mock<SpanBuilder>()
        val span = mock<Span>()
        whenever(tracer.spanBuilder(any())).thenReturn(spanBuilder)
        whenever(spanBuilder.setAttribute(any<String>(), any<String>())).thenReturn(spanBuilder)
        whenever(spanBuilder.setAttribute(any<String>(), any<Long>())).thenReturn(spanBuilder)
        whenever(spanBuilder.startSpan()).thenReturn(span)

        val middlewareInstance = mock<Instance<Middleware>>()
        val middlewares: MutableList<Middleware> = mutableListOf(
            PassthroughMiddleware(40),
            PassthroughMiddleware(50),
        )
        whenever(middlewareInstance.iterator()).thenReturn(middlewares.iterator())

        dispatcher = TaskDispatcher(
            config, taskRepository, taskGroupRepository, handlerRegistry,
            middlewareInstance,
            circuitBreaker, shutdownCoordinator, meterRegistry, tracer,
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
    fun `execute Success completes task via group path and records CB success`() = runTest {
        val task = testTask()
        stubHandler(task, TaskResult.Success("done"))
        whenever(taskGroupRepository.resolveGroupTask(any(), any(), anyOrNull(), any(), anyOrNull(), anyOrNull()))
            .thenReturn(GroupTaskResolution(updated = true, barrierMet = false))

        dispatcher.execute(task)

        verify(taskGroupRepository).resolveGroupTask(eq("task-1"), eq("group-1"), eq("gen-1"), eq(false), anyOrNull(), anyOrNull())
        verify(circuitBreaker).recordSuccess()
        verify(circuitBreaker, never()).recordFailure()
    }

    @Test
    fun `execute Success without groupId completes via taskRepository`() = runTest {
        val task = testTask(groupId = null)
        stubHandler(task, TaskResult.Success("done"))

        dispatcher.execute(task)

        verify(taskRepository).complete("task-1", "gen-1")
        verify(circuitBreaker).recordSuccess()
    }

    // ── execute: Failure ──────────────────────────────────────────

    @Test
    fun `execute Failure fails task and records CB failure`() = runTest {
        val task = testTask()
        stubHandler(task, TaskResult.Failure("boom"))
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
        stubHandler(task, TaskResult.DeadLetter("poison pill"))

        dispatcher.execute(task)

        verify(taskRepository).deadLetter("task-1", "poison pill")
        verify(circuitBreaker).recordFailure()
    }

    // ── execute: Retry(consumeRetry=true) ─────────────────────────

    @Test
    fun `execute Retry with consumeRetry true fails task and records CB failure`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(5), reason = "transient", consumeRetry = true)
        stubHandler(task, retry)
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
        stubHandler(task, retry)

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

    private fun stubHandler(task: Task, result: TaskResult) {
        val handler = mock<com.mapreduce.queue.spi.TaskHandler>()
        whenever(handler.handlerName).thenReturn(task.handler)
        whenever(handlerRegistry.resolve(task.handler)).thenReturn(handler)
        kotlinx.coroutines.runBlocking {
            whenever(handler.handle(any())).thenReturn(result)
        }
    }
}
