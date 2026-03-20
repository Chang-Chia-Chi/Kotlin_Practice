package com.mapreduce.queue.worker

import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.pipeline.TaskPipeline
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.StepFailResult
import com.mapreduce.queue.repository.StepTaskResolution
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.queue.repository.TaskRepository
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
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
    private lateinit var workflowStepRepository: WorkflowStepRepository
    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var pipeline: TaskPipeline
    private lateinit var dispatcher: TaskDispatcher

    @BeforeEach
    fun setUp() {
        taskRepository = mock<TaskRepository>()
        workflowStepRepository = mock<WorkflowStepRepository>()
        handlerRegistry = mock<HandlerRegistry>()
        pipeline = mock<TaskPipeline>()

        dispatcher = TaskDispatcher(
            taskRepository, workflowStepRepository, handlerRegistry, pipeline,
        )
    }

    // ── execute: no handler ───────────────────────────────────────

    @Test
    fun `execute with no handler dead-letters grouped task via group path`() = runTest {
        val task = testTask()
        whenever(handlerRegistry.resolve("test.handler")).thenReturn(null)
        whenever(workflowStepRepository.deadLetterStepTask(any(), any(), any(), anyOrNull()))
            .thenReturn(StepFailResult(taskUpdated = true, deadLettered = true, barrierMet = false))

        dispatcher.execute(task)

        verify(workflowStepRepository).deadLetterStepTask(eq("task-1"), eq("group-1"), any(), eq("gen-1"))
    }

    @Test
    fun `execute with no handler dead-letters non-grouped task via taskRepository`() = runTest {
        val task = testTask(stepId = null)
        whenever(handlerRegistry.resolve("test.handler")).thenReturn(null)
        whenever(taskRepository.deadLetter(any(), any(), anyOrNull())).thenReturn(true)

        dispatcher.execute(task)

        verify(taskRepository).deadLetter(eq("task-1"), any(), eq("gen-1"))
    }

    // ── execute: Success ──────────────────────────────────────────

    @Test
    fun `execute Success completes task via group path`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Success("done"))
        whenever(workflowStepRepository.resolveStepTask(any(), any(), anyOrNull(), any(), anyOrNull(), anyOrNull()))
            .thenReturn(StepTaskResolution(updated = true, barrierMet = false))

        dispatcher.execute(task)

        verify(workflowStepRepository).resolveStepTask(eq("task-1"), eq("group-1"), eq("gen-1"), eq(false), anyOrNull(), anyOrNull())
    }

    @Test
    fun `execute Success without stepId completes via taskRepository`() = runTest {
        val task = testTask(stepId = null)
        stubPipeline(task, TaskResult.Success("done"))

        dispatcher.execute(task)

        verify(taskRepository).complete("task-1", "gen-1")
    }

    // ── execute: Failure ──────────────────────────────────────────

    @Test
    fun `execute Failure with stepId uses failStepTask`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Failure("boom"))
        whenever(workflowStepRepository.failStepTask(any(), any(), any(), anyOrNull(), anyOrNull()))
            .thenReturn(StepFailResult(taskUpdated = true, deadLettered = false, barrierMet = false))

        dispatcher.execute(task)

        verify(workflowStepRepository).failStepTask(eq("task-1"), eq("group-1"), eq("boom"), anyOrNull(), eq("gen-1"))
    }

    @Test
    fun `execute Failure without stepId uses taskRepository fail`() = runTest {
        val task = testTask(stepId = null)
        stubPipeline(task, TaskResult.Failure("boom"))
        whenever(taskRepository.fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail(eq("task-1"), eq("boom"), anyOrNull(), eq("gen-1"))
    }

    // ── execute: DeadLetter ───────────────────────────────────────

    @Test
    fun `execute DeadLetter with stepId uses deadLetterStepTask`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.DeadLetter("poison pill"))
        whenever(workflowStepRepository.deadLetterStepTask(any(), any(), any(), anyOrNull()))
            .thenReturn(StepFailResult(taskUpdated = true, deadLettered = true, barrierMet = false))

        dispatcher.execute(task)

        verify(workflowStepRepository).deadLetterStepTask("task-1", "group-1", "poison pill", "gen-1")
    }

    @Test
    fun `execute DeadLetter without stepId uses taskRepository deadLetter`() = runTest {
        val task = testTask(stepId = null)
        stubPipeline(task, TaskResult.DeadLetter("poison pill"))
        whenever(taskRepository.deadLetter(any(), any(), anyOrNull())).thenReturn(true)

        dispatcher.execute(task)

        verify(taskRepository).deadLetter("task-1", "poison pill", "gen-1")
    }

    // ── execute: Retry(consumeRetry=true) ─────────────────────────

    @Test
    fun `execute Retry consumeRetry with stepId uses failStepTask`() = runTest {
        val task = testTask()
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(5), reason = "transient", consumeRetry = true)
        stubPipeline(task, retry)
        whenever(workflowStepRepository.failStepTask(any(), any(), any(), anyOrNull(), anyOrNull()))
            .thenReturn(StepFailResult(taskUpdated = true, deadLettered = false, barrierMet = false))

        dispatcher.execute(task)

        verify(workflowStepRepository).failStepTask("task-1", "group-1", "transient", Duration.ofSeconds(5), "gen-1")
    }

    @Test
    fun `execute Retry consumeRetry without stepId uses taskRepository fail`() = runTest {
        val task = testTask(stepId = null)
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

    // ── execute: pipeline exception ────────────────────────────────

    @Test
    fun `execute catches pipeline exception and routes as Failure`() = runTest {
        val task = testTask()
        val handler = mock<com.mapreduce.queue.spi.TaskHandler>()
        whenever(handler.handlerName).thenReturn(task.handler)
        whenever(handlerRegistry.resolve(task.handler)).thenReturn(handler)
        kotlinx.coroutines.runBlocking {
            whenever(pipeline.execute(any(), any()))
                .thenThrow(RuntimeException("pipeline exploded"))
        }
        whenever(workflowStepRepository.failStepTask(any(), any(), any(), anyOrNull(), anyOrNull()))
            .thenReturn(StepFailResult(taskUpdated = true, deadLettered = false, barrierMet = false))

        dispatcher.execute(task)

        // Should route to Failure path with exception class info
        verify(workflowStepRepository).failStepTask(
            eq("task-1"), eq("group-1"),
            org.mockito.kotlin.argThat { contains("RuntimeException") && contains("pipeline exploded") },
            anyOrNull(), eq("gen-1"),
        )
    }

    @Test
    fun `execute catches pipeline exception for non-grouped task`() = runTest {
        val task = testTask(stepId = null)
        val handler = mock<com.mapreduce.queue.spi.TaskHandler>()
        whenever(handler.handlerName).thenReturn(task.handler)
        whenever(handlerRegistry.resolve(task.handler)).thenReturn(handler)
        kotlinx.coroutines.runBlocking {
            whenever(pipeline.execute(any(), any()))
                .thenThrow(IllegalStateException("bad state"))
        }
        whenever(taskRepository.fail(any(), any(), anyOrNull(), anyOrNull())).thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail(
            eq("task-1"),
            org.mockito.kotlin.argThat { contains("IllegalStateException") && contains("bad state") },
            anyOrNull(), eq("gen-1"),
        )
    }

    // ── execute: processResult exception propagation ───────────────

    @Test
    fun `processResult exception propagates to caller`() = runTest {
        val task = testTask(stepId = null)
        stubPipeline(task, TaskResult.Success("done"))
        whenever(taskRepository.complete(any(), anyOrNull()))
            .thenThrow(RuntimeException("DB connection lost"))

        val ex = org.junit.jupiter.api.assertThrows<RuntimeException> {
            dispatcher.execute(task)
        }
        assertTrue(ex.message!!.contains("DB connection lost"))
    }

    // ── execute: Retry(consumeRetry=false) with stepId ────────────

    @Test
    fun `execute Retry with consumeRetry false and stepId uses requeue not step path`() = runTest {
        val task = testTask(stepId = "group-1")
        val retry = TaskResult.Retry(delay = Duration.ofSeconds(3), reason = "cb-requeue", consumeRetry = false)
        stubPipeline(task, retry)

        dispatcher.execute(task)

        // Must go through taskRepository.requeue, NOT workflowStepRepository
        verify(taskRepository).requeue("task-1", Duration.ofSeconds(3), "gen-1")
        verify(workflowStepRepository, org.mockito.kotlin.never()).failStepTask(any(), any(), any(), anyOrNull(), anyOrNull())
    }

    // ── execute: Success with outputUri/metadata forwarded ─────────

    @Test
    fun `execute Success forwards outputUri and metadata to step repository`() = runTest {
        val task = testTask()
        stubPipeline(task, TaskResult.Success(
            output = "result-data",
            outputUri = "gs://bucket/output.json",
            outputMetadata = """{"rows":42}""",
        ))
        whenever(workflowStepRepository.resolveStepTask(any(), any(), anyOrNull(), any(), anyOrNull(), anyOrNull()))
            .thenReturn(StepTaskResolution(updated = true, barrierMet = false))

        dispatcher.execute(task)

        verify(workflowStepRepository).resolveStepTask(
            eq("task-1"), eq("group-1"), eq("gen-1"), eq(false),
            eq("gs://bucket/output.json"), eq("""{"rows":42}"""),
        )
    }

    // ── execute: Retry with null delay ─────────────────────────────

    @Test
    fun `execute Retry consumeRetry with null delay`() = runTest {
        val task = testTask(stepId = null)
        val retry = TaskResult.Retry(delay = null, reason = "throttled", consumeRetry = true)
        stubPipeline(task, retry)
        whenever(taskRepository.fail(eq("task-1"), eq("throttled"), eq(null), eq("gen-1")))
            .thenReturn(false)

        dispatcher.execute(task)

        verify(taskRepository).fail("task-1", "throttled", null, "gen-1")
    }

    // ── helpers ───────────────────────────────────────────────────

    private fun testTask(
        taskId: String = "task-1",
        handler: String = "test.handler",
        queue: String = "default",
        stepId: String? = "group-1",
    ) = Task(
        taskId = taskId,
        handler = handler,
        queue = queue,
        payload = "{}",
        stepId = stepId,
        metadata = null,
        retryCount = 0,
        maxRetries = 3,
        claimedAt = Instant.now(),
        claimToken = "gen-1",
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
