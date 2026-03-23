package com.workflow.worker

import com.workflow.config.FrameworkConfig
import com.workflow.engine.BarrierService
import com.workflow.engine.Task
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.shutdown.ShutdownSignal
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerLoopTest {

    // ── Shared mocks ─────────────────────────────────────────────────────

    private lateinit var taskRepo: TaskRepository
    private lateinit var handlerRegistry: HandlerRegistry
    private lateinit var barrierService: BarrierService
    private lateinit var config: FrameworkConfig
    private lateinit var workerConfig: FrameworkConfig.WorkerConfig
    private lateinit var shutdownConfig: FrameworkConfig.ShutdownConfig
    private lateinit var workerLoop: WorkerLoop

    private val pollInterval = Duration.ofSeconds(1)
    private val workerId = "test-worker"
    private val concurrency = 1

    @BeforeEach
    fun setup() {
        taskRepo = mock()
        handlerRegistry = mock()
        barrierService = mock()

        workerConfig = mock<FrameworkConfig.WorkerConfig>().also {
            whenever(it.pollInterval()).thenReturn(pollInterval)
            whenever(it.concurrency()).thenReturn(concurrency)
            whenever(it.id()).thenReturn(workerId)
        }
        shutdownConfig = mock<FrameworkConfig.ShutdownConfig>().also {
            whenever(it.globalTimeout()).thenReturn(Duration.ofSeconds(30))
        }
        config = mock<FrameworkConfig>().also {
            whenever(it.worker()).thenReturn(workerConfig)
            whenever(it.shutdown()).thenReturn(shutdownConfig)
        }

        kotlinx.coroutines.runBlocking {
            whenever(taskRepo.findExpired(any())).thenReturn(emptyList())
        }

        workerLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService)
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun makeTask(
        id: String = UUID.randomUUID().toString(),
        workflowId: String = UUID.randomUUID().toString(),
        sequenceNumber: Int = 1,
        status: TaskStatus = TaskStatus.PROCESSING,
        handlerKey: String = "order.validate",
        payloadJson: String? = """{"orderId":"abc"}""",
        resultJson: String? = null,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = Instant.now().plus(30, ChronoUnit.MINUTES),
    ): Task = Task(
        id = id,
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        status = status,
        handlerKey = handlerKey,
        payloadJson = payloadJson,
        resultJson = resultJson,
        claimedBy = workerId,
        claimedAt = Instant.now(),
        completedAt = null,
        retryCount = retryCount,
        maxRetries = maxRetries,
        deadlineAt = deadlineAt,
    )

    private fun startAndAdvance(testScope: TestScope, ticks: Long = 2) {
        val job = workerLoop.start(testScope)
        testScope.advanceTimeBy(pollInterval.toMillis() * ticks)
        job.cancel()
    }

    // ── A. Happy Path (Contract #1) ─────────────────────────────────────

    @Nested
    inner class HappyPath {

        @Test
        fun `claim task and handler succeeds - barrier receives COMPLETED with resultJson`() = runTest {
            val task = makeTask()
            val handlerResult = HandlerOutput(result = """{"status":"done"}""")
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(handlerResult)

            startAndAdvance(this)

            val inputCaptor = argumentCaptor<HandlerInput>()
            verify(handler).execute(inputCaptor.capture())
            val input = inputCaptor.firstValue
            assertEquals(task.id, input.taskId)
            assertEquals(task.workflowId, input.workflowId)
            assertEquals(task.sequenceNumber, input.sequenceNumber)
            assertEquals(task.payloadJson, input.payload)

            verify(barrierService).onTaskCompleted(
                eq(task.id),
                eq(task.workflowId),
                eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED),
                eq(handlerResult.result),
            )
        }

        @Test
        fun `handler returns null result - barrier receives COMPLETED with null`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput(result = null))

            startAndAdvance(this)

            verify(barrierService).onTaskCompleted(
                eq(task.id),
                eq(task.workflowId),
                eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED),
                eq(null),
            )
        }

        @Test
        fun `multiple tasks claimed sequentially are each processed`() = runTest {
            val task1 = makeTask(handlerKey = "step.one")
            val task2 = makeTask(handlerKey = "step.two")
            val handler1 = mock<TransitionHandler>()
            val handler2 = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task1))
                .thenReturn(listOf(task2))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("step.one")).thenReturn(handler1)
            whenever(handlerRegistry.resolve("step.two")).thenReturn(handler2)
            whenever(handler1.execute(any())).thenReturn(HandlerOutput("""{"r":1}"""))
            whenever(handler2.execute(any())).thenReturn(HandlerOutput("""{"r":2}"""))

            startAndAdvance(this, ticks = 4)

            verify(barrierService).onTaskCompleted(
                eq(task1.id), eq(task1.workflowId), eq(task1.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("""{"r":1}"""),
            )
            verify(barrierService).onTaskCompleted(
                eq(task2.id), eq(task2.workflowId), eq(task2.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("""{"r":2}"""),
            )
        }
    }

    // ── B. Retry Logic (Contracts #2, #3) ────────────────────────────────

    @Nested
    inner class RetryLogic {

        @Test
        fun `handler throws with retries remaining - resetForRetry called, barrier NOT called`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 3)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenThrow(RuntimeException("transient failure"))

            startAndAdvance(this)

            verify(taskRepo).resetForRetry(eq(task.id), eq(1))
            verifyNoInteractions(barrierService)
        }

        @Test
        fun `handler throws at max retry boundary - resetForRetry called`() = runTest {
            val task = makeTask(retryCount = 1, maxRetries = 3)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenThrow(RuntimeException("transient again"))

            startAndAdvance(this)

            verify(taskRepo).resetForRetry(eq(task.id), eq(2))
            verifyNoInteractions(barrierService)
        }

        @Test
        fun `handler throws with retries exhausted - barrier receives FAILED`() = runTest {
            val task = makeTask(retryCount = 3, maxRetries = 3)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenThrow(RuntimeException("permanent failure"))

            startAndAdvance(this)

            verify(taskRepo, never()).resetForRetry(any(), any())
            verify(barrierService).onTaskCompleted(
                eq(task.id),
                eq(task.workflowId),
                eq(task.sequenceNumber),
                eq(TaskStatus.FAILED),
                eq(null),
            )
        }

        @Test
        fun `handler throws with zero maxRetries - barrier receives FAILED immediately`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 0)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenThrow(RuntimeException("no retries"))

            startAndAdvance(this)

            verify(taskRepo, never()).resetForRetry(any(), any())
            verify(barrierService).onTaskCompleted(
                eq(task.id),
                eq(task.workflowId),
                eq(task.sequenceNumber),
                eq(TaskStatus.FAILED),
                eq(null),
            )
        }
    }

    // ── C. ResetForRetry Failure (Contract #4) ──────────────────────────

    @Nested
    inner class ResetForRetryFailure {

        @Test
        fun `resetForRetry throws - falls through to barrier FAILED`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 3)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenThrow(RuntimeException("handler failed"))
            whenever(taskRepo.resetForRetry(eq(task.id), eq(1)))
                .thenThrow(RuntimeException("DB connection lost during retry reset"))

            startAndAdvance(this)

            verify(taskRepo).resetForRetry(eq(task.id), eq(1))
            verify(barrierService).onTaskCompleted(
                eq(task.id),
                eq(task.workflowId),
                eq(task.sequenceNumber),
                eq(TaskStatus.FAILED),
                eq(null),
            )
        }
    }

    // ── D. Unknown Handler Key ───────────────────────────────────────────

    @Nested
    inner class UnknownHandlerKey {

        @Test
        fun `resolve throws IllegalStateException with retries remaining - resetForRetry called`() = runTest {
            val task = makeTask(handlerKey = "nonexistent.handler", retryCount = 0, maxRetries = 2)

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("nonexistent.handler"))
                .thenThrow(IllegalStateException("No handler found for key: nonexistent.handler"))

            startAndAdvance(this)

            verify(taskRepo).resetForRetry(eq(task.id), eq(1))
            verifyNoInteractions(barrierService)
        }

        @Test
        fun `resolve throws IllegalStateException with no retries - barrier receives FAILED`() = runTest {
            val task = makeTask(handlerKey = "missing.key", retryCount = 0, maxRetries = 0)

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("missing.key"))
                .thenThrow(IllegalStateException("No handler found for key: missing.key"))

            startAndAdvance(this)

            verify(barrierService).onTaskCompleted(
                eq(task.id),
                eq(task.workflowId),
                eq(task.sequenceNumber),
                eq(TaskStatus.FAILED),
                eq(null),
            )
        }
    }

    // ── E. Empty Poll (Contract #6) ──────────────────────────────────────

    @Nested
    inner class EmptyPoll {

        @Test
        fun `claimNext returns empty - no handler invocation and no barrier call`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())

            startAndAdvance(this)

            verifyNoInteractions(handlerRegistry)
            verifyNoInteractions(barrierService)
        }

        @Test
        fun `empty poll followed by task available - task is processed`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput("ok"))

            startAndAdvance(this, ticks = 4)

            verify(handler).execute(any())
            verify(barrierService).onTaskCompleted(
                eq(task.id), eq(task.workflowId), eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("ok"),
            )
        }
    }

    // ── F. Claim Error (Contract #5) ─────────────────────────────────────

    @Nested
    inner class ClaimError {

        @Test
        fun `claimNext throws - loop continues on next tick`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenThrow(RuntimeException("DB connection lost"))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput("recovered"))

            startAndAdvance(this, ticks = 4)

            verify(barrierService).onTaskCompleted(
                eq(task.id), eq(task.workflowId), eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("recovered"),
            )
        }
    }

    // ── G. Shutdown (Contract #8) ────────────────────────────────────────

    @Nested
    inner class Shutdown {

        @Test
        fun `shutdown signal stops the poll loop`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())

            val job = workerLoop.start(this)
            advanceTimeBy(pollInterval.toMillis())

            val callCountBefore = org.mockito.Mockito.mockingDetails(taskRepo)
                .invocations
                .count { it.method.name == "claimNext" }

            job.cancel()
            advanceTimeBy(pollInterval.toMillis() * 5)

            val totalCalls = org.mockito.Mockito.mockingDetails(taskRepo)
                .invocations
                .count { it.method.name == "claimNext" }

            assertEquals(callCountBefore, totalCalls, "No further claimNext calls after cancel")
        }

        @Test
        fun `shutdown installs ShutdownSignal in scope context`() = runTest {
            var signalObserved = false
            val task = makeTask()
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    val signal = kotlin.coroutines.coroutineContext[ShutdownSignal]
                    signalObserved = signal != null
                    return HandlerOutput(null)
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            assertTrue(signalObserved, "ShutdownSignal should be present in coroutine context")
        }

        @Test
        fun `shutdown completes without hanging when never started`() = runTest {
            // activeJob is null (never started), so join() is skipped
            workerLoop.shutdown()

            // No crash, no hang — shutdown is safe even without start()
            verify(taskRepo, never()).claimNext(any(), any())
        }
    }

    // ── H. ShutdownParticipant Contract ──────────────────────────────────

    @Nested
    inner class ShutdownParticipantContract {

        @Test
        fun `shutdownOrder is 10`() {
            assertEquals(10, workerLoop.shutdownOrder)
        }

        @Test
        fun `shutdownTimeout comes from config shutdown globalTimeout`() {
            val expected = Duration.ofSeconds(30)
            assertEquals(expected, workerLoop.shutdownTimeout)
        }

        @Test
        fun `shutdownTimeout reflects config value`() {
            whenever(shutdownConfig.globalTimeout()).thenReturn(Duration.ofSeconds(45))

            val freshLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService)
            assertEquals(Duration.ofSeconds(45), freshLoop.shutdownTimeout)
        }
    }

    // ── I. Deadline Reaper (Contract #7) ─────────────────────────────────

    @Nested
    inner class DeadlineReaper {

        @Test
        fun `expired task found by findExpired - marked FAILED via barrier`() = runTest {
            val expiredTask = makeTask(
                status = TaskStatus.PROCESSING,
                deadlineAt = Instant.now().minus(5, ChronoUnit.MINUTES),
            )

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())
            whenever(taskRepo.findExpired(any()))
                .thenReturn(listOf(expiredTask))
                .thenReturn(emptyList())

            startAndAdvance(this, ticks = 3)

            verify(barrierService).onTaskCompleted(
                eq(expiredTask.id),
                eq(expiredTask.workflowId),
                eq(expiredTask.sequenceNumber),
                eq(TaskStatus.FAILED),
                eq(null),
            )
        }

        @Test
        fun `multiple expired tasks each trigger barrier call`() = runTest {
            val expired1 = makeTask(
                id = "expired-1",
                workflowId = "wf-1",
                deadlineAt = Instant.now().minus(10, ChronoUnit.MINUTES),
            )
            val expired2 = makeTask(
                id = "expired-2",
                workflowId = "wf-2",
                deadlineAt = Instant.now().minus(10, ChronoUnit.MINUTES),
            )

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())
            whenever(taskRepo.findExpired(any()))
                .thenReturn(listOf(expired1, expired2))
                .thenReturn(emptyList())

            startAndAdvance(this, ticks = 3)

            verify(barrierService).onTaskCompleted(
                eq("expired-1"), eq("wf-1"), eq(expired1.sequenceNumber),
                eq(TaskStatus.FAILED), eq(null),
            )
            verify(barrierService).onTaskCompleted(
                eq("expired-2"), eq("wf-2"), eq(expired2.sequenceNumber),
                eq(TaskStatus.FAILED), eq(null),
            )
        }

        @Test
        fun `no expired tasks - barrier not called by reaper`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())
            whenever(taskRepo.findExpired(any()))
                .thenReturn(emptyList())

            startAndAdvance(this, ticks = 3)

            verifyNoInteractions(barrierService)
        }

        @Test
        fun `reaper polls at pollInterval cadence`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())
            whenever(taskRepo.findExpired(any()))
                .thenReturn(emptyList())

            val job = workerLoop.start(this)
            advanceTimeBy(pollInterval.toMillis() * 3)

            val findExpiredCalls = org.mockito.Mockito.mockingDetails(taskRepo)
                .invocations
                .count { it.method.name == "findExpired" }
            assertTrue(findExpiredCalls >= 2, "Reaper should poll multiple times, got $findExpiredCalls")

            job.cancel()
        }
    }

    // ── J. In-Flight Tracking (Contract #9) ─────────────────────────────

    @Nested
    inner class InFlightTracking {

        @Test
        fun `inFlightTasks is positive during handler execution and zero after`() = runTest {
            val task = makeTask()
            var inFlightDuringExecution = -1
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    inFlightDuringExecution = workerLoop.inFlightTasks
                    return HandlerOutput(null)
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            assertTrue(inFlightDuringExecution > 0, "inFlightTasks should be > 0 during execution, was $inFlightDuringExecution")
            assertEquals(0, workerLoop.inFlightTasks, "inFlightTasks should be 0 after completion")
        }

        @Test
        fun `inFlightTasks returns to zero after handler failure`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 0)
            var inFlightDuringExecution = -1
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    inFlightDuringExecution = workerLoop.inFlightTasks
                    throw RuntimeException("boom")
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            assertTrue(inFlightDuringExecution > 0, "inFlightTasks should be > 0 during execution, was $inFlightDuringExecution")
            assertEquals(0, workerLoop.inFlightTasks, "inFlightTasks should be 0 after failure")
        }
    }

    // ── K. Health Heartbeat (Contract #10) ───────────────────────────────

    @Nested
    inner class HealthHeartbeat {

        @Test
        fun `lastPollTimestamp advances after poll iterations`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())

            val before = Instant.now().minusMillis(1)

            val job = workerLoop.start(this)
            advanceTimeBy(pollInterval.toMillis() * 2)

            val after = workerLoop.lastPollTimestamp
            assertTrue(
                after.isAfter(before),
                "lastPollTimestamp should advance after polling, was $before, now $after",
            )

            job.cancel()
        }
    }

    // ── L. Error Resilience ──────────────────────────────────────────────

    @Nested
    inner class ErrorResilience {

        @Test
        fun `barrier onTaskCompleted throws - loop continues`() = runTest {
            val task1 = makeTask(id = "t1", handlerKey = "step.one")
            val task2 = makeTask(id = "t2", handlerKey = "step.two")
            val handler1 = mock<TransitionHandler>()
            val handler2 = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task1))
                .thenReturn(listOf(task2))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("step.one")).thenReturn(handler1)
            whenever(handlerRegistry.resolve("step.two")).thenReturn(handler2)
            whenever(handler1.execute(any())).thenReturn(HandlerOutput("r1"))
            whenever(handler2.execute(any())).thenReturn(HandlerOutput("r2"))

            doAnswer { throw RuntimeException("barrier blew up") }
                .doAnswer { }
                .whenever(barrierService).onTaskCompleted(any(), any(), any(), any(), any())

            startAndAdvance(this, ticks = 4)

            verify(handler1).execute(any())
            verify(handler2).execute(any())
            verify(barrierService, times(2)).onTaskCompleted(any(), any(), any(), any(), any())
        }

        @Test
        fun `barrier throws on COMPLETED with retries remaining - resetForRetry called`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 3)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput("success"))
            doThrow(RuntimeException("barrier failed on COMPLETED"))
                .whenever(barrierService).onTaskCompleted(any(), any(), any(), any(), any())

            startAndAdvance(this)

            verify(handler).execute(any())
            verify(taskRepo).resetForRetry(eq(task.id), eq(1))
        }

        @Test
        fun `findExpired throws - reaper continues on next cycle`() = runTest {
            val expiredTask = makeTask(
                status = TaskStatus.PROCESSING,
                deadlineAt = Instant.now().minus(5, ChronoUnit.MINUTES),
            )

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())
            whenever(taskRepo.findExpired(any()))
                .thenThrow(RuntimeException("reaper DB error"))
                .thenReturn(listOf(expiredTask))
                .thenReturn(emptyList())

            startAndAdvance(this, ticks = 4)

            verify(barrierService).onTaskCompleted(
                eq(expiredTask.id),
                eq(expiredTask.workflowId),
                eq(expiredTask.sequenceNumber),
                eq(TaskStatus.FAILED),
                eq(null),
            )
        }

        @Test
        fun `barrier throws during reportTaskFailed on retries exhausted - loop continues`() = runTest {
            val task1 = makeTask(id = "t1", handlerKey = "step.one", retryCount = 3, maxRetries = 3)
            val task2 = makeTask(id = "t2", handlerKey = "step.two")
            val handler1 = mock<TransitionHandler>()
            val handler2 = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task1))
                .thenReturn(listOf(task2))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("step.one")).thenReturn(handler1)
            whenever(handlerRegistry.resolve("step.two")).thenReturn(handler2)
            whenever(handler1.execute(any())).thenThrow(RuntimeException("permanent failure"))
            whenever(handler2.execute(any())).thenReturn(HandlerOutput("r2"))

            // barrier throws on FAILED report for task1, succeeds for task2
            doAnswer { invocation ->
                val status = invocation.getArgument<TaskStatus>(3)
                if (status == TaskStatus.FAILED) throw RuntimeException("barrier blew up on FAILED")
                Unit
            }
                .doAnswer { }
                .whenever(barrierService).onTaskCompleted(any(), any(), any(), any(), any())

            startAndAdvance(this, ticks = 4)

            verify(handler1).execute(any())
            verify(handler2).execute(any())
        }
    }

    // ── M. Concurrent Polling (Spec #9) ────────────────────────────────

    @Nested
    inner class ConcurrentPolling {

        @Test
        fun `concurrency controls parallel polling slots, each claiming one task`() = runTest {
            val slots = 4
            val batchWorkerConfig = mock<FrameworkConfig.WorkerConfig>().also {
                whenever(it.pollInterval()).thenReturn(pollInterval)
                whenever(it.concurrency()).thenReturn(slots)
                whenever(it.id()).thenReturn(workerId)
            }
            val batchConfig = mock<FrameworkConfig>().also {
                whenever(it.worker()).thenReturn(batchWorkerConfig)
                whenever(it.shutdown()).thenReturn(shutdownConfig)
            }
            val batchLoop = WorkerLoop(batchConfig, taskRepo, handlerRegistry, barrierService)

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(emptyList())

            val job = batchLoop.start(this)
            advanceTimeBy(pollInterval.toMillis() * 2)
            job.cancel()

            // Each concurrent slot calls claimNext with batchSize=1
            verify(taskRepo, org.mockito.Mockito.atLeastOnce()).claimNext(eq(workerId), eq(1))
        }
    }

    // ── N. Reaper Excludes In-Flight (Spec #8) ──────────────────────────

    @Nested
    inner class ReaperExcludesInFlight {

        @Test
        fun `reaper does not fail task that is still in-flight`() = runTest {
            val task = makeTask(
                id = "T1",
                workflowId = "wf-inflight",
                deadlineAt = Instant.now().minus(5, ChronoUnit.MINUTES),
            )
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    delay(5000)
                    return HandlerOutput("""{"done":true}""")
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            // Reaper sees T1 as expired while handler is still running
            whenever(taskRepo.findExpired(any()))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())

            val job = workerLoop.start(this)

            // Advance past first poll (claims T1) and first reaper tick
            advanceTimeBy(pollInterval.toMillis() * 2)

            // At this point handler is still in delay(5000), reaper has fired.
            // Reaper should NOT mark T1 as FAILED because it is in-flight.
            val failedCallsBeforeHandlerDone = org.mockito.Mockito.mockingDetails(barrierService)
                .invocations
                .count {
                    it.method.name == "onTaskCompleted" &&
                        it.arguments[0] == "T1" &&
                        it.arguments[3] == TaskStatus.FAILED
                }
            assertEquals(0, failedCallsBeforeHandlerDone, "Reaper must not FAIL in-flight task T1")

            // Now advance past handler delay so it completes
            advanceTimeBy(5000)

            verify(barrierService).onTaskCompleted(
                eq("T1"), eq("wf-inflight"), eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("""{"done":true}"""),
            )

            job.cancel()
        }
    }

    // ── O. Shutdown Lifecycle (Spec #10) ─────────────────────────────────

    @Nested
    inner class ShutdownLifecycle {

        @Test
        fun `start then process then shutdown - full lifecycle`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput("done"))

            // 1. Start
            workerLoop.start(this)

            // 2. Process at least one task
            advanceTimeBy(pollInterval.toMillis() * 2)
            verify(handler).execute(any())

            // 3. Shutdown (launch in separate coroutine to avoid blocking)
            val shutdownJob = launch { workerLoop.shutdown() }
            advanceTimeBy(pollInterval.toMillis())
            shutdownJob.join()

            // 4. shutdown() returned (no hang)
            assertTrue(shutdownJob.isCompleted, "shutdown() should complete without hanging")

            // 5. No further claimNext calls
            val callsBeforeExtra = org.mockito.Mockito.mockingDetails(taskRepo)
                .invocations
                .count { it.method.name == "claimNext" }
            advanceTimeBy(pollInterval.toMillis() * 3)
            val callsAfterExtra = org.mockito.Mockito.mockingDetails(taskRepo)
                .invocations
                .count { it.method.name == "claimNext" }
            assertEquals(callsBeforeExtra, callsAfterExtra, "No further claimNext after shutdown")

            // 6. inFlightTasks == 0
            assertEquals(0, workerLoop.inFlightTasks, "inFlightTasks should be 0 after shutdown")
        }
    }

    // ── P. CancellationException Propagation (Spec #18) ─────────────────

    @Nested
    inner class CancellationExceptionPropagation {

        @Test
        fun `CancellationException from handler propagates - no retry, no barrier`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 3)
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    throw CancellationException("task cancelled")
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1)))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            // CancellationException should NOT trigger retry or barrier
            verify(taskRepo, never()).resetForRetry(any(), any())
            verifyNoInteractions(barrierService)
        }
    }
}
