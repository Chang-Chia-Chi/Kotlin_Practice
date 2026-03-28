package com.workflow.worker

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.config.FrameworkConfig
import com.workflow.engine.BarrierService
import com.workflow.engine.InputResolver
import com.workflow.engine.Task
import com.workflow.engine.TaskRepository
import com.workflow.engine.TaskStatus
import com.workflow.engine.WorkflowRepository
import com.workflow.shutdown.ShutdownSignal
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.slf4j.MDC
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
import kotlin.test.assertFalse
import kotlin.test.assertNull
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
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var inputResolver: InputResolver
    private lateinit var workflowRepo: WorkflowRepository
    private lateinit var objectMapper: ObjectMapper
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
            whenever(it.batchSize()).thenReturn(1)
        }
        shutdownConfig = mock<FrameworkConfig.ShutdownConfig>().also {
            whenever(it.globalTimeout()).thenReturn(Duration.ofSeconds(30))
        }
        config = mock<FrameworkConfig>().also {
            whenever(it.worker()).thenReturn(workerConfig)
            whenever(it.shutdown()).thenReturn(shutdownConfig)
        }

        meterRegistry = SimpleMeterRegistry()
        inputResolver = mock()
        workflowRepo = mock()
        objectMapper = ObjectMapper().registerModule(com.fasterxml.jackson.module.kotlin.KotlinModule.Builder().build())
        workerLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService, meterRegistry, inputResolver, workflowRepo, objectMapper)
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun makeTask(
        id: String = UUID.randomUUID().toString(),
        workflowId: String = UUID.randomUUID().toString(),
        sequenceNumber: Int = 1,
        status: TaskStatus = TaskStatus.PROCESSING,
        handlerKey: String = "order.validate",
        item: String? = """{"orderId":"abc"}""",
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
        item = item,
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
            assertNull(input.inputs)
            assertEquals(task.item, input.item)

            verify(barrierService).onTaskCompleted(
                eq(task.id),
                eq(task.workflowId),
                eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED),
                eq(handlerResult.result),
                eq(workerId),
                any(),
            )
        }

        @Test
        fun `handler returns null result - barrier receives COMPLETED with null`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
                eq(workerId),
                any(),
            )
        }

        @Test
        fun `multiple tasks claimed sequentially are each processed`() = runTest {
            val task1 = makeTask(handlerKey = "step.one")
            val task2 = makeTask(handlerKey = "step.two")
            val handler1 = mock<TransitionHandler>()
            val handler2 = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
                eq(TaskStatus.COMPLETED), eq("""{"r":1}"""), eq(workerId), any(),
            )
            verify(barrierService).onTaskCompleted(
                eq(task2.id), eq(task2.workflowId), eq(task2.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("""{"r":2}"""), eq(workerId), any(),
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
                eq(workerId),
                any(),
            )
        }

        @Test
        fun `handler throws with zero maxRetries - barrier receives FAILED immediately`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 0)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
                eq(workerId),
                any(),
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
                eq(workerId),
                any(),
            )
        }
    }

    // ── D. Unknown Handler Key ───────────────────────────────────────────

    @Nested
    inner class UnknownHandlerKey {

        @Test
        fun `resolve throws IllegalStateException with retries remaining - resetForRetry called`() = runTest {
            val task = makeTask(handlerKey = "nonexistent.handler", retryCount = 0, maxRetries = 2)

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
                eq(workerId),
                any(),
            )
        }
    }

    // ── E. Empty Poll (Contract #6) ──────────────────────────────────────

    @Nested
    inner class EmptyPoll {

        @Test
        fun `claimNext returns empty - no handler invocation and no barrier call`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(emptyList())

            startAndAdvance(this)

            verifyNoInteractions(handlerRegistry)
            verifyNoInteractions(barrierService)
        }

        @Test
        fun `empty poll followed by task available - task is processed`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(emptyList())
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput("ok"))

            startAndAdvance(this, ticks = 4)

            verify(handler).execute(any())
            verify(barrierService).onTaskCompleted(
                eq(task.id), eq(task.workflowId), eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("ok"), eq(workerId), any(),
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenThrow(RuntimeException("DB connection lost"))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput("recovered"))

            startAndAdvance(this, ticks = 4)

            verify(barrierService).onTaskCompleted(
                eq(task.id), eq(task.workflowId), eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("recovered"), eq(workerId), any(),
            )
        }
    }

    // ── G. Shutdown (Contract #8) ────────────────────────────────────────

    @Nested
    inner class Shutdown {

        @Test
        fun `shutdown signal stops the poll loop`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
            verify(taskRepo, never()).claimNext(any(), any(), any())
        }
    }

    // ── G2. Shutdown Drain Window (R0.2) ────────────────────────────────

    @Nested
    inner class ShutdownDrainWindow {

        @Test
        fun `drain completes within window - handler finishes naturally`() = runTest {
            val handlerCompleted = java.util.concurrent.atomic.AtomicBoolean(false)
            val task = makeTask()
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    delay(500) // simulated work within drain window (30s default)
                    handlerCompleted.set(true)
                    return HandlerOutput("""{"drained":"ok"}""")
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            // SupervisorJob + CoroutineExceptionHandler: the takeUntilSignal
            // implementation throws ClosedSendChannelException when the signalJob
            // closes the channelFlow while indefinitelyRepeat is emitting. This is
            // expected flow-close behavior in production (SupervisorJob in onStart).
            val ceh = CoroutineExceptionHandler { _, _ -> }
            val supervisorScope = CoroutineScope(coroutineContext + SupervisorJob() + ceh)
            workerLoop.start(supervisorScope)
            advanceTimeBy(pollInterval.toMillis())

            val shutdownJob = launch { workerLoop.shutdown() }
            advanceTimeBy(pollInterval.toMillis())
            shutdownJob.join()

            assertTrue(handlerCompleted.get(), "Handler should complete within drain window (not cancelled)")
            verify(barrierService).onTaskCompleted(
                eq(task.id), eq(task.workflowId), eq(task.sequenceNumber),
                eq(TaskStatus.COMPLETED), eq("""{"drained":"ok"}"""), eq(workerId), any(),
            )
        }

        @Test
        fun `force-cancel after drain timeout - long handler is cancelled`() = runTest {
            whenever(shutdownConfig.globalTimeout()).thenReturn(Duration.ofMillis(100))
            val shortTimeoutLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService, meterRegistry, inputResolver, workflowRepo, objectMapper)

            val handlerStarted = java.util.concurrent.atomic.AtomicBoolean(false)
            val handlerCompleted = java.util.concurrent.atomic.AtomicBoolean(false)
            val task = makeTask()
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    handlerStarted.set(true)
                    delay(Long.MAX_VALUE) // block indefinitely — exceeds drain window
                    handlerCompleted.set(true)
                    return HandlerOutput(null) // unreachable
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            val ceh = CoroutineExceptionHandler { _, _ -> }
            val supervisorScope = CoroutineScope(coroutineContext + SupervisorJob() + ceh)
            shortTimeoutLoop.start(supervisorScope)
            advanceTimeBy(pollInterval.toMillis())

            val shutdownJob = launch { shortTimeoutLoop.shutdown() }
            advanceTimeBy(pollInterval.toMillis())
            shutdownJob.join()

            assertTrue(handlerStarted.get(), "Handler should have started before shutdown")
            assertFalse(handlerCompleted.get(), "Handler should have been force-cancelled")
            assertTrue(shutdownJob.isCompleted, "Shutdown should complete after force-cancel")
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

            val freshLoop = WorkerLoop(config, taskRepo, handlerRegistry, barrierService, meterRegistry, inputResolver, workflowRepo, objectMapper)
            assertEquals(Duration.ofSeconds(45), freshLoop.shutdownTimeout)
        }
    }

    // ── I. In-Flight Tracking (Contract #9) ─────────────────────────────

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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
        fun `lastActivityTimestamp advances after poll iterations`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(emptyList())

            val before = Instant.now().minusMillis(1)

            val job = workerLoop.start(this)
            advanceTimeBy(pollInterval.toMillis() * 2)

            val after = workerLoop.lastActivityTimestamp
            assertTrue(
                after.isAfter(before),
                "lastActivityTimestamp should advance after polling, was $before, now $after",
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task1))
                .thenReturn(listOf(task2))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("step.one")).thenReturn(handler1)
            whenever(handlerRegistry.resolve("step.two")).thenReturn(handler2)
            whenever(handler1.execute(any())).thenReturn(HandlerOutput("r1"))
            whenever(handler2.execute(any())).thenReturn(HandlerOutput("r2"))

            doAnswer { throw RuntimeException("barrier blew up") }
                .doAnswer { }
                .whenever(barrierService).onTaskCompleted(any(), any(), any(), any(), any(), any(), any())

            startAndAdvance(this, ticks = 4)

            verify(handler1).execute(any())
            verify(handler2).execute(any())
            verify(barrierService, times(2)).onTaskCompleted(any(), any(), any(), any(), any(), any(), any())
        }

        @Test
        fun `barrier throws on COMPLETED with retries remaining - resetForRetry called`() = runTest {
            val task = makeTask(retryCount = 0, maxRetries = 3)
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput("success"))
            doThrow(RuntimeException("barrier failed on COMPLETED"))
                .whenever(barrierService).onTaskCompleted(any(), any(), any(), any(), any(), any(), any())

            startAndAdvance(this)

            verify(handler).execute(any())
            verify(taskRepo).resetForRetry(eq(task.id), eq(1))
        }

        @Test
        fun `barrier throws during reportTaskFailed on retries exhausted - loop continues`() = runTest {
            val task1 = makeTask(id = "t1", handlerKey = "step.one", retryCount = 3, maxRetries = 3)
            val task2 = makeTask(id = "t2", handlerKey = "step.two")
            val handler1 = mock<TransitionHandler>()
            val handler2 = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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
                .whenever(barrierService).onTaskCompleted(any(), any(), any(), any(), any(), any(), any())

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
                whenever(it.batchSize()).thenReturn(1)
            }
            val batchConfig = mock<FrameworkConfig>().also {
                whenever(it.worker()).thenReturn(batchWorkerConfig)
                whenever(it.shutdown()).thenReturn(shutdownConfig)
            }
            val batchLoop = WorkerLoop(batchConfig, taskRepo, handlerRegistry, barrierService, meterRegistry, inputResolver, workflowRepo, objectMapper)

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(emptyList())

            val job = batchLoop.start(this)
            advanceTimeBy(pollInterval.toMillis() * 2)
            job.cancel()

            // Each concurrent slot calls claimNext with batchSize=1
            verify(taskRepo, org.mockito.Mockito.atLeastOnce()).claimNext(eq(workerId), eq(1), eq("default"))
        }
    }

    // ── O. Shutdown Lifecycle (Spec #10) ─────────────────────────────────

    @Nested
    inner class ShutdownLifecycle {

        @Test
        fun `start then process then shutdown - full lifecycle`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
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

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            // CancellationException should NOT trigger retry or barrier
            verify(taskRepo, never()).resetForRetry(any(), any())
            verifyNoInteractions(barrierService)
        }
    }

    // ── Q. MDC Context Propagation (R3.7) ────────────────────────────────

    @Nested
    inner class MdcContextPropagation {

        @Test
        fun `MDC contains worker and task fields during handler execution`() = runTest {
            val task = makeTask(
                id = "task-42",
                workflowId = "wf-7",
                handlerKey = "order.validate",
                retryCount = 1,
            )
            val capturedMdc = mutableMapOf<String, String?>()
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    capturedMdc["worker_id"] = MDC.get("worker_id")
                    capturedMdc["task_id"] = MDC.get("task_id")
                    capturedMdc["workflow_id"] = MDC.get("workflow_id")
                    capturedMdc["handler_key"] = MDC.get("handler_key")
                    capturedMdc["attempt"] = MDC.get("attempt")
                    return HandlerOutput(null)
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            assertEquals(workerId, capturedMdc["worker_id"])
            assertEquals("task-42", capturedMdc["task_id"])
            assertEquals("wf-7", capturedMdc["workflow_id"])
            assertEquals("order.validate", capturedMdc["handler_key"])
            assertEquals("1", capturedMdc["attempt"])
        }

        @Test
        fun `MDC task fields do not leak between sequential tasks`() = runTest {
            val task1 = makeTask(id = "t1", workflowId = "wf-1", handlerKey = "step.one")
            val task2 = makeTask(id = "t2", workflowId = "wf-2", handlerKey = "step.two")
            val capturedMdcTask2 = mutableMapOf<String, String?>()

            val handler1 = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    return HandlerOutput(null)
                }
            }
            val handler2 = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    capturedMdcTask2["task_id"] = MDC.get("task_id")
                    capturedMdcTask2["workflow_id"] = MDC.get("workflow_id")
                    capturedMdcTask2["handler_key"] = MDC.get("handler_key")
                    return HandlerOutput(null)
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task1))
                .thenReturn(listOf(task2))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve("step.one")).thenReturn(handler1)
            whenever(handlerRegistry.resolve("step.two")).thenReturn(handler2)

            startAndAdvance(this, ticks = 4)

            assertEquals("t2", capturedMdcTask2["task_id"])
            assertEquals("wf-2", capturedMdcTask2["workflow_id"])
            assertEquals("step.two", capturedMdcTask2["handler_key"])
        }

        @Test
        fun `MDC context persists through failure handling path`() = runTest {
            val task = makeTask(
                id = "fail-task",
                workflowId = "wf-fail",
                handlerKey = "order.fail",
                retryCount = 2,
                maxRetries = 3,
            )
            var mdcDuringRetry = emptyMap<String, String?>()

            val handler = mock<TransitionHandler>()
            whenever(handler.execute(any())).thenThrow(RuntimeException("boom"))

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            doAnswer {
                mdcDuringRetry = mapOf(
                    "worker_id" to MDC.get("worker_id"),
                    "task_id" to MDC.get("task_id"),
                    "workflow_id" to MDC.get("workflow_id"),
                    "handler_key" to MDC.get("handler_key"),
                    "attempt" to MDC.get("attempt"),
                )
                Unit
            }.whenever(taskRepo).resetForRetry(eq("fail-task"), eq(3))

            startAndAdvance(this)

            assertEquals(workerId, mdcDuringRetry["worker_id"])
            assertEquals("fail-task", mdcDuringRetry["task_id"])
            assertEquals("wf-fail", mdcDuringRetry["workflow_id"])
            assertEquals("order.fail", mdcDuringRetry["handler_key"])
            assertEquals("2", mdcDuringRetry["attempt"])
        }
    }

    // ── R. Metrics (R3.1, R3.2, R3.4 prep) ──────────────────────────────

    @Nested
    inner class MetricsTest {

        @Test
        fun `registers in-flight tasks gauge that reflects actual count`() = runTest {
            val task = makeTask()
            var gaugeValueDuringExecution = -1.0
            val handler = object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    gaugeValueDuringExecution = meterRegistry
                        .find("taskqueue_worker_in_flight_tasks")
                        .tag("pod", workerId)
                        .gauge()
                        ?.value() ?: -1.0
                    return HandlerOutput(null)
                }
            }

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)

            startAndAdvance(this)

            assertEquals(1.0, gaugeValueDuringExecution, "In-flight gauge should be 1.0 during handler execution")
        }

        @Test
        fun `registers concurrency limit gauge from config`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default"))).thenReturn(emptyList())

            startAndAdvance(this)

            val gaugeValue = meterRegistry
                .find("taskqueue_worker_concurrency_limit")
                .tag("pod", workerId)
                .gauge()
                ?.value()
            assertEquals(concurrency.toDouble(), gaugeValue, "Concurrency limit gauge should equal config value")
        }

        @Test
        fun `increments claim counter with empty outcome when no tasks`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default"))).thenReturn(emptyList())

            startAndAdvance(this)

            val count = meterRegistry
                .find("taskqueue_claim_total")
                .tag("pod", workerId)
                .tag("outcome", "empty")
                .counter()
                ?.count() ?: 0.0
            assertTrue(count > 0.0, "claim counter with outcome=empty should be > 0, was $count")
        }

        @Test
        fun `increments claim counter with success outcome and claimed tasks counter`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput(null))

            startAndAdvance(this)

            val successCount = meterRegistry
                .find("taskqueue_claim_total")
                .tag("pod", workerId)
                .tag("outcome", "success")
                .counter()
                ?.count() ?: 0.0
            assertEquals(1.0, successCount, "claim counter with outcome=success should be 1.0")

            val claimedCount = meterRegistry
                .find("taskqueue_claimed_tasks_total")
                .tag("pod", workerId)
                .counter()
                ?.count() ?: 0.0
            assertEquals(1.0, claimedCount, "claimed_tasks_total counter should be 1.0")
        }

        @Test
        fun `increments claim counter with error outcome on claimNext failure`() = runTest {
            val task = makeTask()
            val handler = mock<TransitionHandler>()

            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default")))
                .thenThrow(RuntimeException("DB error"))
                .thenReturn(listOf(task))
                .thenReturn(emptyList())
            whenever(handlerRegistry.resolve(task.handlerKey)).thenReturn(handler)
            whenever(handler.execute(any())).thenReturn(HandlerOutput(null))

            startAndAdvance(this, ticks = 4)

            val errorCount = meterRegistry
                .find("taskqueue_claim_total")
                .tag("pod", workerId)
                .tag("outcome", "error")
                .counter()
                ?.count() ?: 0.0
            assertEquals(1.0, errorCount, "claim counter with outcome=error should be 1.0")
        }

        @Test
        fun `lastActivityTimestamp updates on poll`() = runTest {
            whenever(taskRepo.claimNext(eq(workerId), eq(1), eq("default"))).thenReturn(emptyList())

            val before = Instant.now().minusMillis(1)

            val job = workerLoop.start(this)
            advanceTimeBy(pollInterval.toMillis() * 2)

            val after = workerLoop.lastActivityTimestamp
            assertTrue(
                after.isAfter(before),
                "lastActivityTimestamp should advance after polling, was $before, now $after",
            )

            job.cancel()
        }
    }
}
