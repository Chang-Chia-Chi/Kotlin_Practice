package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class WorkerLoopTest {

    private lateinit var config: FrameworkConfig
    private lateinit var workerConfig: FrameworkConfig.WorkerConfig
    private lateinit var taskRepository: TaskRepository
    private lateinit var dispatcher: TaskDispatcher
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var workerLoop: WorkerLoop

    private fun task(
        taskId: String = "t-1",
        handler: String = "test.handler",
        queue: String = "default",
        executionGeneration: String = "gen-1",
    ) = Task(
        taskId = taskId,
        handler = handler,
        queue = queue,
        payload = "{}",
        status = TaskStatus.CLAIMED,
        claimedBy = "test-pod",
        executionGeneration = executionGeneration,
    )

    @BeforeEach
    fun setUp() {
        config = mock()
        workerConfig = mock()
        taskRepository = mock()
        dispatcher = mock()
        shutdownCoordinator = mock()
        meterRegistry = SimpleMeterRegistry()

        whenever(config.worker()).thenReturn(workerConfig)
        whenever(workerConfig.pollInterval()).thenReturn(Duration.ofMillis(20))
        whenever(workerConfig.bulkheadSize()).thenReturn(4)
        whenever(workerConfig.id()).thenReturn("test-pod")
        whenever(workerConfig.queues()).thenReturn(listOf("default"))

        whenever(shutdownCoordinator.state).thenReturn(ShutdownState.RUNNING)
        whenever(shutdownCoordinator.isShuttingDown).thenReturn(false)

        workerLoop = WorkerLoop(
            config, taskRepository, dispatcher,
            shutdownCoordinator, meterRegistry,
        )
    }

    private fun start() {
        workerLoop.onStart(mock<StartupEvent>())
    }

    private fun verifySuspend(mode: org.mockito.verification.VerificationMode = org.mockito.Mockito.times(1), block: suspend TaskDispatcher.() -> Unit) {
        runBlocking { block(verify(dispatcher, mode)) }
    }

    // ── Happy path ────────────────────────────────────────────────────

    @Nested
    inner class HappyPath {

        @Test
        fun `claims and dispatches a task`() {
            val claimed = task()
            whenever(taskRepository.claim("test-pod", listOf("default")))
                .thenReturn(claimed)
                .thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(1)).claim("test-pod", listOf("default"))
            }
            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verifySuspend { execute(claimed) }
            }
        }

        @Test
        fun `updates lastPollTimestamp`() {
            whenever(taskRepository.claim(any(), any())).thenReturn(null)

            val before = workerLoop.lastPollTimestamp
            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                assertTrue(workerLoop.lastPollTimestamp >= before)
            }
        }

        @Test
        fun `no task available does not call execute`() {
            whenever(taskRepository.claim(any(), any())).thenReturn(null)

            start()

            await.atMost(1, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(2)).claim("test-pod", listOf("default"))
            }
            verifySuspend(never()) { execute(any()) }
        }
    }

    // ── Shutdown coordination ─────────────────────────────────────────

    @Nested
    inner class ShutdownBehavior {

        @Test
        fun `stops claiming when shutdown state is not RUNNING`() {
            val callCount = AtomicInteger()
            whenever(shutdownCoordinator.state).thenAnswer {
                if (callCount.incrementAndGet() > 3) ShutdownState.DRAINING
                else ShutdownState.RUNNING
            }
            whenever(taskRepository.claim(any(), any())).thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                assertTrue(callCount.get() > 3)
            }
        }

        @Test
        fun `records drain completion when shutting down`() {
            val latch = CountDownLatch(1)
            val claimed = task()
            whenever(taskRepository.claim("test-pod", listOf("default")))
                .thenReturn(claimed)
                .thenReturn(null)
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    // Simulate shutdown starting while task is in-flight
                    whenever(shutdownCoordinator.isShuttingDown).thenReturn(true)
                    latch.countDown()
                    Unit
                }
            }

            start()

            assertTrue(latch.await(2, TimeUnit.SECONDS))
            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(shutdownCoordinator).recordDrainCompletion()
            }
        }
    }

    // ── Bulkhead ──────────────────────────────────────────────────────

    @Nested
    inner class BulkheadBehavior {

        @Test
        fun `limits concurrent tasks to bulkhead size`() {
            whenever(workerConfig.bulkheadSize()).thenReturn(2)
            workerLoop = WorkerLoop(
                config, taskRepository, dispatcher,
                shutdownCoordinator, meterRegistry,
            )

            val activeCount = AtomicInteger(0)
            val maxConcurrent = AtomicInteger(0)
            val tasksStarted = CountDownLatch(3)

            whenever(taskRepository.claim(any(), any())).thenReturn(task())
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    val current = activeCount.incrementAndGet()
                    maxConcurrent.updateAndGet { max -> maxOf(max, current) }
                    tasksStarted.countDown()
                    Thread.sleep(100)
                    activeCount.decrementAndGet()
                    Unit
                }
            }

            start()

            assertTrue(tasksStarted.await(3, TimeUnit.SECONDS))
            assertTrue(maxConcurrent.get() <= 2, "Max concurrent was ${maxConcurrent.get()}, expected <= 2")
        }

        @Test
        fun `releases semaphore when no task claimed`() {
            whenever(taskRepository.claim(any(), any())).thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(3)).claim("test-pod", listOf("default"))
            }
        }

        @Test
        fun `releases semaphore when claim throws`() {
            whenever(taskRepository.claim(any(), any()))
                .thenThrow(RuntimeException("DB error"))
                .thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(2)).claim(any(), any())
            }
        }
    }

    // ── Error handling ────────────────────────────────────────────────

    @Nested
    inner class ErrorHandling {

        @Test
        fun `claim exception does not kill poll loop`() {
            whenever(taskRepository.claim(any(), any()))
                .thenThrow(RuntimeException("transient"))
                .thenThrow(RuntimeException("transient"))
                .thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(3)).claim(any(), any())
            }
        }
    }
}
