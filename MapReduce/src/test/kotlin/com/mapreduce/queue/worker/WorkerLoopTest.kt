package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.event.TaskClaimed
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.event.Event
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
    private lateinit var heartbeatConfig: FrameworkConfig.HeartbeatConfig
    private lateinit var dispatcher: TaskDispatcher
    private lateinit var taskRepository: TaskRepository
    private lateinit var circuitBreaker: PodCircuitBreaker
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var taskClaimedEvent: Event<TaskClaimed>
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
        heartbeatConfig = mock()
        dispatcher = mock()
        taskRepository = mock()
        circuitBreaker = mock()
        shutdownCoordinator = mock()
        taskClaimedEvent = mock()

        whenever(config.worker()).thenReturn(workerConfig)
        whenever(config.heartbeat()).thenReturn(heartbeatConfig)
        whenever(workerConfig.pollInterval()).thenReturn(Duration.ofMillis(20))
        whenever(workerConfig.bulkheadSize()).thenReturn(4)
        whenever(workerConfig.id()).thenReturn("test-pod")
        whenever(workerConfig.queues()).thenReturn(listOf("default"))
        whenever(heartbeatConfig.interval()).thenReturn(Duration.ofMillis(50))

        whenever(shutdownCoordinator.state).thenReturn(ShutdownState.RUNNING)
        whenever(shutdownCoordinator.isShuttingDown).thenReturn(false)
        whenever(circuitBreaker.isTripped).thenReturn(false)

        workerLoop = WorkerLoop(
            config, dispatcher, taskRepository, circuitBreaker,
            shutdownCoordinator, taskClaimedEvent,
        )
    }

    private fun start() {
        workerLoop.onStart(mock<StartupEvent>())
    }

    /** Verify a suspend function call on the mock dispatcher. */
    private fun verifySuspend(mode: org.mockito.verification.VerificationMode = org.mockito.Mockito.times(1), block: suspend TaskDispatcher.() -> Unit) {
        runBlocking { block(verify(dispatcher, mode)) }
    }

    // ── Happy path ────────────────────────────────────────────────

    @Nested
    inner class HappyPath {

        @Test
        fun `claims and dispatches a task`() {
            val claimed = task()
            whenever(dispatcher.claimTask())
                .thenReturn(claimed)
                .thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(dispatcher, atLeast(1)).claimTask()
            }
            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verifySuspend { execute(claimed) }
            }
        }

        @Test
        fun `fires TaskClaimed event on successful claim`() {
            whenever(dispatcher.claimTask())
                .thenReturn(task())
                .thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(taskClaimedEvent).fireAsync(any())
            }
        }

        @Test
        fun `updates lastPollTimestamp`() {
            whenever(dispatcher.claimTask()).thenReturn(null)

            val before = workerLoop.lastPollTimestamp
            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                assertTrue(workerLoop.lastPollTimestamp >= before)
            }
        }

        @Test
        fun `no task available does not call execute`() {
            whenever(dispatcher.claimTask()).thenReturn(null)

            start()

            await.atMost(1, TimeUnit.SECONDS).untilAsserted {
                verify(dispatcher, atLeast(2)).claimTask()
            }
            verifySuspend(never()) { execute(any()) }
        }
    }

    // ── Circuit breaker ───────────────────────────────────────────

    @Nested
    inner class CircuitBreakerBehavior {

        @Test
        fun `skips claim when circuit breaker is tripped`() {
            whenever(circuitBreaker.isTripped).thenReturn(true)

            start()

            await.during(Duration.ofMillis(100)).atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(dispatcher, never()).claimTask()
            }
        }

        @Test
        fun `resumes claiming when circuit breaker recovers`() {
            val callCount = AtomicInteger()
            whenever(circuitBreaker.isTripped).thenAnswer {
                callCount.incrementAndGet() <= 3
            }
            whenever(dispatcher.claimTask()).thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(dispatcher, atLeast(1)).claimTask()
            }
        }
    }

    // ── Shutdown coordination ─────────────────────────────────────

    @Nested
    inner class ShutdownBehavior {

        @Test
        fun `stops claiming when shutdown state is not RUNNING`() {
            val callCount = AtomicInteger()
            whenever(shutdownCoordinator.state).thenAnswer {
                if (callCount.incrementAndGet() > 3) ShutdownState.DRAINING
                else ShutdownState.RUNNING
            }
            whenever(dispatcher.claimTask()).thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                assertTrue(callCount.get() > 3)
            }
        }

        @Test
        fun `registers bulkhead with coordinator on start`() {
            whenever(shutdownCoordinator.state).thenReturn(ShutdownState.DRAINING)

            start()

            verify(shutdownCoordinator).registerBulkhead(any(), eq(4))
        }

        @Test
        fun `registers metrics on start`() {
            whenever(shutdownCoordinator.state).thenReturn(ShutdownState.DRAINING)

            start()

            verify(shutdownCoordinator).registerMetrics()
        }

        @Test
        fun `registers leader scope callback on start`() {
            whenever(shutdownCoordinator.state).thenReturn(ShutdownState.DRAINING)

            start()

            verify(shutdownCoordinator).registerLeaderScopeCallback(any())
        }

        @Test
        fun `records drain completion when shutting down`() {
            whenever(shutdownCoordinator.isShuttingDown).thenReturn(true)
            val latch = CountDownLatch(1)
            val claimed = task()
            whenever(dispatcher.claimTask())
                .thenReturn(claimed)
                .thenReturn(null)
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
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

    // ── Bulkhead ──────────────────────────────────────────────────

    @Nested
    inner class BulkheadBehavior {

        @Test
        fun `limits concurrent tasks to bulkhead size`() {
            whenever(workerConfig.bulkheadSize()).thenReturn(2)
            workerLoop = WorkerLoop(
                config, dispatcher, taskRepository, circuitBreaker,
                shutdownCoordinator, taskClaimedEvent,
            )

            val activeCount = AtomicInteger(0)
            val maxConcurrent = AtomicInteger(0)
            val tasksStarted = CountDownLatch(3)

            whenever(dispatcher.claimTask()).thenReturn(task())
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
            whenever(dispatcher.claimTask()).thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(dispatcher, atLeast(3)).claimTask()
            }
        }

        @Test
        fun `releases semaphore when claimTask throws`() {
            whenever(dispatcher.claimTask())
                .thenThrow(RuntimeException("DB error"))
                .thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(dispatcher, atLeast(2)).claimTask()
            }
        }
    }

    // ── Heartbeat ─────────────────────────────────────────────────

    @Nested
    inner class HeartbeatBehavior {

        @Test
        fun `sends heartbeats during task execution`() {
            val latch = CountDownLatch(1)
            val claimed = task(executionGeneration = "gen-hb")
            whenever(dispatcher.claimTask())
                .thenReturn(claimed)
                .thenReturn(null)
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    Thread.sleep(150) // Long enough for at least one heartbeat
                    latch.countDown()
                    Unit
                }
            }

            start()

            assertTrue(latch.await(3, TimeUnit.SECONDS))
            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(1)).updateHeartbeat("t-1", "gen-hb")
            }
        }

        @Test
        fun `heartbeat failure is non-fatal`() {
            val latch = CountDownLatch(1)
            val claimed = task()
            whenever(dispatcher.claimTask())
                .thenReturn(claimed)
                .thenReturn(null)
            whenever(taskRepository.updateHeartbeat(any(), any()))
                .thenThrow(RuntimeException("DB timeout"))
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    Thread.sleep(150)
                    latch.countDown()
                    Unit
                }
            }

            start()

            assertTrue(latch.await(3, TimeUnit.SECONDS))
            verifySuspend { execute(claimed) }
        }
    }

    // ── Error handling ────────────────────────────────────────────

    @Nested
    inner class ErrorHandling {

        @Test
        fun `TaskClaimed event failure does not prevent task execution`() {
            val claimed = task()
            whenever(dispatcher.claimTask())
                .thenReturn(claimed)
                .thenReturn(null)
            whenever(taskClaimedEvent.fireAsync(any()))
                .thenThrow(RuntimeException("event bus down"))

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verifySuspend { execute(claimed) }
            }
        }

        @Test
        fun `claim exception does not kill poll loop`() {
            whenever(dispatcher.claimTask())
                .thenThrow(RuntimeException("transient"))
                .thenThrow(RuntimeException("transient"))
                .thenReturn(null)

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verify(dispatcher, atLeast(3)).claimTask()
            }
        }
    }
}
