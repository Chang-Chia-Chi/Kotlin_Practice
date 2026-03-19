package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeast
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
    private lateinit var shutdownConfig: FrameworkConfig.ShutdownConfig
    private lateinit var taskRepository: TaskRepository
    private lateinit var dispatcher: TaskDispatcher
    private lateinit var workerLoop: WorkerLoop

    private fun task(
        taskId: String = "t-1",
        handler: String = "test.handler",
        queue: String = "default",
        claimToken: String = "gen-1",
    ) = Task(
        taskId = taskId,
        handler = handler,
        queue = queue,
        payload = "{}",
        status = TaskStatus.CLAIMED,
        claimedBy = "test-pod",
        claimToken = claimToken,
    )

    @BeforeEach
    fun setUp() {
        config = mock()
        workerConfig = mock()
        shutdownConfig = mock()
        taskRepository = mock()
        dispatcher = mock()

        whenever(config.worker()).thenReturn(workerConfig)
        whenever(config.shutdown()).thenReturn(shutdownConfig)
        whenever(workerConfig.pollInterval()).thenReturn(Duration.ofMillis(20))
        whenever(workerConfig.bulkheadSize()).thenReturn(4)
        whenever(workerConfig.id()).thenReturn("test-pod")
        whenever(workerConfig.queues()).thenReturn(listOf("default"))
        whenever(shutdownConfig.drainTimeout()).thenReturn(Duration.ofMillis(500))
        whenever(shutdownConfig.logInterval()).thenReturn(Duration.ofMillis(50))

        workerLoop = WorkerLoop(config, taskRepository, dispatcher)
    }

    private fun start() {
        workerLoop.onStart(mock<StartupEvent>())
    }

    private fun verifySuspend(
        mode: org.mockito.verification.VerificationMode = org.mockito.Mockito.times(1),
        block: suspend TaskDispatcher.() -> Unit,
    ) {
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

    // ── Shutdown behavior ─────────────────────────────────────────────

    @Nested
    inner class ShutdownBehavior {

        @Test
        fun `shutdownOrder is 0`() {
            assertEquals(0, workerLoop.shutdownOrder)
        }

        @Test
        fun `shutdownTimeout matches drain timeout config`() {
            assertEquals(Duration.ofMillis(500), workerLoop.shutdownTimeout)
        }

        @Test
        fun `inFlightTasks is 0 initially`() {
            assertEquals(0, workerLoop.inFlightTasks)
        }

        @Test
        fun `inFlightTasks tracks active tasks`() {
            val taskStarted = CountDownLatch(1)
            val taskCanFinish = CountDownLatch(1)
            val claimed = task()

            whenever(taskRepository.claim("test-pod", listOf("default")))
                .thenReturn(claimed)
                .thenReturn(null)
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    taskStarted.countDown()
                    taskCanFinish.await(5, TimeUnit.SECONDS)
                    Unit
                }
            }

            assertEquals(0, workerLoop.inFlightTasks)
            start()

            assertTrue(taskStarted.await(2, TimeUnit.SECONDS))
            assertEquals(1, workerLoop.inFlightTasks)

            taskCanFinish.countDown()
            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                assertEquals(0, workerLoop.inFlightTasks)
            }
        }

        @Test
        fun `shutdown stops claiming new tasks`() {
            whenever(taskRepository.claim(any(), any())).thenReturn(null)

            start()

            await.atMost(1, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(2)).claim(any(), any())
            }

            runBlocking { workerLoop.shutdown() }

            val claimsAtShutdown = org.mockito.Mockito.mockingDetails(taskRepository)
                .invocations.count { it.method.name == "claim" }
            Thread.sleep(100)
            val claimsAfter = org.mockito.Mockito.mockingDetails(taskRepository)
                .invocations.count { it.method.name == "claim" }

            assertEquals(claimsAtShutdown, claimsAfter, "No new claims after shutdown")
        }

        @Test
        fun `shutdown waits for in-flight tasks to drain`() {
            val taskStarted = CountDownLatch(1)
            val taskCanFinish = CountDownLatch(1)
            val claimed = task()

            whenever(taskRepository.claim("test-pod", listOf("default")))
                .thenReturn(claimed)
                .thenReturn(null)
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    taskStarted.countDown()
                    taskCanFinish.await(5, TimeUnit.SECONDS)
                    Unit
                }
            }

            start()
            assertTrue(taskStarted.await(2, TimeUnit.SECONDS))
            assertEquals(1, workerLoop.inFlightTasks)

            // Start shutdown in background
            val shutdownComplete = CountDownLatch(1)
            Thread {
                runBlocking { workerLoop.shutdown() }
                shutdownComplete.countDown()
            }.start()

            // Shutdown should be waiting for drain
            assertFalse(shutdownComplete.await(200, TimeUnit.MILLISECONDS))
            assertEquals(1, workerLoop.inFlightTasks)

            // Let the task finish
            taskCanFinish.countDown()

            // Now shutdown should complete
            assertTrue(shutdownComplete.await(2, TimeUnit.SECONDS))
            assertEquals(0, workerLoop.inFlightTasks)
        }

        @Test
        fun `shutdown releases uncompleted tasks`() {
            whenever(taskRepository.claim(any(), any())).thenReturn(null)

            start()
            runBlocking { workerLoop.shutdown() }

            verify(taskRepository).releaseTasksByPod("test-pod")
        }

        @Test
        fun `shutdown tolerates release exception`() {
            whenever(taskRepository.claim(any(), any())).thenReturn(null)
            whenever(taskRepository.releaseTasksByPod(any()))
                .thenThrow(RuntimeException("DB down"))

            start()
            runBlocking { workerLoop.shutdown() }

            // Should not throw — stale reaper will recover
            assertEquals(0, workerLoop.inFlightTasks)
        }

        @Test
        fun `shutdown completes immediately when no in-flight tasks`() {
            whenever(taskRepository.claim(any(), any())).thenReturn(null)

            start()

            await.atMost(1, TimeUnit.SECONDS).untilAsserted {
                verify(taskRepository, atLeast(1)).claim(any(), any())
            }

            val startTime = System.currentTimeMillis()
            runBlocking { workerLoop.shutdown() }
            val elapsed = System.currentTimeMillis() - startTime

            assertTrue(elapsed < 500, "Shutdown with no in-flight tasks should be fast (took ${elapsed}ms)")
        }
    }

    // ── Bulkhead ──────────────────────────────────────────────────────

    @Nested
    inner class BulkheadBehavior {

        @Test
        fun `limits concurrent tasks to bulkhead size`() {
            whenever(workerConfig.bulkheadSize()).thenReturn(2)
            workerLoop = WorkerLoop(config, taskRepository, dispatcher)

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

        @Test
        fun `dispatcher exception decrements inFlightTasks`() {
            val claimed = task()
            whenever(taskRepository.claim("test-pod", listOf("default")))
                .thenReturn(claimed)
                .thenReturn(null)
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    throw RuntimeException("handler blew up")
                }
            }

            start()

            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                verifySuspend { execute(claimed) }
            }
            // inFlightTasks must return to 0 even after exception (finally block)
            await.atMost(2, TimeUnit.SECONDS).untilAsserted {
                assertEquals(0, workerLoop.inFlightTasks,
                    "inFlightTasks must decrement even when dispatcher throws")
            }
        }

        @Test
        fun `dispatcher exception on one task does not block subsequent claims`() {
            val callCount = AtomicInteger(0)
            whenever(taskRepository.claim("test-pod", listOf("default")))
                .thenReturn(task())
            runBlocking {
                whenever(dispatcher.execute(any())).thenAnswer {
                    val n = callCount.incrementAndGet()
                    if (n == 1) throw RuntimeException("first task fails")
                    // Subsequent tasks succeed
                    Unit
                }
            }

            start()

            // The loop should continue claiming even after the first dispatch failure
            await.atMost(3, TimeUnit.SECONDS).untilAsserted {
                assertTrue(callCount.get() >= 2,
                    "Expected at least 2 dispatch attempts, got ${callCount.get()}")
            }
        }
    }

    // ── Shutdown edge cases ──────────────────────────────────────────

    @Nested
    inner class ShutdownEdgeCases {

        @Test
        fun `shutdown before onStart completes immediately`() {
            // Never call start() — worker not running
            val startTime = System.currentTimeMillis()
            runBlocking { workerLoop.shutdown() }
            val elapsed = System.currentTimeMillis() - startTime

            assertTrue(elapsed < 500, "Shutdown with no running loop should be instant (took ${elapsed}ms)")
            assertEquals(0, workerLoop.inFlightTasks)
        }
    }
}
