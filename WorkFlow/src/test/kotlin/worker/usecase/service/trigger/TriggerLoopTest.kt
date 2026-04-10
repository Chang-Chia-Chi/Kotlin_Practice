package com.workflow.worker.usecase.service.trigger

import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.service.TaskSettler
import com.workflow.workflow.model.TaskCompletionEvent
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class TriggerLoopTest {

    // ── Shared mocks ────────────────────────────────────────────────────

    private lateinit var taskRepo: TaskRepository
    private lateinit var phaseGate: PhaseGate
    private lateinit var leaderGuard: LeaderGuard
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var config: TriggerLoopConfig
    private lateinit var shutdownConfig: ShutdownConfig
    private lateinit var mockDriver: TriggerDriver
    private lateinit var driverBeans: Instance<TriggerDriver>
    private lateinit var taskSettler: TaskSettler
    private lateinit var triggerLoop: TriggerLoop

    @BeforeEach
    fun setUp() {
        taskRepo = mock()
        phaseGate = mock()
        leaderGuard = mock { on { isLeader } doReturn true }
        meterRegistry = SimpleMeterRegistry()
        config = mock {
            on { sweepInterval() } doReturn Duration.ofSeconds(5)
            on { sqlMaxConcurrent() } doReturn 2
        }
        shutdownConfig = mock { on { globalTimeout() } doReturn Duration.ofSeconds(30) }
        mockDriver = mock { on { type() } doReturn "test-driver" }
        driverBeans = mock {
            on { iterator() } doAnswer { mutableListOf(mockDriver).iterator() }
        }
        taskSettler = TaskSettler(taskRepo, phaseGate)

        triggerLoop = TriggerLoop(
            taskRepo, driverBeans, taskSettler, leaderGuard,
            meterRegistry, config, shutdownConfig,
        )
        // Initialize lateinit fields (drivers, pollCounter, sweepTimer) by
        // calling start() on a throw-away scope and immediately cancelling.
        initLoop(triggerLoop)
    }

    /**
     * Triggers internal initialization of the TriggerLoop so that
     * sweep()/shutdown() can be called directly in tests.
     */
    private fun initLoop(loop: TriggerLoop) {
        val scope = TestScope(SupervisorJob())
        val job = loop.start(scope)
        job.cancel()
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun makeDeferredRef(
        taskId: String = "t-1",
        workflowId: String = "wf-1",
        sequenceNumber: Int = 1,
        triggerType: String = "test-driver",
        triggerMeta: String = "{}",
        deadlineAt: Instant? = Instant.now().plusSeconds(3600),
        retryCount: Int = 0,
        maxRetries: Int = 3,
    ) = DeferredTaskRef(
        taskId = taskId,
        workflowId = workflowId,
        sequenceNumber = sequenceNumber,
        triggerType = triggerType,
        triggerMeta = triggerMeta,
        deadlineAt = deadlineAt,
        retryCount = retryCount,
        maxRetries = maxRetries,
    )

    private fun counterCount(name: String, vararg tags: String): Double {
        val counter = meterRegistry.find(name).tags(*tags).counter()
        return counter?.count() ?: 0.0
    }

    // ── A. Happy Paths ──────────────────────────────────────────────────

    @Nested
    inner class HappyPaths {

        @Test
        fun `sweep dispatches DEFERRED tasks to matching driver`() = runTest {
            val ref = makeDeferredRef()
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            verify(mockDriver).start(eq(listOf(ref)))
        }

        @Test
        fun `Succeeded result settles task as COMPLETED via phaseGate`() = runTest {
            val ref = makeDeferredRef()
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-1", """{"ok":true}"""),
                )
            }

            triggerLoop.sweep()

            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.COMPLETED, """{"ok":true}""")),
            )
            assertEquals(1.0, counterCount("trigger_settled_total", "type", "test-driver", "outcome", "succeeded"))
        }

        @Test
        fun `Failed result with retries remaining calls resetForRetry`() = runTest {
            val ref = makeDeferredRef(retryCount = 0, maxRetries = 3)
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-1", "Job failed"),
                )
            }

            taskRepo.stub { onBlocking { resetForRetry(eq("t-1"), eq(1), anyOrNull(), anyOrNull()) } doReturn true }

            triggerLoop.sweep()

            verify(taskRepo).resetForRetry(eq("t-1"), eq(1), anyOrNull(), anyOrNull())
            verify(phaseGate, never()).onTaskCompleted(any())
            assertEquals(1.0, counterCount("trigger_settled_total", "type", "test-driver", "outcome", "retried"))
        }

        @Test
        fun `Failed result with retries exhausted settles as FAILED`() = runTest {
            val ref = makeDeferredRef(retryCount = 3, maxRetries = 3)
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-1", "Job failed permanently"),
                )
            }

            triggerLoop.sweep()

            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.FAILED, null)),
            )
            assertEquals(1.0, counterCount("trigger_settled_total", "type", "test-driver", "outcome", "failed"))
        }

        @Test
        fun `sweep increments poll counter and records timer`() = runTest {
            taskRepo.stub { onBlocking { findDeferred() } doReturn emptyList() }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            assertEquals(1.0, meterRegistry.find("trigger_poll_total").counter()!!.count())
            assertTrue(meterRegistry.find("trigger_sweep_duration_seconds").timer()!!.count() > 0)
        }
    }

    // ── B. Edge Cases ───────────────────────────────────────────────────

    @Nested
    inner class EdgeCases {

        @Test
        fun `sweep skips when not leader - no findDeferred call`() = runTest {
            whenever(leaderGuard.isLeader).thenReturn(false)

            triggerLoop.sweep()

            verify(taskRepo, never()).findDeferred()
        }

        @Test
        fun `orphaned tasks with no driver for type are skipped`() = runTest {
            val ref = makeDeferredRef(triggerType = "unknown-driver")
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            // Driver.start() should NOT be called with orphaned tasks
            verify(mockDriver, never()).start(any())
        }

        @Test
        fun `unknown taskId in poll result is skipped`() = runTest {
            val ref = makeDeferredRef(taskId = "t-1")
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-unknown", "data"),
                )
            }

            triggerLoop.sweep()

            // phaseGate should not be called for unknown tasks
            verify(phaseGate, never()).onTaskCompleted(any())
        }

        @Test
        fun `Succeeded with null result settles correctly`() = runTest {
            val ref = makeDeferredRef()
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-1", null),
                )
            }

            triggerLoop.sweep()

            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.COMPLETED, null)),
            )
        }

        @Test
        fun `empty deferred list causes no dispatch and no settlement`() = runTest {
            taskRepo.stub { onBlocking { findDeferred() } doReturn emptyList() }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            verify(mockDriver, never()).start(any())
            verifyNoInteractions(phaseGate)
        }

        @Test
        fun `expired task triggers cancel and TIMED_OUT settlement`() = runTest {
            val ref = makeDeferredRef(deadlineAt = Instant.now().minusSeconds(60))
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            verify(mockDriver).cancel(eq("t-1"))
            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.TIMED_OUT, null)),
            )
            assertEquals(1.0, counterCount("trigger_settled_total", "type", "test-driver", "outcome", "expired"))
        }

        @Test
        fun `task with null deadlineAt is not expired`() = runTest {
            val ref = makeDeferredRef(deadlineAt = null)
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            verify(mockDriver, never()).cancel(any())
            verify(phaseGate, never()).onTaskCompleted(any())
        }

        @Test
        fun `task settled via poll is not double-expired when deadline has passed`() = runTest {
            val ref = makeDeferredRef(deadlineAt = Instant.now().minusSeconds(60))
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-1", "ok"),
                )
            }

            triggerLoop.sweep()

            // phaseGate should be called exactly once — as COMPLETED, not TIMED_OUT
            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.COMPLETED, "ok")),
            )
            verify(phaseGate, never()).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.TIMED_OUT, null)),
            )
            verify(mockDriver, never()).cancel(any())
        }

        @Test
        fun `task with future deadline is not expired`() = runTest {
            val ref = makeDeferredRef(deadlineAt = Instant.now().plusSeconds(3600))
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            verify(mockDriver, never()).cancel(any())
        }

        @Test
        fun `multiple tasks grouped by type dispatch to correct drivers`() = runTest {
            val localRegistry = SimpleMeterRegistry()
            val driver2 = mock<TriggerDriver> { on { type() } doReturn "other-driver" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(mockDriver, driver2).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                localRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            val ref1 = makeDeferredRef(taskId = "t-1", triggerType = "test-driver")
            val ref2 = makeDeferredRef(taskId = "t-2", triggerType = "other-driver")
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref1, ref2) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }
            driver2.stub { onBlocking { poll() } doReturn emptyList() }

            loop.sweep()

            verify(mockDriver).start(eq(listOf(ref1)))
            verify(driver2).start(eq(listOf(ref2)))
        }

        @Test
        fun `deferred gauge tracks task count`() = runTest {
            val ref1 = makeDeferredRef(taskId = "t-1")
            val ref2 = makeDeferredRef(taskId = "t-2")
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref1, ref2) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            triggerLoop.sweep()

            val gauge = meterRegistry.find("trigger_deferred_tasks").gauge()
            assertTrue(gauge != null)
            assertEquals(2.0, gauge!!.value())
        }
    }

    // ── C. Error Handling ───────────────────────────────────────────────

    @Nested
    inner class ErrorHandling {

        @Test
        fun `driver start() throws - other drivers still dispatched`() = runTest {
            val localRegistry = SimpleMeterRegistry()
            val failingDriver = mock<TriggerDriver> { on { type() } doReturn "failing-driver" }
            val goodDriver = mock<TriggerDriver> { on { type() } doReturn "good-driver" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(failingDriver, goodDriver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                localRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            val ref1 = makeDeferredRef(taskId = "t-1", triggerType = "failing-driver")
            val ref2 = makeDeferredRef(taskId = "t-2", triggerType = "good-driver")
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref1, ref2) }
            failingDriver.stub { onBlocking { start(any()) } doThrow RuntimeException("driver crashed") }
            failingDriver.stub { onBlocking { poll() } doReturn emptyList() }
            goodDriver.stub { onBlocking { poll() } doReturn emptyList() }

            loop.sweep()

            verify(goodDriver).start(eq(listOf(ref2)))
        }

        @Test
        fun `driver poll() throws - other drivers still polled`() = runTest {
            val localRegistry = SimpleMeterRegistry()
            val failingDriver = mock<TriggerDriver> { on { type() } doReturn "failing-driver" }
            val goodDriver = mock<TriggerDriver> { on { type() } doReturn "good-driver" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(failingDriver, goodDriver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                localRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            val ref = makeDeferredRef(taskId = "t-1", triggerType = "good-driver")
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            failingDriver.stub { onBlocking { poll() } doThrow RuntimeException("poll crashed") }
            goodDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-1", "ok"),
                )
            }

            loop.sweep()

            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.COMPLETED, "ok")),
            )
        }

        @Test
        fun `phaseGate onTaskCompleted throws in settleResult - sweep continues`() = runTest {
            val ref1 = makeDeferredRef(taskId = "t-1")
            val ref2 = makeDeferredRef(taskId = "t-2")
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref1, ref2) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-1", "data1"),
                    TriggerResult.Succeeded("t-2", "data2"),
                )
            }
            phaseGate.stub {
                onBlocking {
                    onTaskCompleted(eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.COMPLETED, "data1")))
                } doThrow RuntimeException("phaseGate failed")
            }

            triggerLoop.sweep()

            // Second task should still be settled despite first failure
            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-2", "wf-1", 1, TaskStatus.COMPLETED, "data2")),
            )
        }

        @Test
        fun `resetForRetry throws - falls back to FAILED via phaseGate`() = runTest {
            val ref = makeDeferredRef(retryCount = 0, maxRetries = 3)
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            taskRepo.stub {
                onBlocking { resetForRetry(eq("t-1"), eq(1), anyOrNull(), anyOrNull()) } doThrow RuntimeException("DB error")
            }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-1", "Job failed"),
                )
            }

            triggerLoop.sweep()

            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.FAILED, null)),
            )
            assertEquals(1.0, counterCount("trigger_settled_total", "type", "test-driver", "outcome", "failed"))
        }

        @Test
        fun `expire task cancel throws - TIMED_OUT still settled`() = runTest {
            val ref = makeDeferredRef(deadlineAt = Instant.now().minusSeconds(60))
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }
            mockDriver.stub {
                onBlocking { cancel(eq("t-1")) } doThrow RuntimeException("cancel failed")
            }

            triggerLoop.sweep()

            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.TIMED_OUT, null)),
            )
        }

        @Test
        fun `CancellationException from phaseGate propagates out of sweep`() = runTest {
            val ref = makeDeferredRef()
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-1", "data"),
                )
            }
            phaseGate.stub {
                onBlocking {
                    onTaskCompleted(eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.COMPLETED, "data")))
                } doThrow CancellationException("cancelled")
            }

            kotlin.test.assertFailsWith<CancellationException> {
                triggerLoop.sweep()
            }
        }

        @Test
        fun `expire phaseGate throws - does not crash sweep`() = runTest {
            val ref1 = makeDeferredRef(taskId = "t-1", deadlineAt = Instant.now().minusSeconds(60))
            val ref2 = makeDeferredRef(taskId = "t-2", deadlineAt = Instant.now().minusSeconds(60))
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref1, ref2) }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }
            phaseGate.stub {
                onBlocking {
                    onTaskCompleted(eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.TIMED_OUT, null)))
                } doThrow RuntimeException("phaseGate error")
            }

            triggerLoop.sweep()

            // Second expiry should still be attempted
            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-2", "wf-1", 1, TaskStatus.TIMED_OUT, null)),
            )
        }
    }

    // ── D. Shutdown ─────────────────────────────────────────────────────

    @Nested
    inner class Shutdown {

        @Test
        fun `shutdown calls close on all drivers`() = runTest {
            // Ensure sweep is a no-op so the loop doesn't interfere
            whenever(leaderGuard.isLeader).thenReturn(false)
            taskRepo.stub { onBlocking { findDeferred() } doReturn emptyList() }
            mockDriver.stub { onBlocking { poll() } doReturn emptyList() }

            val job = triggerLoop.start(this)
            advanceTimeBy(100)
            triggerLoop.shutdown()

            verify(mockDriver).close()
        }

        @Test
        fun `shutdown with driver close() throwing - other drivers still closed`() = runTest {
            whenever(leaderGuard.isLeader).thenReturn(false)
            val localRegistry = SimpleMeterRegistry()
            val failingDriver = mock<TriggerDriver> { on { type() } doReturn "failing-driver" }
            val goodDriver = mock<TriggerDriver> { on { type() } doReturn "good-driver" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(failingDriver, goodDriver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                localRegistry, config, shutdownConfig,
            )

            val job = loop.start(this)
            advanceTimeBy(100)

            failingDriver.stub {
                onBlocking { close() } doThrow RuntimeException("close failed")
            }

            loop.shutdown()

            verify(failingDriver).close()
            verify(goodDriver).close()
        }

        @Test
        fun `shutdownOrder is SHUTDOWN_ORDER_TRIGGER`() {
            assertEquals(SHUTDOWN_ORDER_TRIGGER, triggerLoop.shutdownOrder)
            assertEquals(5, triggerLoop.shutdownOrder)
        }

        @Test
        fun `shutdownTimeout delegates to shutdownConfig globalTimeout`() {
            assertEquals(Duration.ofSeconds(30), triggerLoop.shutdownTimeout)
        }
    }

    // ── E. Retry Edge Cases ─────────────────────────────────────────────

    @Nested
    inner class RetryEdgeCases {

        @Test
        fun `Failed at retry boundary - retryCount equals maxRetries minus 1`() = runTest {
            val ref = makeDeferredRef(retryCount = 2, maxRetries = 3)
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-1", "transient"),
                )
            }

            taskRepo.stub { onBlocking { resetForRetry(eq("t-1"), eq(3), anyOrNull(), anyOrNull()) } doReturn true }

            triggerLoop.sweep()

            verify(taskRepo).resetForRetry(eq("t-1"), eq(3), anyOrNull(), anyOrNull())
            assertEquals(1.0, counterCount("trigger_settled_total", "type", "test-driver", "outcome", "retried"))
        }

        @Test
        fun `Failed with zero maxRetries settles immediately as FAILED`() = runTest {
            val ref = makeDeferredRef(retryCount = 0, maxRetries = 0)
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            mockDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-1", "no retries"),
                )
            }

            triggerLoop.sweep()

            verify(phaseGate).onTaskCompleted(
                eq(TaskCompletionEvent("t-1", "wf-1", 1, TaskStatus.FAILED, null)),
            )
            verify(taskRepo, never()).resetForRetry(any(), any(), anyOrNull(), anyOrNull())
        }
    }
}
