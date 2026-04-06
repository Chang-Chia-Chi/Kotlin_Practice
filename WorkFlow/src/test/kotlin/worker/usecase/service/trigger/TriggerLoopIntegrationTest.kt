package com.workflow.worker.usecase.service.trigger

import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.service.TaskSettler
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.port.inbound.orchestration.PhaseGate
import com.workflow.workflow.usecase.port.outbound.persistent.TaskRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import java.time.Duration
import java.time.Instant

/**
 * Mock-based integration tests that verify the full TriggerLoop flow:
 * findDeferred -> dispatch to drivers -> poll results -> settle via PhaseGate.
 *
 * These complement [TriggerLoopTest] by testing multi-step scenarios that
 * exercise the interplay between deferred task discovery, driver dispatch,
 * result settlement, and retry cycles.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class TriggerLoopIntegrationTest {

    private lateinit var taskRepo: TaskRepository
    private lateinit var phaseGate: PhaseGate
    private lateinit var leaderGuard: LeaderGuard
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var config: TriggerLoopConfig
    private lateinit var shutdownConfig: ShutdownConfig
    private lateinit var taskSettler: TaskSettler

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
        taskSettler = TaskSettler(taskRepo, phaseGate)
    }

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

    private fun initLoop(loop: TriggerLoop) {
        val scope = TestScope(SupervisorJob())
        val job = loop.start(scope)
        job.cancel()
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Task 3: Handler defers task, TriggerLoop settles it
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class DeferAndSettle {

        @Test
        fun `handler defers task and TriggerLoop settles it as COMPLETED`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "sql-exec" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(driver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            // Simulate a deferred task discovered by findDeferred
            val ref = makeDeferredRef(
                taskId = "t-deferred-1",
                workflowId = "wf-defer",
                sequenceNumber = 1,
                triggerType = "sql-exec",
                triggerMeta = """{"datasource":"test","sql":"SELECT 1"}""",
            )
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }

            // Driver returns Succeeded on poll
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-deferred-1", """{"rows":1}"""),
                )
            }

            loop.sweep()

            // Verify the driver was started with the deferred task
            verify(driver).start(eq(listOf(ref)))

            // Verify phaseGate.onTaskCompleted was called with COMPLETED
            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-deferred-1"),
                workflowId = eq("wf-defer"),
                sequenceNumber = eq(1),
                status = eq(TaskStatus.COMPLETED),
                resultJson = eq("""{"rows":1}"""),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Task 4: Mixed workflow — multiple tasks with different trigger types
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class MixedTriggerTypes {

        @Test
        fun `multiple tasks with different trigger types dispatched to correct drivers`() = runTest {
            val sqlDriver = mock<TriggerDriver> { on { type() } doReturn "sql-exec" }
            val k8sDriver = mock<TriggerDriver> { on { type() } doReturn "k8s-job" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(sqlDriver, k8sDriver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            val sqlRef = makeDeferredRef(
                taskId = "t-sql-1",
                workflowId = "wf-mixed",
                sequenceNumber = 1,
                triggerType = "sql-exec",
                triggerMeta = """{"sql":"SELECT 1"}""",
            )
            val k8sRef = makeDeferredRef(
                taskId = "t-k8s-1",
                workflowId = "wf-mixed",
                sequenceNumber = 2,
                triggerType = "k8s-job",
                triggerMeta = """{"job":"batch-process"}""",
            )

            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(sqlRef, k8sRef) }

            // Each driver returns Succeeded for its own task
            sqlDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-sql-1", """{"result":"sql-ok"}"""),
                )
            }
            k8sDriver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-k8s-1", """{"result":"k8s-ok"}"""),
                )
            }

            loop.sweep()

            // Verify each driver got only its tasks
            verify(sqlDriver).start(eq(listOf(sqlRef)))
            verify(k8sDriver).start(eq(listOf(k8sRef)))

            // Verify both tasks settled as COMPLETED
            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-sql-1"),
                workflowId = eq("wf-mixed"),
                sequenceNumber = eq(1),
                status = eq(TaskStatus.COMPLETED),
                resultJson = eq("""{"result":"sql-ok"}"""),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )
            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-k8s-1"),
                workflowId = eq("wf-mixed"),
                sequenceNumber = eq(2),
                status = eq(TaskStatus.COMPLETED),
                resultJson = eq("""{"result":"k8s-ok"}"""),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )
        }

        @Test
        fun `tasks from same workflow at different sequences settled independently`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "sql-exec" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(driver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            val ref1 = makeDeferredRef(
                taskId = "t-1", workflowId = "wf-1", sequenceNumber = 1,
                triggerType = "sql-exec",
            )
            val ref2 = makeDeferredRef(
                taskId = "t-2", workflowId = "wf-1", sequenceNumber = 3,
                triggerType = "sql-exec",
            )

            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref1, ref2) }
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-1", "ok1"),
                    TriggerResult.Failed("t-2", "failed"),
                )
            }

            loop.sweep()

            // t-1 settles as COMPLETED
            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-1"),
                workflowId = eq("wf-1"),
                sequenceNumber = eq(1),
                status = eq(TaskStatus.COMPLETED),
                resultJson = eq("ok1"),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )

            // t-2 has retries remaining (retryCount=0, maxRetries=3) -> resetForRetry
            verify(taskRepo).resetForRetry(eq("t-2"), eq(1))
            // phaseGate should NOT be called for t-2 (retried, not failed)
            verify(phaseGate, never()).onTaskCompleted(
                taskId = eq("t-2"),
                workflowId = any(),
                sequenceNumber = any(),
                status = eq(TaskStatus.FAILED),
                resultJson = any(),
                claimedBy = any(),
                claimedAt = any(),
            )
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Deadline enforcement (spec 7.2) and orphaned trigger type handling
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class DeadlineEnforcement {

        @Test
        fun `expired DEFERRED task triggers driver cancel and TIMED_OUT settlement`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "sql-exec" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(driver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            val ref = makeDeferredRef(
                taskId = "t-expired",
                workflowId = "wf-expired",
                sequenceNumber = 1,
                triggerType = "sql-exec",
                deadlineAt = Instant.now().minusSeconds(60),
            )
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            driver.stub { onBlocking { poll() } doReturn emptyList() }

            loop.sweep()

            verify(driver).cancel(eq("t-expired"))
            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-expired"),
                workflowId = eq("wf-expired"),
                sequenceNumber = eq(1),
                status = eq(TaskStatus.TIMED_OUT),
                resultJson = eq(null),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )
        }

        @Test
        fun `orphaned trigger type completes sweep without error and skips phaseGate`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "other-type" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(driver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            val ref = makeDeferredRef(
                taskId = "t-orphan",
                workflowId = "wf-orphan",
                sequenceNumber = 1,
                triggerType = "nonexistent-driver",
                deadlineAt = Instant.now().plusSeconds(3600),
            )
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            driver.stub { onBlocking { poll() } doReturn emptyList() }

            loop.sweep()

            verify(driver, never()).start(any())
            verify(phaseGate, never()).onTaskCompleted(
                any(), any(), any(), any(), any(), any(), any(),
            )
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Task 5: DEFERRED task fails, retries, and eventually completes
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class DeferredRetryThenComplete {

        @Test
        fun `DEFERRED task fails then retries and eventually completes`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "fail-once" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(driver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            // ── Sweep 1: task deferred, driver returns Failed ──
            val ref1 = makeDeferredRef(
                taskId = "t-retry",
                workflowId = "wf-retry",
                sequenceNumber = 1,
                triggerType = "fail-once",
                retryCount = 0,
                maxRetries = 3,
            )
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref1) }
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-retry", "Simulated transient failure"),
                )
            }

            loop.sweep()

            // resetForRetry called (retryCount=0 < maxRetries=3)
            verify(taskRepo).resetForRetry(eq("t-retry"), eq(1))
            // phaseGate NOT called for retry
            verify(phaseGate, never()).onTaskCompleted(
                taskId = eq("t-retry"), any(), any(), any(), any(), any(), any(),
            )

            // ── Sweep 2: task is back as DEFERRED after retry cycle, driver returns Succeeded ──
            val ref2 = makeDeferredRef(
                taskId = "t-retry",
                workflowId = "wf-retry",
                sequenceNumber = 1,
                triggerType = "fail-once",
                retryCount = 1,
                maxRetries = 3,
            )
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref2) }
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-retry", """{"retry":"ok"}"""),
                )
            }

            loop.sweep()

            // phaseGate called with COMPLETED
            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-retry"),
                workflowId = eq("wf-retry"),
                sequenceNumber = eq(1),
                status = eq(TaskStatus.COMPLETED),
                resultJson = eq("""{"retry":"ok"}"""),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )
        }

        @Test
        fun `DEFERRED task exhausts all retries and settles as FAILED`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "always-fail" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(driver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            // Task with retries already exhausted
            val ref = makeDeferredRef(
                taskId = "t-exhaust",
                workflowId = "wf-exhaust",
                sequenceNumber = 1,
                triggerType = "always-fail",
                retryCount = 3,
                maxRetries = 3,
            )
            taskRepo.stub { onBlocking { findDeferred() } doReturn listOf(ref) }
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-exhaust", "Permanent failure"),
                )
            }

            loop.sweep()

            // resetForRetry NOT called (retryCount == maxRetries)
            verify(taskRepo, never()).resetForRetry(any(), any())

            // phaseGate called with FAILED
            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-exhaust"),
                workflowId = eq("wf-exhaust"),
                sequenceNumber = eq(1),
                status = eq(TaskStatus.FAILED),
                resultJson = eq(null),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )
        }

        @Test
        fun `DEFERRED task fails multiple sweeps before succeeding`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "flaky" }
            val beans = mock<Instance<TriggerDriver>> {
                on { iterator() } doAnswer { mutableListOf(driver).iterator() }
            }
            val loop = TriggerLoop(
                taskRepo, beans, taskSettler, leaderGuard,
                meterRegistry, config, shutdownConfig,
            )
            initLoop(loop)

            // ── Sweep 1: retryCount=0 -> fail -> resetForRetry(1) ──
            taskRepo.stub {
                onBlocking { findDeferred() } doReturn listOf(
                    makeDeferredRef(
                        taskId = "t-flaky", workflowId = "wf-flaky", triggerType = "flaky",
                        retryCount = 0, maxRetries = 3,
                    ),
                )
            }
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-flaky", "Failure 1"),
                )
            }
            loop.sweep()
            verify(taskRepo).resetForRetry(eq("t-flaky"), eq(1))

            // ── Sweep 2: retryCount=1 -> fail -> resetForRetry(2) ──
            taskRepo.stub {
                onBlocking { findDeferred() } doReturn listOf(
                    makeDeferredRef(
                        taskId = "t-flaky", workflowId = "wf-flaky", triggerType = "flaky",
                        retryCount = 1, maxRetries = 3,
                    ),
                )
            }
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Failed("t-flaky", "Failure 2"),
                )
            }
            loop.sweep()
            verify(taskRepo).resetForRetry(eq("t-flaky"), eq(2))

            // ── Sweep 3: retryCount=2 -> succeed -> settle as COMPLETED ──
            taskRepo.stub {
                onBlocking { findDeferred() } doReturn listOf(
                    makeDeferredRef(
                        taskId = "t-flaky", workflowId = "wf-flaky", triggerType = "flaky",
                        retryCount = 2, maxRetries = 3,
                    ),
                )
            }
            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded("t-flaky", """{"ok":true}"""),
                )
            }
            loop.sweep()

            verify(phaseGate).onTaskCompleted(
                taskId = eq("t-flaky"),
                workflowId = eq("wf-flaky"),
                sequenceNumber = eq(1),
                status = eq(TaskStatus.COMPLETED),
                resultJson = eq("""{"ok":true}"""),
                claimedBy = eq(null),
                claimedAt = eq(null),
            )
        }
    }
}
