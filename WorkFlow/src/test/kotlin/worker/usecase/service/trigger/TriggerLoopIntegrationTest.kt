package com.workflow.worker.usecase.service.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.service.TaskSettler
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Oracle-backed E2E integration tests for [TriggerLoop]:
 * DEFERRED → sweep → COMPLETED, retry lifecycle, cancel, and mixed workflows.
 *
 * Unlike the mock-based [TriggerLoopTest], these tests wire real [JdbiTaskRepository],
 * [DefaultPhaseGate], and [WorkflowEngine] against [OracleTestContainer], using test
 * [TriggerDriver] implementations to control trigger outcomes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TriggerLoopIntegrationTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var phaseGate: DefaultPhaseGate
    private lateinit var engine: WorkflowEngine
    private val notifier = FakeWorkerNotifier()

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = JdbiWorkflowRepository(jdbi)
        taskRepo = JdbiTaskRepository(jdbi)
        phaseGate = DefaultPhaseGate(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper, notifier)
    }

    @AfterEach
    fun cleanTables() {
        runCatching {
            jdbi.useHandle<Exception> { handle -> handle.execute("DELETE FROM task") }
        }
        runCatching {
            jdbi.useHandle<Exception> { handle -> handle.execute("DELETE FROM workflow") }
        }
    }

    private fun buildLoop(vararg drivers: TriggerDriver): TriggerLoop {
        val beans = mock<Instance<TriggerDriver>> {
            on { iterator() } doAnswer { drivers.toMutableList().iterator() }
        }
        val triggerLoopConfig = object : TriggerLoopConfig {
            override fun sweepInterval(): Duration = Duration.ofSeconds(60)
            override fun sqlMaxConcurrent(): Int = 2
        }
        val shutdownConfig = object : ShutdownConfig {
            override fun globalTimeout(): Duration = Duration.ofSeconds(30)
            override fun leaderTeardownTimeout(): Duration = Duration.ofSeconds(10)
        }
        val taskSettler = TaskSettler(taskRepo, phaseGate)
        val loop = TriggerLoop(
            taskRepo = taskRepo,
            driverBeans = beans,
            taskSettler = taskSettler,
            leaderGuard = LeaderGuard.ALWAYS,
            meterRegistry = SimpleMeterRegistry(),
            triggerLoopConfig = triggerLoopConfig,
            shutdownConfig = shutdownConfig,
        )
        // Initialize drivers map; immediately cancel the background sweep coroutine —
        // tests call loop.sweep() directly for deterministic control.
        val job = loop.start(TestScope(SupervisorJob()))
        job.cancel()
        return loop
    }

    /**
     * Creates a 1-step workflow via [WorkflowEngine] and returns it in DEFERRED state.
     * Uses the real claim → defer lifecycle so [DefaultPhaseGate] can route correctly
     * after sweep (the workflow definition's sequence map must be populated).
     */
    private suspend fun insertDeferredTask(
        handlerKey: String,
        triggerType: String,
        triggerMeta: String,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        queueName: String = "default",
    ): Pair<String, String> {
        val definition = workflow {
            activity("step1") {
                transition(handlerKey)
                retries(maxRetries)
            }
        }
        val wfId = engine.startWorkflow(definition).workflowId
        val tasks = taskRepo.claimNext("test-worker", 1, queueName)
        check(tasks.isNotEmpty()) { "insertDeferredTask: no task for workflow $wfId" }
        val taskId = tasks.first().id
        taskRepo.defer(taskId, triggerType, triggerMeta)
        if (retryCount > 0) {
            jdbi.useHandle<Exception> { h ->
                h.execute("UPDATE task SET retry_count = ? WHERE id = ?", retryCount, taskId)
            }
        }
        return wfId to taskId
    }

    /**
     * Brings a PENDING task (after [resetForRetry]) back to DEFERRED so the next sweep
     * can pick it up. Clears [not_before] to bypass exponential back-off.
     */
    private suspend fun reClaimAndDefer(taskId: String, triggerType: String, triggerMeta: String) {
        jdbi.useHandle<Exception> { h ->
            h.execute("UPDATE task SET not_before = NULL WHERE id = ?", taskId)
        }
        val claimed = taskRepo.claimNext("test-worker", 1)
        check(claimed.any { it.id == taskId }) { "reClaimAndDefer: taskId $taskId not claimed" }
        taskRepo.defer(taskId, triggerType, triggerMeta)
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec 1: DEFERRED → sweep → COMPLETED
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class DeferAndSettle {

        @Test
        fun `DEFERRED task swept by TriggerLoop settles workflow as COMPLETED`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "test-trigger" }
            val loop = buildLoop(driver)

            val (wfId, taskId) = insertDeferredTask(
                handlerKey = "test-handler",
                triggerType = "test-trigger",
                triggerMeta = """{"key":"value"}""",
            )

            driver.stub {
                onBlocking { poll() } doReturn listOf(TriggerResult.Succeeded(taskId, """{"result":"ok"}"""))
            }
            loop.sweep()

            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)?.status)
            assertTrue(taskRepo.findDeferred().isEmpty())
        }

        @Test
        fun `DEFERRED task driver started with correct ref data`() = runTest {
            val startedRefs = mutableListOf<DeferredTaskRef>()
            val driver = object : TriggerDriver {
                override fun type() = "test-trigger"
                override suspend fun start(tasks: List<DeferredTaskRef>) { startedRefs.addAll(tasks) }
                override suspend fun poll() = startedRefs.map { TriggerResult.Succeeded(it.taskId, "{}") }
                override suspend fun cancel(taskId: String) {}
                override suspend fun close() {}
            }
            val loop = buildLoop(driver)

            val (wfId, taskId) = insertDeferredTask(
                handlerKey = "test-handler",
                triggerType = "test-trigger",
                triggerMeta = """{"meta":"data"}""",
            )
            loop.sweep()

            assertEquals(1, startedRefs.size)
            assertEquals(taskId, startedRefs.first().taskId)
            assertEquals(wfId, startedRefs.first().workflowId)
            assertEquals("test-trigger", startedRefs.first().triggerType)
            assertEquals("""{"meta":"data"}""", startedRefs.first().triggerMeta)
            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)?.status)
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec 2: fail → reClaimAndDefer → second sweep → COMPLETED
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class RetryThenComplete {

        @Test
        fun `driver failure triggers retry and second sweep completes the workflow`() = runTest {
            val (wfId, taskId) = insertDeferredTask(
                handlerKey = "test-handler",
                triggerType = "test-trigger",
                triggerMeta = "{}",
                maxRetries = 3,
            )

            val failingDriver = mock<TriggerDriver> { on { type() } doReturn "test-trigger" }
            failingDriver.stub {
                onBlocking { poll() } doReturn listOf(TriggerResult.Failed(taskId, "transient error"))
            }
            buildLoop(failingDriver).sweep()

            assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)?.status)

            reClaimAndDefer(taskId, "test-trigger", "{}")

            val succeedingDriver = mock<TriggerDriver> { on { type() } doReturn "test-trigger" }
            succeedingDriver.stub {
                onBlocking { poll() } doReturn listOf(TriggerResult.Succeeded(taskId, """{"ok":true}"""))
            }
            buildLoop(succeedingDriver).sweep()

            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)?.status)
            assertTrue(taskRepo.findDeferred().isEmpty())
        }

        @Test
        fun `task exhausting all retries settles workflow as FAILED`() = runTest {
            val (wfId, taskId) = insertDeferredTask(
                handlerKey = "test-handler",
                triggerType = "test-trigger",
                triggerMeta = "{}",
                retryCount = 3,
                maxRetries = 3,
            )

            val driver = mock<TriggerDriver> { on { type() } doReturn "test-trigger" }
            driver.stub {
                onBlocking { poll() } doReturn listOf(TriggerResult.Failed(taskId, "permanent error"))
            }
            buildLoop(driver).sweep()

            assertEquals(WorkflowStatus.FAILED, workflowRepo.findById(wfId)?.status)
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec 3: cancelPendingTasksWithHandle cancels DEFERRED tasks
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class CancelDeferred {

        @Test
        fun `cancelPendingTasksWithHandle removes DEFERRED task from deferred list`() = runTest {
            val (wfId, _) = insertDeferredTask(
                handlerKey = "test-handler",
                triggerType = "test-trigger",
                triggerMeta = "{}",
            )

            assertEquals(1, taskRepo.findDeferred().count { it.workflowId == wfId })

            jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.cancelPendingTasksWithHandle(handle, wfId)
            }

            assertTrue(taskRepo.findDeferred().none { it.workflowId == wfId })
        }

        @Test
        fun `cancelPendingTasksWithHandle cancels all DEFERRED tasks in the workflow`() = runTest {
            val (wfId, _) = insertDeferredTask(
                handlerKey = "handler-a",
                triggerType = "test-trigger",
                triggerMeta = """{"step":1}""",
            )
            // Insert a second DEFERRED task for the same workflow directly.
            // Safe here because cancelPendingTasksWithHandle filters by workflowId only and
            // never reads the workflow definition — no PhaseGate routing is triggered.
            val taskId2 = UUID.randomUUID().toString()
            jdbi.useHandle<Exception> { handle ->
                taskRepo.insertBatchWithHandle(
                    handle,
                    listOf(
                        Task(
                            id = taskId2,
                            workflowId = wfId,
                            activityName = "step2",
                            sequenceNumber = 2,
                            status = TaskStatus.DEFERRED,
                            handlerKey = "handler-b",
                            item = null,
                            resultJson = null,
                            claimedBy = null,
                            claimedAt = null,
                            completedAt = null,
                            retryCount = 0,
                            maxRetries = 3,
                            deadlineAt = Instant.now().plusSeconds(3600),
                            triggerType = "test-trigger",
                            triggerMeta = """{"step":2}""",
                        )
                    )
                )
            }

            assertEquals(2, taskRepo.findDeferred().count { it.workflowId == wfId })

            jdbi.inTransaction<Int, Exception> { handle ->
                taskRepo.cancelPendingTasksWithHandle(handle, wfId)
            }

            assertTrue(taskRepo.findDeferred().none { it.workflowId == wfId })
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec 4: 2-step workflow via engine → claim → phaseGate → claim → defer → sweep
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class MixedWorkflow {

        @Test
        fun `2-step workflow completes via engine startWorkflow then trigger sweep`() = runTest {
            val driver = mock<TriggerDriver> { on { type() } doReturn "test-trigger" }
            val loop = buildLoop(driver)

            val definition = workflow {
                activity("step-1") {
                    transition("handler-step1")
                    next("step-2")
                }
                activity("step-2") {
                    transition("handler-step2")
                }
            }

            val wfId = engine.startWorkflow(definition).workflowId

            val step1Claims = taskRepo.claimNext("test-worker", 1)
            assertEquals(1, step1Claims.size)
            val step1 = step1Claims.first()

            phaseGate.onTaskCompleted(
                taskId = step1.id,
                workflowId = wfId,
                sequenceNumber = step1.sequenceNumber,
                status = TaskStatus.COMPLETED,
                resultJson = "{}",
            )

            val step2Claims = taskRepo.claimNext("test-worker", 1)
            assertEquals(1, step2Claims.size)
            val step2TaskId = step2Claims.first().id

            taskRepo.defer(step2TaskId, "test-trigger", "{}")
            assertTrue(taskRepo.findDeferred().any { it.taskId == step2TaskId })

            driver.stub {
                onBlocking { poll() } doReturn listOf(
                    TriggerResult.Succeeded(step2TaskId, """{"final":"result"}"""),
                )
            }
            loop.sweep()

            assertEquals(WorkflowStatus.COMPLETED, workflowRepo.findById(wfId)?.status)
            assertTrue(taskRepo.findDeferred().isEmpty())
        }

        @Test
        fun `2-step workflow remains RUNNING when step-2 trigger fails with retries remaining`() = runTest {
            val definition = workflow {
                activity("step-1") {
                    transition("handler-step1")
                    next("step-2")
                }
                activity("step-2") {
                    transition("handler-step2")
                    retries(2)
                }
            }

            val wfId = engine.startWorkflow(definition).workflowId

            val step1Claims = taskRepo.claimNext("test-worker", 1)
            val step1 = step1Claims.first()
            phaseGate.onTaskCompleted(
                taskId = step1.id,
                workflowId = wfId,
                sequenceNumber = step1.sequenceNumber,
                status = TaskStatus.COMPLETED,
                resultJson = "{}",
            )

            val step2Claims = taskRepo.claimNext("test-worker", 1)
            val step2TaskId = step2Claims.first().id
            taskRepo.defer(step2TaskId, "test-trigger", "{}")

            val driver = mock<TriggerDriver> { on { type() } doReturn "test-trigger" }
            driver.stub {
                onBlocking { poll() } doReturn listOf(TriggerResult.Failed(step2TaskId, "not ready"))
            }
            buildLoop(driver).sweep()

            assertEquals(WorkflowStatus.RUNNING, workflowRepo.findById(wfId)?.status)
        }
    }
}
