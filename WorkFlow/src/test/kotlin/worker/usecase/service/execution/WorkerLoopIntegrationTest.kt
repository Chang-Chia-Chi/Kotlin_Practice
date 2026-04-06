package com.workflow.worker.usecase.service.execution

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.adapter.http.FakeWorkerNotifier
import com.workflow.worker.config.WorkerLoopConfig
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import com.workflow.worker.usecase.service.TaskSettler
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.usecase.service.orchestration.ActivityInputResolver
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import java.time.Duration
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerLoopIntegrationTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: JdbiWorkflowRepository
    private lateinit var taskRepo: JdbiTaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var engine: WorkflowEngine
    private lateinit var phaseGate: DefaultPhaseGate
    private val notifier = FakeWorkerNotifier()

    private val testWorkerConfig = object : WorkerLoopConfig {
        override fun id(): String = "e2e-worker"
        override fun pollInterval(): Duration = Duration.ofMillis(200)
        override fun fallbackPollInterval(): Duration = Duration.ofMillis(200)
        override fun concurrency(): Int = 1
        override fun batchSize(): Int = 1
        override fun maxBatchSize(): Int = 1
        override fun podIp(): String = "localhost"
        override fun serviceName(): String = "workflow-engine"
    }

    private val testShutdownConfig = object : ShutdownConfig {
        override fun globalTimeout(): Duration = Duration.ofSeconds(10)
        override fun leaderTeardownTimeout(): Duration = Duration.ofSeconds(5)
    }

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
        jdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    private fun buildWorkerLoop(vararg handlers: TransitionHandler): Job {
        val handlerBeans = mock<Instance<TransitionHandler>> {
            on { iterator() } doReturn Collections.emptyIterator()
        }
        val handlerRegistry = HandlerRegistry(handlerBeans)
        handlers.forEach { handlerRegistry.register(it.key(), it) }

        val taskSettler = TaskSettler(taskRepo, phaseGate)
        val inputResolver = ActivityInputResolver(objectMapper)
        val loop = WorkerLoop(
            testWorkerConfig,
            testShutdownConfig,
            taskRepo,
            handlerRegistry,
            taskSettler,
            SimpleMeterRegistry(),
            inputResolver,
            workflowRepo,
            objectMapper,
            notifier,
        )
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val job = loop.start(scope)
        return job
    }

    @Nested
    inner class LinearWorkflow {

        @Test
        fun `WorkerLoop claims and completes 2-step linear workflow end-to-end`() {
            val handler = object : TransitionHandler {
                override fun key(): String = "e2e.complete"
                override suspend fun execute(input: HandlerInput): HandlerResult =
                    HandlerResult.Completed("""{"step":${input.sequenceNumber}}""")
            }

            val job = buildWorkerLoop(handler)
            try {
                val definition = workflow {
                    activity("step1") { transition("e2e.complete"); next("step2") }
                    activity("step2") { transition("e2e.complete") }
                }
                val wfId = runBlocking { engine.startWorkflow(definition, idempotencyKey = null, initialItem = null) }.workflowId

                await().atMost(Duration.ofSeconds(30)).untilAsserted {
                    val wf = runBlocking { workflowRepo.findById(wfId) }
                    assertEquals(WorkflowStatus.COMPLETED, wf?.status)
                }
            } finally {
                job.cancel()
            }
        }
    }

    @Nested
    inner class RetryPath {

        @Test
        fun `WorkerLoop retries failed task and succeeds on second attempt`() {
            val attempts = AtomicInteger(0)
            val handler = object : TransitionHandler {
                override fun key(): String = "e2e.fail-once"
                override suspend fun execute(input: HandlerInput): HandlerResult {
                    val attempt = attempts.incrementAndGet()
                    if (attempt == 1) throw RuntimeException("Simulated transient failure")
                    return HandlerResult.Completed("""{"attempt":$attempt}""")
                }
            }

            val job = buildWorkerLoop(handler)
            try {
                val definition = workflow {
                    activity("step1") { transition("e2e.fail-once"); retries(3) }
                }
                val wfId = runBlocking { engine.startWorkflow(definition, idempotencyKey = null, initialItem = null) }.workflowId

                await().atMost(Duration.ofSeconds(30)).untilAsserted {
                    val wf = runBlocking { workflowRepo.findById(wfId) }
                    assertEquals(WorkflowStatus.COMPLETED, wf?.status)
                }

                assertEquals(2, attempts.get())
            } finally {
                job.cancel()
            }
        }
    }
}
