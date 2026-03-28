package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.config.FrameworkConfig
import com.workflow.dsl.JoinPolicy
import com.workflow.dsl.workflow
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Clob
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkflowIntegrationTest {

    private lateinit var jdbi: Jdbi
    private lateinit var workflowRepo: WorkflowRepository
    private lateinit var taskRepo: TaskRepository
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())
    private lateinit var engine: WorkflowEngine
    private lateinit var barrier: BarrierService
    private lateinit var sweeper: Sweeper

    private val gracePeriod = Duration.ofMinutes(2)
    private val staleTaskThreshold = Duration.ofMinutes(10)

    private val testConfig = object : FrameworkConfig {
        override fun worker() = object : FrameworkConfig.WorkerConfig {
            override fun id() = "test-worker"
            override fun pollInterval(): Duration = Duration.ofSeconds(1)
            override fun concurrency() = 4
            override fun batchSize() = 1
        }

        override fun leaderElection() = object : FrameworkConfig.LeaderElectionConfig {
            override fun namespace() = "default"
            override fun leaseName() = "test-lease"
            override fun leaseDuration(): Duration = Duration.ofSeconds(15)
            override fun renewDeadline(): Duration = Duration.ofSeconds(10)
            override fun retryPeriod(): Duration = Duration.ofSeconds(2)
            override fun healthThreshold(): Duration = Duration.ofSeconds(45)
        }

        override fun shutdown() = object : FrameworkConfig.ShutdownConfig {
            override fun globalTimeout(): Duration = Duration.ofSeconds(30)
            override fun leaderTeardownTimeout(): Duration = Duration.ofSeconds(10)
        }

        override fun sweeper() = object : FrameworkConfig.SweeperConfig {
            override fun interval(): Duration = Duration.ofSeconds(30)
            override fun gracePeriod(): Duration = gracePeriod
            override fun staleTaskThreshold(): Duration = staleTaskThreshold
        }
    }

    @BeforeAll
    fun setup() {
        jdbi = OracleTestContainer.jdbi
        workflowRepo = WorkflowRepository(jdbi)
        taskRepo = TaskRepository(jdbi)
        engine = WorkflowEngine(jdbi, workflowRepo, taskRepo, objectMapper)
        barrier = BarrierService(jdbi, workflowRepo, taskRepo, objectMapper, PhaseStrategyRegistry(objectMapper))
        sweeper = Sweeper(jdbi, workflowRepo, taskRepo, barrier, testConfig)
    }

    @AfterEach
    fun cleanTables() {
        jdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun readWorkflowDirect(id: String): Map<String, Any?>? {
        return jdbi.withHandle<Map<String, Any?>?, Exception> { handle ->
            handle.createQuery("SELECT * FROM workflow WHERE id = :id")
                .bind("id", id)
                .mapToMap()
                .findOne()
                .map { raw ->
                    val ci = java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
                    raw.forEach { (k, v) -> ci[k] = if (v is Clob) v.characterStream.readText() else v }
                    ci
                }
                .orElse(null)
        }
    }

    private fun countTasksDirect(workflowId: String, sequenceNumber: Int): Int {
        return jdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND sequence_number = :seq",
            )
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .mapTo(Int::class.java)
                .one()
        }
    }

    private fun readTasksDirect(workflowId: String, sequenceNumber: Int): List<Map<String, Any?>> {
        return jdbi.withHandle<List<Map<String, Any?>>, Exception> { handle ->
            handle.createQuery(
                "SELECT * FROM task WHERE workflow_id = :wfId AND sequence_number = :seq",
            )
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .mapToMap()
                .list()
                .map { raw ->
                    val ci = java.util.TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
                    raw.forEach { (k, v) -> ci[k] = if (v is Clob) v.characterStream.readText() else v }
                    ci
                }
        }
    }

    private fun updateWorkflowUpdatedAtDirect(id: String, updatedAt: Instant) {
        jdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE workflow SET updated_at = :updatedAt WHERE id = :id")
                .bind("id", id)
                .bind("updatedAt", LocalDateTime.ofInstant(updatedAt, ZoneOffset.UTC))
                .execute()
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #17: Linear workflow end-to-end
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class LinearWorkflowE2E {

        @Test
        fun `3-activity linear workflow completes end-to-end`() = runTest {
            val definition = workflow {
                activity("validate") { transition("order.validate") }
                activity("process") { transition("order.process") }
                activity("notify") { transition("order.notify") }
            }
            // Start workflow
            val runId = engine.startWorkflow(definition)

            // Verify: workflow RUNNING at seq 1, one PENDING task
            var wf = readWorkflowDirect(runId)!!
            assertEquals("RUNNING", wf["STATUS"])
            assertEquals(1, (wf["CURRENT_SEQUENCE"] as Number).toInt())

            var tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            assertEquals(1, tasks.size)
            assertEquals("order.validate", tasks[0].handlerKey)

            // Complete task 1 with result
            val task1Result = """{"validated":true}"""
            barrier.onTaskCompleted(
                tasks[0].id, runId, 1, TaskStatus.COMPLETED, task1Result,
            )

            // Verify: workflow advanced to seq 2
            wf = readWorkflowDirect(runId)!!
            assertEquals(2, (wf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (wf["VERSION"] as Number).toInt())

            tasks = taskRepo.findByWorkflowAndSequence(runId, 2)
            assertEquals(1, tasks.size)
            assertEquals("order.process", tasks[0].handlerKey)

            // Complete task 2 with result
            val task2Result = """{"processed":true}"""
            barrier.onTaskCompleted(
                tasks[0].id, runId, 2, TaskStatus.COMPLETED, task2Result,
            )

            // Verify: workflow advanced to seq 3
            wf = readWorkflowDirect(runId)!!
            assertEquals(3, (wf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(2, (wf["VERSION"] as Number).toInt())

            tasks = taskRepo.findByWorkflowAndSequence(runId, 3)
            assertEquals(1, tasks.size)
            assertEquals("order.notify", tasks[0].handlerKey)

            // Complete task 3
            barrier.onTaskCompleted(
                tasks[0].id, runId, 3, TaskStatus.COMPLETED, """{"notified":true}""",
            )

            // Verify: workflow COMPLETED
            wf = readWorkflowDirect(runId)!!
            assertEquals("COMPLETED", wf["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #18: Fan-out workflow end-to-end
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class FanOutWorkflowE2E {

        @Test
        fun `scatter to 50 parallel sub-tasks then linear completes workflow`() = runTest {
            val definition = workflow {
                activity("batch") {
                    transition("batch.worker")
                    fanOut {
                        transition("batch.scatter")
                        joinPolicy(JoinPolicy.All)
                    }
                }
                activity("aggregate") { transition("batch.aggregate") }
            }
            // Start workflow — creates scatter task at seq 1
            val runId = engine.startWorkflow(definition)

            var wf = readWorkflowDirect(runId)!!
            assertEquals(1, (wf["CURRENT_SEQUENCE"] as Number).toInt())

            val scatterTasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            assertEquals(1, scatterTasks.size)
            // Scatter task uses activity.transition as handler key
            assertEquals("batch.worker", scatterTasks[0].handlerKey)

            // Complete scatter task with JSON array of 50 payloads
            val payloads = (1..50).map { """{"item":$it}""" }
            val scatterResult = objectMapper.writeValueAsString(payloads)
            barrier.onTaskCompleted(
                scatterTasks[0].id, runId, 1, TaskStatus.COMPLETED, scatterResult,
            )

            // Verify: workflow at seq 2, 50 PENDING sub-tasks created
            wf = readWorkflowDirect(runId)!!
            assertEquals(2, (wf["CURRENT_SEQUENCE"] as Number).toInt())

            val parallelTasks = taskRepo.findByWorkflowAndSequence(runId, 2)
            assertEquals(50, parallelTasks.size)
            // Parallel sub-tasks use fanOut.transition as handler key
            assertTrue(parallelTasks.all { it.handlerKey == "batch.scatter" })
            assertTrue(parallelTasks.all { it.status == TaskStatus.PENDING })
            // Each sub-task item matches one of the scatter payloads
            val actualPayloads = parallelTasks.map { it.item }.toSet()
            assertEquals(payloads.toSet(), actualPayloads)

            // Complete all 50 sub-tasks
            for (task in parallelTasks) {
                barrier.onTaskCompleted(
                    task.id, runId, 2, TaskStatus.COMPLETED, """{"done":true}""",
                )
            }

            // Verify: workflow at seq 3, JoinPolicy.All evaluated, next linear task created
            wf = readWorkflowDirect(runId)!!
            assertEquals(3, (wf["CURRENT_SEQUENCE"] as Number).toInt())

            // PARALLEL->LINEAR: payload is null (multiple parallel results, no single value)
            val aggregateTasks = taskRepo.findByWorkflowAndSequence(runId, 3)
            assertEquals(1, aggregateTasks.size)
            assertEquals("batch.aggregate", aggregateTasks[0].handlerKey)

            // Complete final task
            barrier.onTaskCompleted(
                aggregateTasks[0].id, runId, 3, TaskStatus.COMPLETED, """{"aggregated":true}""",
            )

            // Verify: workflow COMPLETED
            wf = readWorkflowDirect(runId)!!
            assertEquals("COMPLETED", wf["STATUS"])
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #19: Worker death simulation
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class WorkerDeathSimulation {

        @Test
        fun `sweeper recovers stuck workflow when worker died after task completion but before CAS`() = runTest {
            // Build a 2-step linear definition
            val definition = workflow {
                activity("step1") { transition("step1.handler") }
                activity("step2") { transition("step2.handler") }
            }

            // Start workflow normally
            val runId = engine.startWorkflow(definition)

            // Simulate worker completing task but dying before barrier:
            // Set task at seq 1 to COMPLETED directly via SQL (bypassing barrier CAS)
            val tasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            assertEquals(1, tasks.size)
            jdbi.useHandle<Exception> { handle ->
                handle.createUpdate(
                    "UPDATE task SET status = 'COMPLETED', result = :result WHERE id = :id",
                )
                    .bind("id", tasks[0].id)
                    .bind("result", """{"out":"step1-done"}""")
                    .execute()
            }

            // Workflow is still at seq 1, version 0 — CAS was never executed
            var wf = readWorkflowDirect(runId)!!
            assertEquals(1, (wf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(0, (wf["VERSION"] as Number).toInt())

            // Push updated_at into the past so sweeper's findStuck picks it up
            updateWorkflowUpdatedAtDirect(
                runId,
                Instant.now().minus(gracePeriod).minusSeconds(120),
            )

            // Sweeper patrol detects and recovers
            sweeper.patrol()

            // Verify: workflow advanced to seq 2, downstream task created
            wf = readWorkflowDirect(runId)!!
            assertEquals(2, (wf["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (wf["VERSION"] as Number).toInt())
            assertEquals("RUNNING", wf["STATUS"])

            val seq2Tasks = readTasksDirect(runId, 2)
            assertEquals(1, seq2Tasks.size)
            assertEquals("step2.handler", seq2Tasks[0]["HANDLER_KEY"])
            assertEquals("PENDING", seq2Tasks[0]["STATUS"])

            // Sweeper idempotency: second patrol is a no-op (CAS version already advanced)
            sweeper.patrol()
            val wfAfter = readWorkflowDirect(runId)!!
            assertEquals(2, (wfAfter["CURRENT_SEQUENCE"] as Number).toInt())
            assertEquals(1, (wfAfter["VERSION"] as Number).toInt())
            assertEquals(1, countTasksDirect(runId, 2))
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Spec #20: High-concurrency barrier
    // Spec calls for 100+ sub-tasks. Reduced to 20 because Oracle Free
    // container has ~20 PROCESSES and exhausts listener handlers (ORA-12516)
    // under high connection concurrency. 20 sub-tasks with Semaphore(3)
    // still exercises real CAS contention across concurrent barrier calls.
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    inner class HighConcurrencyBarrier {

        @Test
        fun `concurrent barrier completions produce exactly one phase transition`() = runTest {
            val subTaskCount = 20
            val definition = workflow {
                activity("parallel-work") {
                    transition("parallel.handler")
                    fanOut {
                        transition("scatter.handler")
                        joinPolicy(JoinPolicy.All)
                    }
                }
                activity("post-join") { transition("post.handler") }
            }

            // Start workflow — creates scatter task at seq 1
            val runId = engine.startWorkflow(definition)

            // Complete scatter with sub-task payloads
            val scatterTasks = taskRepo.findByWorkflowAndSequence(runId, 1)
            val payloads = (1..subTaskCount).map { """{"i":$it}""" }
            barrier.onTaskCompleted(
                scatterTasks[0].id, runId, 1, TaskStatus.COMPLETED,
                objectMapper.writeValueAsString(payloads),
            )

            // Verify sub-tasks at seq 2
            val parallelTasks = taskRepo.findByWorkflowAndSequence(runId, 2)
            assertEquals(subTaskCount, parallelTasks.size)

            // Complete all near-simultaneously via async/awaitAll
            // Semaphore throttles concurrent JDBC connections to avoid ORA-12516
            // (Oracle Free has limited PROCESSES). 3 concurrent barrier calls
            // still exercise real CAS contention without exhausting the listener.
            val semaphore = Semaphore(3)
            parallelTasks.map { task ->
                async {
                    semaphore.withPermit {
                        barrier.onTaskCompleted(
                            task.id, runId, 2, TaskStatus.COMPLETED, """{"ok":true}""",
                        )
                    }
                }
            }.awaitAll()

            // Verify exactly ONE phase transition: workflow at seq 3
            val wf = readWorkflowDirect(runId)!!
            assertEquals(3, (wf["CURRENT_SEQUENCE"] as Number).toInt())
            // Version should be 2 (scatter->parallel was v0->v1, parallel->linear is v1->v2)
            assertEquals(2, (wf["VERSION"] as Number).toInt())

            // Verify exactly one set of downstream tasks (no duplicates)
            val seq3Count = countTasksDirect(runId, 3)
            assertEquals(1, seq3Count)

            // The single downstream task has the correct handler
            val seq3Tasks = readTasksDirect(runId, 3)
            assertEquals("post.handler", seq3Tasks[0]["HANDLER_KEY"])
            assertEquals("PENDING", seq3Tasks[0]["STATUS"])
        }
    }
}
