package com.workflow.stress

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.config.WorkerLoopConfig
import com.workflow.workflow.config.WatchdogConfig
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.persistence.ToxiproxyTestContainer
import com.workflow.workflow.adapter.persistent.JdbiTaskRepository
import com.workflow.workflow.adapter.persistent.JdbiWorkflowRepository
import com.workflow.workflow.usecase.service.orchestration.DefaultPhaseGate
import com.workflow.workflow.usecase.service.orchestration.ActivityInputResolver
import com.workflow.workflow.usecase.service.orchestration.WorkflowWatchdog
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine

import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import com.workflow.worker.adapter.http.HttpWorkerNotifier
import com.workflow.worker.usecase.service.execution.HandlerRegistry
import com.workflow.worker.adapter.http.PeerRegistry
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import jakarta.enterprise.inject.Instance
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import com.workflow.worker.usecase.service.execution.WorkerLoop
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestWatcher
import org.testcontainers.containers.ToxiproxyContainer
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.TreeMap
import java.util.UUID
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class StressTestBase {

    protected val scale: StressScale = StressScale.resolve()

    // --- Containers & JDBI (through Toxiproxy) ---

    protected lateinit var proxyJdbi: Jdbi
    protected lateinit var directJdbi: Jdbi
    private lateinit var directPooledJdbi: Jdbi
    protected lateinit var oracleProxy: ToxiproxyContainer.ContainerProxy
    private lateinit var proxyDataSource: HikariDataSource
    private lateinit var directDataSource: HikariDataSource
    protected val faultInjector = FaultInjector()

    // --- Components (proxy path — for fault injection and resilience tests) ---

    protected lateinit var workflowRepo: JdbiWorkflowRepository
    protected lateinit var activityInputResolver: ActivityInputResolver
    protected lateinit var taskRepo: JdbiTaskRepository
    protected lateinit var engine: WorkflowEngine
    protected lateinit var barrier: DefaultPhaseGate
    protected lateinit var watchdog: WorkflowWatchdog
    protected lateinit var handlerRegistry: HandlerRegistry
    protected lateinit var meterRegistry: SimpleMeterRegistry

    // --- Direct components (bypass proxy — for throughput benchmarks) ---

    protected lateinit var directWorkflowRepo: JdbiWorkflowRepository
    protected lateinit var directTaskRepo: JdbiTaskRepository
    protected lateinit var directEngine: WorkflowEngine
    protected lateinit var directBarrier: DefaultPhaseGate
    protected lateinit var directWorkflowWatchdog: WorkflowWatchdog

    protected val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())

    // --- Worker pool ---

    protected val workerJobs = mutableListOf<Job>()
    private val workerScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // --- Config ---

    protected open val gracePeriod: Duration = Duration.ofSeconds(2)
    protected open val sweepInterval: Duration = Duration.ofSeconds(1)
    protected open val staleTaskThreshold: Duration = Duration.ofSeconds(3)
    protected open val pollInterval: Duration = Duration.ofMillis(200)
    protected open val workerConcurrency: Int = scale.workers

    protected val notifier: WorkerNotifier = run {
        val registry = mock<PeerRegistry>()
        whenever(registry.peers()).thenReturn(emptyList())
        HttpWorkerNotifier(registry, HttpClient(MockEngine { respond("") }))
    }

    protected val testWorkerConfig: WorkerLoopConfig by lazy {
        object : WorkerLoopConfig {
            override fun id() = "stress-worker"
            override fun pollInterval() = this@StressTestBase.pollInterval
            override fun concurrency() = workerConcurrency
            override fun batchSize() = 1
            override fun fallbackPollInterval() = this@StressTestBase.pollInterval
            override fun maxBatchSize() = 16
            override fun podIp() = "localhost"
            override fun serviceName() = "workflow-engine"
        }
    }

    protected val testShutdownConfig: ShutdownConfig by lazy {
        object : ShutdownConfig {
            override fun globalTimeout() = Duration.ofSeconds(10)
            override fun leaderTeardownTimeout() = Duration.ofSeconds(5)
        }
    }

    protected val testWatchdogConfig: WatchdogConfig by lazy {
        object : WatchdogConfig {
            override fun interval() = this@StressTestBase.sweepInterval
            override fun gracePeriod() = this@StressTestBase.gracePeriod
            override fun staleTaskThreshold() = this@StressTestBase.staleTaskThreshold
        }
    }

    @BeforeAll
    fun initInfrastructure() {
        // Direct JDBI (bypass proxy — for setup/assertions)
        directJdbi = OracleTestContainer.jdbi

        // Pooled direct DataSource (bypass proxy — for throughput benchmarks)
        val oracle = OracleTestContainer.oracle
        directDataSource = HikariDataSource(HikariConfig().apply {
            jdbcUrl = oracle.jdbcUrl
            username = oracle.username
            password = oracle.password
            maximumPoolSize = 20
            minimumIdle = 2
            connectionTimeout = 10_000
        })
        directPooledJdbi = Jdbi.create(directDataSource)

        // Toxiproxy wrapping Oracle — shared singleton, one container per JVM
        oracleProxy = ToxiproxyTestContainer.sharedOracleProxy()

        // Pooled JDBI through Toxiproxy (production components use this)
        Class.forName("oracle.jdbc.OracleDriver")
        val proxyUrl = "jdbc:oracle:thin:@${oracleProxy.containerIpAddress}:${oracleProxy.proxyPort}/testdb"
        proxyDataSource = HikariDataSource(HikariConfig().apply {
            jdbcUrl = proxyUrl
            username = "testuser"
            password = "testpass"
            maximumPoolSize = 20
            minimumIdle = 2
            connectionTimeout = 10_000
        })
        proxyJdbi = Jdbi.create(FaultInjectingDataSource(proxyDataSource, faultInjector))

        // Init components
        workflowRepo = JdbiWorkflowRepository(proxyJdbi)
        taskRepo = JdbiTaskRepository(proxyJdbi)
        barrier = DefaultPhaseGate(proxyJdbi, workflowRepo, taskRepo, objectMapper, notifier)
        engine = WorkflowEngine(proxyJdbi, workflowRepo, taskRepo, objectMapper, notifier)
        watchdog = WorkflowWatchdog(proxyJdbi, workflowRepo, taskRepo, barrier, testWatchdogConfig)
        activityInputResolver = ActivityInputResolver(objectMapper)
        val emptyBeans = mock<Instance<com.workflow.worker.usecase.port.inbound.execution.TransitionHandler>>()
        whenever(emptyBeans.iterator()).thenReturn(mutableListOf<com.workflow.worker.usecase.port.inbound.execution.TransitionHandler>().iterator())
        handlerRegistry = HandlerRegistry(emptyBeans)
        meterRegistry = SimpleMeterRegistry()

        // Init direct components (bypass proxy — for throughput benchmarks)
        directWorkflowRepo = JdbiWorkflowRepository(directPooledJdbi)
        directTaskRepo = JdbiTaskRepository(directPooledJdbi)
        directBarrier = DefaultPhaseGate(directPooledJdbi, directWorkflowRepo, directTaskRepo, objectMapper, notifier)
        directEngine = WorkflowEngine(directPooledJdbi, directWorkflowRepo, directTaskRepo, objectMapper, notifier)
        directWorkflowWatchdog = WorkflowWatchdog(directPooledJdbi, directWorkflowRepo, directTaskRepo, directBarrier, testWatchdogConfig)
    }

    @AfterAll
    fun tearDownInfrastructure() {
        (notifier as HttpWorkerNotifier).shutdown()
        proxyDataSource.close()
        directDataSource.close()
    }

    @AfterEach
    fun cleanUp() {
        runBlocking(Dispatchers.Default) {
            // Stop workers
            workerJobs.forEach { it.cancelAndJoin() }
            workerJobs.clear()
        }

        // Reset proxy (remove all toxics)
        try {
            oracleProxy.toxics().all.forEach { it.remove() }
        } catch (_: Exception) { }

        // Reset fault injection rules
        faultInjector.reset()

        // Clean tables via direct JDBI (bypasses proxy)
        directJdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    // --- Worker lifecycle ---

    protected fun startWorkers(handlerKey: String, handler: com.workflow.worker.usecase.port.inbound.execution.TransitionHandler): List<Job> {
        handlerRegistry.register(handlerKey, handler)
        return startWorkerPool()
    }

    protected fun startWorkerPool(): List<Job> {
        val taskSettler = com.workflow.worker.usecase.service.TaskSettler(taskRepo, barrier)
        val loop = WorkerLoop(testWorkerConfig, testShutdownConfig, taskRepo, handlerRegistry, taskSettler, meterRegistry, activityInputResolver, workflowRepo, objectMapper, notifier)
        val job = loop.start(workerScope)
        workerJobs.add(job)
        return listOf(job)
    }

    protected fun startDirectWorkerPool(): List<Job> {
        val directSettler = com.workflow.worker.usecase.service.TaskSettler(directTaskRepo, directBarrier)
        val loop = WorkerLoop(testWorkerConfig, testShutdownConfig, directTaskRepo, handlerRegistry, directSettler, meterRegistry, activityInputResolver, directWorkflowRepo, objectMapper, notifier)
        val job = loop.start(workerScope)
        workerJobs.add(job)
        return listOf(job)
    }

    // --- WorkflowWatchdog ---

    protected suspend fun runSweep() {
        watchdog.patrol()
    }

    protected suspend fun runDirectSweep() {
        directWorkflowWatchdog.patrol()
    }

    // --- Assertions ---

    protected fun assertWorkflowTerminates(workflowId: String, timeout: Duration = scale.outerTimeout) {
        await atMost timeout untilAsserted {
            val wf = readWorkflowDirect(workflowId)
            assertNotNull(wf, "Workflow $workflowId not found")
            val status = wf["STATUS"]?.toString()
            assertContains(
                listOf("COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"),
                status,
                "Workflow $workflowId stuck in status $status",
            )
        }
    }

    protected fun assertWorkflowStatus(workflowId: String, expected: String, timeout: Duration = scale.outerTimeout) {
        await atMost timeout untilAsserted {
            val wf = readWorkflowDirect(workflowId)
            assertNotNull(wf)
            assertEquals(expected, wf["STATUS"]?.toString())
        }
    }

    protected fun assertWorkflowWatchdogRecovers(
        workflowId: String,
        previousSequence: Int,
        timeout: Duration = gracePeriod + sweepInterval + scale.innerMargin,
    ) {
        await atMost timeout untilAsserted {
            val wf = readWorkflowDirect(workflowId)
            assertNotNull(wf)
            val ver = (wf["VERSION"] as Number).toInt()
            assertTrue(ver > previousSequence, "Workflow $workflowId version still at $previousSequence")
        }
    }

    protected fun assertTaskCount(workflowId: String, sequenceNumber: Int, expectedCount: Int) {
        val count = countTasksDirect(workflowId, sequenceNumber)
        assertEquals(expectedCount, count, "Expected $expectedCount tasks at seq $sequenceNumber, got $count")
    }

    protected fun assertNoTaskDuplicates(workflowId: String, sequenceNumber: Int) {
        val tasks = readTasksDirect(workflowId, sequenceNumber)
        val ids = tasks.map { it["ID"]?.toString() }
        assertEquals(ids.size, ids.toSet().size, "Duplicate task IDs found at seq $sequenceNumber")
    }

    // --- Direct SQL helpers (bypass proxy, always work) ---

    protected fun readWorkflowDirect(workflowId: String): Map<String, Any?>? =
        directJdbi.withHandle<Map<String, Any?>?, Exception> { handle ->
            handle.createQuery("SELECT * FROM workflow WHERE id = :id")
                .bind("id", workflowId)
                .mapToMap()
                .findOne()
                .orElse(null)
                ?.let { caseInsensitive(it) }
        }

    protected fun readTasksDirect(workflowId: String, sequenceNumber: Int? = null): List<Map<String, Any?>> =
        directJdbi.withHandle<List<Map<String, Any?>>, Exception> { handle ->
            val sql = if (sequenceNumber != null) {
                "SELECT * FROM task WHERE workflow_id = :wfId AND sequence_number = :seq"
            } else {
                "SELECT * FROM task WHERE workflow_id = :wfId"
            }
            val query = handle.createQuery(sql).bind("wfId", workflowId)
            if (sequenceNumber != null) query.bind("seq", sequenceNumber)
            query.mapToMap().list().map { caseInsensitive(it) }
        }

    protected fun countTasksDirect(workflowId: String, sequenceNumber: Int): Int =
        directJdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND sequence_number = :seq",
            ).bind("wfId", workflowId).bind("seq", sequenceNumber)
                .mapTo(Int::class.java).one()
        }

    protected fun countTasksWithStatusDirect(workflowId: String, status: String): Int =
        directJdbi.withHandle<Int, Exception> { handle ->
            handle.createQuery(
                "SELECT COUNT(*) FROM task WHERE workflow_id = :wfId AND status = :status",
            ).bind("wfId", workflowId).bind("status", status)
                .mapTo(Int::class.java).one()
        }

    protected fun updateWorkflowUpdatedAtDirect(workflowId: String, updatedAt: Instant) {
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE workflow SET updated_at = :ts WHERE id = :id")
                .bind("ts", LocalDateTime.ofInstant(updatedAt, ZoneOffset.UTC))
                .bind("id", workflowId)
                .execute()
        }
    }

    protected fun updateTaskClaimedAtDirect(taskId: String, claimedAt: Instant) {
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate("UPDATE task SET claimed_at = :ts WHERE id = :id")
                .bind("ts", LocalDateTime.ofInstant(claimedAt, ZoneOffset.UTC))
                .bind("id", taskId)
                .execute()
        }
    }

    protected fun insertWorkflowDirect(
        id: String,
        definitionJson: String,
        version: Int = 0,
        status: String = "RUNNING",
        deadlineAt: Instant = Instant.now().plus(1, ChronoUnit.HOURS),
    ) {
        val now = LocalDateTime.ofInstant(Instant.now().truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC)
        val deadline = LocalDateTime.ofInstant(deadlineAt.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC)
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO workflow (id, definition, version, status, created_at, updated_at, deadline_at)
                   VALUES (:id, :def, :ver, :status, :now, :now, :deadline)""",
            )
                .bind("id", id)
                .bind("def", definitionJson)
                .bind("ver", version)
                .bind("status", status)
                .bind("now", now)
                .bind("deadline", deadline)
                .execute()
        }
    }

    protected fun insertTaskDirect(
        id: String = UUID.randomUUID().toString(),
        workflowId: String,
        sequenceNumber: Int = 1,
        status: String = "PENDING",
        handlerKey: String = "test.handler",
        item: String? = null,
        result: String? = null,
        claimedBy: String? = null,
        claimedAt: Instant? = null,
        retryCount: Int = 0,
        maxRetries: Int = 3,
        deadlineAt: Instant? = Instant.now().plus(30, ChronoUnit.MINUTES),
    ): String {
        val now = LocalDateTime.ofInstant(Instant.now().truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC)
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                """INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, item, result,
                   claimed_by, claimed_at, retry_count, max_retries, deadline_at, enqueued_at)
                   VALUES (:id, :wfId, :seq, :status, :key, :item, :result,
                   :claimedBy, :claimedAt, :retryCount, :maxRetries, :deadlineAt, :enqueuedAt)""",
            )
                .bind("id", id)
                .bind("wfId", workflowId)
                .bind("seq", sequenceNumber)
                .bind("status", status)
                .bind("key", handlerKey)
                .bind("item", item)
                .bind("result", result)
                .bind("claimedBy", claimedBy)
                .apply {
                    if (claimedAt != null) {
                        bind("claimedAt", LocalDateTime.ofInstant(claimedAt.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC))
                    } else {
                        bindNull("claimedAt", java.sql.Types.TIMESTAMP)
                    }
                    if (deadlineAt != null) {
                        bind("deadlineAt", LocalDateTime.ofInstant(deadlineAt.truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC))
                    } else {
                        bindNull("deadlineAt", java.sql.Types.TIMESTAMP)
                    }
                }
                .bind("retryCount", retryCount)
                .bind("maxRetries", maxRetries)
                .bind("enqueuedAt", now)
                .execute()
        }
        return id
    }

    // --- Table cleanup (for mid-test resets, e.g. C5 boundary tests) ---

    protected fun cleanUpTables() {
        runBlocking(Dispatchers.Default) {
            workerJobs.forEach { it.cancelAndJoin() }
            workerJobs.clear()
        }
        directJdbi.useHandle<Exception> { handle ->
            handle.execute("DELETE FROM task")
            handle.execute("DELETE FROM workflow")
        }
    }

    // --- Utilities ---

    protected fun randomId(): String = UUID.randomUUID().toString()

    protected fun now(): Instant = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun caseInsensitive(map: Map<String, Any?>): Map<String, Any?> {
        val ci = TreeMap<String, Any?>(String.CASE_INSENSITIVE_ORDER)
        ci.putAll(map)
        // Handle CLOBs
        for ((k, v) in ci) {
            if (v is java.sql.Clob) ci[k] = v.characterStream.readText()
        }
        return ci
    }

    internal fun dumpState(workflowId: String): String = buildString {
        val wf = readWorkflowDirect(workflowId)
        appendLine("=== Workflow $workflowId ===")
        appendLine("  status=${wf?.get("STATUS")}, ver=${wf?.get("VERSION")}")
        val tasks = readTasksDirect(workflowId)
        for (t in tasks) {
            appendLine("  task=${t["ID"]} seq=${t["SEQUENCE_NUMBER"]} status=${t["STATUS"]} claimed=${t["CLAIMED_BY"]} retry=${t["RETRY_COUNT"]}")
        }
    }
}

/**
 * JUnit extension that dumps workflow/task state on test failure.
 * Tests register workflow IDs via [trackedWorkflows] for diagnostic output.
 */
class StressTestDiagnostics(private val base: StressTestBase) : TestWatcher {
    val trackedWorkflows = mutableListOf<String>()

    override fun testFailed(context: ExtensionContext?, cause: Throwable?) {
        if (trackedWorkflows.isEmpty()) return
        System.err.println("=== STRESS TEST DIAGNOSTIC DUMP ===")
        for (wfId in trackedWorkflows) {
            System.err.println(base.dumpState(wfId))
        }
    }
}
