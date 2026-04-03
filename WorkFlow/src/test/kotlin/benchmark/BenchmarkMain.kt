package com.workflow.benchmark

import com.workflow.workflow.model.workflowId
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.config.WorkerLoopConfig
import com.workflow.workflow.config.WatchdogConfig
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.workflow.usecase.service.orchestration.WorkflowWatchdog
import com.workflow.workflow.usecase.service.orchestration.WorkflowEngine
import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import com.workflow.worker.usecase.service.execution.HandlerRegistry
import com.workflow.worker.usecase.service.execution.WorkerLoop
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.jdbi.v3.core.Jdbi
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

private class NoOpWorkerNotifier : WorkerNotifier {
    override suspend fun signal(queueName: String) {}
    override fun onRemoteSignal(queueName: String) {}
    override suspend fun awaitWork(queueName: String, timeout: Duration): Boolean = false
}

fun main() {
    val config = BenchmarkConfig.parse()
    println("=== Benchmark Suite ===")
    println("Scale: ${config.scale} | Scenarios: ${config.scenarios} | Metrics: ${config.metricsEnabled}")

    // 1. Boot Oracle
    println("Starting Oracle container...")
    val directJdbi = OracleTestContainer.jdbi
    val oracle = OracleTestContainer.oracle
    println("Oracle ready: ${oracle.jdbcUrl}")

    // 2. Create pooled DataSource
    val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = oracle.jdbcUrl
        username = oracle.username
        password = oracle.password
        maximumPoolSize = 30
        minimumIdle = 5
        connectionTimeout = 10_000
    })
    val pooledJdbi = Jdbi.create(dataSource)

    // 3. Wire components
    val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())

    val timer = PhaseTimer()
    val notifier = NoOpWorkerNotifier()
    val workflowRepo = InstrumentedWorkflowRepository(pooledJdbi, timer)
    val taskRepo = InstrumentedTaskRepository(pooledJdbi, timer)
    val barrier = InstrumentedDefaultPhaseGate(pooledJdbi, workflowRepo, taskRepo, objectMapper, notifier, timer)
    val engine = WorkflowEngine(pooledJdbi, workflowRepo, taskRepo, objectMapper, notifier)
    val activityInputResolver = InstrumentedActivityInputResolver(objectMapper, timer)
    val emptyBeans = mock<jakarta.enterprise.inject.Instance<com.workflow.worker.usecase.port.inbound.execution.TransitionHandler>>()
    whenever(emptyBeans.iterator()).thenReturn(mutableListOf<com.workflow.worker.usecase.port.inbound.execution.TransitionHandler>().iterator())
    val handlerRegistry = HandlerRegistry(emptyBeans)

    // 4. Metrics
    val metrics = MetricsSupport.create(config.metricsEnabled)

    // 5. Generate matrix
    val allPoints = config.scenarios.flatMap { scenario ->
        BenchmarkConfig.matrixFor(config.scale, scenario)
    }
    println("Matrix: ${allPoints.size} points across ${config.scenarios.size} scenario(s)\n")

    val results = mutableListOf<ScenarioResult>()
    val oracleVersion = directJdbi.withHandle<String, Exception> { h ->
        h.createQuery("SELECT banner FROM v\$version WHERE ROWNUM = 1")
            .mapTo(String::class.java)
            .findOne()
            .orElse(oracle.dockerImageName)
    }
    val gitCommit = BenchmarkReporter.captureGitCommit()
    val timeout = BenchmarkConfig.timeoutForScale(config.scale)

    // SIGINT handler
    val shutdownRequested = java.util.concurrent.atomic.AtomicBoolean(false)
    Runtime.getRuntime().addShutdownHook(Thread {
        shutdownRequested.set(true)
        if (results.isNotEmpty()) {
            val report = buildReport(config, gitCommit, oracleVersion, results, objectMapper)
            BenchmarkReporter.saveReport(report, Path.of("benchmarks/results"), objectMapper)
            println("Partial results saved (${results.size} scenarios)")
        }
        metrics.stop()
        dataSource.close()
    })

    // 6. Warmup (discarded)
    val warmupPoint = allPoints.firstOrNull()
    if (warmupPoint != null) {
        println("Warmup run (discarded)...")
        val smallWarmup = warmupPoint.copy(workflows = 5, submissionRate = 0, durationSeconds = 0)
        runScenario(smallWarmup, engine, handlerRegistry, barrier, taskRepo, activityInputResolver,
            workflowRepo, objectMapper, timer, metrics, directJdbi, timeout, config, notifier)
        cleanTables(directJdbi)
        timer.reset()
        println("Warmup complete.\n")
    }

    // 7. Run matrix
    for ((index, point) in allPoints.withIndex()) {
        if (shutdownRequested.get()) break

        val applyPoint = applyOverrides(point, config)
        println("[${index + 1}/${allPoints.size}] Running ${applyPoint.scenarioName} ${applyPoint.toParameterMap()}")

        cleanTables(directJdbi)
        timer.reset()

        val result = runScenario(applyPoint, engine, handlerRegistry, barrier, taskRepo,
            activityInputResolver, workflowRepo, objectMapper, timer, metrics, directJdbi, timeout, config, notifier)

        if (result != null) {
            results.add(result)
            println("  ${BenchmarkReporter.formatScenarioLine(result)}")
        } else {
            println("  TIMEOUT — scenario did not complete within ${timeout.seconds}s")
            dumpDiagnostics(directJdbi)
        }
        println()
    }

    // 8. Report
    if (results.isNotEmpty()) {
        val report = buildReport(config, gitCommit, oracleVersion, results, objectMapper)
        BenchmarkReporter.saveReport(report, Path.of("benchmarks/results"), objectMapper)
        println("\n${BenchmarkReporter.formatComparisonTable(results)}")
    }

    if (config.metricsEnabled) {
        metrics.printSummary()
    }

    // 9. Shutdown
    metrics.stop()
    dataSource.close()
    println("Done.")
}

private fun runScenario(
    point: MatrixPoint,
    engine: WorkflowEngine,
    handlerRegistry: HandlerRegistry,
    barrier: InstrumentedDefaultPhaseGate,
    taskRepo: InstrumentedTaskRepository,
    activityInputResolver: InstrumentedActivityInputResolver,
    workflowRepo: InstrumentedWorkflowRepository,
    objectMapper: ObjectMapper,
    timer: PhaseTimer,
    metrics: MetricsSupport,
    directJdbi: Jdbi,
    timeout: Duration,
    config: BenchmarkRunConfig,
    notifier: WorkerNotifier,
): ScenarioResult? = runBlocking(Dispatchers.Default) {
    val definition = BenchmarkScenarios.definitionFor(point)

    // Wrap handlers with TimedHandler
    val timedBeans = mock<jakarta.enterprise.inject.Instance<com.workflow.worker.usecase.port.inbound.execution.TransitionHandler>>()
    whenever(timedBeans.iterator()).thenReturn(mutableListOf<com.workflow.worker.usecase.port.inbound.execution.TransitionHandler>().iterator())
    val timedRegistry = HandlerRegistry(timedBeans)
    BenchmarkScenarios.registerHandlers(timedRegistry, objectMapper, point)
    wrapRegistryWithTiming(handlerRegistry, timedRegistry, timer)

    val testWorkerConfig = createTestWorkerConfig(point.workers)
    val testShutdownConfig = createTestShutdownConfig()
    val testWatchdogConfig = createTestWatchdogConfig()
    val workerScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val loop = WorkerLoop(testWorkerConfig, testShutdownConfig, taskRepo, handlerRegistry, barrier, metrics.registry,
        activityInputResolver, workflowRepo, objectMapper, notifier)
    val workerJob = loop.start(workerScope)

    val watchdog = WorkflowWatchdog(directJdbi, workflowRepo, taskRepo, barrier, testWatchdogConfig)
    val sweepJob = launch(Dispatchers.IO) {
        while (isActive) {
            delay(1000)
            timer.suspendTime("watchdog.cycle") { watchdog.patrol() }
        }
    }

    val harness = EnhancedBenchmarkHarness()
    val result: ScenarioResult?

    try {
        if (point.isSustained) {
            result = runSustained(point, definition, engine, harness, timer, timeout, directJdbi)
        } else {
            result = runBatch(point, definition, engine, harness, timer, timeout, directJdbi)
        }
    } finally {
        sweepJob.cancel()
        workerJob.cancelAndJoin()
    }

    result
}

private suspend fun runBatch(
    point: MatrixPoint,
    definition: com.workflow.workflow.model.WorkflowDefinition,
    engine: WorkflowEngine,
    harness: EnhancedBenchmarkHarness,
    timer: PhaseTimer,
    timeout: Duration,
    directJdbi: Jdbi,
): ScenarioResult? {
    val wfIds = (1..point.workflows).map {
        val wfId = engine.startWorkflow(definition).workflowId
        harness.recordSubmission(wfId)
        wfId
    }

    val completed = awaitCompletions(wfIds.toSet(), harness, directJdbi, timeout)
    if (!completed) return null

    return harness.batchResult(
        label = point.scenarioName,
        tasksPerWorkflow = point.tasksPerWorkflow,
        phaseBreakdown = timer.summary(),
        parameters = point.toParameterMap(),
    )
}

private suspend fun CoroutineScope.runSustained(
    point: MatrixPoint,
    definition: com.workflow.workflow.model.WorkflowDefinition,
    engine: WorkflowEngine,
    harness: EnhancedBenchmarkHarness,
    timer: PhaseTimer,
    timeout: Duration,
    directJdbi: Jdbi,
): ScenarioResult? {
    val intervalMs = 1000L / point.submissionRate.coerceAtLeast(1)
    val durationMs = point.durationSeconds * 1000L
    val runStart = Instant.now()
    val allIds = ConcurrentHashMap.newKeySet<String>()
    val inflightSamples = mutableListOf<WindowSample>()

    val submitterJob = launch(Dispatchers.IO) {
        val end = runStart.plusMillis(durationMs)
        while (isActive && Instant.now().isBefore(end)) {
            val wfId = engine.startWorkflow(definition).workflowId
            harness.recordSubmission(wfId)
            allIds.add(wfId)
            delay(intervalMs)
        }
    }

    val samplerJob = launch(Dispatchers.IO) {
        while (isActive) {
            delay(10_000)
            inflightSamples.add(WindowSample(Instant.now(), harness.inflightCount()))
        }
    }

    val pollerJob = launch(Dispatchers.IO) {
        while (isActive) {
            delay(500)
            pollAndRecordCompletions(allIds, harness, directJdbi)
        }
    }

    submitterJob.join()

    val grace = Duration.ofSeconds(60)
    val completed = awaitCompletions(allIds, harness, directJdbi, grace)

    samplerJob.cancel()
    pollerJob.cancel()

    if (!completed && harness.completedIds().size < allIds.size / 2) return null

    return harness.sustainedResult(
        label = point.scenarioName,
        tasksPerWorkflow = point.tasksPerWorkflow,
        phaseBreakdown = timer.summary(),
        parameters = point.toParameterMap(),
        inflightSamples = inflightSamples,
    )
}

private suspend fun awaitCompletions(
    wfIds: Set<String>,
    harness: EnhancedBenchmarkHarness,
    directJdbi: Jdbi,
    timeout: Duration,
): Boolean {
    val deadline = Instant.now().plus(timeout)
    while (harness.completedIds().size < wfIds.size && Instant.now().isBefore(deadline)) {
        pollAndRecordCompletions(wfIds, harness, directJdbi)
        delay(200)
    }
    return harness.completedIds().size >= wfIds.size
}

private fun pollAndRecordCompletions(
    wfIds: Set<String>,
    harness: EnhancedBenchmarkHarness,
    directJdbi: Jdbi,
) {
    val pending = wfIds - harness.completedIds()
    if (pending.isEmpty()) return
    directJdbi.useHandle<Exception> { handle ->
        pending.chunked(500).forEach { chunk ->
            val completed = handle.createQuery(
                "SELECT id FROM workflow WHERE id IN (<ids>) AND status != 'RUNNING'",
            ).bindList("ids", chunk)
                .mapTo(String::class.java)
                .list()
            for (wfId in completed) {
                harness.recordCompletion(wfId)
            }
        }
    }
}

private fun wrapRegistryWithTiming(
    target: HandlerRegistry,
    source: HandlerRegistry,
    timer: PhaseTimer,
) {
    for (key in listOf(
        "bench.single.process",
        "bench.fanout.scatter", "bench.fanout.parallel", "bench.fanout.join",
        "bench.multistep.step",
    )) {
        try {
            val handler = source.resolve(key)
            target.register(key, TimedHandler(handler, timer))
        } catch (_: Exception) {
            // Handler not registered for this scenario — skip
        }
    }
}

private fun cleanTables(directJdbi: Jdbi) {
    directJdbi.useHandle<Exception> { handle ->
        handle.execute("DELETE FROM task")
        handle.execute("DELETE FROM workflow")
    }
}

private fun applyOverrides(point: MatrixPoint, config: BenchmarkRunConfig): MatrixPoint {
    var p = point
    config.workerOverride?.let { p = p.copy(workers = it) }
    config.fanOutOverride?.let { if (p.fanOutFactor > 0) p = p.copy(fanOutFactor = it) }
    return p
}

private fun createTestWorkerConfig(workers: Int): WorkerLoopConfig = object : WorkerLoopConfig {
    override fun id() = "bench-worker"
    override fun pollInterval() = Duration.ofMillis(100)
    override fun fallbackPollInterval() = Duration.ofSeconds(5)
    override fun concurrency() = workers
    override fun batchSize() = 1
    override fun maxBatchSize() = 16
    override fun podIp() = "localhost"
    override fun serviceName() = "workflow-engine"
}

private fun createTestShutdownConfig(): ShutdownConfig = object : ShutdownConfig {
    override fun globalTimeout() = Duration.ofSeconds(30)
    override fun leaderTeardownTimeout() = Duration.ofSeconds(5)
}

private fun createTestWatchdogConfig(): WatchdogConfig = object : WatchdogConfig {
    override fun interval() = Duration.ofSeconds(1)
    override fun gracePeriod() = Duration.ofSeconds(2)
    override fun staleTaskThreshold() = Duration.ofSeconds(3)
}

private fun dumpDiagnostics(directJdbi: Jdbi) {
    directJdbi.useHandle<Exception> { h ->
        println("  --- Diagnostic Dump ---")
        val wfCounts = h.createQuery(
            "SELECT status, COUNT(*) AS cnt FROM workflow GROUP BY status ORDER BY status",
        ).mapToMap().list()
        println("  Workflows: ${wfCounts.joinToString { "${it["STATUS"]}=${it["CNT"]}" }}")

        val taskCounts = h.createQuery(
            "SELECT status, COUNT(*) AS cnt FROM task GROUP BY status ORDER BY status",
        ).mapToMap().list()
        println("  Tasks: ${taskCounts.joinToString { "${it["STATUS"]}=${it["CNT"]}" }}")

        val stuck = h.createQuery(
            "SELECT id, current_sequence, status FROM workflow WHERE status = 'RUNNING' FETCH FIRST 5 ROWS ONLY",
        ).mapToMap().list()
        if (stuck.isNotEmpty()) {
            println("  Stuck workflows (sample):")
            for (wf in stuck) {
                println("    ${wf["ID"]} seq=${wf["CURRENT_SEQUENCE"]} status=${wf["STATUS"]}")
            }
        }
        println("  ---")
    }
}

private fun buildReport(
    config: BenchmarkRunConfig,
    gitCommit: String,
    oracleVersion: String,
    results: List<ScenarioResult>,
    objectMapper: ObjectMapper,
): BenchmarkReport = BenchmarkReport(
    timestamp = java.time.LocalDateTime.now().toString(),
    scale = config.scale.name.lowercase(),
    gitCommit = gitCommit,
    environment = BenchmarkReporter.captureEnvironment(oracleVersion),
    scenarios = results,
)
