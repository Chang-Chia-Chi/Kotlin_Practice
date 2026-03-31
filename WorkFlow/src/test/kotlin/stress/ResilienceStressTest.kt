package com.workflow.stress

import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import eu.rekawek.toxiproxy.model.ToxicDirection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@Tag("stress")
@Tag("stress-network")
class ResilienceStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- R1: Oracle unavailable then recovers ----

    @Test
    fun `R1 - Oracle outage then recovery - workflows complete`() = runBlocking(Dispatchers.Default) {
        val batchSize = scale.workflowBatchSize
        val def = workflow {
            activity("step1") { transition("r1.handler") }
        }

        handlerRegistry.register("r1.handler", PassThroughHandler())

        val wfIds = (1..batchSize).map {
            engine.startWorkflow(def).workflowId.also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()

        // Let some workflows start processing
        delay(pollInterval.toMillis() * 2)

        // Cut Oracle connection
        oracleProxy.toxics().bandwidth("cut-r1", ToxicDirection.DOWNSTREAM, 0)

        // Hold outage
        delay(3000)

        // Restore
        oracleProxy.toxics().get("cut-r1").remove()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R2: Oracle latency spike ----

    @Test
    fun `R2 - Oracle latency spike - no spurious timeouts and backlog drains`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") {
                transition("r2.handler")
                deadline(Duration.ofSeconds(30)) // Generous deadline
            }
        }

        handlerRegistry.register("r2.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def).workflowId.also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()

        // Inject latency
        oracleProxy.toxics().latency("slow-r2", ToxicDirection.DOWNSTREAM, 3000)

        delay(5000)

        // Remove latency
        oracleProxy.toxics().get("slow-r2").remove()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
        }
        sweepJob.cancel()
    }

    // ---- R3: Connection pool exhaustion ----

    @Test
    fun `R3 - connection pool exhaustion - workers back off and recover`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("r3.handler"); retries(5) }
        }

        handlerRegistry.register("r3.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def).workflowId.also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()

        // Throttle bandwidth to simulate pool pressure
        oracleProxy.toxics().limitData("throttle-r3", ToxicDirection.DOWNSTREAM, 512)

        delay(5000)

        // Release throttle
        oracleProxy.toxics().get("throttle-r3").remove()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R4: Full worker pool dies and restarts ----

    @Test
    fun `R4 - worker pool death and restart - all workflows recover`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("r4.handler"); retries(3) }
        }

        handlerRegistry.register("r4.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def).workflowId.also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        // Start and let workers claim tasks
        val jobs = startWorkerPool()
        delay(pollInterval.toMillis() * 3)

        // Kill all workers
        jobs.forEach { it.cancelAndJoin() }
        workerJobs.clear()

        // Age stale tasks past threshold
        directJdbi.useHandle<Exception> { handle ->
            handle.createUpdate(
                "UPDATE task SET claimed_at = :ts WHERE status = 'PROCESSING'",
            ).bind("ts", java.time.LocalDateTime.ofInstant(
                Instant.now().minus(staleTaskThreshold.multipliedBy(2)),
                java.time.ZoneOffset.UTC,
            )).execute()
        }

        // Restart fresh workers
        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R5: No leader for extended period, then elected ----

    @Test
    fun `R5 - leaderless period then recovery - stuck workflows batch recovered`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("r5.handler") }
            activity("step2") { transition("r5.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)

        // Create stuck workflows (simulating no sweeper running)
        val wfIds = (1..scale.workflowBatchSize).map { i ->
            val wfId = randomId()
            diagnostics.trackedWorkflows.add(wfId)
            insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
            insertTaskDirect(
                workflowId = wfId,
                sequenceNumber = 1,
                status = "COMPLETED",
                handlerKey = "r5.handler",
                result = """{"test":"R5-$i"}""",
            )
            updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(3)))
            wfId
        }

        handlerRegistry.register("r5.handler", PassThroughHandler())
        startWorkerPool()

        // Wait (simulating leaderless period — no sweeps)
        delay(2000)

        // "New leader elected" — start sweeping
        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
        }
        sweepJob.cancel()
    }

    // ---- R6: Network partition heals after multiple stale reclaim cycles ----

    @Test
    fun `R6 - extended partition then heal - system converges`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("r6.handler"); retries(5) }
        }

        handlerRegistry.register("r6.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def).workflowId.also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()
        delay(pollInterval.toMillis() * 2)

        // Extended outage (longer than stale threshold)
        oracleProxy.toxics().bandwidth("cut-r6", ToxicDirection.DOWNSTREAM, 0)
        delay(staleTaskThreshold.toMillis() * 2)
        oracleProxy.toxics().get("cut-r6").remove()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }

    // ---- R7: Rapid leader election flaps ----

    @Test
    fun `R7 - rapid leader flaps - no orphaned state`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("r7.handler") }
            activity("step2") { transition("r7.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "r7.handler",
            result = """{"test":"R7"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        handlerRegistry.register("r7.handler", PassThroughHandler())
        startWorkerPool()

        // Simulate rapid leader flaps: sweep, pause, sweep, pause, sweep
        repeat(4) {
            runSweep()
            delay(200)
        }

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // No duplicate tasks from flapping sweeps
        assertTaskCount(wfId, 2, 1)
        sweepJob.cancel()
    }

    // ---- R8: Oracle restarts (connections reset) ----

    @Test
    fun `R8 - Oracle connection reset - pool reconnects and workflows resume`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("r8.handler"); retries(3) }
        }

        handlerRegistry.register("r8.handler", PassThroughHandler())

        val wfIds = (1..scale.workflowBatchSize).map {
            engine.startWorkflow(def).workflowId.also {
                diagnostics.trackedWorkflows.add(it)
            }
        }

        startWorkerPool()
        delay(pollInterval.toMillis() * 2)

        // Simulate connection reset (disable then re-enable proxy)
        oracleProxy.toxics().resetPeer("reset-r8", ToxicDirection.DOWNSTREAM, 0)
        delay(1000)
        oracleProxy.toxics().get("reset-r8").remove()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        for (wfId in wfIds) {
            assertWorkflowTerminates(wfId)
        }
        sweepJob.cancel()
    }
}
