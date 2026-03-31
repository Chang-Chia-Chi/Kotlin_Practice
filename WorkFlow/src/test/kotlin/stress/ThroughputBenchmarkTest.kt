package com.workflow.stress

import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import eu.rekawek.toxiproxy.model.ToxicDirection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@Tag("benchmark")
class ThroughputBenchmarkTest : StressTestBase() {
    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- B1: Single-activity throughput ----

    @Test
    fun `B1 - single activity throughput`() =
        runBlocking(Dispatchers.Default) {
            val n = scale.fanOutSize // 50 in MODERATE
            val def =
                workflow {
                    activity("step1") { transition("b1.handler") }
                }

            handlerRegistry.register("b1.handler", PassThroughHandler())

            val harness = BenchmarkHarness()
            startDirectWorkerPool()
            val wfIds =
                (1..n).map {
                    val wfId = directEngine.startWorkflow(def).workflowId
                    harness.recordSubmission(wfId)
                    diagnostics.trackedWorkflows.add(wfId)
                    wfId
                }

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runDirectSweep()
                    }
                }

            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
                harness.recordCompletion(wfId)
            }
            sweepJob.cancel()

            harness.result("B1: Single-Activity Throughput", tasksPerWorkflow = 1).print()
        }

    // ---- B2: Fan-out/join throughput ----

    @Test
    fun `B2 - fan-out join throughput`() =
        runBlocking(Dispatchers.Default) {
            val n = scale.workflowBatchSize // 5 in MODERATE
            val fanOut = scale.fanOutSize // 50 in MODERATE
            val def =
                workflow {
                    activity("scatter") {
                        transition("b2.scatter")
                        fanOut("parallel")
                    }
                    activity("parallel") {
                        transition("b2.parallel")
                        joinPolicy(JoinPolicy.All)
                    }
                    activity("final") { transition("b2.final") }
                }

            handlerRegistry.register(
                "b2.scatter",
                object : TransitionHandler {
                    override suspend fun execute(input: HandlerInput): HandlerOutput {
                        val payloads = (1..fanOut).map { """{"item":$it}""" }
                        return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
                    }
                },
            )
            handlerRegistry.register("b2.parallel", PassThroughHandler())
            handlerRegistry.register("b2.final", PassThroughHandler())

            val harness = BenchmarkHarness()

            val wfIds =
                (1..n).map {
                    val wfId = directEngine.startWorkflow(def).workflowId
                    harness.recordSubmission(wfId)
                    diagnostics.trackedWorkflows.add(wfId)
                    wfId
                }

            startDirectWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runDirectSweep()
                    }
                }

            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
                harness.recordCompletion(wfId)
            }
            sweepJob.cancel()

            // tasksPerWorkflow = 1 scatter + fanOut parallel + 1 final
            harness.result("B2: Fan-Out/Join Throughput", tasksPerWorkflow = 1 + fanOut + 1).print()
        }

    // ---- B3: Multi-phase pipeline throughput ----

    @Test
    fun `B3 - multi-phase pipeline throughput`() =
        runBlocking(Dispatchers.Default) {
            val n = scale.workflowBatchSize
            val def =
                workflow {
                    activity("phase1") { transition("b3.handler") }
                    activity("phase2") { transition("b3.handler") }
                    activity("phase3") { transition("b3.handler") }
                    activity("phase4") { transition("b3.handler") }
                    activity("phase5") { transition("b3.handler") }
                }

            handlerRegistry.register("b3.handler", PassThroughHandler())

            val harness = BenchmarkHarness()

            val wfIds =
                (1..n).map {
                    val wfId = directEngine.startWorkflow(def).workflowId
                    harness.recordSubmission(wfId)
                    diagnostics.trackedWorkflows.add(wfId)
                    wfId
                }

            startDirectWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runDirectSweep()
                    }
                }

            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
                harness.recordCompletion(wfId)
            }
            sweepJob.cancel()

            harness.result("B3: Multi-Phase Pipeline (5 phases)", tasksPerWorkflow = 5).print()
        }

    // ---- B4: Throughput under network fault ----

    @Test
    fun `B4 - throughput under network latency`() =
        runBlocking(Dispatchers.Default) {
            val n = scale.fanOutSize
            val def =
                workflow {
                    activity("step1") {
                        transition("b4.handler")
                        retries(3)
                    }
                }

            handlerRegistry.register("b4.handler", PassThroughHandler())

            val harness = BenchmarkHarness()

            val wfIds =
                (1..n).map {
                    val wfId = engine.startWorkflow(def).workflowId
                    harness.recordSubmission(wfId)
                    diagnostics.trackedWorkflows.add(wfId)
                    wfId
                }

            startWorkerPool()

            // Inject 500ms latency after workflows are submitted
            oracleProxy.toxics().latency("slow-b4", ToxicDirection.DOWNSTREAM, 500)

            try {
                val sweepJob =
                    launch(Dispatchers.IO) {
                        while (true) {
                            delay(sweepInterval.toMillis())
                            runSweep()
                        }
                    }

                for (wfId in wfIds) {
                    assertWorkflowTerminates(wfId)
                    harness.recordCompletion(wfId)
                }
                sweepJob.cancel()

                harness.result("B4: Throughput Under 500ms Network Latency", tasksPerWorkflow = 1).print()
            } finally {
                oracleProxy
                    .toxics()
                    .all
                    .firstOrNull { it.name == "slow-b4" }
                    ?.remove()
            }
        }

    // ---- B5: Sweep overhead at scale ----

    @Test
    fun `B5 - sweep overhead at scale`() =
        runBlocking(Dispatchers.Default) {
            val n = 100
            val def =
                workflow {
                    activity("step1") { transition("b5.handler") }
                    activity("step2") { transition("b5.handler") }
                }
            val defJson = objectMapper.writeValueAsString(def)

            handlerRegistry.register("b5.handler", PassThroughHandler())

            // Create N stuck workflows via direct SQL (no workers needed for setup)
            val wfIds =
                (1..n).map { i ->
                    val wfId = randomId()
                    diagnostics.trackedWorkflows.add(wfId)
                    insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
                    insertTaskDirect(
                        workflowId = wfId,
                        sequenceNumber = 1,
                        status = "COMPLETED",
                        handlerKey = "b5.handler",
                        result = """{"test":"B5-$i"}""",
                    )
                    updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))
                    wfId
                }

            // Start workers for step2
            startDirectWorkerPool()

            val harness = BenchmarkHarness()
            wfIds.forEach { harness.recordSubmission(it) }

            // Measure sweep + recovery time
            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runDirectSweep()
                    }
                }

            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
                harness.recordCompletion(wfId)
            }
            sweepJob.cancel()

            harness.result("B5: Sweep Overhead ($n stuck workflows)", tasksPerWorkflow = 2).print()
        }
}
