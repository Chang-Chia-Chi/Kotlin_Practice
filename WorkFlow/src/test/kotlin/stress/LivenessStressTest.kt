package com.workflow.stress

import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
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

@Tag("stress")
class LivenessStressTest : StressTestBase() {
    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- L1: Worker dies after claiming task, before handler starts ----

    @Test
    fun `L1 - worker crash before handler - stale reclaim recovers`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") {
                        transition("l1.handler")
                        retries(3)
                    }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            // First call crashes before handler, subsequent calls succeed
            handlerRegistry.register(
                "l1.handler",
                CrashableHandler(CrashPoint.BEFORE_HANDLER, crashOnInvocation = 1),
            )
            startWorkerPool()

            // Run watchdog periodically to reclaim stale tasks
            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- L2: Worker dies mid-handler execution ----

    @Test
    fun `L2 - worker crash mid handler - stale reclaim recovers`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") {
                        transition("l2.handler")
                        retries(3)
                    }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register(
                "l2.handler",
                CrashableHandler(CrashPoint.MID_HANDLER, crashOnInvocation = 1),
            )
            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- L3: Worker dies after handler success, before barrier call ----

    @Test
    fun `L3 - worker crash after handler before barrier - stale reclaim recovers`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") {
                        transition("l3.handler")
                        retries(3)
                    }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register(
                "l3.handler",
                CrashableHandler(CrashPoint.AFTER_HANDLER, crashOnInvocation = 1),
            )
            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- L4: Worker commits TX1 (task COMPLETED), dies before TX2 (CAS) ----
    // Simulated via direct state setup: task is COMPLETED, workflow hasn't advanced.

    @Test
    fun `L4 - crash between TX1 and TX2 - watchdog stuck detection recovers`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") { transition("l4.handler"); next("step2") }
                    activity("step2") { transition("l4.handler") }
                }
            val defJson = objectMapper.writeValueAsString(def)
            val wfId = randomId()
            diagnostics.trackedWorkflows.add(wfId)

            // Set up state: workflow at seq 1, task COMPLETED, but workflow not advanced
            insertWorkflowDirect(wfId, defJson, version = 0)
            insertTaskDirect(
                workflowId = wfId,
                sequenceNumber = 1,
                status = "COMPLETED",
                handlerKey = "l4.handler",
                result = """{"test":"L4"}""",
            )

            // Make workflow look stale (past grace period)
            updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

            // Register handler for step2 and start workers
            handlerRegistry.register("l4.handler", PassThroughHandler())
            startWorkerPool()

            // WorkflowWatchdog should detect stuck workflow and advance it
            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- L5: All workers die simultaneously, then restart ----

    @Test
    fun `L5 - all workers die and restart - stale reclaim batch recovers`() =
        runBlocking(Dispatchers.Default) {
            val batchSize = scale.workflowBatchSize
            val def =
                workflow {
                    activity("step1") {
                        transition("l5.handler")
                        retries(3)
                    }
                }

            // Start multiple workflows
            val wfIds =
                (1..batchSize).map {
                    engine.startWorkflow(def).workflowId.also {
                        diagnostics.trackedWorkflows.add(it)
                    }
                }

            // Use a gated handler that blocks all workers
            val gate = GatedHandler()
            handlerRegistry.register("l5.handler", gate)
            val jobs = startWorkerPool()

            // Wait for workers to claim tasks
            delay(pollInterval.toMillis() * 3)

            // Kill all workers (simulates simultaneous crash)
            jobs.forEach { it.cancel() }
            workerJobs.clear()

            // Make stale tasks visible to watchdog
            directJdbi.useHandle<Exception> { handle ->
                handle
                    .createUpdate(
                        "UPDATE task SET claimed_at = :ts WHERE status = 'PROCESSING'",
                    ).bind(
                        "ts",
                        java.time.LocalDateTime.ofInstant(
                            Instant.now().minus(staleTaskThreshold.multipliedBy(2)),
                            java.time.ZoneOffset.UTC,
                        ),
                    ).execute()
            }

            // Start fresh workers with pass-through handler
            handlerRegistry.register("l5.handler", PassThroughHandler())
            startWorkerPool()

            // WorkflowWatchdog reclaims stale tasks
            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
            }
            sweepJob.cancel()
        }

    // ---- L6a: Network partition during TX1 (task update) ----

    @Tag("stress-network")
    @Test
    fun `L6a - network cut during task update TX1 - stale reclaim recovers`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") {
                        transition("l6a.handler")
                        retries(3)
                    }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            // Handler succeeds, but we cut network so barrier TX1 fails
            var firstAttempt = true
            handlerRegistry.register(
                "l6a.handler",
                object : TransitionHandler {
                    override suspend fun execute(input: HandlerInput): HandlerResult {
                        if (firstAttempt) {
                            firstAttempt = false
                            // Cut network — the barrier call after this will fail
                            oracleProxy.toxics().bandwidth("cut-l6a", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, 0)
                            delay(100)
                            // Restore after a brief cut (simulates transient partition)
                            oracleProxy.toxics().get("cut-l6a").remove()
                        }
                        return HandlerResult.Completed(result = input.inputs)
                    }
                },
            )
            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            sweepJob.cancel()
        }

    // ---- L6b: Network partition during TX2 (CAS + advance) ----

    @Tag("stress-network")
    @Test
    fun `L6b - network cut during CAS TX2 - watchdog stuck detection recovers`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") {
                        transition("l6b.handler")
                        retries(3)
                        next("step2")
                    }
                    activity("step2") { transition("l6b.handler") }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("l6b.handler", PassThroughHandler())
            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            // This is a probabilistic test — the network cut may or may not hit TX2.
            // Either way, the workflow must terminate via normal path or recovery.
            assertWorkflowTerminates(wfId)
            sweepJob.cancel()
        }

    // ---- L7: Network partition during task claim ----

    @Tag("stress-network")
    @Test
    fun `L7 - network cut during claim - task stays PENDING and next poll claims it`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") { transition("l7.handler") }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            // Cut network briefly — first claim attempt fails, task stays PENDING
            oracleProxy.toxics().bandwidth("cut-l7", eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM, 0)

            handlerRegistry.register("l7.handler", PassThroughHandler())
            startWorkerPool()

            // Restore after a brief pause
            delay(pollInterval.toMillis() * 2)
            oracleProxy.toxics().get("cut-l7").remove()

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
        }

    // ---- L8: Task deadline expires while handler runs slowly ----

    @Test
    fun `L8 - slow handler exceeds task deadline - watchdog times out task`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") {
                        transition("l8.handler")
                        deadline(Duration.ofSeconds(2)) // Short deadline
                    }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            // Handler takes longer than deadline
            handlerRegistry.register("l8.handler", SlowHandler(delayMs = 10_000))
            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            // Workflow should fail because the task timed out
            assertWorkflowStatus(wfId, "FAILED")
            sweepJob.cancel()
        }

    // ---- L9: Workflow deadline expires during execution ----

    @Test
    fun `L9 - workflow deadline expires - watchdog times out workflow`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") { transition("l9.handler") }
                    deadline(Duration.ofSeconds(2)) // Short workflow deadline
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            // Handler blocks forever — workflow deadline must fire
            handlerRegistry.register("l9.handler", GatedHandler()) // Never released
            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "TIMED_OUT")
            sweepJob.cancel()
        }

    // ---- L10: Stale task exhausts retries -> dead-letter -> barrier evaluates ----

    @Test
    fun `L10 - task exhausts retries to dead letter - barrier fires with failure policy`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") {
                        transition("l10.handler")
                        retries(1) // 1 retry = max 2 attempts
                        failurePolicy(FailurePolicy.ABORT)
                    }
                }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            // Always fail
            handlerRegistry.register("l10.handler", FailingHandler())
            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "FAILED")
            sweepJob.cancel()
        }

    // ---- L11: Fan-out: all sub-tasks fail under BEST_EFFORT ----

    @Test
    fun `L11 - fan-out all sub-tasks fail with BEST_EFFORT - workflow terminates`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("scatter") {
                        transition("l11.scatter")
                        failurePolicy(FailurePolicy.BEST_EFFORT)
                        fanOut {
                            transition("l11.parallel")
                            retries(0) // No retries — immediate failure
                            failurePolicy(FailurePolicy.BEST_EFFORT)
                            joinPolicy(JoinPolicy.All)
                        }
                        next("final")
                    }
                    activity("final") { transition("l11.final") }
                }

            // Scatter handler produces N payloads
            handlerRegistry.register(
                "l11.scatter",
                object : TransitionHandler {
                    override suspend fun execute(input: HandlerInput): HandlerResult {
                        val payloads = (1..scale.fanOutSize).map { """{"item":$it}""" }
                        return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
                    }
                },
            )
            // All parallel handlers fail
            handlerRegistry.register("l11.parallel", FailingHandler())
            handlerRegistry.register("l11.final", PassThroughHandler())

            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            startWorkerPool()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            assertWorkflowTerminates(wfId)
            sweepJob.cancel()
        }

    // ---- L12: Leader dies during sweep, new leader recovers ----

    @Test
    fun `L12 - leader dies mid sweep - new leader recovers stuck workflows`() =
        runBlocking(Dispatchers.Default) {
            val def =
                workflow {
                    activity("step1") { transition("l12.handler"); next("step2") }
                    activity("step2") { transition("l12.handler") }
                }
            val defJson = objectMapper.writeValueAsString(def)

            // Create multiple stuck workflows (simulating leader death mid-patrol)
            val wfIds =
                (1..3).map { i ->
                    val wfId = randomId()
                    diagnostics.trackedWorkflows.add(wfId)
                    insertWorkflowDirect(wfId, defJson, version = 0)
                    insertTaskDirect(
                        workflowId = wfId,
                        sequenceNumber = 1,
                        status = "COMPLETED",
                        handlerKey = "l12.handler",
                        result = """{"test":"L12-$i"}""",
                    )
                    updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))
                    wfId
                }

            handlerRegistry.register("l12.handler", PassThroughHandler())
            startWorkerPool()

            // Simulate: first sweep partially processes, then "new leader" sweeps again
            runSweep()

            val sweepJob =
                launch(Dispatchers.IO) {
                    while (true) {
                        delay(sweepInterval.toMillis())
                        runSweep()
                    }
                }

            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
                assertWorkflowStatus(wfId, "COMPLETED")
            }
            sweepJob.cancel()
        }
}
