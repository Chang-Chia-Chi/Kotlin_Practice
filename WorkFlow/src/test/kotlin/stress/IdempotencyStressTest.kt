package com.workflow.stress

import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Tag("stress")
class IdempotencyStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- I1: Sweeper + worker race on same stuck workflow ----

    @Test
    fun `I1 - sweeper and worker race on stuck workflow - exactly one CAS wins`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("i1.handler") }
            activity("step2") { transition("i1.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        // State: task COMPLETED, workflow not advanced (stuck)
        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "i1.handler",
            result = """{"test":"I1"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        val recorder = HistoryRecorder(PassThroughHandler())
        handlerRegistry.register("i1.handler", recorder)

        // Race: sweeper recovery and worker barrier completion fire simultaneously
        val latch = CountDownLatch(1)
        val sweeperResult = async {
            latch.await(5, TimeUnit.SECONDS)
            runSweep()
        }
        val barrierResult = async {
            latch.await(5, TimeUnit.SECONDS)
            barrier.recoverStuckWorkflow(wfId)
        }

        latch.countDown() // Fire both simultaneously
        sweeperResult.await()
        barrierResult.await()

        // Start workers for step2
        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Critical: exactly 1 task at seq 2 (no duplicates from race)
        assertTaskCount(wfId, 2, 1)
        assertNoTaskDuplicates(wfId, 2)
        HistoryChecker.assertNoDuplicateExecution(recorder.snapshot())
        sweepJob.cancel()
    }

    // ---- I2: Two sweeper patrols overlap (dual-leader) ----

    @Test
    fun `I2 - two sweeper patrols overlap - state consistent`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("i2.handler") }
            activity("step2") { transition("i2.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "i2.handler",
            result = """{"test":"I2"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        handlerRegistry.register("i2.handler", PassThroughHandler())

        // Two sweepers fire simultaneously
        val latch = CountDownLatch(1)
        val sweep1 = async { latch.await(5, TimeUnit.SECONDS); runSweep() }
        val sweep2 = async { latch.await(5, TimeUnit.SECONDS); runSweep() }

        latch.countDown()
        sweep1.await()
        sweep2.await()

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertTaskCount(wfId, 2, 1) // No duplicates
        sweepJob.cancel()
    }

    // ---- I3: Sweeper expires task at same moment worker completes it ----

    @Test
    fun `I3 - timeout and completion race - barrier fires exactly once`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") {
                transition("i3.handler")
                deadline(Duration.ofSeconds(3))
                failurePolicy(FailurePolicy.ABORT)
            }
        }

        // Handler that takes just long enough to race with deadline
        handlerRegistry.register("i3.handler", SlowHandler(delayMs = 2500))

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        // Either COMPLETED (worker wins) or FAILED (sweeper timeout wins)
        // But must terminate — not hang
        assertWorkflowTerminates(wfId)

        val wf = readWorkflowDirect(wfId)!!
        val status = wf["STATUS"]?.toString()
        assertTrue(
            status == "COMPLETED" || status == "FAILED",
            "Expected COMPLETED or FAILED, got $status",
        )
        sweepJob.cancel()
    }

    // ---- I4: Sweeper reclaims stale task while worker about to complete ----

    @Test
    fun `I4 - stale reclaim races with task completion - no corruption`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("i4.handler"); retries(3) }
        }
        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        // Slow handler that takes longer than stale threshold
        handlerRegistry.register("i4.handler", SlowHandler(
            delayMs = staleTaskThreshold.toMillis() + 1000,
            delegate = PassThroughHandler(),
        ))
        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        // Workflow should still complete despite the race
        assertWorkflowTerminates(wfId)
        sweepJob.cancel()
    }

    // ---- I5: Replay called while sweeper mid-recovery ----

    @Test
    fun `I5 - replay during sweeper recovery - no conflict`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") {
                transition("i5.handler")
                retries(0)
                failurePolicy(FailurePolicy.ABORT)
            }
            activity("step2") { transition("i5.handler") }
        }

        // Step1 fails → workflow FAILED
        handlerRegistry.register("i5.handler", FailNThenSucceedHandler(failCount = 1))

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowStatus(wfId, "FAILED")

        // Race: replay and sweeper both act on the workflow
        val latch = CountDownLatch(1)
        val replayResult = async {
            latch.await(5, TimeUnit.SECONDS)
            engine.replayWorkflow(wfId)
        }
        val sweeperResult = async {
            latch.await(5, TimeUnit.SECONDS)
            runSweep()
        }

        latch.countDown()
        replayResult.await()
        sweeperResult.await()

        // Should eventually complete (replay re-queues the failed task)
        assertWorkflowTerminates(wfId, timeout = scale.outerTimeout)
        sweepJob.cancel()
    }

    // ---- I6: Sweeper detects same stuck workflow on consecutive patrols ----

    @Test
    fun `I6 - consecutive sweeper patrols on same stuck workflow - second is no-op`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("i6.handler") }
            activity("step2") { transition("i6.handler") }
        }
        val defJson = objectMapper.writeValueAsString(def)
        val wfId = randomId()
        diagnostics.trackedWorkflows.add(wfId)

        insertWorkflowDirect(wfId, defJson, currentSequence = 1, version = 0)
        insertTaskDirect(
            workflowId = wfId,
            sequenceNumber = 1,
            status = "COMPLETED",
            handlerKey = "i6.handler",
            result = """{"test":"I6"}""",
        )
        updateWorkflowUpdatedAtDirect(wfId, Instant.now().minus(gracePeriod.multipliedBy(2)))

        handlerRegistry.register("i6.handler", PassThroughHandler())

        // First patrol: recovers and advances
        runSweep()

        // Second patrol: workflow now has non-terminal tasks at new seq → skips
        runSweep()

        // Start workers to complete step2
        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // Exactly 1 task at seq 2 (second patrol didn't create duplicates)
        assertTaskCount(wfId, 2, 1)
        sweepJob.cancel()
    }

    // ---- I7: Double-claim prevention via SKIP LOCKED ----

    @Test
    fun `I7 - concurrent claims on same task - SKIP LOCKED prevents double claim`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("i7.handler") }
        }
        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        val counting = CountingHandler()
        val recorder = HistoryRecorder(counting)
        handlerRegistry.register("i7.handler", recorder)

        // Start multiple worker pools to maximize claim contention
        repeat(3) { startWorkerPool() }

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // The handler should have been invoked exactly once for the single task
        assertEquals(1, counting.totalInvocations.get(), "Task should be processed exactly once")
        HistoryChecker.assertNoDuplicateExecution(recorder.snapshot())
        sweepJob.cancel()
    }

    // ---- I8: Cancel workflow while barrier in-flight ----

    @Test
    fun `I8 - cancel workflow while barrier in-flight - no post-cancel advancement`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("i8.handler") }
            activity("step2") { transition("i8.step2") }
        }

        // Slow handler to give us time to cancel
        val gate = GatedHandler()
        handlerRegistry.register("i8.handler", gate)
        handlerRegistry.register("i8.step2", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        // Wait for task to be claimed
        delay(pollInterval.toMillis() * 3)

        // Cancel workflow while handler is blocked
        val cancelled = engine.cancelWorkflow(wfId)
        assertTrue(cancelled, "Cancel should succeed")

        // Release the gate — handler completes, but barrier should fail CAS
        gate.release()

        delay(1000) // Give time for any erroneous advancement

        // Workflow should stay CANCELLED, no step2 tasks
        val wf = readWorkflowDirect(wfId)!!
        assertEquals("CANCELLED", wf["STATUS"]?.toString())
        val step2Tasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(0, step2Tasks.size, "No step2 tasks should exist after cancel")
    }
}
