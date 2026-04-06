package com.workflow.stress

import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@Tag("stress")
class CorrectnessStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- C1: N workers complete final task of a phase simultaneously (CAS race) ----

    @Test
    fun `C1 - concurrent CAS race - exactly one set of next-phase tasks created`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("scatter") {
                transition("c1.scatter")
                fanOut {
                    transition("c1.parallel")
                    joinPolicy(JoinPolicy.All)
                }
                next("final")
            }
            activity("final") { transition("c1.final") }
        }

        handlerRegistry.register("c1.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val payloads = (1..scale.fanOutSize).map { """{"item":$it}""" }
                return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
            }
        })
        val recorder = HistoryRecorder(PassThroughHandler())
        handlerRegistry.register("c1.parallel", recorder)
        handlerRegistry.register("c1.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Assert: exactly 1 task at the final sequence (no duplicates from CAS race)
        val wf = readWorkflowDirect(wfId)!!
        // The final activity is the last sequence
        val allTasks = readTasksDirect(wfId)
        val maxSeq = allTasks.maxOf { (it["SEQUENCE_NUMBER"] as Number).toInt() }
        val finalTasks = allTasks.filter { (it["SEQUENCE_NUMBER"] as Number).toInt() == maxSeq }
        assertEquals(1, finalTasks.size, "Expected exactly 1 final task, got ${finalTasks.size}")
        assertNoTaskDuplicates(wfId, maxSeq)
        HistoryChecker.assertNoDuplicateExecution(recorder.snapshot())

        sweepJob.cancel()
    }

    // ---- C2: Fan-out scatter produces N payloads → N sub-tasks atomically ----

    @Test
    fun `C2 - scatter produces N payloads - exactly N sub-tasks created`() = runBlocking(Dispatchers.Default) {
        val n = scale.fanOutSize
        val def = workflow {
            activity("scatter") {
                transition("c2.scatter")
                fanOut {
                    transition("c2.parallel")
                    joinPolicy(JoinPolicy.All)
                }
            }
        }

        handlerRegistry.register("c2.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("c2.parallel", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Parallel tasks are at sequence 2 (scatter=seq1, parallel=seq2)
        val parallelTasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(n, parallelTasks.size, "Expected $n parallel tasks, got ${parallelTasks.size}")
        assertNoTaskDuplicates(wfId, 2)

        sweepJob.cancel()
    }

    // ---- C3: JoinPolicy.ALL - 1 of N fails ----

    @Test
    fun `C3 - JoinPolicy ALL with one failure and ABORT - workflow fails`() = runBlocking(Dispatchers.Default) {
        val n = 10
        val def = workflow {
            activity("scatter") {
                transition("c3.scatter")
                failurePolicy(FailurePolicy.ABORT)
                fanOut {
                    transition("c3.parallel")
                    retries(0)
                    failurePolicy(FailurePolicy.ABORT)
                    joinPolicy(JoinPolicy.All)
                }
            }
        }

        handlerRegistry.register("c3.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
            }
        })

        // Fail the first sub-task, succeed the rest
        val count = AtomicInteger(0)
        handlerRegistry.register("c3.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                if (count.incrementAndGet() == 1) throw RuntimeException("Simulated failure")
                return HandlerResult.Completed(result = input.item)
            }
        })

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "FAILED")
        sweepJob.cancel()
    }

    // ---- C4: JoinPolicy.Percentage(95) boundary precision ----

    @Test
    fun `C4 - JoinPolicy Percentage 95 at threshold - passes`() = runBlocking(Dispatchers.Default) {
        // 95 of 100 succeed (5 fail) → 95% ≥ 95% → pass
        val wfId = startPercentageTest(totalTasks = 100, failCount = 5, threshold = 95)
        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }
        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")
        sweepJob.cancel()
    }

    @Test
    fun `C4 - JoinPolicy Percentage 95 below threshold - fails`() = runBlocking(Dispatchers.Default) {
        // 94 of 100 succeed (6 fail) → 94% < 95% → fail
        val wfId = startPercentageTest(totalTasks = 100, failCount = 6, threshold = 95)
        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }
        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "FAILED")
        sweepJob.cancel()
    }

    private suspend fun startPercentageTest(totalTasks: Int, failCount: Int, threshold: Int): String {
        val handlerKey = "c4-$totalTasks-$failCount"
        val def = workflow {
            activity("scatter") {
                transition("$handlerKey.scatter")
                fanOut {
                    transition("$handlerKey.parallel")
                    retries(0)
                    joinPolicy(JoinPolicy.Percentage(threshold))
                }
                next("final")
            }
            activity("final") { transition("$handlerKey.final") }
        }

        handlerRegistry.register("$handlerKey.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val payloads = (1..totalTasks).map { """{"item":$it}""" }
                return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
            }
        })

        val failCounter = AtomicInteger(0)
        handlerRegistry.register("$handlerKey.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                if (failCounter.incrementAndGet() <= failCount) throw RuntimeException("Simulated failure")
                return HandlerResult.Completed(result = input.item)
            }
        })
        handlerRegistry.register("$handlerKey.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)
        startWorkerPool()
        return wfId
    }

    // ---- C5: JoinPolicy.Threshold(N) boundary precision ----

    @Test
    fun `C5 - JoinPolicy Threshold boundary precision`() = runBlocking(Dispatchers.Default) {
        val total = 20
        val threshold = 15

        // At threshold: 15 succeed → pass
        verifyJoinPolicyThreshold(total, failCount = total - threshold, threshold = threshold, expectedStatus = "COMPLETED")
        cleanUpTables()

        // Below threshold: 14 succeed → fail
        verifyJoinPolicyThreshold(total, failCount = total - threshold + 1, threshold = threshold, expectedStatus = "FAILED")
    }

    private suspend fun verifyJoinPolicyThreshold(
        totalTasks: Int,
        failCount: Int,
        threshold: Int,
        expectedStatus: String,
    ) {
        val handlerKey = "c5-$totalTasks-$failCount"
        val def = workflow {
            activity("scatter") {
                transition("$handlerKey.scatter")
                fanOut {
                    transition("$handlerKey.parallel")
                    retries(0)
                    joinPolicy(JoinPolicy.Threshold(threshold))
                }
                next("final")
            }
            activity("final") { transition("$handlerKey.final") }
        }

        handlerRegistry.register("$handlerKey.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val payloads = (1..totalTasks).map { """{"item":$it}""" }
                return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
            }
        })

        val failCounter = AtomicInteger(0)
        handlerRegistry.register("$handlerKey.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                if (failCounter.incrementAndGet() <= failCount) throw RuntimeException("Simulated failure")
                return HandlerResult.Completed(result = input.item)
            }
        })
        handlerRegistry.register("$handlerKey.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        coroutineScope {
            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, expectedStatus)
            sweepJob.cancel()
        }
    }

    // ---- C6: FailurePolicy.ABORT mid-phase ----

    @Test
    fun `C6 - ABORT mid-phase - workflow fails and no new phase started`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") {
                transition("c6.handler")
                retries(0)
                failurePolicy(FailurePolicy.ABORT)
                next("step2")
            }
            activity("step2") { transition("c6.step2") }
        }

        handlerRegistry.register("c6.handler", FailingHandler())
        handlerRegistry.register("c6.step2", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "FAILED")

        // No step2 tasks should exist
        val step2Tasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(0, step2Tasks.size, "No tasks should exist at seq 2 after ABORT")

        sweepJob.cancel()
    }

    // ---- C7: FailurePolicy.BEST_EFFORT - all tasks fail ----

    @Test
    fun `C7 - BEST_EFFORT with all failures - workflow advances to next phase`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") {
                transition("c7.handler")
                retries(0)
                failurePolicy(FailurePolicy.BEST_EFFORT)
                next("step2")
            }
            activity("step2") { transition("c7.step2") }
        }

        handlerRegistry.register("c7.handler", FailingHandler())
        handlerRegistry.register("c7.step2", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        sweepJob.cancel()
    }

    // ---- C8: Explicit input resolution across phases ----

    @Test
    fun `C8 - explicit inputs resolve correctly across phase boundaries`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("c8.step1"); next("step2") }
            activity("step2") {
                transition("c8.step2")
                inputs { "prev" from "step1" }
                next("step3")
            }
            activity("step3") {
                transition("c8.step3")
                inputs { "prev" from "step2" }
            }
        }

        handlerRegistry.register("c8.step1", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult =
                HandlerResult.Completed(result = """{"phase":1,"data":"origin"}""")
        })
        handlerRegistry.register("c8.step2", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult =
                HandlerResult.Completed(result = """{"phase":2,"prev":${input.inputs}}""")
        })
        handlerRegistry.register("c8.step3", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult =
                HandlerResult.Completed(result = """{"phase":3,"prev":${input.inputs}}""")
        })

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // Verify input chain: each phase received the previous phase's result via explicit inputs
        val tasks = readTasksDirect(wfId).sortedBy { (it["SEQUENCE_NUMBER"] as Number).toInt() }
        assertEquals(3, tasks.size)

        // Step1 has no inputs (first activity)
        val step1Result = tasks[0]["RESULT"]?.toString()
        assertNotNull(step1Result)
        assertTrue(step1Result.contains("phase\":1"))

        // Step2 result contains step1's output (resolved via inputs)
        val step2Result = tasks[1]["RESULT"]?.toString()
        assertNotNull(step2Result)
        assertTrue(step2Result.contains("phase\":2"))
        assertTrue(step2Result.contains("phase\":1"))

        // Step3 result contains step2's output (resolved via inputs)
        val step3Result = tasks[2]["RESULT"]?.toString()
        assertNotNull(step3Result)
        assertTrue(step3Result.contains("phase\":3"))
        assertTrue(step3Result.contains("phase\":2"))

        sweepJob.cancel()
    }

    // ---- C9: Fan-out sub-task results → join handler receives all ----

    @Test
    fun `C9 - join handler receives complete result set from all sub-tasks`() = runBlocking(Dispatchers.Default) {
        val n = 10
        val def = workflow {
            activity("scatter") {
                transition("c9.scatter")
                fanOut {
                    transition("c9.parallel")
                    joinPolicy(JoinPolicy.All)
                }
            }
        }

        handlerRegistry.register("c9.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("c9.parallel", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult =
                HandlerResult.Completed(result = """{"processed":${input.item}}""")
        })

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)

        // Verify all parallel tasks completed with results
        val parallelTasks = readTasksDirect(wfId, sequenceNumber = 2)
        assertEquals(n, parallelTasks.size)
        for (task in parallelTasks) {
            assertEquals("COMPLETED", task["STATUS"]?.toString())
            assertTrue(task["RESULT"]?.toString()?.contains("processed") == true)
        }

        sweepJob.cancel()
    }

    // ---- C10: Replay after FAILED ----

    @Test
    fun `C10 - replay resumes from current sequence without re-executing completed phases`() = runBlocking(Dispatchers.Default) {
        val def = workflow {
            activity("step1") { transition("c10.step1"); next("step2") }
            activity("step2") {
                transition("c10.step2")
                retries(0)
                failurePolicy(FailurePolicy.ABORT)
                next("step3")
            }
            activity("step3") { transition("c10.step3") }
        }

        // Step1 succeeds, step2 fails
        handlerRegistry.register("c10.step1", PassThroughHandler())
        val step2Counter = AtomicInteger(0)
        handlerRegistry.register("c10.step2", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                if (step2Counter.incrementAndGet() == 1) throw RuntimeException("First attempt fails")
                return HandlerResult.Completed(result = input.inputs)
            }
        })
        handlerRegistry.register("c10.step3", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        // Wait for failure
        assertWorkflowStatus(wfId, "FAILED")

        // Step1 was completed — verify
        val step1Tasks = readTasksDirect(wfId, sequenceNumber = 1)
        assertEquals(1, step1Tasks.size)
        assertEquals("COMPLETED", step1Tasks[0]["STATUS"]?.toString())

        // Replay
        val replayed = engine.replayWorkflow(wfId)
        assertTrue(replayed, "Replay should succeed")

        // After replay, workflow should eventually complete
        assertWorkflowStatus(wfId, "COMPLETED", timeout = scale.outerTimeout)

        // Step1 should still have only 1 task (not re-executed)
        val step1After = readTasksDirect(wfId, sequenceNumber = 1)
        assertEquals(1, step1After.size, "Step1 should not be re-executed on replay")

        sweepJob.cancel()
    }

    // ---- C11: Concurrent barrier probes see consistent count under high write load ----

    @Test
    fun `C11 - concurrent barrier probes under high fanout - MVCC consistency`() = runBlocking(Dispatchers.Default) {
        val n = scale.fanOutSize
        val def = workflow {
            activity("scatter") {
                transition("c11.scatter")
                fanOut {
                    transition("c11.parallel")
                    joinPolicy(JoinPolicy.All)
                }
                next("final")
            }
            activity("final") { transition("c11.final") }
        }

        handlerRegistry.register("c11.scatter", object : TransitionHandler {
            override suspend fun execute(input: HandlerInput): HandlerResult {
                val payloads = (1..n).map { """{"item":$it}""" }
                return HandlerResult.Completed(result = null, items = objectMapper.writeValueAsString(payloads))
            }
        })
        handlerRegistry.register("c11.parallel", PassThroughHandler())
        handlerRegistry.register("c11.final", PassThroughHandler())

        val wfId = engine.startWorkflow(def).workflowId
        diagnostics.trackedWorkflows.add(wfId)

        // Use maximum workers to maximize concurrent barrier probes
        startWorkerPool()

        val sweepJob = launch(Dispatchers.IO) {
            while (true) { delay(sweepInterval.toMillis()); runSweep() }
        }

        assertWorkflowTerminates(wfId)
        assertWorkflowStatus(wfId, "COMPLETED")

        // Critical assertion: exactly 1 final task (proves no duplicate CAS wins)
        val allTasks = readTasksDirect(wfId)
        val maxSeq = allTasks.maxOf { (it["SEQUENCE_NUMBER"] as Number).toInt() }
        assertTaskCount(wfId, maxSeq, 1)
        assertNoTaskDuplicates(wfId, maxSeq)

        sweepJob.cancel()
    }
}
