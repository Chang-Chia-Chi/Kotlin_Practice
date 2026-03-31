package com.workflow.stress

import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.workflowId
import com.workflow.workflow.dsl.workflow
import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerOutput
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.sql.SQLException
import java.time.Duration

@Tag("stress")
class FaultInjectionStressTest : StressTestBase() {

    @JvmField
    @RegisterExtension
    val diagnostics = StressTestDiagnostics(this)

    // ---- F1: CAS deadlock during phase advance ----

    @Test
    fun `F1 - CAS deadlock during phase advance - watchdog retries and recovers`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") { transition("f1.handler") }
                activity("step2") { transition("f1.handler") }
            }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("f1.handler", PassThroughHandler())
            startWorkerPool()

            // Fail the next CAS update on workflow version — simulates deadlock
            faultInjector.onSql("UPDATE workflow.*version").failNext(1, SQLException("ORA-00060: deadlock detected"))

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- F2: Full task contention — all tasks locked by other workers ----

    @Test
    fun `F2 - full task contention - workers back off then claim after rules expire`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") { transition("f2.handler") }
            }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("f2.handler", PassThroughHandler())

            // First 3 claim attempts return no tasks (simulates all locked by others)
            faultInjector.onSql("FOR UPDATE SKIP LOCKED").returnEmpty(3)

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            sweepJob.cancel()
        }

    // ---- F3: Slow INSERT during fan-out scatter ----

    @Test
    fun `F3 - slow INSERT during fan-out - completes correctly despite delay`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("scatter") {
                    transition("f3.scatter")
                    fanOut("parallel")
                }
                activity("parallel") {
                    transition("f3.parallel")
                }
            }

            handlerRegistry.register("f3.scatter", object : TransitionHandler {
                override suspend fun execute(input: HandlerInput): HandlerOutput {
                    val payloads = (1..10).map { """{"item":$it}""" }
                    return HandlerOutput(result = objectMapper.writeValueAsString(payloads))
                }
            })
            handlerRegistry.register("f3.parallel", PassThroughHandler())

            // Slow down task INSERT by 3 seconds (simulates slow disk during scatter)
            faultInjector.onSql("INSERT INTO task").delay(Duration.ofSeconds(3))

            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            sweepJob.cancel()

            // Verify all 10 parallel tasks were created correctly
            val parallelTasks = readTasksDirect(wfId, sequenceNumber = 2)
            kotlin.test.assertEquals(10, parallelTasks.size)
        }

    // ---- F4: Partial commit — task UPDATE ok, workflow CAS fails ----

    @Test
    fun `F4 - partial commit - task completes but CAS fails - watchdog recovers`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") { transition("f4.handler") }
                activity("step2") { transition("f4.handler") }
            }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            val recorder = HistoryRecorder(PassThroughHandler())
            handlerRegistry.register("f4.handler", recorder)

            // Fail the 2nd SQL execution matching workflow version update within a transaction.
            // 1st execution = task status update (succeeds), 2nd = workflow CAS advance (fails).
            faultInjector.onSql("UPDATE workflow.*version").failNth(2, SQLException("ORA-00060: simulated partial commit"))

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            assertWorkflowStatus(wfId, "COMPLETED")
            val allTasks = readTasksDirect(wfId)
            HistoryChecker.assertNoLostTasks(recorder.snapshot(), allTasks)
            sweepJob.cancel()
        }

    // ---- F5: Intermittent barrier stale read ----

    @Test
    fun `F5 - barrier stale read - recovers on subsequent sweep`() =
        runBlocking(Dispatchers.Default) {
            val def = workflow {
                activity("step1") {
                    transition("f5.handler")
                    retries(1)
                    failurePolicy(FailurePolicy.ABORT)
                }
            }
            val wfId = engine.startWorkflow(def).workflowId
            diagnostics.trackedWorkflows.add(wfId)

            handlerRegistry.register("f5.handler", PassThroughHandler())

            // First barrier COUNT query returns empty — simulates stale MVCC snapshot
            faultInjector.onSql("SELECT COUNT.*task").returnEmpty(1)

            startWorkerPool()

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            assertWorkflowTerminates(wfId)
            sweepJob.cancel()
        }

    // ---- F6: Deadlock storm then recovery ----

    @Test
    fun `F6 - deadlock storm then recovery - system converges after storm`() =
        runBlocking(Dispatchers.Default) {
            val batchSize = scale.workflowBatchSize
            val def = workflow {
                activity("step1") { transition("f6.handler") }
                activity("step2") { transition("f6.handler") }
            }

            val recorder = HistoryRecorder(PassThroughHandler())
            handlerRegistry.register("f6.handler", recorder)

            val wfIds = (1..batchSize).map {
                engine.startWorkflow(def).workflowId.also {
                    diagnostics.trackedWorkflows.add(it)
                }
            }

            startWorkerPool()

            // Storm: all CAS updates fail for next 20 attempts
            faultInjector.onSql("UPDATE workflow.*version").failNext(20, SQLException("ORA-00060: deadlock storm"))

            val sweepJob = launch(Dispatchers.IO) {
                while (true) { delay(sweepInterval.toMillis()); runSweep() }
            }

            // Wait for storm to clear (rules auto-expire after 20 failures)
            delay(5000)

            // System should converge — all workflows complete
            for (wfId in wfIds) {
                assertWorkflowTerminates(wfId)
            }
            val allTasks = wfIds.flatMap { readTasksDirect(it) }
            HistoryChecker.assertNoLostTasks(recorder.snapshot(), allTasks)
            sweepJob.cancel()
        }
}
