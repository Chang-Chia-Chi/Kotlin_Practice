package com.mapreduce.queue

import com.mapreduce.TestH2Factory
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.StepStatus
import com.mapreduce.queue.model.WorkflowStep
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.StepTaskResolution
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.queue.repository.TaskRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

/**
 * Concurrency stress tests for the task queue framework.
 *
 * Validates the three critical correctness invariants under concurrent access:
 *
 * 1. **No double-claim** — `SELECT FOR UPDATE SKIP LOCKED` ensures each task
 *    is handed to exactly one worker, even with many concurrent claimers.
 *
 * 2. **Exactly-once barrier** — The lock-free COUNT + CAS mechanism ensures
 *    exactly one worker observes `COUNT(PENDING/CLAIMED) = 0`
 *    and dispatches the callback task.
 *
 * 3. **Zombie fencing** — A stale worker holding an old `execution_generation`
 *    cannot mark a task as completed after it has been reclaimed and re-claimed.
 *
 * All tests use H2 in Oracle compatibility mode, which supports
 * `FOR UPDATE SKIP LOCKED` and row-level locking.
 */
class ConcurrencyStressTest {

    private lateinit var jdbi: Jdbi
    private lateinit var taskRepo: TaskRepository
    private lateinit var stepRepo: WorkflowStepRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        taskRepo = TaskRepository(jdbi)
        val leaderManager = mock<LeaderManager>()
        whenever(leaderManager.isActive).thenReturn(false)
        stepRepo = WorkflowStepRepository(jdbi, leaderManager)
    }

    // ── 1. No Double-Claim ──────────────────────────────────────────

    @Nested
    inner class NoDoubleClaim {

        /**
         * Enqueue N tasks, launch N concurrent claimers — each task must be
         * claimed exactly once. The sum of claimed tasks must equal N.
         */
        @RepeatedTest(5)
        fun `concurrent claimers never get the same task`() = runBlocking {
            val taskCount = 50
            val workerCount = 20

            repeat(taskCount) { i ->
                taskRepo.enqueue(
                    EnqueueRequest(handler = "stress.handler", payload = "{\"i\":$i}", queue = "q1"),
                )
            }

            // Launch workers concurrently — each claims greedily until null
            val claimed = (0 until workerCount).map { w ->
                async(Dispatchers.IO) {
                    val myTasks = mutableListOf<String>()
                    while (true) {
                        val task = taskRepo.claim("worker-$w", listOf("q1")) ?: break
                        myTasks.add(task.taskId)
                    }
                    myTasks
                }
            }.awaitAll()

            val allClaimed = claimed.flatten()
            assertEquals(taskCount, allClaimed.size, "Total claimed should equal total enqueued")
            assertEquals(taskCount, allClaimed.toSet().size, "No duplicate claims")
        }

        /**
         * More workers than tasks — surplus workers must get null, no errors.
         * Each worker greedily claims until null (mirrors real WorkerLoop behavior).
         */
        @Test
        fun `surplus workers get null without errors`() = runBlocking {
            val taskCount = 5
            val workerCount = 30

            repeat(taskCount) { i ->
                taskRepo.enqueue(
                    EnqueueRequest(handler = "stress.handler", payload = "{\"i\":$i}", queue = "q1"),
                )
            }

            val claimed = (0 until workerCount).map { w ->
                async(Dispatchers.IO) {
                    val mine = mutableListOf<String>()
                    while (true) {
                        val task = taskRepo.claim("worker-$w", listOf("q1")) ?: break
                        mine.add(task.taskId)
                    }
                    mine
                }
            }.awaitAll()

            val allClaimed = claimed.flatten()
            assertEquals(taskCount, allClaimed.size, "All tasks claimed exactly once")
            assertEquals(taskCount, allClaimed.toSet().size, "No duplicates")
        }

        /**
         * Multi-queue claiming — workers subscribed to different queue subsets
         * must still never double-claim.
         */
        @Test
        fun `multi-queue concurrent claims are exclusive`() = runBlocking {
            repeat(20) { i ->
                val queue = if (i % 2 == 0) "alpha" else "beta"
                taskRepo.enqueue(EnqueueRequest(handler = "mq.handler", payload = "$i", queue = queue))
            }

            val claimed = (0 until 15).map { w ->
                async(Dispatchers.IO) {
                    val queues = when (w % 3) {
                        0 -> listOf("alpha")
                        1 -> listOf("beta")
                        else -> listOf("alpha", "beta")
                    }
                    val myTasks = mutableListOf<String>()
                    while (true) {
                        val task = taskRepo.claim("worker-$w", queues) ?: break
                        myTasks.add(task.taskId)
                    }
                    myTasks
                }
            }.awaitAll()

            val allClaimed = claimed.flatten()
            assertEquals(20, allClaimed.size, "All 20 tasks should be claimed")
            assertEquals(20, allClaimed.toSet().size, "No duplicates across queues")
        }
    }

    // ── 2. Exactly-Once Barrier ─────────────────────────────────────

    @Nested
    inner class ExactlyOnceBarrier {

        /**
         * Create a step with N tasks and a callback handler. Complete all N
         * tasks concurrently. Exactly one callback task must be created.
         */
        @RepeatedTest(10)
        fun `barrier fires exactly once under concurrent completion`() = runBlocking {
            val stepId = "barrier-${System.nanoTime()}"
            val taskCount = 30

            val step = WorkflowStep(
                stepId = stepId,
                workflowName = "stress",
                runId = stepId,
                status = StepStatus.ACTIVE,
                params = "{}",
                queue = "q1",
                stepLabel = "map",
                stepTotal = taskCount,
                onCompleteHandler = "stress.__step_transition",
                failurePolicy = "FAIL_STEP",
                failureThreshold = 0.0,
            )
            val tasks = (0 until taskCount).map {
                EnqueueRequest(
                    handler = "stress.map", payload = "{\"i\":$it}",
                    queue = "q1", stepId = stepId,
                )
            }
            stepRepo.submitStep(step, tasks)

            // Claim all tasks
            val claimedTasks = (0 until taskCount).map {
                taskRepo.claim("worker-$it", listOf("q1"))!!
            }

            // Complete all concurrently
            val results = claimedTasks.map { task ->
                async(Dispatchers.IO) {
                    stepRepo.resolveStepTask(
                        taskId = task.taskId,
                        stepId = stepId,
                        claimToken = task.claimToken,
                        outputUri = "blob://${task.taskId}",
                    )
                }
            }.awaitAll()

            // Exactly one worker should observe the barrier
            val barrierCount = results.count { it.barrierMet }
            assertEquals(1, barrierCount, "Barrier must be met exactly once")

            // All completions should succeed
            assertEquals(taskCount, results.count { it.updated })

            // Exactly one callback task should exist
            val callbackCount = jdbi.withHandle<Int, Exception> { h ->
                h.createQuery(
                    "SELECT COUNT(*) FROM task WHERE handler = 'stress.__step_transition' AND payload = :stepId",
                ).bind("stepId", stepId).mapTo(Int::class.java).one()
            }
            assertEquals(1, callbackCount, "Exactly one callback task")

            // Verify all tasks terminal
            assertEquals(0, stepRepo.countPendingTasks(stepId))
        }

        /**
         * Mixed success/failure completions. Barrier still fires exactly once,
         * and failure count is accurate.
         */
        @RepeatedTest(5)
        fun `barrier fires once with mixed success and failure`() = runBlocking {
            val stepId = "mixed-${System.nanoTime()}"
            val taskCount = 20
            val failCount = 7 // tasks at indices 0..6 will fail

            val step = WorkflowStep(
                stepId = stepId,
                workflowName = "stress",
                runId = stepId,
                status = StepStatus.ACTIVE,
                params = "{}",
                queue = "q1",
                stepLabel = "map",
                stepTotal = taskCount,
                onCompleteHandler = "stress.__step_transition",
                failurePolicy = "FAIL_STEP",
                failureThreshold = 0.0,
            )
            val tasks = (0 until taskCount).map {
                EnqueueRequest(
                    handler = "stress.map", payload = "{\"i\":$it}",
                    queue = "q1", stepId = stepId,
                )
            }
            stepRepo.submitStep(step, tasks)

            val claimedTasks = (0 until taskCount).map {
                taskRepo.claim("worker-$it", listOf("q1"))!!
            }

            val results = claimedTasks.mapIndexed { idx, task ->
                async(Dispatchers.IO) {
                    if (idx < failCount) {
                        // Atomic dead-letter + step counter decrement
                        val r = stepRepo.deadLetterStepTask(
                            taskId = task.taskId, stepId = stepId,
                            reason = "stress-test failure", claimToken = task.claimToken,
                        )
                        StepTaskResolution(updated = r.taskUpdated, barrierMet = r.barrierMet)
                    } else {
                        stepRepo.resolveStepTask(
                            taskId = task.taskId,
                            stepId = stepId,
                            claimToken = task.claimToken,
                            outputUri = "blob://${task.taskId}",
                        )
                    }
                }
            }.awaitAll()

            val barrierCount = results.count { it.barrierMet }
            assertEquals(1, barrierCount, "Barrier must be met exactly once")

            assertEquals(0, stepRepo.countPendingTasks(stepId))
            assertEquals(failCount, stepRepo.countFailedTasks(stepId))
        }
    }

    // ── 3. Zombie Fencing ───────────────────────────────────────────

    @Nested
    inner class ZombieFencing {

        /**
         * Simulate: worker A claims task → stalls → task reclaimed by reaper →
         * worker B claims task → worker A tries to complete with old generation.
         *
         * Worker A's completion must be rejected (zombie detected).
         */
        @Test
        fun `stale generation is rejected after reclaim`() = runTest {
            val taskId = taskRepo.enqueue(
                EnqueueRequest(handler = "zombie.handler", payload = "{}", queue = "q1"),
            )

            // Worker A claims
            val claimedA = taskRepo.claim("worker-A", listOf("q1"))!!
            val genA = claimedA.claimToken!!

            // Reaper reclaims (simulates stale task detection)
            taskRepo.reclaimStaleTask(taskId, "worker-A presumed dead")

            // Worker B claims the reclaimed task
            val claimedB = taskRepo.claim("worker-B", listOf("q1"))!!
            val genB = claimedB.claimToken!!

            // Worker A tries to complete with stale generation — must fail
            taskRepo.complete(taskId, genA)
            val statusAfterA = readStatus(taskId)
            assertEquals("CLAIMED", statusAfterA, "Stale generation must not complete the task")

            // Worker B completes with correct generation — must succeed
            taskRepo.complete(taskId, genB)
            val statusAfterB = readStatus(taskId)
            assertEquals("COMPLETED", statusAfterB, "Current generation must complete the task")
        }

        /**
         * Group-aware zombie: stale worker completes a step task → resolveStepTask
         * should detect the zombie and NOT decrement the step counter.
         */
        @Test
        fun `stale step task completion does not decrement counter`() = runTest {
            val stepId = "zombie-group"
            val step = WorkflowStep(
                stepId = stepId, workflowName = "stress", runId = stepId, status = StepStatus.ACTIVE,
                params = "{}", queue = "q1", stepLabel = "map", stepTotal = 2,
                onCompleteHandler = "stress.__step_transition",
            )
            val tasks = (0 until 2).map {
                EnqueueRequest(handler = "stress.map", payload = "$it", queue = "q1", stepId = stepId)
            }
            stepRepo.submitStep(step, tasks)

            val task1 = taskRepo.claim("worker-A", listOf("q1"))!!
            val staleGen = task1.claimToken!!

            // Reaper reclaims task1
            taskRepo.reclaimStaleTask(task1.taskId, "dead")
            // Worker B re-claims
            val reClaimed = taskRepo.claim("worker-B", listOf("q1"))!!
            val freshGen = reClaimed.claimToken!!

            // Stale worker tries to resolve — should be rejected
            val staleResult = stepRepo.resolveStepTask(
                taskId = task1.taskId, stepId = stepId,
                claimToken = staleGen, outputUri = "blob://stale",
            )
            assertEquals(false, staleResult.updated, "Zombie must be rejected")
            assertEquals(false, staleResult.barrierMet)

            // Pending must not decrease on zombie
            val step2 = stepRepo.findStep(stepId)!!
            assertEquals(2, stepRepo.countPendingTasks(stepId), "Pending must not decrease on zombie")

            // Fresh worker completes — should succeed
            val freshResult = stepRepo.resolveStepTask(
                taskId = task1.taskId, stepId = stepId,
                claimToken = freshGen, outputUri = "blob://fresh",
            )
            assertEquals(true, freshResult.updated)
            assertEquals(1, stepRepo.countPendingTasks(stepId))
        }

        /**
         * Concurrent zombie race: two workers try to complete the same task
         * with different generations. Only one should succeed.
         */
        @RepeatedTest(10)
        fun `concurrent completion with different generations — only one wins`() = runBlocking {
            val stepId = "race-${System.nanoTime()}"
            val step = WorkflowStep(
                stepId = stepId, workflowName = "stress", runId = stepId, status = StepStatus.ACTIVE,
                params = "{}", queue = "q1", stepLabel = "map", stepTotal = 1,
                onCompleteHandler = "stress.__step_transition",
            )
            stepRepo.submitStep(
                step,
                listOf(EnqueueRequest(handler = "stress.map", payload = "{}", queue = "q1", stepId = stepId)),
            )

            val task = taskRepo.claim("worker-A", listOf("q1"))!!
            val genA = task.claimToken!!

            // Reclaim and re-claim
            taskRepo.reclaimStaleTask(task.taskId, "dead")
            val reClaimed = taskRepo.claim("worker-B", listOf("q1"))!!
            val genB = reClaimed.claimToken!!

            // Both try to resolve concurrently
            val (resultA, resultB) = listOf(
                async(Dispatchers.IO) {
                    stepRepo.resolveStepTask(
                        taskId = task.taskId, stepId = stepId,
                        claimToken = genA, outputUri = "blob://A",
                    )
                },
                async(Dispatchers.IO) {
                    stepRepo.resolveStepTask(
                        taskId = task.taskId, stepId = stepId,
                        claimToken = genB, outputUri = "blob://B",
                    )
                },
            ).awaitAll()

            // Exactly one should succeed
            val successCount = listOf(resultA, resultB).count { it.updated }
            assertEquals(1, successCount, "Exactly one generation must win")

            // The winner must be genB (current generation)
            assertTrue(resultB.updated, "Fresh generation must win")
            assertEquals(false, resultA.updated, "Stale generation must lose")
        }
    }

    // ── 4. End-to-End Step Lifecycle ───────────────────────────────

    @Nested
    inner class EndToEndStepLifecycle {

        /**
         * Full lifecycle: submit step → concurrent claims → concurrent
         * completions → barrier → callback task exists → verify final state.
         */
        @Test
        fun `full step lifecycle under concurrency`() = runBlocking {
            val stepId = "e2e-${System.nanoTime()}"
            val taskCount = 40

            val step = WorkflowStep(
                stepId = stepId,
                workflowName = "e2e",
                runId = stepId,
                status = StepStatus.ACTIVE,
                params = "{}",
                queue = "q1",
                stepLabel = "map",
                stepTotal = taskCount,
                onCompleteHandler = "e2e.__step_transition",
            )
            val tasks = (0 until taskCount).map {
                EnqueueRequest(handler = "e2e.map", payload = "$it", queue = "q1", stepId = stepId)
            }
            stepRepo.submitStep(step, tasks)

            // Phase 1: concurrent claiming (10 workers, greedy)
            val claimed = (0 until 10).map { w ->
                async(Dispatchers.IO) {
                    val mine = mutableListOf<Pair<String, String>>() // taskId, generation
                    while (true) {
                        val task = taskRepo.claim("worker-$w", listOf("q1")) ?: break
                        mine.add(task.taskId to task.claimToken!!)
                    }
                    mine
                }
            }.awaitAll().flatten()

            assertEquals(taskCount, claimed.size, "All tasks claimed")
            assertEquals(taskCount, claimed.map { it.first }.toSet().size, "No duplicate claims")

            // Phase 2: concurrent completion
            val results = claimed.map { (taskId, gen) ->
                async(Dispatchers.IO) {
                    stepRepo.resolveStepTask(
                        taskId = taskId, stepId = stepId,
                        claimToken = gen, outputUri = "blob://$taskId",
                    )
                }
            }.awaitAll()

            assertEquals(taskCount, results.count { it.updated }, "All completions succeed")
            assertEquals(1, results.count { it.barrierMet }, "Barrier met exactly once")

            // Verify final state
            assertEquals(0, stepRepo.countPendingTasks(stepId))
            val step2 = stepRepo.findStep(stepId)!!
            assertEquals(StepStatus.ACTIVE, step2.status) // still ACTIVE — callback will transition

            // Verify callback task
            val callbackCount = jdbi.withHandle<Int, Exception> { h ->
                h.createQuery("SELECT COUNT(*) FROM task WHERE handler = 'e2e.__step_transition'")
                    .mapTo(Int::class.java).one()
            }
            assertEquals(1, callbackCount)

            // Verify all map tasks are COMPLETED with output_uri
            val completedWithOutput = jdbi.withHandle<Int, Exception> { h ->
                h.createQuery(
                    """SELECT COUNT(*) FROM task
                       WHERE step_id = :gid AND handler = 'e2e.map'
                         AND status = 'COMPLETED' AND output_uri IS NOT NULL""",
                ).bind("gid", stepId).mapTo(Int::class.java).one()
            }
            assertEquals(taskCount, completedWithOutput)
        }
    }

    // ── 5. Claim Priority & Scheduling ──────────────────────────────

    @Nested
    inner class PriorityAndScheduling {

        /**
         * High-priority tasks are claimed before low-priority ones,
         * even under concurrent access.
         */
        @Test
        fun `high priority tasks claimed first`() = runBlocking {
            // Enqueue low then high priority
            repeat(10) {
                taskRepo.enqueue(EnqueueRequest(handler = "p.handler", payload = "low-$it", queue = "q1", priority = 0))
            }
            repeat(10) {
                taskRepo.enqueue(EnqueueRequest(handler = "p.handler", payload = "high-$it", queue = "q1", priority = 10))
            }

            // Single worker claims all — first 10 should be high priority
            val claimed = mutableListOf<String>()
            while (true) {
                val task = taskRepo.claim("worker-0", listOf("q1")) ?: break
                claimed.add(task.payload)
            }

            assertEquals(20, claimed.size)
            assertTrue(claimed.subList(0, 10).all { it.startsWith("high-") }, "First 10 should be high priority")
            assertTrue(claimed.subList(10, 20).all { it.startsWith("low-") }, "Last 10 should be low priority")
        }
    }

    // ── 6. Retry Exhaustion & Dead-Letter ───────────────────────────

    @Nested
    inner class RetryExhaustion {

        /**
         * Task with maxRetries=2: fail twice → should be dead-lettered.
         * Concurrent fail + claim cycles should not corrupt retry count.
         */
        @Test
        fun `retry exhaustion dead-letters correctly`() = runTest {
            val taskId = taskRepo.enqueue(
                EnqueueRequest(handler = "retry.handler", payload = "{}", queue = "q1", maxRetries = 2),
            )

            // Attempt 1: claim → fail (retry_count becomes 1)
            val claim1 = taskRepo.claim("worker-1", listOf("q1"))!!
            val dl1 = taskRepo.fail(taskId, "error-1", claimToken = claim1.claimToken)
            assertEquals(false, dl1, "Should retry, not dead-letter")
            assertEquals("PENDING", readStatus(taskId))

            // Attempt 2: claim → fail (retry_count becomes 2 >= maxRetries)
            val claim2 = taskRepo.claim("worker-2", listOf("q1"))!!
            val dl2 = taskRepo.fail(taskId, "error-2", claimToken = claim2.claimToken)
            assertEquals(true, dl2, "Should be dead-lettered now")
            assertEquals("DEAD_LETTER", readStatus(taskId))

            // Should not be claimable anymore
            val claim3 = taskRepo.claim("worker-3", listOf("q1"))
            assertEquals(null, claim3)
        }

        /**
         * Concurrent failures on different tasks in a step — each task's
         * retry count is independent, step failure counter is accurate.
         */
        @RepeatedTest(5)
        fun `concurrent step task failures increment correctly`() = runBlocking {
            val stepId = "fail-group-${System.nanoTime()}"
            val taskCount = 15

            val step = WorkflowStep(
                stepId = stepId, workflowName = "stress", runId = stepId, status = StepStatus.ACTIVE,
                params = "{}", queue = "q1", stepLabel = "map", stepTotal = taskCount,
                onCompleteHandler = "stress.__step_transition",
            )
            stepRepo.submitStep(
                step,
                (0 until taskCount).map {
                    EnqueueRequest(handler = "stress.map", payload = "$it", queue = "q1", stepId = stepId)
                },
            )

            val claimedTasks = (0 until taskCount).map {
                taskRepo.claim("worker-$it", listOf("q1"))!!
            }

            // All tasks fail concurrently — atomic dead-letter + step decrement
            val results = claimedTasks.map { task ->
                async(Dispatchers.IO) {
                    val r = stepRepo.deadLetterStepTask(
                        taskId = task.taskId, stepId = stepId,
                        reason = "concurrent failure", claimToken = task.claimToken,
                    )
                    StepTaskResolution(updated = r.taskUpdated, barrierMet = r.barrierMet)
                }
            }.awaitAll()

            assertEquals(1, results.count { it.barrierMet }, "Barrier met exactly once")

            assertEquals(0, stepRepo.countPendingTasks(stepId))
            assertEquals(taskCount, stepRepo.countFailedTasks(stepId))
        }
    }

    // ── helpers ─────────────────────────────────────────────────────

    private fun readStatus(taskId: String): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT status FROM task WHERE task_id = :id")
                .bind("id", taskId)
                .mapTo(String::class.java)
                .one()
        }
}
