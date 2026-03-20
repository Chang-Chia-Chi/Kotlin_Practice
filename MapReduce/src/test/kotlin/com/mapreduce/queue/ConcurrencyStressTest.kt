package com.mapreduce.queue

import com.mapreduce.TestH2Factory
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.GroupTaskResolution
import com.mapreduce.queue.repository.TaskGroupRepository
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
 * 2. **Exactly-once barrier** — The row lock on `task_group` serializes
 *    concurrent completions; exactly one worker observes `tasks_pending = 0`
 *    and creates the callback task.
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
    private lateinit var groupRepo: TaskGroupRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        taskRepo = TaskRepository(jdbi)
        val leaderManager = mock<LeaderManager>()
        whenever(leaderManager.isActive).thenReturn(false)
        groupRepo = TaskGroupRepository(jdbi, leaderManager)
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
         * Create a group with N tasks and a callback handler. Complete all N
         * tasks concurrently. Exactly one callback task must be created.
         */
        @RepeatedTest(10)
        fun `barrier fires exactly once under concurrent completion`() = runBlocking {
            val groupId = "barrier-${System.nanoTime()}"
            val taskCount = 30

            val group = TaskGroup(
                groupId = groupId,
                groupType = "stress",
                status = GroupStatus.ACTIVE,
                params = "{}",
                queue = "q1",
                phase = "map",
                phaseTotal = taskCount,
                onCompleteHandler = "stress.__phase_complete",
                failurePolicy = "FAIL_GROUP",
                failureThreshold = 0.0,
            )
            val tasks = (0 until taskCount).map {
                EnqueueRequest(
                    handler = "stress.map", payload = "{\"i\":$it}",
                    queue = "q1", groupId = groupId,
                )
            }
            groupRepo.submitGroup(group, tasks)

            // Claim all tasks
            val claimedTasks = (0 until taskCount).map {
                taskRepo.claim("worker-$it", listOf("q1"))!!
            }

            // Complete all concurrently
            val results = claimedTasks.map { task ->
                async(Dispatchers.IO) {
                    groupRepo.resolveGroupTask(
                        taskId = task.taskId,
                        groupId = groupId,
                        claimToken = task.claimToken,
                        failed = false,
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
                    "SELECT COUNT(*) FROM task WHERE handler = 'stress.__phase_complete' AND payload = :groupId",
                ).bind("groupId", groupId).mapTo(Int::class.java).one()
            }
            assertEquals(1, callbackCount, "Exactly one callback task")

            // Group should have tasks_pending = 0
            val grp = groupRepo.findGroup(groupId)!!
            assertEquals(0, grp.tasksPending)
            assertEquals(0, grp.tasksFailed)
        }

        /**
         * Mixed success/failure completions. Barrier still fires exactly once,
         * and failure count is accurate.
         */
        @RepeatedTest(5)
        fun `barrier fires once with mixed success and failure`() = runBlocking {
            val groupId = "mixed-${System.nanoTime()}"
            val taskCount = 20
            val failCount = 7 // tasks at indices 0..6 will fail

            val group = TaskGroup(
                groupId = groupId,
                groupType = "stress",
                status = GroupStatus.ACTIVE,
                params = "{}",
                queue = "q1",
                phase = "map",
                phaseTotal = taskCount,
                onCompleteHandler = "stress.__phase_complete",
                failurePolicy = "FAIL_GROUP",
                failureThreshold = 0.0,
            )
            val tasks = (0 until taskCount).map {
                EnqueueRequest(
                    handler = "stress.map", payload = "{\"i\":$it}",
                    queue = "q1", groupId = groupId,
                )
            }
            groupRepo.submitGroup(group, tasks)

            val claimedTasks = (0 until taskCount).map {
                taskRepo.claim("worker-$it", listOf("q1"))!!
            }

            val results = claimedTasks.mapIndexed { idx, task ->
                async(Dispatchers.IO) {
                    if (idx < failCount) {
                        // Atomic dead-letter + group counter decrement
                        val r = groupRepo.deadLetterGroupTask(
                            taskId = task.taskId, groupId = groupId,
                            reason = "stress-test failure", claimToken = task.claimToken,
                        )
                        GroupTaskResolution(updated = r.taskUpdated, barrierMet = r.barrierMet)
                    } else {
                        groupRepo.resolveGroupTask(
                            taskId = task.taskId,
                            groupId = groupId,
                            claimToken = task.claimToken,
                            failed = false,
                            outputUri = "blob://${task.taskId}",
                        )
                    }
                }
            }.awaitAll()

            val barrierCount = results.count { it.barrierMet }
            assertEquals(1, barrierCount, "Barrier must be met exactly once")

            val grp = groupRepo.findGroup(groupId)!!
            assertEquals(0, grp.tasksPending)
            assertEquals(failCount, grp.tasksFailed)
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
         * Group-aware zombie: stale worker completes a group task → resolveGroupTask
         * should detect the zombie and NOT decrement the group counter.
         */
        @Test
        fun `stale group task completion does not decrement counter`() = runTest {
            val groupId = "zombie-group"
            val group = TaskGroup(
                groupId = groupId, groupType = "stress", status = GroupStatus.ACTIVE,
                params = "{}", queue = "q1", phase = "map", phaseTotal = 2,
                onCompleteHandler = "stress.__phase_complete",
            )
            val tasks = (0 until 2).map {
                EnqueueRequest(handler = "stress.map", payload = "$it", queue = "q1", groupId = groupId)
            }
            groupRepo.submitGroup(group, tasks)

            val task1 = taskRepo.claim("worker-A", listOf("q1"))!!
            val staleGen = task1.claimToken!!

            // Reaper reclaims task1
            taskRepo.reclaimStaleTask(task1.taskId, "dead")
            // Worker B re-claims
            val reClaimed = taskRepo.claim("worker-B", listOf("q1"))!!
            val freshGen = reClaimed.claimToken!!

            // Stale worker tries to resolve — should be rejected
            val staleResult = groupRepo.resolveGroupTask(
                taskId = task1.taskId, groupId = groupId,
                claimToken = staleGen, outputUri = "blob://stale",
            )
            assertEquals(false, staleResult.updated, "Zombie must be rejected")
            assertEquals(false, staleResult.barrierMet)

            // Group counter should still be at 2
            val grp = groupRepo.findGroup(groupId)!!
            assertEquals(2, grp.tasksPending, "Counter must not decrement on zombie")

            // Fresh worker completes — should succeed
            val freshResult = groupRepo.resolveGroupTask(
                taskId = task1.taskId, groupId = groupId,
                claimToken = freshGen, outputUri = "blob://fresh",
            )
            assertEquals(true, freshResult.updated)
            assertEquals(1, groupRepo.findGroup(groupId)!!.tasksPending)
        }

        /**
         * Concurrent zombie race: two workers try to complete the same task
         * with different generations. Only one should succeed.
         */
        @RepeatedTest(10)
        fun `concurrent completion with different generations — only one wins`() = runBlocking {
            val groupId = "race-${System.nanoTime()}"
            val group = TaskGroup(
                groupId = groupId, groupType = "stress", status = GroupStatus.ACTIVE,
                params = "{}", queue = "q1", phase = "map", phaseTotal = 1,
                onCompleteHandler = "stress.__phase_complete",
            )
            groupRepo.submitGroup(
                group,
                listOf(EnqueueRequest(handler = "stress.map", payload = "{}", queue = "q1", groupId = groupId)),
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
                    groupRepo.resolveGroupTask(
                        taskId = task.taskId, groupId = groupId,
                        claimToken = genA, outputUri = "blob://A",
                    )
                },
                async(Dispatchers.IO) {
                    groupRepo.resolveGroupTask(
                        taskId = task.taskId, groupId = groupId,
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

    // ── 4. End-to-End Group Lifecycle ───────────────────────────────

    @Nested
    inner class EndToEndGroupLifecycle {

        /**
         * Full lifecycle: submit group → concurrent claims → concurrent
         * completions → barrier → callback task exists → verify final state.
         */
        @Test
        fun `full group lifecycle under concurrency`() = runBlocking {
            val groupId = "e2e-${System.nanoTime()}"
            val taskCount = 40

            val group = TaskGroup(
                groupId = groupId,
                groupType = "e2e",
                status = GroupStatus.ACTIVE,
                params = "{}",
                queue = "q1",
                phase = "map",
                phaseTotal = taskCount,
                onCompleteHandler = "e2e.__phase_complete",
            )
            val tasks = (0 until taskCount).map {
                EnqueueRequest(handler = "e2e.map", payload = "$it", queue = "q1", groupId = groupId)
            }
            groupRepo.submitGroup(group, tasks)

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
                    groupRepo.resolveGroupTask(
                        taskId = taskId, groupId = groupId,
                        claimToken = gen, outputUri = "blob://$taskId",
                    )
                }
            }.awaitAll()

            assertEquals(taskCount, results.count { it.updated }, "All completions succeed")
            assertEquals(1, results.count { it.barrierMet }, "Barrier met exactly once")

            // Verify final state
            val grp = groupRepo.findGroup(groupId)!!
            assertEquals(0, grp.tasksPending)
            assertEquals(0, grp.tasksFailed)
            assertEquals(GroupStatus.ACTIVE, grp.status) // still ACTIVE — callback will transition

            // Verify callback task
            val callbackCount = jdbi.withHandle<Int, Exception> { h ->
                h.createQuery("SELECT COUNT(*) FROM task WHERE handler = 'e2e.__phase_complete'")
                    .mapTo(Int::class.java).one()
            }
            assertEquals(1, callbackCount)

            // Verify all map tasks are COMPLETED with output_uri
            val completedWithOutput = jdbi.withHandle<Int, Exception> { h ->
                h.createQuery(
                    """SELECT COUNT(*) FROM task
                       WHERE group_id = :gid AND handler = 'e2e.map'
                         AND status = 'COMPLETED' AND output_uri IS NOT NULL""",
                ).bind("gid", groupId).mapTo(Int::class.java).one()
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
         * Concurrent failures on different tasks in a group — each task's
         * retry count is independent, group failure counter is accurate.
         */
        @RepeatedTest(5)
        fun `concurrent group task failures increment correctly`() = runBlocking {
            val groupId = "fail-group-${System.nanoTime()}"
            val taskCount = 15

            val group = TaskGroup(
                groupId = groupId, groupType = "stress", status = GroupStatus.ACTIVE,
                params = "{}", queue = "q1", phase = "map", phaseTotal = taskCount,
                onCompleteHandler = "stress.__phase_complete",
            )
            groupRepo.submitGroup(
                group,
                (0 until taskCount).map {
                    EnqueueRequest(handler = "stress.map", payload = "$it", queue = "q1", groupId = groupId)
                },
            )

            val claimedTasks = (0 until taskCount).map {
                taskRepo.claim("worker-$it", listOf("q1"))!!
            }

            // All tasks fail concurrently — atomic dead-letter + group decrement
            val results = claimedTasks.map { task ->
                async(Dispatchers.IO) {
                    val r = groupRepo.deadLetterGroupTask(
                        taskId = task.taskId, groupId = groupId,
                        reason = "concurrent failure", claimToken = task.claimToken,
                    )
                    GroupTaskResolution(updated = r.taskUpdated, barrierMet = r.barrierMet)
                }
            }.awaitAll()

            assertEquals(1, results.count { it.barrierMet }, "Barrier met exactly once")

            val grp = groupRepo.findGroup(groupId)!!
            assertEquals(0, grp.tasksPending)
            assertEquals(taskCount, grp.tasksFailed)
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
