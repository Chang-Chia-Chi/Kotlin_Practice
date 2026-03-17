package com.mapreduce.mr.repository

import com.mapreduce.TestH2Factory
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.FailurePolicy
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.repository.TaskGroupRepository
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue

class JobRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var repo: TaskGroupRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        repo = TaskGroupRepository(jdbi)
    }

    // ── submitGroup ───────────────────────────────────────────────

    @Test
    fun `submitGroup inserts group and task rows atomically`() {
        val group = testGroup("g-1", phaseTotal = 3)
        val tasks = (0 until 3).map { testTask("g-1", "wc.map", "input-$it") }

        repo.submitGroup(group, tasks)

        val found = repo.findGroup("g-1")
        assertNotNull(found)
        assertEquals("wc", found!!.groupType)
        assertEquals(GroupStatus.ACTIVE, found.status)
        assertEquals(3, found.phaseTotal)
        assertEquals(0, found.phaseCompleted)

        val taskCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE group_id = 'g-1'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(3, taskCount)
    }

    @Test
    fun `submitGroup creates tasks with correct handler name`() {
        val group = testGroup("g-h", phaseTotal = 1)
        val tasks = listOf(testTask("g-h", "email.map", "a"))

        repo.submitGroup(group, tasks)

        val handler = jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT handler FROM task WHERE group_id = 'g-h'")
                .mapTo(String::class.java).one()
        }
        assertEquals("email.map", handler)
    }

    // ── findGroup ─────────────────────────────────────────────────

    @Test
    fun `findGroup returns null for nonexistent group`() {
        assertNull(repo.findGroup("nonexistent"))
    }

    @Test
    fun `findGroup returns the group with all fields`() {
        val group = testGroup("g-f", failurePolicy = FailurePolicy.THRESHOLD, failureThreshold = 0.5)
        repo.submitGroup(group, listOf(testTask("g-f", "wc.map", "i")))

        val found = repo.findGroup("g-f")
        assertNotNull(found)
        assertEquals("g-f", found!!.groupId)
        assertEquals(FailurePolicy.THRESHOLD, found.failurePolicy)
        assertEquals(0.5, found.failureThreshold)
    }

    // ── casGroupStatus ────────────────────────────────────────────

    @Test
    fun `casGroupStatus succeeds with correct version and status`() {
        val group = testGroup("g-cas")
        repo.submitGroup(group, listOf(testTask("g-cas", "wc.map", "a")))

        val result = repo.casGroupStatus("g-cas", GroupStatus.ACTIVE, GroupStatus.COMPLETED, 0)
        assertTrue(result)

        val updated = repo.findGroup("g-cas")!!
        assertEquals(GroupStatus.COMPLETED, updated.status)
        assertEquals(1, updated.version)
    }

    @Test
    fun `casGroupStatus fails with wrong version`() {
        val group = testGroup("g-cv")
        repo.submitGroup(group, listOf(testTask("g-cv", "wc.map", "a")))

        val result = repo.casGroupStatus("g-cv", GroupStatus.ACTIVE, GroupStatus.COMPLETED, 999)
        assertFalse(result)

        assertEquals(GroupStatus.ACTIVE, repo.findGroup("g-cv")!!.status)
    }

    @Test
    fun `casGroupStatus fails with wrong expected status`() {
        val group = testGroup("g-cs")
        repo.submitGroup(group, listOf(testTask("g-cs", "wc.map", "a")))

        val result = repo.casGroupStatus("g-cs", GroupStatus.COMPLETED, GroupStatus.FAILED, 0)
        assertFalse(result)
    }

    // ── completeGroupTask ─────────────────────────────────────────

    @Test
    fun `completeGroupTask increments phase_completed when task is CLAIMED`() {
        val group = testGroup("g-cgt", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-cgt", "wc.map", "a")))

        val taskId = getFirstTaskId("g-cgt")
        claimTask(taskId, "gen-1")

        val result = repo.completeGroupTask(taskId, "g-cgt", "gen-1", "blob://test", null)

        assertTrue(result.updated)
        assertTrue(result.barrierMet)

        val updated = repo.findGroup("g-cgt")!!
        assertEquals(1, updated.phaseCompleted)

        assertEquals("COMPLETED", getTaskStatus(taskId))
    }

    @Test
    fun `completeGroupTask zombie detection -- zero rows when execution_generation mismatches`() {
        val group = testGroup("g-zombie", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-zombie", "wc.map", "a")))

        val taskId = getFirstTaskId("g-zombie")
        claimTask(taskId, "gen-correct")

        val result = repo.completeGroupTask(taskId, "g-zombie", "gen-wrong", "blob://zombie", null)

        assertFalse(result.updated)
        assertFalse(result.barrierMet)

        val found = repo.findGroup("g-zombie")!!
        assertEquals(0, found.phaseCompleted)
        assertEquals("CLAIMED", getTaskStatus(taskId))
    }

    @Test
    fun `completeGroupTask stores output_uri on task row`() {
        val group = testGroup("g-out", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-out", "wc.map", "a")))

        val taskId = getFirstTaskId("g-out")
        claimTask(taskId, "gen-1")

        repo.completeGroupTask(taskId, "g-out", "gen-1", "blob://my-output", """{"key":"val"}""")

        val outputUri = jdbi.withHandle<String?, Exception> { h ->
            h.createQuery("SELECT output_uri FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java).one()
        }
        assertEquals("blob://my-output", outputUri)
    }

    @Test
    fun `completeGroupTask creates callback task when barrier is met`() {
        val group = testGroup("g-barrier", phaseTotal = 1, onCompleteHandler = "wc.__phase_complete")
        repo.submitGroup(group, listOf(testTask("g-barrier", "wc.map", "a")))

        val taskId = getFirstTaskId("g-barrier")
        claimTask(taskId, "gen-1")

        val result = repo.completeGroupTask(taskId, "g-barrier", "gen-1", "blob://x", null)

        assertTrue(result.barrierMet)

        // Check that callback task was created with NULL group_id
        val callbackCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'wc.__phase_complete' AND group_id IS NULL",
            ).mapTo(Int::class.java).one()
        }
        assertEquals(1, callbackCount)
    }

    // ── transitionPhase ───────────────────────────────────────────

    @Test
    fun `transitionPhase atomically transitions and creates new tasks`() {
        val group = testGroup("g-tp", phaseTotal = 2)
        repo.submitGroup(group, (0 until 2).map { testTask("g-tp", "wc.map", "i-$it") })

        val reduceTasks = (0 until 2).map { testTask("g-tp", "wc.reduce", "{}") }
        val result = repo.transitionPhase("g-tp", 0, "reduce", 2, reduceTasks, "wc.__phase_complete")
        assertTrue(result)

        val updated = repo.findGroup("g-tp")!!
        assertEquals("reduce", updated.phase)
        assertEquals(2, updated.phaseTotal)
        assertEquals(0, updated.phaseCompleted)
        assertEquals(1, updated.version)

        val reduceCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE group_id = 'g-tp' AND handler = 'wc.reduce'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(2, reduceCount)
    }

    @Test
    fun `transitionPhase fails with wrong version and creates no tasks`() {
        val group = testGroup("g-tp-fail", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-tp-fail", "wc.map", "a")))

        val result = repo.transitionPhase("g-tp-fail", 999, "reduce", 1,
            listOf(testTask("g-tp-fail", "wc.reduce", "{}")), "wc.__phase_complete")
        assertFalse(result)

        val found = repo.findGroup("g-tp-fail")!!
        assertEquals("map", found.phase)
        assertEquals(0, found.version)
    }

    // ── recordGroupTaskFailure ────────────────────────────────────

    @Test
    fun `recordGroupTaskFailure increments phase_failed`() {
        val group = testGroup("g-fail", phaseTotal = 2)
        repo.submitGroup(group, (0 until 2).map { testTask("g-fail", "wc.map", "i-$it") })

        repo.recordGroupTaskFailure("g-fail")

        val updated = repo.findGroup("g-fail")!!
        assertEquals(1, updated.phaseFailed)
    }

    @Test
    fun `recordGroupTaskFailure creates callback when barrier met`() {
        val group = testGroup("g-fail-barrier", phaseTotal = 1, onCompleteHandler = "wc.__phase_complete")
        repo.submitGroup(group, listOf(testTask("g-fail-barrier", "wc.map", "a")))

        repo.recordGroupTaskFailure("g-fail-barrier")

        val callbackCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'wc.__phase_complete' AND group_id IS NULL AND payload = 'g-fail-barrier'",
            ).mapTo(Int::class.java).one()
        }
        assertEquals(1, callbackCount)
    }

    // ── streamTaskOutputs ─────────────────────────────────────────

    @Test
    fun `streamTaskOutputs returns output URIs from completed tasks`() = runTest {
        val group = testGroup("g-stream", phaseTotal = 2)
        repo.submitGroup(group, (0 until 2).map { testTask("g-stream", "wc.map", "i-$it") })

        // Claim and complete tasks with output URIs
        val taskIds = getAllTaskIds("g-stream")
        taskIds.forEachIndexed { i, taskId ->
            claimTask(taskId, "gen-$i")
            repo.completeGroupTask(taskId, "g-stream", "gen-$i", "blob://$i", null)
        }

        val outputs = repo.streamTaskOutputs("g-stream", "wc.map").toList()
        assertEquals(2, outputs.size)
        assertTrue(outputs.map { it.uri }.containsAll(listOf("blob://0", "blob://1")))
    }

    // ── Helpers ──────────────────────────────────────────────────

    private fun testGroup(
        groupId: String,
        phaseTotal: Int = 1,
        failurePolicy: FailurePolicy = FailurePolicy.FAIL_GROUP,
        failureThreshold: Double = 0.0,
        onCompleteHandler: String? = null,
    ) = TaskGroup(
        groupId = groupId,
        groupType = "wc",
        status = GroupStatus.ACTIVE,
        params = "{}",
        queue = "mr",
        phase = "map",
        phaseTotal = phaseTotal,
        onCompleteHandler = onCompleteHandler,
        failurePolicy = failurePolicy,
        failureThreshold = failureThreshold,
    )

    private fun testTask(groupId: String, handler: String, payload: String) =
        EnqueueRequest(
            handler = handler,
            payload = payload,
            queue = "mr",
            groupId = groupId,
            maxRetries = 3,
        )

    private fun getFirstTaskId(groupId: String): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT task_id FROM task WHERE group_id = :groupId")
                .bind("groupId", groupId)
                .mapTo(String::class.java).first()
        }

    private fun getAllTaskIds(groupId: String): List<String> =
        jdbi.withHandle<List<String>, Exception> { h ->
            h.createQuery("SELECT task_id FROM task WHERE group_id = :groupId ORDER BY created_at")
                .bind("groupId", groupId)
                .mapTo(String::class.java).list()
        }

    private fun getTaskStatus(taskId: String): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT status FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java).one()
        }

    private fun claimTask(taskId: String, generation: String) {
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE task SET status = 'CLAIMED', execution_generation = :gen WHERE task_id = :taskId",
            ).bind("taskId", taskId).bind("gen", generation).execute()
        }
    }
}
