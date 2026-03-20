package com.mapreduce.mr.repository

import com.mapreduce.TestH2Factory
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.model.EnqueueRequest
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
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class JobRepositoryTest {

    private lateinit var jdbi: Jdbi
    private lateinit var repo: TaskGroupRepository

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        val leaderManager = mock<LeaderManager>()
        whenever(leaderManager.isActive).thenReturn(false)
        repo = TaskGroupRepository(jdbi, leaderManager)
    }

    // ── submitGroup ───────────────────────────────────────────────

    @Test
    fun `submitGroup inserts group and task rows atomically`() = runTest {
        val group = testGroup("g-1", phaseTotal = 3)
        val tasks = (0 until 3).map { testTask("g-1", "wc.map", "input-$it") }

        repo.submitGroup(group, tasks)

        val found = repo.findGroup("g-1")
        assertNotNull(found)
        assertEquals("wc", found!!.groupType)
        assertEquals(GroupStatus.ACTIVE, found.status)
        assertEquals(3, found.phaseTotal)
        assertEquals(3, found.tasksPending)
        assertEquals(0, found.tasksFailed)

        val taskCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE group_id = 'g-1'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(3, taskCount)
    }

    @Test
    fun `submitGroup creates tasks with correct handler name`() = runTest {
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
    fun `findGroup returns null for nonexistent group`() = runTest {
        assertNull(repo.findGroup("nonexistent"))
    }

    @Test
    fun `findGroup returns the group with all fields`() = runTest {
        val group = testGroup("g-f", failurePolicy = "THRESHOLD", failureThreshold = 0.5)
        repo.submitGroup(group, listOf(testTask("g-f", "wc.map", "i")))

        val found = repo.findGroup("g-f")
        assertNotNull(found)
        assertEquals("g-f", found!!.groupId)
        assertEquals("THRESHOLD", found.failurePolicy)
        assertEquals(0.5, found.failureThreshold)
    }

    // ── casGroupStatus ────────────────────────────────────────────

    @Test
    fun `casGroupStatus succeeds with correct version and status`() = runTest {
        val group = testGroup("g-cas")
        repo.submitGroup(group, listOf(testTask("g-cas", "wc.map", "a")))

        val result = repo.casGroupStatus("g-cas", GroupStatus.ACTIVE, GroupStatus.COMPLETED, 0)
        assertTrue(result)

        val updated = repo.findGroup("g-cas")!!
        assertEquals(GroupStatus.COMPLETED, updated.status)
        assertEquals(1, updated.version)
    }

    @Test
    fun `casGroupStatus fails with wrong version`() = runTest {
        val group = testGroup("g-cv")
        repo.submitGroup(group, listOf(testTask("g-cv", "wc.map", "a")))

        val result = repo.casGroupStatus("g-cv", GroupStatus.ACTIVE, GroupStatus.COMPLETED, 999)
        assertFalse(result)

        assertEquals(GroupStatus.ACTIVE, repo.findGroup("g-cv")!!.status)
    }

    @Test
    fun `casGroupStatus fails with wrong expected status`() = runTest {
        val group = testGroup("g-cs")
        repo.submitGroup(group, listOf(testTask("g-cs", "wc.map", "a")))

        val result = repo.casGroupStatus("g-cs", GroupStatus.COMPLETED, GroupStatus.FAILED, 0)
        assertFalse(result)
    }

    // ── resolveGroupTask (success path) ────────────────────────────

    @Test
    fun `resolveGroupTask decrements tasks_pending on success`() = runTest {
        val group = testGroup("g-cgt", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-cgt", "wc.map", "a")))

        val taskId = getFirstTaskId("g-cgt")
        claimTask(taskId, "gen-1")

        val result = repo.resolveGroupTask(taskId, "g-cgt", "gen-1", outputUri = "blob://test")

        assertTrue(result.updated)
        assertTrue(result.barrierMet)

        val updated = repo.findGroup("g-cgt")!!
        assertEquals(0, updated.tasksPending)
        assertEquals(0, updated.tasksFailed)

        assertEquals("COMPLETED", getTaskStatus(taskId))
    }

    @Test
    fun `resolveGroupTask zombie detection -- zero rows when execution_generation mismatches`() = runTest {
        val group = testGroup("g-zombie", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-zombie", "wc.map", "a")))

        val taskId = getFirstTaskId("g-zombie")
        claimTask(taskId, "gen-correct")

        val result = repo.resolveGroupTask(taskId, "g-zombie", "gen-wrong", outputUri = "blob://zombie")

        assertFalse(result.updated)
        assertFalse(result.barrierMet)

        val found = repo.findGroup("g-zombie")!!
        assertEquals(1, found.tasksPending)
        assertEquals("CLAIMED", getTaskStatus(taskId))
    }

    @Test
    fun `resolveGroupTask stores output_uri on task row`() = runTest {
        val group = testGroup("g-out", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-out", "wc.map", "a")))

        val taskId = getFirstTaskId("g-out")
        claimTask(taskId, "gen-1")

        repo.resolveGroupTask(taskId, "g-out", "gen-1", outputUri = "blob://my-output", outputMetadata = """{"key":"val"}""")

        val outputUri = jdbi.withHandle<String?, Exception> { h ->
            h.createQuery("SELECT output_uri FROM task WHERE task_id = :taskId")
                .bind("taskId", taskId)
                .mapTo(String::class.java).one()
        }
        assertEquals("blob://my-output", outputUri)
    }

    @Test
    fun `resolveGroupTask creates callback task when barrier is met`() = runTest {
        val group = testGroup("g-barrier", phaseTotal = 1, onCompleteHandler = "wc.__phase_complete")
        repo.submitGroup(group, listOf(testTask("g-barrier", "wc.map", "a")))

        val taskId = getFirstTaskId("g-barrier")
        claimTask(taskId, "gen-1")

        val result = repo.resolveGroupTask(taskId, "g-barrier", "gen-1", outputUri = "blob://x")

        assertTrue(result.barrierMet)

        val callbackCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'wc.__phase_complete' AND group_id IS NULL",
            ).mapTo(Int::class.java).one()
        }
        assertEquals(1, callbackCount)
    }

    // ── resolveGroupTask (failure path) ───────────────────────────

    @Test
    fun `resolveGroupTask with failed=true decrements pending and increments failed`() = runTest {
        val group = testGroup("g-fail", phaseTotal = 2)
        repo.submitGroup(group, (0 until 2).map { testTask("g-fail", "wc.map", "i-$it") })

        repo.resolveGroupTask(groupId = "g-fail", failed = true)

        val updated = repo.findGroup("g-fail")!!
        assertEquals(1, updated.tasksPending)
        assertEquals(1, updated.tasksFailed)
    }

    @Test
    fun `resolveGroupTask with failed=true creates callback when barrier met`() = runTest {
        val group = testGroup("g-fail-barrier", phaseTotal = 1, onCompleteHandler = "wc.__phase_complete")
        repo.submitGroup(group, listOf(testTask("g-fail-barrier", "wc.map", "a")))

        repo.resolveGroupTask(groupId = "g-fail-barrier", failed = true)

        val callbackCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                "SELECT COUNT(*) FROM task WHERE handler = 'wc.__phase_complete' AND group_id IS NULL AND payload = 'g-fail-barrier'",
            ).mapTo(Int::class.java).one()
        }
        assertEquals(1, callbackCount)
    }

    // ── transitionPhase ───────────────────────────────────────────

    @Test
    fun `transitionPhase atomically transitions and creates new tasks`() = runTest {
        val group = testGroup("g-tp", phaseTotal = 2)
        repo.submitGroup(group, (0 until 2).map { testTask("g-tp", "wc.map", "i-$it") })

        val reduceTasks = (0 until 2).map { testTask("g-tp", "wc.reduce", "{}") }
        val result = repo.transitionPhase("g-tp", 0, "reduce", 2, reduceTasks, "wc.__phase_complete")
        assertTrue(result)

        val updated = repo.findGroup("g-tp")!!
        assertEquals("reduce", updated.phase)
        assertEquals(2, updated.phaseTotal)
        assertEquals(2, updated.tasksPending)
        assertEquals(0, updated.tasksFailed)
        assertEquals(1, updated.version)

        val reduceCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE group_id = 'g-tp' AND handler = 'wc.reduce'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(2, reduceCount)
    }

    @Test
    fun `transitionPhase fails with wrong version and creates no tasks`() = runTest {
        val group = testGroup("g-tp-fail", phaseTotal = 1)
        repo.submitGroup(group, listOf(testTask("g-tp-fail", "wc.map", "a")))

        val result = repo.transitionPhase("g-tp-fail", 999, "reduce", 1,
            listOf(testTask("g-tp-fail", "wc.reduce", "{}")), "wc.__phase_complete")
        assertFalse(result)

        val found = repo.findGroup("g-tp-fail")!!
        assertEquals("map", found.phase)
        assertEquals(0, found.version)
    }

    // ── streamTaskOutputs ─────────────────────────────────────────

    @Test
    fun `streamTaskOutputs returns output URIs from completed tasks`() = runTest {
        val group = testGroup("g-stream", phaseTotal = 2)
        repo.submitGroup(group, (0 until 2).map { testTask("g-stream", "wc.map", "i-$it") })

        val taskIds = getAllTaskIds("g-stream")
        taskIds.forEachIndexed { i, taskId ->
            claimTask(taskId, "gen-$i")
            repo.resolveGroupTask(taskId, "g-stream", "gen-$i", outputUri = "blob://$i")
        }

        val outputs = repo.streamTaskOutputs("g-stream", "wc.map").toList()
        assertEquals(2, outputs.size)
        assertTrue(outputs.map { it.uri }.containsAll(listOf("blob://0", "blob://1")))
    }

    // ── Helpers ──────────────────────────────────────────────────

    private fun testGroup(
        groupId: String,
        phaseTotal: Int = 1,
        failurePolicy: String = "FAIL_GROUP",
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
