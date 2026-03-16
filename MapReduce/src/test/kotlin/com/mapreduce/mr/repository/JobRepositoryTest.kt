package com.mapreduce.mr.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.TestH2Factory
import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.model.JobStatus
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
    private lateinit var repo: JobRepository
    private val objectMapper = ObjectMapper()

    @BeforeEach
    fun setUp() {
        jdbi = TestH2Factory.create()
        repo = JobRepository(jdbi, objectMapper)
    }

    // ── submitJob ────────────────────────────────────────────────

    @Test
    fun `submitJob inserts job and task rows atomically`() {
        val inputs = listOf("input-0", "input-1", "input-2")
        repo.submitJob(
            jobId = "j-1", jobType = "wc", jobParams = "{}",
            taskInputs = inputs, maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        val job = repo.findJobById("j-1")
        assertNotNull(job)
        assertEquals("wc", job!!.jobType)
        assertEquals(JobStatus.RUNNING, job.status)
        assertEquals(3, job.totalTasks)
        assertEquals(0, job.completedTasks)

        val taskCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE group_id = 'j-1'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(3, taskCount)
    }

    @Test
    fun `submitJob creates tasks with correct handler name`() {
        repo.submitJob(
            jobId = "j-h", jobType = "email", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 2,
            failurePolicy = FailurePolicy.BEST_EFFORT, failureThreshold = 0.0,
            queue = "default",
        )

        val handler = jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT handler FROM task WHERE group_id = 'j-h'")
                .mapTo(String::class.java).one()
        }
        assertEquals("email.map", handler)
    }

    // ── findJobById ──────────────────────────────────────────────

    @Test
    fun `findJobById returns null for nonexistent job`() {
        assertNull(repo.findJobById("nonexistent"))
    }

    @Test
    fun `findJobById returns the job`() {
        repo.submitJob(
            jobId = "j-f", jobType = "wc", jobParams = """{"k":"v"}""",
            taskInputs = listOf("i"), maxRetries = 3,
            failurePolicy = FailurePolicy.THRESHOLD, failureThreshold = 0.5,
            queue = "mr",
        )

        val job = repo.findJobById("j-f")
        assertNotNull(job)
        assertEquals("j-f", job!!.jobId)
        assertEquals(FailurePolicy.THRESHOLD, job.failurePolicy)
        assertEquals(0.5, job.failureThreshold)
    }

    // ── casJobStatus ─────────────────────────────────────────────

    @Test
    fun `casJobStatus succeeds with correct version and status`() {
        repo.submitJob(
            jobId = "j-cas", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        val result = repo.casJobStatus("j-cas", JobStatus.RUNNING, JobStatus.REDUCING, 0)
        assertTrue(result)

        val updated = repo.findJobById("j-cas")!!
        assertEquals(JobStatus.REDUCING, updated.status)
        assertEquals(1, updated.version)
    }

    @Test
    fun `casJobStatus fails with wrong version`() {
        repo.submitJob(
            jobId = "j-cv", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        val result = repo.casJobStatus("j-cv", JobStatus.RUNNING, JobStatus.REDUCING, 999)
        assertFalse(result)

        // Status should not have changed
        assertEquals(JobStatus.RUNNING, repo.findJobById("j-cv")!!.status)
    }

    @Test
    fun `casJobStatus fails with wrong expected status`() {
        repo.submitJob(
            jobId = "j-cs", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        // Job is RUNNING, not REDUCING
        val result = repo.casJobStatus("j-cs", JobStatus.REDUCING, JobStatus.COMPLETED, 0)
        assertFalse(result)
    }

    // ── completeMapTask ──────────────────────────────────────────

    @Test
    fun `completeMapTask increments completed_tasks when task is CLAIMED`() {
        repo.submitJob(
            jobId = "j-cmt", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        val taskId = getFirstTaskId("j-cmt")
        claimTask(taskId, "gen-1")

        repo.completeMapTask(taskId, "j-cmt", "blob://test", "gen-1", 0)

        val job = repo.findJobById("j-cmt")!!
        assertEquals(1, job.completedTasks)

        val taskStatus = getTaskStatus(taskId)
        assertEquals("COMPLETED", taskStatus)
    }

    @Test
    fun `completeMapTask zombie detection -- zero rows when execution_generation mismatches`() {
        repo.submitJob(
            jobId = "j-zombie", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        val taskId = getFirstTaskId("j-zombie")
        claimTask(taskId, "gen-correct")

        // Try to complete with wrong generation (zombie worker)
        repo.completeMapTask(taskId, "j-zombie", "blob://zombie", "gen-wrong", 0)

        // completed_tasks should NOT have incremented
        val job = repo.findJobById("j-zombie")!!
        assertEquals(0, job.completedTasks)

        // Task should still be CLAIMED (not COMPLETED)
        assertEquals("CLAIMED", getTaskStatus(taskId))
    }

    @Test
    fun `completeMapTask rolls back mr_output on zombie detection`() {
        repo.submitJob(
            jobId = "j-rollback", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        val taskId = getFirstTaskId("j-rollback")
        claimTask(taskId, "gen-real")

        // Zombie completes with wrong generation
        repo.completeMapTask(taskId, "j-rollback", "blob://orphan", "gen-zombie", 0)

        // mr_output should have been cleaned up
        val outputCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM mr_output WHERE job_id = 'j-rollback'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(0, outputCount)
    }

    // ── completeReduceTask ───────────────────────────────────────

    @Test
    fun `completeReduceTask updates result_metadata`() {
        repo.submitJob(
            jobId = "j-red", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        // Insert a reduce task manually
        val reduceTaskId = insertReduceTask("j-red", "wc.reduce")
        claimTask(reduceTaskId, "gen-r")

        repo.completeReduceTask(reduceTaskId, "j-red", """{"total":42}""", "gen-r")

        val job = repo.findJobById("j-red")!!
        assertEquals("""{"total":42}""", job.resultMetadata)

        assertEquals("COMPLETED", getTaskStatus(reduceTaskId))
    }

    // ── insertReduceTasks ────────────────────────────────────────

    @Test
    fun `insertReduceTasks creates correct number of reduce tasks`() {
        repo.submitJob(
            jobId = "j-irt", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        repo.insertReduceTasks("j-irt", "wc", maxRetries = 3, queue = "mr", totalPartitions = 4)

        val reduceCount = jdbi.withHandle<Int, Exception> { h ->
            h.createQuery("SELECT COUNT(*) FROM task WHERE group_id = 'j-irt' AND handler = 'wc.reduce'")
                .mapTo(Int::class.java).one()
        }
        assertEquals(4, reduceCount)
    }

    @Test
    fun `insertReduceTasks with single partition has REDUCE phase metadata`() {
        repo.submitJob(
            jobId = "j-sp", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        repo.insertReduceTasks("j-sp", "wc", maxRetries = 3, queue = "mr", totalPartitions = 1)

        val metadata = jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT metadata FROM task WHERE group_id = 'j-sp' AND handler = 'wc.reduce'")
                .mapTo(String::class.java).one()
        }
        assertTrue(metadata.contains("REDUCE"))
    }

    // ── streamBlobUris ───────────────────────────────────────────

    @Test
    fun `streamBlobUris returns blob URIs`() = runTest {
        repo.submitJob(
            jobId = "j-blob", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a", "b"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        // Insert output rows directly
        insertMrOutput("j-blob", "t-a", "blob://a", 0)
        insertMrOutput("j-blob", "t-b", "blob://b", 0)

        val uris = repo.streamBlobUris("j-blob").toList()
        assertEquals(2, uris.size)
        assertTrue(uris.containsAll(listOf("blob://a", "blob://b")))
    }

    @Test
    fun `streamBlobUris filters by partition hash`() = runTest {
        repo.submitJob(
            jobId = "j-part", jobType = "wc", jobParams = "{}",
            taskInputs = listOf("a"), maxRetries = 3,
            failurePolicy = FailurePolicy.FAIL_JOB, failureThreshold = 0.0,
            queue = "mr",
        )

        insertMrOutput("j-part", "t-1", "blob://p0", 0)
        insertMrOutput("j-part", "t-2", "blob://p1", 1)
        insertMrOutput("j-part", "t-3", "blob://p0b", 0)

        val partition0 = repo.streamBlobUris("j-part", 0).toList()
        assertEquals(2, partition0.size)
        assertTrue(partition0.all { it.contains("p0") })

        val partition1 = repo.streamBlobUris("j-part", 1).toList()
        assertEquals(1, partition1.size)
        assertEquals("blob://p1", partition1.first())
    }

    // ── Helpers ──────────────────────────────────────────────────

    private fun getFirstTaskId(jobId: String): String =
        jdbi.withHandle<String, Exception> { h ->
            h.createQuery("SELECT task_id FROM task WHERE group_id = :jobId")
                .bind("jobId", jobId)
                .mapTo(String::class.java).first()
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
                "UPDATE task SET status = 'CLAIMED', execution_generation = :gen WHERE task_id = :taskId"
            ).bind("taskId", taskId).bind("gen", generation).execute()
        }
    }

    private fun insertReduceTask(jobId: String, handler: String): String {
        val taskId = java.util.UUID.randomUUID().toString()
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO task (task_id, handler, queue, payload, status, priority,
                    group_id, metadata, retry_count, max_retries, created_at)
                VALUES (:taskId, :handler, 'mr', '{}', 'PENDING', 0,
                    :groupId, '{"phase":"REDUCE"}', 0, 3, CURRENT_TIMESTAMP)
                """
            ).bind("taskId", taskId)
                .bind("handler", handler)
                .bind("groupId", jobId)
                .execute()
        }
        return taskId
    }

    private fun insertMrOutput(jobId: String, taskId: String, blobUri: String, partitionHash: Int) {
        val outputId = java.util.UUID.randomUUID().toString()
        jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                INSERT INTO mr_output (output_id, job_id, task_id, blob_uri, partition_hash, created_at)
                VALUES (:outputId, :jobId, :taskId, :blobUri, :partitionHash, CURRENT_TIMESTAMP)
                """
            ).bind("outputId", outputId)
                .bind("jobId", jobId)
                .bind("taskId", taskId)
                .bind("blobUri", blobUri)
                .bind("partitionHash", partitionHash)
                .execute()
        }
    }
}
