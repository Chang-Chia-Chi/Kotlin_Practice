package com.mapreduce.mr.api

import com.mapreduce.mr.api.dto.SubmitJobRequest
import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.registry.MapReduceRegistry
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.repository.TaskGroupRepository
import jakarta.ws.rs.core.Response
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Instant

class JobResourceTest {

    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var registry: MapReduceRegistry
    private lateinit var resource: JobResource

    @BeforeEach
    fun setUp() {
        taskGroupRepository = mock()
        registry = mock()
        resource = JobResource(taskGroupRepository, registry)
    }

    private fun mockDefinition(
        taskInputs: List<Any> = listOf("input1", "input2"),
    ): MapReduceDefinition<Any, Any, Any, Any> {
        val def = mock<MapReduceDefinition<Any, Any, Any, Any>>()
        whenever(def.deserializeParams(any())).thenReturn("params")
        whenever(def.split(any())).thenReturn(taskInputs)
        whenever(def.serializeInput(any())).thenReturn("{}")
        whenever(def.queue).thenReturn("mr")
        whenever(def.failurePolicy).thenReturn(FailurePolicy.FAIL_GROUP)
        whenever(def.failureThreshold).thenReturn(0.0)
        whenever(def.maxRetries).thenReturn(3)
        return def
    }

    // ── submitJob ──────────────────────────────────────────────────

    @Nested
    inner class SubmitJob {

        @Test
        fun `returns 400 for unknown job type`() = runTest {
            whenever(registry.getDefinition("unknown")).thenReturn(null)

            val response = resource.submitJob(SubmitJobRequest("unknown", "{}"))

            assertEquals(Response.Status.BAD_REQUEST.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertTrue(entity["error"].toString().contains("Unknown job type"))
        }

        @Test
        fun `returns 400 when split produces zero tasks`() = runTest {
            val def = mockDefinition(taskInputs = emptyList())
            whenever(registry.getDefinition("wc")).thenReturn(def)

            val response = resource.submitJob(SubmitJobRequest("wc", """{"text":"hi"}"""))

            assertEquals(Response.Status.BAD_REQUEST.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertTrue(entity["error"].toString().contains("zero tasks"))
        }

        @Test
        fun `returns 201 with jobId and task count`() = runTest {
            val def = mockDefinition(taskInputs = listOf("a", "b", "c"))
            whenever(registry.getDefinition("wc")).thenReturn(def)

            val response = resource.submitJob(SubmitJobRequest("wc", """{"text":"hello"}"""))

            assertEquals(Response.Status.CREATED.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertNotNull(entity["jobId"])
            assertEquals(3, entity["totalTasks"])
        }

        @Test
        fun `returns 201 with single task`() = runTest {
            val def = mockDefinition(taskInputs = listOf("only-one"))
            whenever(registry.getDefinition("single")).thenReturn(def)

            val response = resource.submitJob(SubmitJobRequest("single", "{}"))

            assertEquals(Response.Status.CREATED.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertEquals(1, entity["totalTasks"])
        }
    }

    // ── getJob ─────────────────────────────────────────────────────

    @Nested
    inner class GetJob {

        @Test
        fun `returns 404 when group not found`() = runTest {
            whenever(taskGroupRepository.findGroup("non-existent")).thenReturn(null)

            val response = resource.getJob("non-existent")

            assertEquals(Response.Status.NOT_FOUND.statusCode, response.status)
        }

        @Test
        fun `returns 200 with JobResponse when group found`() = runTest {
            val group = TaskGroup(
                groupId = "job-1",
                groupType = "wordcount",
                status = GroupStatus.ACTIVE,
                phase = "map",
                phaseTotal = 5,
                tasksPending = 3,
                tasksFailed = 1,
                failurePolicy = "FAIL_GROUP",
                resultMetadata = null,
                createdAt = Instant.now(),
                updatedAt = Instant.now(),
            )
            whenever(taskGroupRepository.findGroup("job-1")).thenReturn(group)

            val response = resource.getJob("job-1")

            assertEquals(Response.Status.OK.statusCode, response.status)
        }
    }

    // ── listJobs ───────────────────────────────────────────────────

    @Nested
    inner class ListJobs {

        @Test
        fun `returns 200 with all groups when no status filter`() = runTest {
            val groups = listOf(
                TaskGroup("g-1", "wc", GroupStatus.ACTIVE, phase = "map"),
                TaskGroup("g-2", "wc", GroupStatus.COMPLETED, phase = "reduce"),
            )
            whenever(taskGroupRepository.findAllGroups(any())).thenReturn(groups)

            val response = resource.listJobs(null)

            assertEquals(Response.Status.OK.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val list = response.entity as List<*>
            assertEquals(2, list.size)
        }

        @Test
        fun `returns 200 with filtered groups when valid status provided`() = runTest {
            val groups = listOf(
                TaskGroup("g-1", "wc", GroupStatus.ACTIVE, phase = "map"),
            )
            whenever(taskGroupRepository.findGroupsByStatus(GroupStatus.ACTIVE)).thenReturn(groups)

            val response = resource.listJobs("ACTIVE")

            assertEquals(Response.Status.OK.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val list = response.entity as List<*>
            assertEquals(1, list.size)
        }

        @Test
        fun `returns 200 with case-insensitive status`() = runTest {
            whenever(taskGroupRepository.findGroupsByStatus(GroupStatus.COMPLETED)).thenReturn(emptyList())

            val response = resource.listJobs("completed")

            assertEquals(Response.Status.OK.statusCode, response.status)
        }

        @Test
        fun `returns 400 for invalid status value`() = runTest {
            val response = resource.listJobs("INVALID_STATUS")

            assertEquals(Response.Status.BAD_REQUEST.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val entity = response.entity as Map<String, Any>
            assertTrue(entity["error"].toString().contains("Invalid status"))
        }

        @Test
        fun `returns empty list when no groups exist`() = runTest {
            whenever(taskGroupRepository.findAllGroups(any())).thenReturn(emptyList())

            val response = resource.listJobs(null)

            assertEquals(Response.Status.OK.statusCode, response.status)
            @Suppress("UNCHECKED_CAST")
            val list = response.entity as List<*>
            assertTrue(list.isEmpty())
        }
    }
}
