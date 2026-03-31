package com.workflow.workflow.usecase.service.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.usecase.service.orchestration.ActivityInputResolver
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.assertThrows

class ActivityInputResolverTest {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    private val resolver = ActivityInputResolver(objectMapper)

    // ── Helpers ──

    private fun task(
        sequenceNumber: Int,
        status: TaskStatus = TaskStatus.COMPLETED,
        resultJson: String? = null,
    ) = Task(
        id = "t-${sequenceNumber}-${System.nanoTime()}", workflowId = "wf1",
        sequenceNumber = sequenceNumber, status = status,
        handlerKey = "h", resultJson = resultJson,
        claimedBy = null, claimedAt = null, completedAt = null,
        retryCount = 0, maxRetries = 0, deadlineAt = null,
    )

    private fun linearSequenceMap(): Map<Int, SequenceInfo> {
        val act1 = ActivityDefinition(name = "step1", transition = "step1.handler")
        val act2 = ActivityDefinition(name = "step2", transition = "step2.handler")
        return mapOf(
            1 to SequenceInfo(1, 0, act1, PhaseType.LINEAR, 2),
            2 to SequenceInfo(2, 1, act2, PhaseType.LINEAR, null),
        )
    }

    private fun fanOutSequenceMap(): Map<Int, SequenceInfo> {
        val scatterAct = ActivityDefinition(
            name = "scatter", transition = "scatter.handler", fanOut = "split",
        )
        val splitAct = ActivityDefinition(name = "split", transition = "parallel.handler")
        val notifyAct = ActivityDefinition(name = "notify", transition = "notify.handler")
        return mapOf(
            1 to SequenceInfo(1, 0, scatterAct, PhaseType.LINEAR, 2),
            2 to SequenceInfo(2, 1, splitAct, PhaseType.PARALLEL, 3),
            3 to SequenceInfo(3, 2, notifyAct, PhaseType.LINEAR, null),
        )
    }

    // ── Tests ──

    @Test
    fun `empty inputs returns null`() = runTest {
        val result = resolver.resolve(emptyMap(), linearSequenceMap()) { emptyList() }
        assertNull(result)
    }

    @Test
    fun `whole-result reference from linear activity`() = runTest {
        val inputs = mapOf("data" to "step1")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 1) listOf(task(1, resultJson = """{"uri":"s3://data","count":42}"""))
            else emptyList()
        }
        val result = resolver.resolve(inputs, linearSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("""{"uri":"s3://data","count":42}""", parsed.get("data").toString())
    }

    @Test
    fun `field-level reference from linear activity`() = runTest {
        val inputs = mapOf("uri" to "step1.uri", "count" to "step1.count")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 1) listOf(task(1, resultJson = """{"uri":"s3://data","count":42}"""))
            else emptyList()
        }
        val result = resolver.resolve(inputs, linearSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("s3://data", parsed.get("uri").asText())
        assertEquals(42, parsed.get("count").asInt())
    }

    @Test
    fun `whole-result reference from fan-out activity aggregates parallel results`() = runTest {
        val inputs = mapOf("results" to "split")
        val tasksBySeq: suspend (Int) -> List<Task> = { seq ->
            if (seq == 2) listOf(
                task(2, resultJson = """{"r":"one"}"""),
                task(2, resultJson = """{"r":"two"}"""),
            ) else emptyList()
        }
        val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        val arr = parsed.get("results")
        assertEquals(2, arr.size())
        assertEquals("one", arr[0].get("r").asText())
        assertEquals("two", arr[1].get("r").asText())
    }

    @Test
    fun `field-level reference from fan-out activity extracts per-element`() = runTest {
        val inputs = mapOf("uris" to "split.uri")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 2) listOf(
                task(2, resultJson = """{"uri":"s3://a","count":1}"""),
                task(2, resultJson = """{"uri":"s3://b","count":2}"""),
            ) else emptyList()
        }
        val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        val arr = parsed.get("uris")
        assertEquals(2, arr.size())
        assertEquals("s3://a", arr[0].asText())
        assertEquals("s3://b", arr[1].asText())
    }

    @Test
    fun `fan-out aggregation skips non-completed tasks`() = runTest {
        val inputs = mapOf("results" to "split")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 2) listOf(
                task(2, resultJson = """{"r":"ok"}"""),
                task(2, status = TaskStatus.FAILED, resultJson = null),
            ) else emptyList()
        }
        val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals(1, parsed.get("results").size())
    }

    @Test
    fun `reference to scatter activity returns single result, not aggregation`() = runTest {
        val inputs = mapOf("token" to "scatter.batchId")
        val tasksBySeq: suspend (Int) -> List<Task> = { seq ->
            if (seq == 1) listOf(task(1, resultJson = """{"batchId":"batch-123"}"""))
            else emptyList()
        }
        val result = resolver.resolve(inputs, fanOutSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("batch-123", parsed.get("token").asText())
    }

    @Test
    fun `inputs from multiple activities`() = runTest {
        val act1 = ActivityDefinition(name = "init", transition = "init.handler")
        val act2 = ActivityDefinition(name = "enrich", transition = "enrich.handler")
        val act3 = ActivityDefinition(name = "final", transition = "final.handler")
        val seqMap = mapOf(
            1 to SequenceInfo(1, 0, act1, PhaseType.LINEAR, 2),
            2 to SequenceInfo(2, 1, act2, PhaseType.LINEAR, 3),
            3 to SequenceInfo(3, 2, act3, PhaseType.LINEAR, null),
        )
        val inputs = mapOf("cfg" to "init.config", "meta" to "enrich.summary")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            when (seq) {
                1 -> listOf(task(1, resultJson = """{"config":"prod"}"""))
                2 -> listOf(task(2, resultJson = """{"summary":"done"}"""))
                else -> emptyList()
            }
        }
        val result = resolver.resolve(inputs, seqMap, tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("prod", parsed.get("cfg").asText())
        assertEquals("done", parsed.get("meta").asText())
    }

    @Test
    fun `completed task with null resultJson returns json null`() = runTest {
        val inputs = mapOf("data" to "step1")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 1) listOf(task(1, resultJson = null))
            else emptyList()
        }
        val result = resolver.resolve(inputs, linearSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertTrue(parsed.get("data").isNull)
    }

    @Test
    fun `unknown activity name throws with descriptive message`() = runTest {
        val inputs = mapOf("data" to "nonexistent")
        val ex = assertThrows<IllegalArgumentException> {
            resolver.resolve(inputs, linearSequenceMap()) { emptyList() }
        }
        assertTrue(ex.message!!.contains("nonexistent"))
        assertTrue(ex.message!!.contains("step1"))
        assertTrue(ex.message!!.contains("step2"))
    }

    @Test
    fun `nested field path traverses multiple levels`() = runTest {
        val inputs = mapOf("city" to "step1.address.city")
        val tasksBySeq: (Int) -> List<Task> = { seq ->
            if (seq == 1) listOf(task(1, resultJson = """{"address":{"city":"NYC","zip":"10001"}}"""))
            else emptyList()
        }
        val result = resolver.resolve(inputs, linearSequenceMap(), tasksBySeq)
        val parsed = objectMapper.readTree(result)
        assertEquals("NYC", parsed.get("city").asText())
    }
}
