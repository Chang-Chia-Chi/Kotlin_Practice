package com.mapreduce.mr.handler

import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf

class MapTaskHandlerTest {

    private lateinit var definition: MapReduceDefinition<Any, Any, Any, Any>
    private lateinit var blobStore: BlobStore
    private lateinit var handler: MapTaskHandler

    @BeforeEach
    fun setUp() {
        definition = mock()
        blobStore = mock()

        whenever(definition.jobType).thenReturn("word-count")
        handler = MapTaskHandler(definition, blobStore)
    }

    @Test
    fun `handlerName follows jobType dot map convention`() {
        assertEquals("word-count.map", handler.handlerName)
    }

    @Test
    fun `returns Failure when groupId is null`() = runTest {
        val ctx = TaskContext(
            taskId = "task-1", payload = "{}", groupId = null,
            metadata = null, executionGeneration = "gen-1",
        )

        val result = handler.handle(ctx)

        assertInstanceOf(TaskResult.Failure::class.java, result)
        verify(blobStore, never()).write(any(), any(), any(), any())
    }

    @Test
    fun `happy path -- deserializes, maps, serializes, writes blob, returns output in TaskResult`() = runTest {
        val ctx = TaskContext(
            taskId = "task-1", payload = "input-json", groupId = "job-1",
            metadata = null, executionGeneration = "gen-1",
        )

        whenever(definition.deserializeInput("input-json")).thenReturn("raw-input")
        whenever(definition.map("raw-input")).thenReturn(flowOf("output-1", "output-2"))
        whenever(definition.serializeOutput("output-1")).thenReturn("ser-1")
        whenever(definition.serializeOutput("output-2")).thenReturn("ser-2")
        whenever(blobStore.write(eq("job-1"), eq("task-1"), eq(0), any())).thenReturn("blob://job-1/task-1")

        val result = handler.handle(ctx)

        val success = assertInstanceOf(TaskResult.Success::class.java, result)
        assertEquals("blob://job-1/task-1", success.outputUri)
        verify(definition).deserializeInput("input-json")
        verify(definition).map("raw-input")
        verify(blobStore).write(eq("job-1"), eq("task-1"), eq(0), any())
    }

    @Test
    fun `writes to blob store with correct parameters`() = runTest {
        val ctx = TaskContext(
            taskId = "t-99", payload = "p", groupId = "j-42",
            metadata = null, executionGeneration = "gen-x",
        )

        whenever(definition.deserializeInput(any())).thenReturn("in")
        whenever(definition.map(any())).thenReturn(flowOf("out"))
        whenever(definition.serializeOutput(any())).thenReturn("s-out")
        whenever(blobStore.write(any(), any(), any(), any())).thenReturn("blob://uri")

        handler.handle(ctx)

        verify(blobStore).write(eq("j-42"), eq("t-99"), eq(0), any())
    }

    @Test
    fun `returns outputUri and outputMetadata in Success result`() = runTest {
        val ctx = TaskContext(
            taskId = "t-1", payload = "p", groupId = "j-1",
            metadata = null, executionGeneration = "gen-abc",
        )

        whenever(definition.deserializeInput(any())).thenReturn("in")
        whenever(definition.map(any())).thenReturn(flowOf("out"))
        whenever(definition.serializeOutput(any())).thenReturn("s-out")
        whenever(blobStore.write(any(), any(), any(), any())).thenReturn("blob://u")

        val result = handler.handle(ctx)

        val success = assertInstanceOf(TaskResult.Success::class.java, result)
        assertEquals("blob://u", success.outputUri)
        assertEquals("""{"partition_hash":0}""", success.outputMetadata)
    }
}
