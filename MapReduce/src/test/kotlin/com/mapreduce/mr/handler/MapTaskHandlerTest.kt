package com.mapreduce.mr.handler

import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.mr.spi.PartitionedMapReduceDefinition
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
    private lateinit var jobRepository: JobRepository
    private lateinit var blobStore: BlobStore
    private lateinit var handler: MapTaskHandler

    @BeforeEach
    fun setUp() {
        definition = mock()
        jobRepository = mock()
        blobStore = mock()

        whenever(definition.jobType).thenReturn("word-count")
        handler = MapTaskHandler(definition, jobRepository, blobStore)
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
        verify(jobRepository, never()).completeMapTask(any(), any(), any(), any(), any())
    }

    @Test
    fun `happy path -- deserializes, maps, serializes, writes blob, completes task`() = runTest {
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

        assertInstanceOf(TaskResult.Success::class.java, result)
        verify(definition).deserializeInput("input-json")
        verify(definition).map("raw-input")
        verify(blobStore).write(eq("job-1"), eq("task-1"), eq(0), any())
        verify(jobRepository).completeMapTask("task-1", "job-1", "blob://job-1/task-1", "gen-1", 0)
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
    fun `calls completeMapTask with execution generation for fencing`() = runTest {
        val ctx = TaskContext(
            taskId = "t-1", payload = "p", groupId = "j-1",
            metadata = null, executionGeneration = "gen-abc",
        )

        whenever(definition.deserializeInput(any())).thenReturn("in")
        whenever(definition.map(any())).thenReturn(flowOf("out"))
        whenever(definition.serializeOutput(any())).thenReturn("s-out")
        whenever(blobStore.write(any(), any(), any(), any())).thenReturn("blob://u")

        handler.handle(ctx)

        verify(jobRepository).completeMapTask("t-1", "j-1", "blob://u", "gen-abc", 0)
    }

    @Test
    fun `handles PartitionedMapReduceDefinition correctly`() = runTest {
        val partitionedDef: PartitionedMapReduceDefinition<Any, Any, Any, Any> = mock()
        whenever(partitionedDef.jobType).thenReturn("partitioned-job")
        whenever(partitionedDef.deserializeInput(any())).thenReturn("raw")
        whenever(partitionedDef.map(any())).thenReturn(flowOf("o"))
        whenever(partitionedDef.serializeOutput(any())).thenReturn("s")
        whenever(partitionedDef.partitionFor("raw")).thenReturn(3)
        whenever(blobStore.write(any(), any(), any(), any())).thenReturn("blob://p")

        @Suppress("UNCHECKED_CAST")
        val partitionedHandler = MapTaskHandler(
            partitionedDef as MapReduceDefinition<Any, Any, Any, Any>,
            jobRepository, blobStore,
        )

        val ctx = TaskContext(
            taskId = "t-p", payload = "input", groupId = "j-p",
            metadata = null, executionGeneration = "gen-p",
        )

        val result = partitionedHandler.handle(ctx)

        assertInstanceOf(TaskResult.Success::class.java, result)
        verify(partitionedDef).partitionFor("raw")
        verify(blobStore).write(eq("j-p"), eq("t-p"), eq(3), any())
        verify(jobRepository).completeMapTask("t-p", "j-p", "blob://p", "gen-p", 3)
    }
}
