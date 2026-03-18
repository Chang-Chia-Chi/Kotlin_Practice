package com.mapreduce.mr.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.repository.TaskOutput
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf

@kotlinx.coroutines.ExperimentalCoroutinesApi
class ReduceTaskHandlerTest {

    private lateinit var definition: MapReduceDefinition<Any, Any, Any, Any>
    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var blobStore: BlobStore
    private lateinit var handler: ReduceTaskHandler

    @BeforeEach
    fun setUp() {
        definition = mock()
        taskGroupRepository = mock()
        blobStore = mock()

        whenever(definition.jobType).thenReturn("word-count")
        handler = ReduceTaskHandler(definition, taskGroupRepository, blobStore, ObjectMapper())
    }

    @Test
    fun `handlerName follows jobType dot reduce convention`() {
        assertEquals("word-count.reduce", handler.handlerName)
    }

    @Test
    fun `returns Failure when groupId is null`() = runTest {
        val ctx = TaskContext(
            taskId = "t-1", handler = "word-count.reduce", queue = "default",
            payload = "{}", groupId = null,
            metadata = null, claimToken = "gen-1",
        )

        val result = handler.handle(ctx)

        assertInstanceOf(TaskResult.Failure::class.java, result)
        verify(definition, never()).onCompleted(any())
    }

    @Test
    fun `happy path -- streams task outputs, reads blobs, reduces, calls onCompleted`() = runTest {
        val ctx = TaskContext(
            taskId = "t-r", handler = "word-count.reduce", queue = "default",
            payload = "{}", groupId = "job-1",
            metadata = null, claimToken = "gen-r",
        )

        whenever(taskGroupRepository.streamTaskOutputs("job-1", "word-count.map"))
            .thenReturn(flowOf(TaskOutput("blob://a", null), TaskOutput("blob://b", null)))
        whenever(blobStore.read("blob://a")).thenReturn(flowOf("ser-1"))
        whenever(blobStore.read("blob://b")).thenReturn(flowOf("ser-2"))
        whenever(definition.deserializeOutput("ser-1")).thenReturn("out-1")
        whenever(definition.deserializeOutput("ser-2")).thenReturn("out-2")
        whenever(definition.reduce(any())).thenAnswer {
            @Suppress("UNCHECKED_CAST")
            val flow = it.getArgument<Flow<Any>>(0)
            kotlinx.coroutines.runBlocking { flow.toList() }
            "final-result"
        }
        whenever(definition.serializeResult("final-result")).thenReturn("result-json")

        val result = handler.handle(ctx)

        val success = assertInstanceOf(TaskResult.Success::class.java, result)
        assertEquals("result-json", success.outputMetadata)
        verify(taskGroupRepository).streamTaskOutputs("job-1", "word-count.map")
        verify(blobStore).read("blob://a")
        verify(blobStore).read("blob://b")
        verify(definition).reduce(any())
        verify(definition).serializeResult("final-result")
        verify(definition).onCompleted("final-result")
    }

    @Test
    fun `returns result metadata in Success outputMetadata`() = runTest {
        val ctx = TaskContext(
            taskId = "t-ord", handler = "word-count.reduce", queue = "default",
            payload = "{}", groupId = "job-5",
            metadata = null, claimToken = "gen-o",
        )

        whenever(taskGroupRepository.streamTaskOutputs(any(), any()))
            .thenReturn(flowOf(TaskOutput("blob://f", null)))
        whenever(blobStore.read(any())).thenReturn(flowOf("ser-f"))
        whenever(definition.deserializeOutput(any())).thenReturn("out-f")
        whenever(definition.reduce(any())).thenReturn("result-f")
        whenever(definition.serializeResult(any())).thenReturn("rf-json")

        val result = handler.handle(ctx)

        val success = assertInstanceOf(TaskResult.Success::class.java, result)
        assertEquals("rf-json", success.outputMetadata)
        verify(definition).onCompleted("result-f")
    }
}
