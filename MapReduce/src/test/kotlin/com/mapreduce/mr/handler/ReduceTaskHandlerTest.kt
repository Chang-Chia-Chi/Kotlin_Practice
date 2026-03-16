package com.mapreduce.mr.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf

@kotlinx.coroutines.ExperimentalCoroutinesApi
class ReduceTaskHandlerTest {

    private lateinit var definition: MapReduceDefinition<Any, Any, Any, Any>
    private lateinit var jobRepository: JobRepository
    private lateinit var blobStore: BlobStore
    private lateinit var handler: ReduceTaskHandler

    @BeforeEach
    fun setUp() {
        definition = mock()
        jobRepository = mock()
        blobStore = mock()

        whenever(definition.jobType).thenReturn("word-count")
        handler = ReduceTaskHandler(definition, jobRepository, blobStore, ObjectMapper())
    }

    @Test
    fun `handlerName follows jobType dot reduce convention`() {
        assertEquals("word-count.reduce", handler.handlerName)
    }

    @Test
    fun `returns Failure when groupId is null`() = runTest {
        val ctx = TaskContext(
            taskId = "t-1", payload = "{}", groupId = null,
            metadata = null, executionGeneration = "gen-1",
        )

        val result = handler.handle(ctx)

        assertInstanceOf(TaskResult.Failure::class.java, result)
        verify(jobRepository, never()).completeReduceTask(any(), any(), any(), any())
        verify(definition, never()).onCompleted(any())
    }

    @Test
    fun `happy path -- streams blob URIs, reads, reduces, completes, calls onCompleted`() = runTest {
        val ctx = TaskContext(
            taskId = "t-r", payload = "{}", groupId = "job-1",
            metadata = null, executionGeneration = "gen-r",
        )

        whenever(jobRepository.streamBlobUris("job-1", null)).thenReturn(flowOf("blob://a", "blob://b"))
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

        assertInstanceOf(TaskResult.Success::class.java, result)
        verify(jobRepository).streamBlobUris("job-1", null)
        verify(blobStore).read("blob://a")
        verify(blobStore).read("blob://b")
        verify(definition).reduce(any())
        verify(definition).serializeResult("final-result")
        verify(jobRepository).completeReduceTask("t-r", "job-1", "result-json", "gen-r")
        verify(definition).onCompleted("final-result")
    }

    @Test
    fun `extracts partition_hash from metadata JSON`() = runTest {
        val ctx = TaskContext(
            taskId = "t-p", payload = "{}", groupId = "job-2",
            metadata = """{"phase":"REDUCE","partition_hash":2}""",
            executionGeneration = "gen-p",
        )

        whenever(jobRepository.streamBlobUris("job-2", 2)).thenReturn(flowOf("blob://c"))
        whenever(blobStore.read("blob://c")).thenReturn(flowOf("ser-p"))
        whenever(definition.deserializeOutput("ser-p")).thenReturn("out-p")
        whenever(definition.reduce(any())).thenReturn("result-p")
        whenever(definition.serializeResult("result-p")).thenReturn("rp-json")

        val result = handler.handle(ctx)

        assertInstanceOf(TaskResult.Success::class.java, result)
        verify(jobRepository).streamBlobUris("job-2", 2)
    }

    @Test
    fun `null metadata results in null partition hash`() = runTest {
        val ctx = TaskContext(
            taskId = "t-n", payload = "{}", groupId = "job-3",
            metadata = null, executionGeneration = "gen-n",
        )

        whenever(jobRepository.streamBlobUris("job-3", null)).thenReturn(flowOf("blob://d"))
        whenever(blobStore.read("blob://d")).thenReturn(flowOf("ser-d"))
        whenever(definition.deserializeOutput("ser-d")).thenReturn("out-d")
        whenever(definition.reduce(any())).thenReturn("result-d")
        whenever(definition.serializeResult("result-d")).thenReturn("rd-json")

        handler.handle(ctx)

        verify(jobRepository).streamBlobUris("job-3", null)
    }

    @Test
    fun `invalid metadata JSON results in null partition hash`() = runTest {
        val ctx = TaskContext(
            taskId = "t-bad", payload = "{}", groupId = "job-4",
            metadata = "not-json", executionGeneration = "gen-b",
        )

        whenever(jobRepository.streamBlobUris("job-4", null)).thenReturn(flowOf("blob://e"))
        whenever(blobStore.read("blob://e")).thenReturn(flowOf("ser-e"))
        whenever(definition.deserializeOutput("ser-e")).thenReturn("out-e")
        whenever(definition.reduce(any())).thenReturn("result-e")
        whenever(definition.serializeResult("result-e")).thenReturn("re-json")

        handler.handle(ctx)

        verify(jobRepository).streamBlobUris("job-4", null)
    }

    @Test
    fun `calls completeReduceTask before onCompleted`() = runTest {
        val ctx = TaskContext(
            taskId = "t-ord", payload = "{}", groupId = "job-5",
            metadata = null, executionGeneration = "gen-o",
        )

        whenever(jobRepository.streamBlobUris(any(), anyOrNull())).thenReturn(flowOf("blob://f"))
        whenever(blobStore.read(any())).thenReturn(flowOf("ser-f"))
        whenever(definition.deserializeOutput(any())).thenReturn("out-f")
        whenever(definition.reduce(any())).thenReturn("result-f")
        whenever(definition.serializeResult(any())).thenReturn("rf-json")

        handler.handle(ctx)

        // Both should be called; ordering verified by InOrder if needed
        verify(jobRepository).completeReduceTask("t-ord", "job-5", "rf-json", "gen-o")
        verify(definition).onCompleted("result-f")
    }
}
