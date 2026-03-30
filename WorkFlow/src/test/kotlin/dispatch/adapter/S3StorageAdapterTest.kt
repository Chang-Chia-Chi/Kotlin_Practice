package com.workflow.dispatch.adapter

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals

class S3StorageAdapterTest {

    private val mockClient = mock<S3AsyncClient>()
    private val adapter = S3StorageAdapter(mockClient, "test-bucket")

    @Test
    fun `uploadCsv calls putObject with csv content type`() = runTest {
        whenever(mockClient.putObject(any<PutObjectRequest>(), any<AsyncRequestBody>()))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()))

        adapter.uploadCsv("path/to/file.csv", "data".toByteArray())

        val requestCaptor = argumentCaptor<PutObjectRequest>()
        verify(mockClient).putObject(requestCaptor.capture(), any<AsyncRequestBody>())

        assertEquals("test-bucket", requestCaptor.firstValue.bucket())
        assertEquals("path/to/file.csv", requestCaptor.firstValue.key())
        assertEquals("text/csv", requestCaptor.firstValue.contentType())
    }

    @Test
    fun `uploadParquet calls putObject with octet-stream content type`() = runTest {
        whenever(mockClient.putObject(any<PutObjectRequest>(), any<AsyncRequestBody>()))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()))

        adapter.uploadParquet("path/to/file.parquet", byteArrayOf(1, 2, 3))

        val requestCaptor = argumentCaptor<PutObjectRequest>()
        verify(mockClient).putObject(requestCaptor.capture(), any<AsyncRequestBody>())

        assertEquals("application/octet-stream", requestCaptor.firstValue.contentType())
    }
}
