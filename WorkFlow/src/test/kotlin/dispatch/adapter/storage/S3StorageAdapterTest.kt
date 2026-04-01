package com.workflow.dispatch.adapter.storage

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectResponse
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals

class S3StorageAdapterTest {

    private val mockClient = mock<S3Client>()
    private val adapter = S3StorageAdapter(mockClient, "test-bucket")

    @Test
    fun `uploadCsv calls putObject with gzip content type`(@TempDir tempDir: Path) = runTest {
        whenever(mockClient.putObject(any<PutObjectRequest>())).thenReturn(PutObjectResponse {})

        val file = tempDir.resolve("data.csv.gz").toFile().also { it.writeBytes(byteArrayOf()) }
        adapter.uploadCsv("path/to/file.csv.gz", file)

        val captor = argumentCaptor<PutObjectRequest>()
        verify(mockClient).putObject(captor.capture())

        assertEquals("test-bucket", captor.firstValue.bucket)
        assertEquals("path/to/file.csv.gz", captor.firstValue.key)
        assertEquals("application/gzip", captor.firstValue.contentType)
    }

    @Test
    fun `uploadParquet calls putObject with octet-stream content type`() = runTest {
        whenever(mockClient.putObject(any<PutObjectRequest>())).thenReturn(PutObjectResponse {})

        adapter.uploadParquet("path/to/file.parquet", byteArrayOf(1, 2, 3))

        val captor = argumentCaptor<PutObjectRequest>()
        verify(mockClient).putObject(captor.capture())

        assertEquals("application/octet-stream", captor.firstValue.contentType)
    }
}
