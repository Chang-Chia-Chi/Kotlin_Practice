package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.future.await
import org.eclipse.microprofile.config.inject.ConfigProperty
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest

@ApplicationScoped
class S3StorageAdapter(
    private val client: S3AsyncClient,
    @ConfigProperty(name = "storage.bucket") private val bucket: String,
) : StorageGateway {

    override suspend fun uploadCsv(path: String, content: ByteArray) {
        upload(path, content, "text/csv")
    }

    override suspend fun uploadParquet(path: String, content: ByteArray) {
        upload(path, content, "application/octet-stream")
    }

    private suspend fun upload(key: String, content: ByteArray, contentType: String) {
        client.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(contentType)
                .build(),
            AsyncRequestBody.fromBytes(content),
        ).await()
    }
}
