package com.workflow.dispatch.adapter.storage

import aws.sdk.kotlin.services.s3.S3Client
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.fromFile
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.io.File

@ApplicationScoped
class S3StorageAdapter(
    private val client: S3Client,
    @ConfigProperty(name = "storage.bucket") private val bucket: String,
) : StorageGateway {

    override suspend fun uploadCsv(path: String, file: File) {
        client.putObject {
            bucket = this@S3StorageAdapter.bucket
            key = path
            contentType = "application/gzip"
            body = ByteStream.fromFile(file)
        }
    }

    override suspend fun uploadParquet(path: String, content: ByteArray) {
        client.putObject {
            bucket = this@S3StorageAdapter.bucket
            key = path
            contentType = "application/octet-stream"
            body = ByteStream.fromBytes(content)
        }
    }
}
