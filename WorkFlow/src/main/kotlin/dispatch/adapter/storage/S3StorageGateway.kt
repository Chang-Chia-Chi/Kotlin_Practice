package com.workflow.dispatch.adapter.storage

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.asByteStream
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.io.File

@ApplicationScoped
class S3StorageGateway(
    private val s3Client: S3Client,
    @ConfigProperty(name = "storage.bucket") private val bucket: String,
) : StorageGateway {

    private val log = LoggerFactory.getLogger(S3StorageGateway::class.java)

    override suspend fun uploadCsv(path: String, file: File) {
        s3Client.putObject(
            PutObjectRequest {
                this.bucket = this@S3StorageGateway.bucket
                key = path
                contentType = "application/gzip"
                body = file.asByteStream()
            },
        )
        log.debug("Uploaded CSV to s3://{}/{} ({} bytes)", bucket, path, file.length())
    }

    override suspend fun uploadParquet(path: String, content: ByteArray) {
        s3Client.putObject(
            PutObjectRequest {
                this.bucket = this@S3StorageGateway.bucket
                key = path
                contentType = "application/octet-stream"
                body = ByteStream.fromBytes(content)
            },
        )
        log.debug("Uploaded Parquet to s3://{}/{} ({} bytes)", bucket, path, content.size)
    }
}
