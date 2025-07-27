package service

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.asByteStream
import jakarta.enterprise.context.ApplicationScoped
import java.nio.file.Paths

@ApplicationScoped
class S3Service {
    suspend fun upload(
        filePath: String,
        bucketName: String,
        objectKey: String,
    ) {
        S3Client.fromEnvironment().use { s3 ->
            val path = Paths.get(filePath)
            val contentBody = path.asByteStream()
            s3.putObject(
                PutObjectRequest {
                    bucket = bucketName
                    key = objectKey
                    body = contentBody
                },
            )
        }
    }
}
