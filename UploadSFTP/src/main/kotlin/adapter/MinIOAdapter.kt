package adapter

import aws.sdk.kotlin.services.s3.S3Client

class MinIOAdapter(
    private val client: S3Client,
) {
    suspend fun download(
        bucket: String,
        objectKey: String,
        filename: String,
    ) {
    }
}
