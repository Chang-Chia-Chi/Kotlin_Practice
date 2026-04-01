package com.workflow.infrastructure.storage

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.smithy.kotlin.runtime.net.url.Url
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Disposes
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class S3ClientProducer {

    @Produces
    @ApplicationScoped
    fun s3Client(
        @ConfigProperty(name = "storage.endpoint") endpoint: String,
        @ConfigProperty(name = "storage.region") region: String,
        @ConfigProperty(name = "storage.access-key") accessKey: String,
        @ConfigProperty(name = "storage.secret-key") secretKey: String,
    ): S3Client = S3Client {
        endpointUrl = Url.parse(endpoint)
        this.region = region
        credentialsProvider = StaticCredentialsProvider {
            this.accessKeyId = accessKey
            this.secretAccessKey = secretKey
        }
        forcePathStyle = true
    }

    fun closeS3Client(@Disposes client: S3Client) = client.close()
}
