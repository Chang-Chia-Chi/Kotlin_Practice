package com.workflow.infrastructure.storage

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.BucketAlreadyExists
import aws.sdk.kotlin.services.s3.model.BucketAlreadyOwnedByYou
import aws.sdk.kotlin.services.s3.model.CreateBucketRequest
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy

object MinioTestContainer {

    private const val ACCESS_KEY = "minioadmin"
    private const val SECRET_KEY = "minioadmin"
    const val BUCKET = "dispatch-test"

    private val container = GenericContainer("minio/minio:RELEASE.2024-10-02T17-50-41Z")
        .withCommand("server /data")
        .withExposedPorts(9000)
        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .waitingFor(
            HttpWaitStrategy()
                .forPort(9000)
                .forPath("/minio/health/ready"),
        )
        .apply { start() }

    val endpoint: String get() = "http://${container.host}:${container.getMappedPort(9000)}"

    val s3Client: S3Client by lazy {
        val client = S3Client {
            region = "us-east-1"
            endpointUrl = Url.parse(endpoint)
            credentialsProvider = StaticCredentialsProvider {
                accessKeyId = ACCESS_KEY
                secretAccessKey = SECRET_KEY
            }
            forcePathStyle = true
        }
        runBlocking {
            try {
                client.createBucket(CreateBucketRequest { bucket = BUCKET })
            } catch (_: BucketAlreadyOwnedByYou) {
                // idempotent: already created in a prior JVM run
            } catch (_: BucketAlreadyExists) {
                // idempotent: bucket exists globally (MinIO edge case)
            }
        }
        client
    }
}
