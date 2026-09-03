package infra.shuttle.s3

import org.testcontainers.containers.MinIOContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest
import software.amazon.awssdk.services.s3.model.VersioningConfiguration
import kotlin.time.Duration.Companion.seconds

/** One MinIO for every `minio`-tagged class in the JVM; Ryuk removes it when the JVM ends. */
object Minio {
    private val container by lazy {
        MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-10-02T17-50-41Z")).also { it.start() }
    }
    val client: S3Client by lazy {
        S3Target.client(container.s3URL, "us-east-1", pathStyle = true, container.userName, container.password,
            connect = 5.seconds, socket = 30.seconds, apiCall = 45.seconds)
    }
    private var buckets = 0

    fun versionedBucket(): String {
        val name = "landing-${++buckets}"
        client.createBucket(CreateBucketRequest.builder().bucket(name).build())
        client.putBucketVersioning(PutBucketVersioningRequest.builder().bucket(name)
            .versioningConfiguration(VersioningConfiguration.builder().status(BucketVersioningStatus.ENABLED).build()).build())
        return name
    }
}
