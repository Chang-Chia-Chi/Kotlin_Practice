package infra.shuttle.s3

import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.TargetMetadata
import infra.shuttle.core.TargetRef
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Spec 7.2. `store` is one PUT, then a HEAD that checks the length and, on a single-part unencrypted
 * object written with MD5, the ETag. Nothing here deletes: the bucket's lifecycle rule owns older
 * versions (D5). Every SDK call is blocking and runs on [io], the module's bounded view of IO.
 *
 * Single `PutObject`, never multipart: the multipart threshold is therefore the S3 single-PUT limit
 * of 5 GiB, above the largest file this deployment moves (10 MB, S13), so the ETag is the MD5 (D16).
 *
 * [betweenPutAndHead] is the adapter's own crash point for I6; production leaves it a no-op.
 */
class S3Target(
    private val client: S3Client,
    private val bucket: String,
    private val io: CoroutineDispatcher,
    private val betweenPutAndHead: suspend () -> Unit = {},
) : ObjectStoreTarget {
    private val log = Logger.getLogger(S3Target::class.java)

    /** What the last `store` or `probe` warned about, for tests; production reads the log. */
    val warnings: MutableList<String> = CopyOnWriteArrayList()

    override suspend fun store(key: String, file: Path, metadata: Map<String, String>): TargetRef {
        val size = Files.size(file)
        val md5Hex = metadata[TargetMetadata.DIGEST]
            ?.takeIf { metadata[TargetMetadata.DIGEST_ALGORITHM].equals(DigestAlgorithm.MD5.name, ignoreCase = true) }
        val put = withContext(io) {
            client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).contentLength(size).metadata(metadata)
                    .apply { if (md5Hex != null) contentMD5(Base64.getEncoder().encodeToString(hexToBytes(md5Hex))) }
                    .build(),
                RequestBody.fromFile(file),
            )
        }
        betweenPutAndHead()
        val head = withContext(io) {
            client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).versionId(put.versionId()).build())
        }
        check(head.contentLength() == size) { "stored $bucket/$key has ${head.contentLength()} bytes, expected $size" }
        if (md5Hex != null) {
            if (head.serverSideEncryptionAsString() != null) {
                warn("$bucket/$key is server-side encrypted; ETag check skipped, size and metadata verified")
            } else {
                check(head.eTag().trim('"') == md5Hex) { "ETag ${head.eTag()} of $bucket/$key is not its MD5 $md5Hex" }
            }
        }
        return TargetRef("s3", bucket, key, put.versionId(), size)
    }

    override suspend fun verify(ref: TargetRef): Boolean = withContext(io) {
        try {
            client.headObject(HeadObjectRequest.builder().bucket(bucket).key(ref.key).versionId(ref.ref).build())
            true
        } catch (e: S3Exception) {
            if (e.statusCode() == 404 || e.statusCode() == 400) false else throw e
        }
    }

    override suspend fun probe() = withContext(io) {
        headBucket(client, bucket, io)
        val expiresNonCurrent = try {
            client.getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest.builder().bucket(bucket).build())
                .rules().any { it.statusAsString() == "Enabled" && it.noncurrentVersionExpiration() != null }
        } catch (e: S3Exception) {
            if (e.statusCode() == 404) false else throw e
        }
        if (!expiresNonCurrent) warn("bucket $bucket has no lifecycle rule expiring non-current versions; every overwrite keeps its old version for ever (D5)")
    }

    private fun warn(message: String) {
        warnings += message
        log.warn(message)
    }

    companion object {
        /**
         * Spec 12.1 step 3's HEAD on one bucket: it is there or the deployment ends naming it. The host runs
         * it on its own for a subscribed route's `fetch.bucket`, which has no target adapter to probe (D15).
         */
        suspend fun headBucket(client: S3Client, bucket: String, io: CoroutineDispatcher): Unit = withContext(io) {
            try {
                client.headBucket(HeadBucketRequest.builder().bucket(bucket).build())
            } catch (e: NoSuchBucketException) {
                throw IllegalStateException("bucket $bucket does not exist; the bucket is never created here (D15)", e)
            }
        }

        /**
         * Spec 7.2 and D4: synchronous, Apache client, endpoint override, path style. SDK 2.29 computes
         * checksums only when an operation requires one, which is D4's "when-required"; the explicit
         * switch exists from 2.30 and must be set to WHEN_REQUIRED if the SDK is ever raised.
         */
        fun client(
            endpoint: String, region: String, pathStyle: Boolean, accessKey: String, secretKey: String,
            connect: Duration, socket: Duration, apiCall: Duration,
        ): S3Client = S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.of(region))
            .forcePathStyle(pathStyle)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .httpClientBuilder(ApacheHttpClient.builder().connectionTimeout(connect.toJavaDuration()).socketTimeout(socket.toJavaDuration()))
            .overrideConfiguration(ClientOverrideConfiguration.builder().apiCallTimeout(apiCall.toJavaDuration()).build())
            .build()

        internal fun hexToBytes(hex: String): ByteArray = ByteArray(hex.length / 2) { hex.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
    }
}
