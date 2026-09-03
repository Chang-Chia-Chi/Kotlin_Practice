package infra.shuttle.s3

import infra.shuttle.core.Digest
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.Fetcher
import infra.shuttle.core.StagedObject
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.nio.file.Files
import java.nio.file.Path
import java.security.DigestInputStream
import java.security.MessageDigest

/**
 * Spec 4.1 stage 1 for an S3 object store: one GET streamed to the staging path, digested as the
 * bytes pass (D22 of the connector: the transport computes, the application compares). The
 * whole transfer happens inside the dispatcher switch, so a cancellation at the switch back
 * finds the stream already closed and nothing half-written left behind.
 */
class S3Fetcher(private val client: S3Client, private val bucket: String, private val io: CoroutineDispatcher) {

    val fetcher: Fetcher = ::fetch

    suspend fun fetch(path: String, into: Path, algorithm: DigestAlgorithm): StagedObject = withContext(io) {
        val digest = MessageDigest.getInstance(algorithm.jca)
        val response = try {
            client.getObject(GetObjectRequest.builder().bucket(bucket).key(path).build()).use { body ->
                Files.newOutputStream(into).use { out -> DigestInputStream(body, digest).transferTo(out) }
                body.response()
            }
        } catch (e: Throwable) {
            Files.deleteIfExists(into)
            throw e
        }
        StagedObject(
            name = path.substringAfterLast('/'),
            path = into,
            size = response.contentLength(),
            mtime = response.lastModified(),
            digest = Digest(algorithm, digest.digest().joinToString("") { "%02x".format(it) }),
            contentType = response.contentType(),
        )
    }
}

internal val DigestAlgorithm.jca: String
    get() = when (this) {
        DigestAlgorithm.MD5 -> "MD5"
        DigestAlgorithm.SHA256 -> "SHA-256"
        DigestAlgorithm.SHA1 -> "SHA-1"
    }
