package infra.shuttle.s3

import infra.shuttle.core.DigestAlgorithm
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

@Tag("minio")
class S3FetcherTest {
    companion object {
        private val bucket by lazy { Minio.versionedBucket() }
    }

    @TempDir lateinit var staging: Path
    private val client get() = Minio.client

    @Test
    fun the_fetcher_streams_the_object_to_staging_and_its_digest_matches_the_objects() = runTest {
        val content = "metadata: {\"images\": [\"a.png\"]}".toByteArray()
        client.putObject(PutObjectRequest.builder().bucket(bucket).key("meta/batch-7.json").contentType("application/json").build(),
            RequestBody.fromBytes(content))
        val fetch = S3Fetcher(client, bucket, Dispatchers.IO).fetcher

        val staged = fetch("meta/batch-7.json", staging.resolve("batch-7.json"), DigestAlgorithm.SHA256)

        assertEquals("batch-7.json", staged.name)
        assertEquals(content.size.toLong(), staged.size)
        assertEquals(DigestAlgorithm.SHA256, staged.digest.algorithm)
        assertEquals(MessageDigest.getInstance("SHA-256").digest(content).joinToString("") { "%02x".format(it) }, staged.digest.hex)
        assertEquals("application/json", staged.contentType)
        assertTrue(Files.readAllBytes(staged.path).contentEquals(content))
        assertTrue(staged.mtime.isAfter(java.time.Instant.EPOCH))
    }

    @Test
    fun a_missing_object_surfaces_as_the_SDKs_NoSuchKey_and_leaves_no_file() = runTest {
        val fetch = S3Fetcher(client, bucket, Dispatchers.IO).fetcher
        val error = runCatching { fetch("meta/absent.json", staging.resolve("absent.json"), DigestAlgorithm.MD5) }.exceptionOrNull()
        assertTrue(error is NoSuchKeyException, "$error")
        assertTrue(Files.notExists(staging.resolve("absent.json")))
    }
}
