package infra.shuttle.s3

import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.TargetMetadata
import infra.shuttle.testkit.ObjectStoreTargetContract
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.never
import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import software.amazon.awssdk.services.s3.model.BucketLifecycleConfiguration
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.ExpirationStatus
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.LifecycleRule
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest
import software.amazon.awssdk.services.s3.model.NoncurrentVersionExpiration
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.ServerSideEncryption
import java.security.MessageDigest

@Tag("minio")
class S3TargetTest : ObjectStoreTargetContract() {
    private val client get() = Minio.client

    private lateinit var bucket: String
    private lateinit var target: S3Target

    @BeforeEach fun bucket() {
        bucket = Minio.versionedBucket()
        target = S3Target(client, bucket, Dispatchers.IO)
    }

    override fun target(): ObjectStoreTarget = target
    override fun location() = bucket
    override suspend fun currentBytes(key: String): ByteArray =
        client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build()).asByteArray()

    private fun md5(content: String) = mapOf(
        TargetMetadata.DIGEST to MessageDigest.getInstance("MD5").digest(content.toByteArray()).joinToString("") { "%02x".format(it) },
        TargetMetadata.DIGEST_ALGORITHM to "md5",
    )

    private fun versions(key: String) =
        client.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucket).prefix(key).build())

    @Test
    fun I6_three_stores_read_back_the_newest_by_key_a_crash_between_PUT_and_HEAD_is_repaired_by_the_next_store_and_nothing_is_deleted() = runTest {
        val spy = spy(client)
        var crash = false
        val target = S3Target(spy, bucket, Dispatchers.IO) {
            if (crash) { crash = false; throw CancellationException("process died between PUT and HEAD") }
        }
        target.store("in/a.csv", file("1", "v1"), md5("v1"))
        target.store("in/a.csv", file("2", "v2"), md5("v2"))
        crash = true
        assertTrue(runCatching { target.store("in/a.csv", file("3", "v3"), md5("v3")) }.exceptionOrNull() is CancellationException)
        val ref = target.store("in/a.csv", file("4", "v3"), md5("v3"))

        assertEquals("v3", String(currentBytes("in/a.csv")))
        assertTrue(target.verify(ref))
        val listed = versions("in/a.csv")
        assertEquals(4, listed.versions().size, "three completed stores plus the one that crashed after its PUT")
        assertEquals(ref.ref, listed.versions().single { it.isLatest }.versionId())
        assertTrue(listed.deleteMarkers().isEmpty())
        verify(spy, never()).deleteObject(any(DeleteObjectRequest::class.java))
        verify(spy, never()).deleteObjects(any(DeleteObjectsRequest::class.java))
        assertTrue(target.warnings.isEmpty(), "an unencrypted single-part object passes the ETag check silently")
    }

    @Test
    fun the_ETag_check_is_skipped_with_a_WARN_when_the_HEAD_reports_encryption() = runTest {
        val spy = spy(client)
        doAnswer { call ->
            val real = call.callRealMethod() as HeadObjectResponse
            real.toBuilder().serverSideEncryption(ServerSideEncryption.AES256).eTag("\"not-an-md5\"").build()
        }.`when`(spy).headObject(any(HeadObjectRequest::class.java))
        val target = S3Target(spy, bucket, Dispatchers.IO)
        val ref = target.store("in/c.csv", file("c", "cipher"), md5("cipher"))
        assertEquals(6, ref.size)
        assertEquals(1, target.warnings.size)
        assertTrue(target.warnings.single().contains("encrypted"))
    }

    @Test
    fun verify_of_a_version_expired_by_hand_is_false() = runTest {
        val ref = target.store("in/d.csv", file("d", "gone soon"), md5("gone soon"))
        assertTrue(target.verify(ref))
        client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(ref.key).versionId(ref.ref).build()) // the lifecycle rule, by hand
        assertFalse(target.verify(ref))
    }

    @Test
    fun probe_warns_without_a_non_current_expiry_is_silent_with_one_and_fails_on_a_missing_bucket() = runTest {
        target.probe()
        assertEquals(1, target.warnings.size)
        assertTrue(target.warnings.single().contains("non-current"))

        client.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder().bucket(bucket)
            .lifecycleConfiguration(BucketLifecycleConfiguration.builder().rules(LifecycleRule.builder()
                .id("expire-old-versions").status(ExpirationStatus.ENABLED).filter(LifecycleRuleFilter.builder().prefix("").build())
                .noncurrentVersionExpiration(NoncurrentVersionExpiration.builder().noncurrentDays(365).build()).build()).build()).build())
        target.warnings.clear()
        target.probe()
        assertTrue(target.warnings.isEmpty())

        val missing = S3Target(client, "no-such-bucket", Dispatchers.IO)
        val error = runCatching { missing.probe() }.exceptionOrNull()
        assertTrue(error is IllegalStateException && error.message!!.contains("no-such-bucket"), "$error")
    }

    @Test
    fun a_corrupted_body_is_rejected_by_Content_MD5_and_leaves_no_version() = runTest {
        val error = runCatching { target.store("in/b.csv", file("b", "what arrived"), md5("what was digested")) }.exceptionOrNull()
        assertTrue(error is S3Exception, "$error")
        assertEquals(400, (error as S3Exception).statusCode())
        assertTrue(versions("in/b.csv").versions().isEmpty())
    }
}
