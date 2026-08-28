package infra.snapshotarchive

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.nio.file.Files
import java.nio.file.Path

/**
 * The one thing [ArchiverTest]'s fake store cannot prove: that [ObjectStore]'s two methods
 * really do talk to MinIO the way the fake pretends.
 *
 * Everything about the publish protocol is asserted against the fake, because a crash matrix
 * and a set of latch-driven interleavings have no business waiting on a container. What is
 * left over is exactly this - upload, size-of-an-object-that-exists, and the null that
 * spec 18.3 step 4 and the ticket-04 watchdog both read as "it did not land" - and that part
 * is only worth anything against the real server.
 */
@Testcontainers
class ObjectStoreTest {

    @Test
    fun `uploads a file and reports its stored size`(@TempDir dir: Path) {
        val file = dir.resolve("t_a.parquet")
        val bytes = ByteArray(4096) { it.toByte() }
        Files.write(file, bytes)

        store.put("snapshots/g/v1/t_a.parquet", file)

        assertThat(store.sizeOf("snapshots/g/v1/t_a.parquet")).isEqualTo(bytes.size.toLong())
    }

    /**
     * Absence has to come back as null rather than as an exception: it is the normal answer
     * when a crashed run left an inventory entry with no object behind it, which is the
     * whole question the watchdog asks.
     */
    @Test
    fun `sizeOf is null for an object that was never uploaded`() {
        assertThat(store.sizeOf("snapshots/g/v1/never-written.parquet")).isNull()
    }

    companion object {

        /** Pinned to the tag this environment already has; the module's default is not present. */
        @Container
        @JvmStatic
        val minio: MinIOContainer = MinIOContainer("minio/minio:RELEASE.2024-10-02T17-50-41Z")

        private lateinit var store: ObjectStore

        @BeforeAll
        @JvmStatic
        fun createBucket() {
            val client = MinioClient.builder()
                .endpoint(minio.s3URL)
                .credentials(minio.userName, minio.password)
                .build()
            client.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build())
            store = ObjectStore(client, BUCKET)
        }

        private const val BUCKET = "snapshot-archive"
    }
}
