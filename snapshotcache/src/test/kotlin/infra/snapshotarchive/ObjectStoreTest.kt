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
 * The one thing the fake stores cannot prove: that [ObjectStore]'s four methods really do
 * talk to MinIO the way the fakes pretend.
 *
 * Everything about the publish protocol is asserted against the fake, because a crash matrix
 * and a set of latch-driven interleavings have no business waiting on a container. What is
 * left over is exactly this - upload, size-of-an-object-that-exists, the null that the publish
 * protocol and the ticket-04 watchdog both read as "it did not land", the download the diff
 * helper joins on, and the delete the purge reclaims with - and that part is only worth
 * anything against the real server.
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

    /**
     * The diff helper reads its baseline out of these bytes, so a `get` that wrote the wrong
     * file, or a truncated one, would surface as unreadable Parquet rather than as a wrong
     * answer - loud, but only if the round trip is exercised at all against the real server.
     */
    @Test
    fun `downloads an object back to a byte-identical file`(@TempDir dir: Path) {
        val source = dir.resolve("source.parquet")
        val bytes = ByteArray(4096) { (it * 7).toByte() }
        Files.write(source, bytes)
        store.put("snapshots/g/v2/t_a.parquet", source)

        val target = dir.resolve("downloaded.parquet")
        store.get("snapshots/g/v2/t_a.parquet", target)

        assertThat(Files.readAllBytes(target)).isEqualTo(bytes)
    }

    /**
     * The destructive one, and the only method whose fake had never been checked against the
     * real server. The purge deletes objects before their manifest row, so a
     * delete that silently did nothing would leave storage growing without bound while every
     * test still passed.
     */
    @Test
    fun `deletes an object so it no longer has a size`(@TempDir dir: Path) {
        val file = dir.resolve("doomed.parquet")
        Files.write(file, ByteArray(128))
        store.put("snapshots/g/v3/doomed.parquet", file)
        assertThat(store.sizeOf("snapshots/g/v3/doomed.parquet")).isEqualTo(128L)

        store.delete("snapshots/g/v3/doomed.parquet")

        assertThat(store.sizeOf("snapshots/g/v3/doomed.parquet")).isNull()
    }

    /** Purge retries a pass as one unit, so deleting what is already gone must not throw. */
    @Test
    fun `deleting an absent object is a no-op`() {
        store.delete("snapshots/g/v3/never-existed.parquet")

        assertThat(store.sizeOf("snapshots/g/v3/never-existed.parquet")).isNull()
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
