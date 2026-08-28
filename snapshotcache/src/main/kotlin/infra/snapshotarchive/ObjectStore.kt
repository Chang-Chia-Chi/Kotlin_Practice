package infra.snapshotarchive

import io.minio.MinioClient
import io.minio.StatObjectArgs
import io.minio.UploadObjectArgs
import io.minio.errors.ErrorResponseException
import java.nio.file.Path

/**
 * The archive layer's object store (spec 18.2), wrapping the MinIO client.
 *
 * This is a testability seam, not an abstraction: it is a concrete class with two methods,
 * and the spec 2.3 five-interface budget is a framework budget this layer adds nothing to
 * (plan 3c). Both methods are `open` so the crash-matrix and interleaving tests can drive a
 * subclass instead of a container - the whole point is that those tests stay fast and
 * deterministic, while `ObjectStoreTest` proves against a real MinIO that the two calls
 * below actually do what the fake pretends they do.
 *
 * Creating [bucket] is not this class's job. Like the manifest table it is provisioned
 * ahead of the process, and a bucket auto-created by whichever pod started first is exactly
 * the sort of ambient side effect the layer's ordering guarantees are meant to avoid.
 */
open class ObjectStore(private val client: MinioClient, val bucket: String) {

    /** Uploads [file] to [key], overwriting whatever was there. */
    open fun put(key: String, file: Path) {
        client.uploadObject(
            UploadObjectArgs.builder()
                .bucket(bucket)
                .`object`(key)
                .filename(file.toAbsolutePath().toString())
                .build(),
        )
    }

    /**
     * Size of [key] in bytes, or null when the object is absent.
     *
     * Absence is a normal answer here - it is how spec 18.3 step 4's verification and the
     * ticket-04 watchdog both ask "did this object actually land" - so only `NoSuchKey` is
     * folded into null. A missing bucket or a refused credential still throws, because
     * treating those as "object absent" would let a misconfigured deployment report an
     * unverifiable checkpoint as merely incomplete.
     */
    open fun sizeOf(key: String): Long? =
        try {
            client.statObject(StatObjectArgs.builder().bucket(bucket).`object`(key).build()).size()
        } catch (e: ErrorResponseException) {
            if (e.errorResponse().code() == "NoSuchKey") null else throw e
        }
}
