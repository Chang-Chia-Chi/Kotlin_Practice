package infra.snapshotarchive

import io.minio.DownloadObjectArgs
import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.StatObjectArgs
import io.minio.UploadObjectArgs
import io.minio.errors.ErrorResponseException
import java.nio.file.Path

/**
 * The archive layer's object store (spec 18.2), wrapping the MinIO client.
 *
 * This is a testability seam, not an abstraction: it is a concrete class with four methods,
 * and the spec 2.3 five-interface budget is a framework budget this layer adds nothing to
 * (plan 3c). All four are `open` so the crash-matrix and interleaving tests can drive a
 * subclass instead of a container - the whole point is that those tests stay fast and
 * deterministic, while `ObjectStoreTest` proves against a real MinIO that the calls below
 * actually do what the fake pretends they do.
 *
 * There is deliberately no `list`. D33 puts a manifest row carrying the full inventory in
 * front of every upload, so what a version contains is always readable without asking the
 * bucket, and an object nothing points at cannot exist. A LIST method would be the first
 * half of the orphan sweep this design exists to avoid; `ArchitectureTest` fails the build
 * if one appears.
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

    /**
     * Removes [key], for the purge's reclaim step (spec 18.5).
     *
     * Absence is success, because S3 delete is idempotent - which is exactly what lets a
     * purge that died halfway through a version simply run again on the next pass instead of
     * needing to remember where it stopped.
     */
    open fun delete(key: String) {
        client.removeObject(RemoveObjectArgs.builder().bucket(bucket).`object`(key).build())
    }

    /**
     * Downloads [key] to [file], for the ticket-05 diff helper's baseline (spec 18.4 step 2).
     *
     * Download-then-read rather than reading the object in place: D36 keeps httpfs off the
     * pinned DuckDB 1.1.3's surface, so the Parquet a diff joins against is always a local
     * file. A missing key throws here, unlike [sizeOf] - the caller only ever downloads what
     * a COMPLETE row's inventory names, and a version whose objects have gone is marked
     * FAILED before the first one is deleted, so absence at this call is a broken invariant
     * rather than a normal answer.
     */
    open fun get(key: String, file: Path) {
        client.downloadObject(
            DownloadObjectArgs.builder()
                .bucket(bucket)
                .`object`(key)
                .filename(file.toAbsolutePath().toString())
                .build(),
        )
    }
}
