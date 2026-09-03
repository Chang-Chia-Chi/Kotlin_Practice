package infra.shuttle.core

import java.time.Instant

@JvmInline value class TransferId(val value: Long)
@JvmInline value class DeliveryId(val value: Long)
@JvmInline value class RouteName(val value: String)
@JvmInline value class ChannelName(val value: String)

/** Spec 4.2. */
enum class TransferState { SEEN, FETCHED, PROCESSED, STORED, ACKED, DONE, REJECTED, FAILED }
enum class TransferKind { OBJECT, MESSAGE, CHILD }
enum class SourceKind { SFTP, S3, NATS }
enum class DigestAlgorithm { MD5, SHA256, SHA1 }

/** Algorithm plus hex, computed as the bytes stream. The companion spells the DSL's `digest = Digest.MD5`. */
data class Digest(val algorithm: DigestAlgorithm, val hex: String) {
    companion object {
        val MD5 = DigestAlgorithm.MD5
        val SHA256 = DigestAlgorithm.SHA256
        val SHA1 = DigestAlgorithm.SHA1
    }
}

/** Spec 5.2: the unique key of `file_transfer`; `revision` grows only through `supersede`. */
data class SourceIdentity(
    val route: RouteName,
    val sourceKind: SourceKind,
    val sourceRef: String,
    val sourceName: String,
    val sourceSize: Long?,
    val sourceMtime: Instant?,
    val revision: Int = 1,
)

/** One row of `file_transfer` (spec 8.1). */
data class Transfer(
    val id: TransferId,
    val identity: SourceIdentity,
    val kind: TransferKind,
    val state: TransferState,
    val parentId: TransferId? = null,
    val supersedesId: TransferId? = null,
    val sourceDigest: Digest? = null,
    val digest: Digest? = null,
    val storedName: String? = null,
    val storedMtime: Instant? = null,
    val attempts: Int = 0,
    val lastError: String? = null,
    val attributes: Map<String, String> = emptyMap(),
    val target: TargetRef? = null,
    val firstSeenAt: Instant,
    val updatedAt: Instant,
    val ackedAt: Instant? = null,
    val completedAt: Instant? = null,
)

/** Spec 7.1. */
data class TargetRef(val kind: String, val location: String, val key: String, val ref: String?, val size: Long)

/** Spec 7.1's metadata keys the pipeline writes and a target may read; attributes go under `attr-<name>`. */
object TargetMetadata {
    const val DIGEST = "digest"
    const val DIGEST_ALGORITHM = "digest-algorithm"
    const val ATTRIBUTE_PREFIX = "attr-"
}

/** The row-side facts of a staged object: everything in spec 6.1 except the local path. */
data class StagedSummary(val name: String, val size: Long, val mtime: Instant, val digest: Digest, val contentType: String?)
