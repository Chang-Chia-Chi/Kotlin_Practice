package infra.shuttle.core

import java.nio.file.Path
import java.time.Clock
import java.time.Instant

/** Spec 6.1. */
data class StagedObject(
    val name: String,
    val path: Path,
    val size: Long,
    val mtime: Instant,
    val digest: Digest,
    val contentType: String?,
) {
    val summary get() = StagedSummary(name, size, mtime, digest, contentType)
}

data class Payload(val objects: List<StagedObject>)

/** Spec 6.2: one step of the chain. The built-ins are each other's second implementation. */
interface Processor {
    val produces: Set<String>
    suspend fun process(payload: Payload, ctx: ProcessContext): Outcome
}

sealed interface Outcome {
    data class Continue(val payload: Payload) : Outcome
    data class Reject(val reason: String) : Outcome
}

/** Spec 6.2. No logger here, by D34: correlation comes through the MDC. */
interface ProcessContext {
    val transfer: TransferView
    val source: SourceView
    val attributes: Map<String, String>
    fun setAttribute(name: String, value: String)
    fun newStagedFile(name: String): Path
    suspend fun fetch(store: String, path: String): StagedObject
    val clock: Clock
}

data class TransferView(
    val id: TransferId,
    val route: RouteName,
    val identity: SourceIdentity,
    val sourcePath: String,
    val firstSeenAt: Instant,
    val parentId: TransferId?,
)

/** Poll: the listing entry. Subscribe: the message body and headers. */
class SourceView(val path: String, val body: ByteArray? = null, val headers: Map<String, String> = emptyMap())

/** Brings one object's bytes to `into`, digesting with `algorithm` as they stream (spec 4.1 stage 1). */
typealias Fetcher = suspend (path: String, into: Path, algorithm: DigestAlgorithm) -> StagedObject
