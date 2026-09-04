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

/**
 * Spec 6.2: one step of the chain. The built-ins are each other's second implementation.
 *
 * D52: `process` is called on the module's bounded view of `Dispatchers.IO` (spec 3.3), so a processor
 * blocks where it stands - reading its file, writing an archive - and must not switch to a dispatcher of
 * its own to do it. Only then does rule 9's arithmetic bound the module's blocking work.
 */
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

/**
 * Spec 6.2 over one run's staging directory: the attributes accumulate, every file a processor creates is
 * allocated inside [dir] so it dies with the run (I9, I18), and `expand`'s fetch goes through [fetchers],
 * keyed by store name. The pipeline and `shuttle try` share it, so what an operator tries offline is the
 * context the route runs under (D35).
 */
class StagingContext(
    override val transfer: TransferView,
    override val source: SourceView,
    private val dir: Path,
    override val clock: Clock,
    private val algorithm: DigestAlgorithm,
    private val fetchers: Map<String, Fetcher>,
) : ProcessContext {
    override val attributes = LinkedHashMap<String, String>()
    private var created = 0

    override fun setAttribute(name: String, value: String) { attributes[name] = value }

    override fun newStagedFile(name: String): Path = dir.resolve("${created++}-${name.substringAfterLast('/')}")

    override suspend fun fetch(store: String, path: String): StagedObject {
        val fetcher = fetchers[store] ?: throw IllegalStateException("route ${transfer.route.value}: no fetcher for store $store")
        return fetcher(path, newStagedFile(path), algorithm)
    }
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
