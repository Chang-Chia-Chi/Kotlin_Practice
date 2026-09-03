package infra.shuttle.core

import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 13: the immutable model YAML and the Kotlin DSL both produce. `Rules` judges it. */
data class ShuttleConfig(
    val stateStore: StateStoreConfig?,
    val notifier: NotifierConfig,
    val supervision: SupervisionConfig,
    val digest: DigestAlgorithm,
    val drainTimeout: Duration,
    val objectStores: List<ObjectStore>,
    val channels: List<Channel>,
    val routes: List<Route>,
)

data class StateStoreConfig(val datasource: String)
data class NotifierConfig(val workers: Int = 4, val batch: Int = 50, val sweepEvery: Duration = 30.seconds)
enum class Readiness { AllRoutesDown, AnyRouteDown }
data class SupervisionConfig(
    val restartBackoff: Backoff = Backoff(initial = 30.seconds, max = 15.minutes),
    val readiness: Readiness = Readiness.AllRoutesDown,
)

/** Rule 25: a secret is a reference to the environment; a literal is representable only so the rule can name it. */
sealed interface Secret {
    data class Env(val variable: String) : Secret
    data class Literal(val value: String) : Secret
}

data class Pool(val maxSize: Int, val maxConcurrentTransfers: Int = maxSize)

/** Binary gigabytes, for the byte-sized knobs (`1.gib`, `10.gib`). */
val Int.gib: Long get() = this * (1L shl 30)

/** D41: the local directory fetches land in, and the usable space below which a fetch is deferred. */
data class Staging(val dir: Path, val minFree: Long = 1.gib)

sealed interface ObjectStore {
    val name: String
    val pool: Pool?
}

sealed interface HostKey {
    data class Strict(val knownHosts: Path) : HostKey
    data object AcceptAll : HostKey
}

data class SftpStore(
    override val name: String,
    val host: String?,
    val port: Int = 22,
    val user: Secret?,
    val password: Secret?,
    val hostKey: HostKey = HostKey.AcceptAll,
    val keepAlive: Duration = 30.seconds,
    val idleTimeout: Duration = 4.minutes,
    val idleCutoff: Duration = 5.minutes,
    val drainTimeout: Duration = 30.seconds,
    val cancelGrace: Duration = 5.seconds,
    override val pool: Pool = Pool(maxSize = 5),
    val staging: Staging?,
) : ObjectStore

data class S3Credentials(val accessKey: Secret, val secretKey: Secret)
data class S3Timeouts(val connect: Duration = 5.seconds, val socket: Duration = 30.seconds, val apiCall: Duration = 45.seconds)
data class S3Store(
    override val name: String,
    val endpoint: String?,
    val region: String = "us-east-1",
    val pathStyle: Boolean = false,
    val credentials: S3Credentials?,
    val timeouts: S3Timeouts = S3Timeouts(),
    override val pool: Pool? = null,
) : ObjectStore

sealed interface Channel {
    val name: String
}

enum class HttpMethod { POST, PUT }
sealed interface HttpAuth {
    data class Bearer(val token: Secret) : HttpAuth
    data class Basic(val user: Secret, val password: Secret) : HttpAuth
    data class Header(val name: String, val value: Secret) : HttpAuth
}
data class ResponseSpec(val success: Set<Int> = (200..299).toSet(), val retry: Set<Int> = emptySet(), val reference: String? = null)
data class HttpChannel(
    override val name: String,
    val method: HttpMethod = HttpMethod.POST,
    val url: String?,
    val auth: HttpAuth? = null,
    val timeout: Duration = 10.seconds,
    val response: ResponseSpec = ResponseSpec(),
    val policy: DeliveryPolicy = DeliveryPolicy(),
    val body: MappingTable = MappingTable(emptyList()),
) : Channel

data class NatsChannel(override val name: String, val url: String?, val credentials: Secret? = null) : Channel

/** Spec 5.3. */
sealed interface AckAction {
    data class Move(val folder: String) : AckAction
    data object Delete : AckAction
    data object None : AckAction
    data class Tag(val key: String, val value: String) : AckAction
    data object Ack : AckAction
    data object Term : AckAction
    data object Nak : AckAction
    data class Callback(val channel: String) : AckAction
}

sealed interface FileReadiness {
    data class SizeStable(val checks: Int = 2, val interval: Duration = 10.seconds) : FileReadiness
    data class MinAge(val age: Duration) : FileReadiness
}

sealed interface Source {
    data class Poll(
        val store: String,
        val directory: String,
        val every: Duration,
        val readiness: List<FileReadiness> = listOf(FileReadiness.SizeStable(), FileReadiness.MinAge(1.minutes)),
        val onAck: AckAction? = null,
        val onNack: AckAction? = null,
    ) : Source

    data class Subscribe(
        val channel: String,
        val subject: String,
        val onAck: AckAction? = null,
        val onNack: AckAction? = null,
        val inProgressEvery: Duration = 10.seconds,
    ) : Source
}

data class Fetch(val store: String, val path: String)
data class Target(val store: String, val bucket: String? = null, val directory: String? = null, val key: String = "{name}")
data class Notify(val on: DeliveryMoment, val channel: String)
enum class ExtractFrom { FileName, SourcePath, Content, Message }

/** Spec 6.3: the chain as data. `produces` is what rule 17 and 22 count as declared attributes. */
sealed interface ProcessorSpec {
    val produces: Set<String> get() = emptySet()

    data object Quality : ProcessorSpec
    data class Rename(val pattern: String) : ProcessorSpec
    data object Zip : ProcessorSpec
    /** D41: an archive past either limit is rejected, not extracted. */
    data class Unzip(val maxEntries: Int = 10_000, val maxBytes: Long = 10.gib) : ProcessorSpec
    data class Extract(
        val from: ExtractFrom,
        val regex: String? = null,
        val into: List<String>? = null,
        val json: Map<String, String>? = null,
    ) : ProcessorSpec {
        override val produces: Set<String>
            get() = into?.toSet() ?: json?.keys ?: NAMED_GROUP.findAll(regex.orEmpty()).map { it.groupValues[1] }.toSet()

        private companion object {
            val NAMED_GROUP = Regex("""\(\?<([A-Za-z][A-Za-z0-9]*)>""")
        }
    }
    data class Expand(val format: String, val files: String, val from: String) : ProcessorSpec
    data class VerifyDigest(val attribute: String) : ProcessorSpec
    data class Custom(val name: String, val config: Map<String, Any?> = emptyMap()) : ProcessorSpec
}

data class Route(
    val name: String,
    val source: Source?,
    val fetch: Fetch? = null,
    val process: List<ProcessorSpec> = emptyList(),
    val target: Target?,
    val notify: List<Notify> = emptyList(),
    val parallelism: Int = 1,
    val maxAttempts: Int = 5,
    val stuckAfter: Duration? = null,
    val digest: DigestAlgorithm? = null,
    /** D40: a finished identity still listed is digested again at most this often; `0s` means every poll. */
    val recheckFinished: Duration = 24.hours,
)
