package infra.shuttle.core

import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 13.2. Builds the model and judges nothing: `Rules.validate` reports every violation by number. */
fun shuttle(configure: ShuttleBuilder.() -> Unit): ShuttleConfig = ShuttleBuilder().apply(configure).build()

@DslMarker
annotation class ShuttleDsl

fun env(variable: String): Secret = Secret.Env(variable)
fun fromEnvironment(accessKey: String, secretKey: String) = S3Credentials(env(accessKey), env(secretKey))
fun bearer(token: Secret): HttpAuth = HttpAuth.Bearer(token)
fun basic(user: Secret, password: Secret): HttpAuth = HttpAuth.Basic(user, password)
fun header(name: String, value: Secret): HttpAuth = HttpAuth.Header(name, value)
fun move(folder: String): AckAction = AckAction.Move(folder)
fun callback(channel: ChannelRef): AckAction = AckAction.Callback(channel.name)

fun quality(): ProcessorSpec = ProcessorSpec.Quality
fun rename(pattern: String): ProcessorSpec = ProcessorSpec.Rename(pattern)
fun zip(): ProcessorSpec = ProcessorSpec.Zip
fun unzip(maxEntries: Int = 10_000, maxBytes: Long = 10.gib): ProcessorSpec = ProcessorSpec.Unzip(maxEntries, maxBytes)
fun extract(from: ExtractFrom, regex: String? = null, into: List<String>? = null, json: Map<String, String>? = null): ProcessorSpec =
    ProcessorSpec.Extract(from, regex, into, json)
fun expand(format: String, files: String, from: StoreRef): ProcessorSpec = ProcessorSpec.Expand(format, files, from.name)
fun verifyDigest(attribute: String): ProcessorSpec = ProcessorSpec.VerifyDigest(attribute)
fun custom(name: String, config: Map<String, Any?> = emptyMap()): ProcessorSpec = ProcessorSpec.Custom(name, config)
infix fun ProcessorSpec.then(next: ProcessorSpec): List<ProcessorSpec> = listOf(this, next)
infix fun List<ProcessorSpec>.then(next: ProcessorSpec): List<ProcessorSpec> = this + next

data class StoreRef(val name: String)
data class ChannelRef(val name: String)
fun objectStore(name: String) = StoreRef(name)
fun channel(name: String) = ChannelRef(name)
fun StoreRef.bucket(bucket: String, configure: TargetBuilder.() -> Unit = {}) =
    Target(name, bucket = bucket, key = TargetBuilder().apply(configure).key)
fun StoreRef.directory(directory: String, configure: TargetBuilder.() -> Unit = {}) =
    Target(name, directory = directory, key = TargetBuilder().apply(configure).key)

@ShuttleDsl
class TargetBuilder {
    var key: String = "{name}"
}

@ShuttleDsl
class ShuttleBuilder {
    private var stateStore: StateStoreConfig? = null
    private val notifier = NotifierBuilder()
    private val supervision = SupervisionBuilder()
    private val objectStores = ObjectStoresBuilder()
    private val channels = ChannelsBuilder()
    private val routes = mutableListOf<Route>()
    var digest: DigestAlgorithm = DigestAlgorithm.MD5
    var drainTimeout: Duration = 60.seconds

    fun shuttleStateStore(configure: StateStoreBuilder.() -> Unit) {
        stateStore = StateStoreBuilder().apply(configure).config
    }

    fun notifier(configure: NotifierBuilder.() -> Unit) = notifier.apply(configure).let { }
    fun supervision(configure: SupervisionBuilder.() -> Unit) = supervision.apply(configure).let { }
    fun objectStores(configure: ObjectStoresBuilder.() -> Unit) = objectStores.apply(configure).let { }
    fun channels(configure: ChannelsBuilder.() -> Unit) = channels.apply(configure).let { }
    fun route(name: String, configure: RouteBuilder.() -> Unit) {
        routes += RouteBuilder(name).apply(configure).build()
    }

    internal fun build() = ShuttleConfig(
        stateStore = stateStore,
        notifier = NotifierConfig(notifier.workers, notifier.batch, notifier.sweepEvery),
        supervision = SupervisionConfig(supervision.restartBackoff, supervision.readiness),
        digest = digest,
        drainTimeout = drainTimeout,
        objectStores = objectStores.stores.toList(),
        channels = channels.channels.toList(),
        routes = routes.toList(),
    )
}

@ShuttleDsl
class StateStoreBuilder {
    internal var config: StateStoreConfig? = null
    fun oracle(datasource: String) {
        config = StateStoreConfig(datasource)
    }
}

@ShuttleDsl
class NotifierBuilder {
    var workers: Int = 4
    var batch: Int = 50
    var sweepEvery: Duration = 30.seconds
}

@ShuttleDsl
class SupervisionBuilder {
    internal var restartBackoff = Backoff(initial = 30.seconds, max = 15.minutes)
    var readiness: Readiness = Readiness.AllRoutesDown
    fun restartBackoff(initial: Duration, max: Duration) {
        restartBackoff = Backoff(initial, max)
    }
}

@ShuttleDsl
class ObjectStoresBuilder {
    internal val stores = mutableListOf<ObjectStore>()
    fun sftp(name: String, configure: SftpStoreBuilder.() -> Unit) {
        stores += SftpStoreBuilder(name).apply(configure).build()
    }
    fun s3(name: String, configure: S3StoreBuilder.() -> Unit) {
        stores += S3StoreBuilder(name).apply(configure).build()
    }
}

@ShuttleDsl
class SftpStoreBuilder(private val name: String) {
    private var host: String? = null
    private var port = 22
    private var user: Secret? = null
    private var password: Secret? = null
    private val pool = PoolBuilder()
    var hostKey: HostKey = HostKey.AcceptAll
    var keepAlive: Duration = 30.seconds
    var idleTimeout: Duration = 4.minutes
    var idleCutoff: Duration = 5.minutes
    var drainTimeout: Duration = 30.seconds
    var cancelGrace: Duration = 5.seconds
    private var staging: Staging? = null

    fun staging(configure: StagingBuilder.() -> Unit) = StagingBuilder().apply(configure).let { b -> staging = b.dir?.let { Staging(it, b.minFree) } }
    fun endpoint(configure: Endpoint.() -> Unit) = Endpoint().apply(configure).let { host = it.host; port = it.port }
    fun auth(configure: Auth.() -> Unit) = Auth().apply(configure).let { user = it.user; password = it.password }
    fun pool(configure: PoolBuilder.() -> Unit) = pool.apply(configure).let { }

    @ShuttleDsl
    class Endpoint {
        var host: String? = null
        var port: Int = 22
    }

    @ShuttleDsl
    class StagingBuilder {
        var dir: Path? = null
        var minFree: Long = 1.gib
    }

    @ShuttleDsl
    class Auth {
        internal var user: Secret? = null
        internal var password: Secret? = null
        fun password(user: Secret, password: Secret) {
            this.user = user
            this.password = password
        }
    }

    internal fun build() = SftpStore(
        name, host, port, user, password, hostKey, keepAlive, idleTimeout, idleCutoff, drainTimeout, cancelGrace,
        Pool(pool.maxSize, pool.maxConcurrentTransfers ?: pool.maxSize), staging,
    )
}

@ShuttleDsl
class PoolBuilder {
    var maxSize: Int = 5
    var maxConcurrentTransfers: Int? = null
}

@ShuttleDsl
class S3StoreBuilder(private val name: String) {
    private val pool = PoolBuilder()
    private var hasPool = false
    var endpoint: String? = null
    var region: String = "us-east-1"
    var pathStyle: Boolean = false
    var credentials: S3Credentials? = null
    var timeouts: S3Timeouts = S3Timeouts()
    fun pool(configure: PoolBuilder.() -> Unit) = pool.apply(configure).let { hasPool = true }

    internal fun build() = S3Store(
        name, endpoint, region, pathStyle, credentials, timeouts,
        if (hasPool) Pool(pool.maxSize, pool.maxConcurrentTransfers ?: pool.maxSize) else null,
    )
}

@ShuttleDsl
class ChannelsBuilder {
    internal val channels = mutableListOf<Channel>()
    fun http(name: String, configure: HttpChannelBuilder.() -> Unit) {
        channels += HttpChannelBuilder(name).apply(configure).build()
    }
    fun nats(name: String, configure: NatsChannelBuilder.() -> Unit) {
        channels += NatsChannelBuilder(name).apply(configure).build()
    }
}

@ShuttleDsl
class HttpChannelBuilder(private val name: String) {
    private val response = Response()
    var method: HttpMethod = HttpMethod.POST
    var url: String? = null
    var auth: HttpAuth? = null
    var timeout: Duration = 10.seconds
    var policy: DeliveryPolicy = DeliveryPolicy()
    var body: MappingTable = MappingTable(emptyList())
    fun response(configure: Response.() -> Unit) = response.apply(configure).let { }

    @ShuttleDsl
    class Response {
        var success: Iterable<Int> = 200..299
        var retry: Iterable<Int> = emptySet()
        var reference: String? = null
    }

    internal fun build() = HttpChannel(
        name, method, url, auth, timeout, ResponseSpec(response.success.toSet(), response.retry.toSet(), response.reference), policy, body,
    )
}

@ShuttleDsl
class NatsChannelBuilder(private val name: String) {
    var url: String? = null
    var credentials: Secret? = null
    var subject: String? = null
    internal fun build() = NatsChannel(name, url, credentials, subject)
}

/** Spec 9.6 as infix rows; `row` admits a raw row for what the infix forms cannot say. */
fun mapping(configure: MappingBuilder.() -> Unit) = MappingTable(MappingBuilder().apply(configure).rows.toList())

data class ProviderRef(val bean: String, val select: String? = null)
fun provider(bean: String, select: String? = null) = ProviderRef(bean, select)

@ShuttleDsl
class MappingBuilder {
    internal val rows = mutableListOf<MappingRow>()
    infix fun String.from(field: Field) = row(MappingRow(this, field = field))
    infix fun String.fromAttribute(attribute: String) = row(MappingRow(this, attribute = attribute))
    infix fun String.by(provider: ProviderRef) = row(MappingRow(this, provider = provider.bean, select = provider.select))
    infix fun String.value(literal: String) = row(MappingRow(this, value = literal))
    fun row(row: MappingRow) {
        rows += row
    }
}

@ShuttleDsl
class RouteBuilder(private val name: String) {
    private val notify = mutableListOf<Notify>()
    var source: Source? = null
    var fetch: Fetch? = null
    var process: List<ProcessorSpec> = emptyList()
    var target: Target? = null
    var parallelism: Int = 1
    var maxAttempts: Int = 5
    var stuckAfter: Duration? = null
    var digest: DigestAlgorithm? = null
    var recheckFinished: Duration = 24.hours

    val Fetched = DeliveryMoment.FETCHED
    val Stored = DeliveryMoment.STORED
    val Acked = DeliveryMoment.ACKED

    fun poll(store: StoreRef, directory: String, configure: PollBuilder.() -> Unit = {}): Source =
        PollBuilder().apply(configure).let { Source.Poll(store.name, directory, it.every, it.readiness, it.onAck, it.onNack) }

    fun subscribe(channel: ChannelRef, subject: String, configure: SubscribeBuilder.() -> Unit = {}): Source =
        SubscribeBuilder().apply(configure).let { Source.Subscribe(channel.name, subject, it.onAck, it.onNack, it.inProgressEvery) }

    fun fetch(store: StoreRef, path: String, bucket: String? = null) {
        fetch = Fetch(store.name, path, bucket)
    }

    fun notify(on: DeliveryMoment, channel: ChannelRef) {
        notify += Notify(on, channel.name)
    }

    @ShuttleDsl
    class PollBuilder {
        var every: Duration = 1.hours
        var readiness: List<FileReadiness> = listOf(FileReadiness.SizeStable(), FileReadiness.MinAge(1.minutes))
        var onAck: AckAction? = null
        var onNack: AckAction? = null
    }

    @ShuttleDsl
    class SubscribeBuilder {
        var onAck: AckAction? = null
        var onNack: AckAction? = null
        var inProgressEvery: Duration = 10.seconds
    }

    internal fun build() = Route(name, source, fetch, process, target, notify.toList(), parallelism, maxAttempts, stuckAfter, digest, recheckFinished)
}
