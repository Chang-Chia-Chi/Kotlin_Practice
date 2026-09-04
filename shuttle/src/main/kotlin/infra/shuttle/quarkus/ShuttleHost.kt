package infra.shuttle.quarkus

import infra.shuttle.core.Channel
import infra.shuttle.core.ChannelName
import infra.shuttle.core.Delivery
import infra.shuttle.core.DeliveryChannel
import infra.shuttle.core.DeliveryId
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryRequest
import infra.shuttle.core.DeliveryState
import infra.shuttle.core.Fetcher
import infra.shuttle.core.Hook
import infra.shuttle.core.MappingRenderer
import infra.shuttle.core.Notifier
import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.ProcessingChain
import infra.shuttle.core.Processor
import infra.shuttle.core.ProcessorSpec
import infra.shuttle.core.Provider
import infra.shuttle.core.Route
import infra.shuttle.core.RouteEvent
import infra.shuttle.core.RouteName
import infra.shuttle.core.RouteRunner
import infra.shuttle.core.RouteSupervisor
import infra.shuttle.core.Rules
import infra.shuttle.core.S3Store
import infra.shuttle.core.Secret
import infra.shuttle.core.SftpStore
import infra.shuttle.core.ShuttleConfig
import infra.shuttle.core.ShuttleMetrics
import infra.shuttle.core.Source
import infra.shuttle.core.Staging
import infra.shuttle.core.StateStore
import infra.shuttle.core.Transfer
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferPipeline
import infra.shuttle.core.TransferState
import infra.shuttle.core.processorFor
import infra.shuttle.http.HttpChannel
import infra.shuttle.jdbi.StateStoreSchema
import infra.shuttle.nats.NatsChannel
import infra.shuttle.s3.S3Fetcher
import infra.shuttle.s3.S3Target
import infra.shuttle.sftp.SftpPollSource
import infra.shuttle.sftp.SftpTarget
import infra.shuttle.sftp.sftpConnectorConfig
import infra.shuttle.sftp.sftpFetcher
import infra.shuttle.yaml.YamlLoader
import io.micrometer.core.instrument.MeterRegistry
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.jboss.logging.Logger
import sftp.connector.SftpConnector
import software.amazon.awssdk.services.s3.S3Client
import java.net.http.HttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import infra.shuttle.core.HttpChannel as HttpChannelConfig
import infra.shuttle.core.NatsChannel as NatsChannelConfig

/** Spec 12.1 step 5 and rules 15 and 17: what a CDI name resolves to. The host passes `Arc`; tests pass a map. */
class NamedBeans(private val lookup: (String) -> Any?) {
    fun processor(name: String): Processor? = lookup(name) as? Processor
    fun provider(name: String): Provider? = lookup(name) as? Provider

    /** The attributes a named `Processor` produces, an empty set for any other bean, null when nothing has the name. */
    fun produces(name: String): Set<String>? = when (val bean = lookup(name)) {
        null -> null
        is Processor -> bean.produces
        else -> emptySet()
    }

    companion object {
        val none = NamedBeans { null }
    }
}

/**
 * Spec 14.1's read side, which the `StateStore` seam deliberately lacks: the whole of both tables. The
 * Oracle store and the test kit's store both offer these views off the seam (ticket 10's merge note).
 */
// ponytail: whole-table reads filtered in memory; a WHERE clause on the store's view when a table outgrows one admin page.
class StoreReads(val transfers: suspend () -> List<Transfer>, val outbox: suspend () -> List<Delivery>)

/**
 * The running application (spec 12): every adapter constructed from one `ShuttleConfig`, started in the order
 * of 12.1 and stopped in the order of 12.3 inside `drainTimeout`, and the operations of 14.1 on top. It imports
 * no Quarkus: `ShuttleLifecycle` hands it the host's registry, clock and datasource, and a test hands it the
 * test kit's store and target. Every client the host opens, it closes.
 *
 * `targets` replaces a store's target adapter by store name and `deliveryChannels` a channel's delivery
 * adapter by channel name (the test kit's in-memory target and recording channel; nothing in production
 * sets either). A replaced channel keeps no trigger, so a route may not `subscribe` to one. `s3Client`
 * and `natsConnection` are the two client factories, overridable at the same boundary a mock would sit at.
 */
class ShuttleHost(
    private val config: ShuttleConfig,
    private val env: (String) -> String?,
    private val beans: NamedBeans,
    private val store: StateStore,
    private val reads: StoreReads,
    private val registry: MeterRegistry,
    private val clock: Clock,
    private val targets: Map<String, ObjectStoreTarget> = emptyMap(),
    private val deliveryChannels: Map<String, DeliveryChannel> = emptyMap(),
    private val s3Client: (S3Store) -> S3Client = { s3ClientFor(it, env) },
    private val natsConnection: (NatsChannelConfig) -> Connection = { natsConnectionFor(it, env) },
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
    private val hook: Hook = Hook.None,
    /** Spec 3.3's bounded IO view; the lifecycle passes the same one it built the JDBI store on. */
    private val io: CoroutineDispatcher = ioDispatcher(config),
) : AutoCloseable {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("shuttle"))
    private val s3Clients = HashMap<String, S3Client>()
    private val natsConnections = HashMap<String, Connection>()
    private val natsChannels = HashMap<String, NatsChannel>()
    private val sources = ConcurrentHashMap<String, SftpPollSource>()
    private val connectors = HashMap<String, SftpConnector>()
    private val lastTrigger = ConcurrentHashMap<String, Instant>()
    private lateinit var notifier: Notifier
    private lateinit var supervisor: RouteSupervisor
    private var routes: Job? = null
    private var deliveries: Job? = null

    @Volatile private var started = false
    @Volatile private var shuttingDown = false

    /** Spec 10's rule over the supervisor's gauges; false before startup has finished and from the first moment of shutdown. */
    fun ready(): Boolean = started && !shuttingDown && supervisor.ready()

    /** Spec 12.1 steps 2 to 7. Throws on any check that fails, which is the deployment ending. */
    fun start(): Unit = runBlocking {
        roundTrips()
        val routeTargets = config.routes.associate { it.name to targetFor(it) }
        probeStores(routeTargets.values)
        val channels = config.channels.associate { ChannelName(it.name) to channelFor(it) }
        emptyStaging()
        val renderer = MappingRenderer(beans::provider)
        // Spec 9.6: a body belongs to the channel, not to its kind, so every declared channel is in the map (SPEC6).
        val bodies = config.channels.associate { ChannelName(it.name) to it.body }
        notifier = Notifier(store, channels.values, bodies, renderer, config.notifier, registry, clock, hook = hook)
        val runners = config.routes.map { route ->
            val pipeline = TransferPipeline(
                route, route.digest ?: config.digest, store, routeTargets.getValue(route.name), chainFor(route), bodies,
                { beans.provider(it) != null }, notifier::wake, hook, clock, registry, stagingFor(route), channels = channels, renderer = renderer,
                fetchers = fetchersFor(route),
            )
            RouteRunner(route, pipeline, fetcherFor(route), store, notifier::wake, clock, registry)
        }
        supervisor = RouteSupervisor(runners, ::eventsFor, config.supervision.restartBackoff, config.supervision.readiness, registry)
        deliveries = scope.launch { notifier.run() }
        routes = scope.launch { supervisor.run() }
        started = true
        log.infov("shuttle started: {0} routes, {1} channels", config.routes.size, config.channels.size)
    }

    /**
     * Spec 12.3, from the Quarkus shutdown event: readiness false, the route collectors cancelled (each connector
     * drains under its own bound as its flow ends), the notifier cancelled leaving every PENDING row PENDING, then
     * the target connectors, the S3 clients and the NATS connections closed. The datasource is Quarkus's and closes after this returns.
     */
    override fun close() {
        shuttingDown = true
        val drained = runBlocking {
            withTimeoutOrNull(config.drainTimeout) {
                routes?.cancelAndJoin()
                deliveries?.cancelAndJoin()
            }
        }
        if (drained == null) log.warnv("shutdown overran drainTimeout {0}; in-flight work abandoned", config.drainTimeout)
        scope.cancel()
        runBlocking { connectors.values.forEach { runCatching { it.close() }.onFailure { e -> log.warn("closing an SFTP connector failed", e) } } }
        s3Clients.values.forEach { it.close() }
        natsConnections.values.forEach { runCatching { it.close() }.onFailure { e -> log.warn("closing a NATS connection failed", e) } }
    }

    // ---- spec 14.1: the admin operations; none triggers a poll ----

    enum class Outcome { DONE, NOT_FOUND, WRONG_STATE }

    /** Per route: up or down, last trigger, restart count, counts by state. */
    suspend fun routes(): List<Map<String, Any?>> {
        val byRoute = reads.transfers().groupBy { it.identity.route.value }
        return config.routes.map { route ->
            mapOf(
                "name" to route.name,
                "up" to (registry.find(ShuttleMetrics.ROUTE_UP).tag("route", route.name).gauge()?.value() == 1.0),
                "lastTrigger" to lastTrigger[route.name]?.toString(),
                "restarts" to (registry.find(ShuttleMetrics.ROUTE_RESTARTS).tag("route", route.name).counter()?.count() ?: 0.0).toLong(),
                "counts" to byRoute[route.name].orEmpty().groupingBy { it.state.name }.eachCount(),
            )
        }
    }

    /** Transfer rows, newest first, children folded under their parents. */
    suspend fun transfers(route: String?, state: TransferState?, limit: Int): List<Map<String, Any?>> {
        val all = reads.transfers()
        val children = all.filter { it.parentId != null }.groupBy { it.parentId!! }
        return all.asSequence()
            .filter { it.parentId == null && (route == null || it.identity.route.value == route) && (state == null || it.state == state) }
            .sortedByDescending { it.id.value }.take(limit)
            .map { row(it) + ("children" to children[it.id].orEmpty().map(::row)) }
            .toList()
    }

    /** The outbox rows of one transfer; null when there is no such transfer. */
    suspend fun deliveries(id: TransferId): List<Map<String, Any?>>? {
        store.byId(id) ?: return null
        return reads.outbox().filter { it.transferId == id }.map { d ->
            mapOf(
                "id" to d.id.value, "event" to d.moment.name.lowercase(), "channel" to d.channel.value, "state" to d.state.name,
                "attempts" to d.attempts, "lastStatus" to d.lastStatus, "lastError" to d.lastError, "reference" to d.reference,
                "nextAttemptAt" to d.nextAttemptAt.toString(), "deliveredAt" to d.deliveredAt?.toString(),
            )
        }
    }

    /** REJECTED or FAILED to SEEN; the next trigger re-runs it from fetch. */
    suspend fun redrive(id: TransferId): Outcome {
        val row = store.byId(id) ?: return Outcome.NOT_FOUND
        if (row.state != TransferState.REJECTED && row.state != TransferState.FAILED) return Outcome.WRONG_STATE
        store.redrive(id)
        return Outcome.DONE
    }

    /** STORED to ACKED by hand, with the route's `acked` deliveries: the operator override for a source the process can no longer reach. */
    suspend fun ack(id: TransferId): Outcome {
        val row = store.byId(id) ?: return Outcome.NOT_FOUND
        if (row.state != TransferState.STORED) return Outcome.WRONG_STATE
        val route = config.routes.firstOrNull { it.name == row.identity.route.value } ?: return Outcome.NOT_FOUND
        val events = route.notify.filter { it.on == DeliveryMoment.ACKED }.map { DeliveryRequest(DeliveryMoment.ACKED, ChannelName(it.channel)) }
        store.acked(id, events)
        if (events.isNotEmpty()) notifier.wake()
        log.warnv("transfer {0} acked by the operator", id.value)
        return Outcome.DONE
    }

    /** FAILED to PENDING; the notifier is woken. */
    suspend fun redriveDelivery(id: DeliveryId): Outcome {
        val row = reads.outbox().firstOrNull { it.id == id } ?: return Outcome.NOT_FOUND
        if (row.state != DeliveryState.FAILED) return Outcome.WRONG_STATE
        store.redriveDelivery(id)
        notifier.wake()
        return Outcome.DONE
    }

    /** Restart a route now, resetting its backoff; false when no route has the name. */
    fun restart(route: String): Boolean = supervisor.restart(route)

    private fun row(t: Transfer): Map<String, Any?> = mapOf(
        "id" to t.id.value, "route" to t.identity.route.value, "kind" to t.kind.name, "state" to t.state.name,
        "sourceRef" to t.identity.sourceRef, "sourceName" to t.identity.sourceName, "revision" to t.identity.revision,
        "parentId" to t.parentId?.value, "supersedesId" to t.supersedesId?.value,
        "sourceDigest" to t.sourceDigest?.hex, "digest" to t.digest?.hex, "storedName" to t.storedName,
        "attempts" to t.attempts, "lastError" to t.lastError, "attributes" to t.attributes,
        "target" to t.target?.let { mapOf("kind" to it.kind, "location" to it.location, "key" to it.key, "ref" to it.ref, "size" to it.size) },
        "firstSeenAt" to t.firstSeenAt.toString(), "updatedAt" to t.updatedAt.toString(),
        "ackedAt" to t.ackedAt?.toString(), "completedAt" to t.completedAt?.toString(),
    )

    /** Step 2: one read per table through the seam; a table that is not there ends startup naming the DDL. */
    private suspend fun roundTrips() {
        try {
            store.byId(TransferId(0))
            store.outboxPending()
        } catch (e: Exception) {
            throw IllegalStateException("state store round trip failed; apply StateStoreSchema.DDL (spec 8.1) before starting:\n${StateStoreSchema.DDL}", e)
        }
    }

    /** Step 3: a channel adapter; an `http` one resolves its secrets here, so a missing variable ends startup. */
    private fun channelFor(channel: Channel): DeliveryChannel = deliveryChannels[channel.name] ?: when (channel) {
        is HttpChannelConfig -> HttpChannel(channel, httpClient, env)
        // The trigger's pull is a one second long-poll (ticket 16): on the bounded view it would hold a route's whole
        // IO budget at parallelism 1 and every ledger write behind it waited a second (measured by ticket 20, S28). The
        // NATS client owns its own threads, as the connector does for JSch (spec 3.3).
        is NatsChannelConfig -> natsChannels.getOrPut(channel.name) {
            NatsChannel(channel, natsConnections.getOrPut(channel.name) { natsConnection(channel) }, Dispatchers.IO)
        }
    }

    /**
     * Step 3: every store a route touches, before any channel is opened, so a bucket nobody created ends the
     * deployment rather than the first message (SPEC8). A route's target is its adapter's own `probe()`; a
     * subscribed route's `fetch.bucket` is the same HEAD on the fetch store's client, because the store
     * declaration is an endpoint and the bucket is the route's (spec 5.1, rule 6). An SFTP fetch needs none:
     * `connectorFor` starts the connector with its probe.
     */
    private suspend fun probeStores(routeTargets: Collection<ObjectStoreTarget>) {
        routeTargets.distinct().forEach { it.probe() }
        config.routes.flatMap { route ->
            val fetch = route.fetch?.bucket?.let { bucket -> (storeNamed(route.fetch.store) as? S3Store)?.let { it to bucket } }
            listOfNotNull(fetch) + route.divergentExpands()
        }.distinct().forEach { (store, bucket) -> S3Target.headBucket(s3ClientFor(store), bucket, io) }
    }

    /** Step 3: the route's target adapter, one S3 client or one SFTP connector per store, shared with the fetcher. */
    private suspend fun targetFor(route: Route): ObjectStoreTarget {
        val target = checkNotNull(route.target) { "route ${route.name} has no target" }
        targets[target.store]?.let { return it }
        return when (val declared = storeNamed(target.store)) {
            is S3Store -> S3Target(s3ClientFor(declared), checkNotNull(target.bucket) { "route ${route.name}: an S3 target needs a bucket" }, io, clock)
            is SftpStore -> SftpTarget(
                connectorFor(declared).client,
                checkNotNull(target.directory) { "route ${route.name}: an SFTP target needs a directory" },
                io,
            )
        }
    }

    private fun s3ClientFor(store: S3Store): S3Client = s3Clients.getOrPut(store.name) { s3Client(store) }

    /**
     * Spec 10 and ticket 18's note: one connector per SFTP store used as a target or as a subscribed
     * route's `fetch.store`, shared by every route on it and opened here at step 3, so the connector's
     * own start-up and the target's `probe()` both run while a failure is still the deployment's. It
     * carries no `polling` block - no directory, no `onAck` - which is exactly what lets one connector
     * serve every route on the store, the opposite of a poll's constraint (deviation 8 of progress 13).
     * Rule 9's remaining budget is its pool: the sum of `parallelism` over the routes that target or
     * fetch from the store, the polled routes' own connectors having taken `parallelism + 1` each.
     */
    private suspend fun connectorFor(store: SftpStore): SftpConnector = connectors.getOrPut(store.name) {
        val sessions = config.routes.sumOf { route ->
            route.parallelism * listOf(route.fetch?.store, route.target?.store).count { it == store.name }
        }
        SftpConnector.start(
            sftpConnectorConfig(
                store,
                poll = null,
                algorithm = config.digest,
                resolve = ::resolve,
                sessions = maxOf(1, sessions),
                transfers = maxOf(1, sessions),
            ),
            meterRegistry = registry,
            clock = clock,
        )
    }

    /** Step 4 (D17): nothing in staging belongs to anyone at boot. */
    private fun emptyStaging() {
        (config.objectStores.filterIsInstance<SftpStore>().mapNotNull { it.staging?.dir } + config.routes.map { stagingFor(it).dir }).distinct().forEach { dir ->
            Files.list(dir).use { children -> children.forEach { it.toFile().deleteRecursively() } }
        }
    }

    /** Step 5: the chain, every `custom` name resolved now, on the host's bounded IO view (spec 3.3, ticket 34). */
    private fun chainFor(route: Route) = ProcessingChain(
        route.process.map { spec -> processorFor(spec) { beans.processor(it.name) } },
        route.digest ?: config.digest,
        io,
    )

    /**
     * D41's staging directory: where this route's fetches land and what `minFree` defers them against.
     * An SFTP store declares one and rule 11 has already checked it. An S3 store has no such knob - a
     * bucket has no local disk to name - so a route fetching from one stages under the JVM's temp
     * directory in a folder of the store's name: local disk, one per store, and no YAML key for a path
     * nobody would set. It is emptied at boot with the declared ones (D17).
     */
    internal fun stagingFor(route: Route): Staging {
        val store = storeNamed(checkNotNull(route.fetch?.store ?: (route.source as? Source.Poll)?.store) { "route ${route.name} fetches from nowhere" })
        return (store as? SftpStore)?.staging
            ?: Staging(Files.createDirectories(Path.of(System.getProperty("java.io.tmpdir"), "shuttle-staging", store.name)))
    }

    /**
     * Stage 1's bytes: the connector's download for a polled route, through whichever source its current
     * run holds; for a subscribed route the `fetch.store`'s own fetcher, at the path read from the message
     * (spec 5.1). `internal` because there is no way to reach a subscribed route's fetcher through a
     * running host without a broker, and this is the seam that decides it.
     */
    internal suspend fun fetcherFor(route: Route): Fetcher = when (route.source) {
        is Source.Poll -> { path, into, algorithm -> sources.getValue(route.name).fetcher(path, into, algorithm) }
        is Source.Subscribe -> {
            val fetch = checkNotNull(route.fetch) { "route ${route.name} subscribes without a fetch" }
            when (val declared = storeNamed(fetch.store)) {
                is S3Store -> S3Fetcher(
                    s3ClientFor(declared),
                    checkNotNull(fetch.bucket) { "route ${route.name}: a fetch from S3 store ${declared.name} needs a bucket" },
                    io,
                ).fetcher
                is SftpStore -> sftpFetcher(connectorFor(declared).client)
            }
        }
        null -> throw IllegalStateException("route ${route.name} has no source")
    }

    /**
     * Stage 3's other bytes: an `expand` pulls its children from `expand.from`, which may be a store the
     * route does not fetch from, and the pipeline asks this map for one it is not already holding. Rule 14
     * leaves only one shape here - an S3 store with an `expand.bucket` - because no other store offers a
     * fetch by path a route has not fetched through. One client per declaration, shared with every other
     * role on that store, as `fetcherFor` shares it. `internal` for the same reason `fetcherFor` is.
     */
    internal fun fetchersFor(route: Route): Map<String, Fetcher> =
        route.divergentExpands().associate { (store, bucket) -> store.name to S3Fetcher(s3ClientFor(store), bucket, io).fetcher }

    /** The `expand.from` stores of [route] that its own fetcher does not cover, with the bucket each states (rule 14, D53). */
    private fun Route.divergentExpands(): List<Pair<S3Store, String>> =
        process.filterIsInstance<ProcessorSpec.Expand>().filter { it.from != fetch?.store }.distinct()
            .map { (storeNamed(it.from) as S3Store) to checkNotNull(it.bucket) { "route $name: an expand from S3 store ${it.from} needs a bucket" } }

    /** Steps 6 and 7 for one route, run afresh at every supervised start. */
    private fun eventsFor(route: Route): Flow<RouteEvent> = when (val source = route.source) {
        is Source.Poll -> polled(route, source)
        is Source.Subscribe -> natsChannels.getValue(source.channel).events(RouteName(route.name), source)
        null -> throw IllegalStateException("route ${route.name} has no source")
    }.onEach { if (it is RouteEvent.Seen || it is RouteEvent.PollCompleted) lastTrigger[route.name] = clock.instant() }

    /**
     * One connector per polled route (ticket 13's deviation 8, settled here): started, with its probe, when the
     * route starts and closed when its run ends, so a rejected password is one route down and restarted with
     * backoff (S18) rather than a deployment that will not boot, and a second route on the same store keeps
     * working (S23). The pool is sized to the route's share of rule 9's arithmetic: `parallelism` fetches plus
     * one lister, so the routes on one store together never exceed the store's `maxSize`.
     */
    private fun polled(route: Route, poll: Source.Poll): Flow<RouteEvent> = flow {
        val declared = storeNamed(poll.store) as SftpStore
        val connectorConfig = sftpConnectorConfig(
            declared,
            poll,
            route.digest ?: config.digest,
            ::resolve,
            sessions = route.parallelism + 1,
            transfers = route.parallelism,
        )
        val connector = SftpConnector.start(connectorConfig, meterRegistry = registry, clock = clock)
        try {
            val source = SftpPollSource(connector.source, RouteName(route.name), poll, clock)
            sources[route.name] = source
            emitAll(source.events())
        } finally {
            withContext(NonCancellable) { connector.close() }
        }
    }.catch { emit(RouteEvent.RouteDown(it)) }

    private fun storeNamed(name: String) = config.objectStores.first { it.name == name }

    private fun resolve(secret: Secret): String = when (secret) {
        is Secret.Env -> checkNotNull(env(secret.variable)) { "environment variable ${secret.variable} is not set" }
        is Secret.Literal -> secret.value
    }

    companion object {
        private val log: Logger = Logger.getLogger(ShuttleHost::class.java)

        /** Spec 3.3: one bounded view of IO for JDBI, the S3 client and archive writing, sized to the sum of route parallelism. */
        fun ioDispatcher(config: ShuttleConfig): CoroutineDispatcher = Dispatchers.IO.limitedParallelism(maxOf(1, config.routes.sumOf { it.parallelism }))

        /** Spec 12.1 step 1: the files become a configuration or the deployment ends listing every violation. */
        fun load(files: List<Path>, env: Map<String, String>, beans: NamedBeans): ShuttleConfig {
            val config = YamlLoader.load(files.map { Files.readString(it) }, env)
            val report = Rules.validate(config, beans::produces)
            check(report.ok) { "configuration ${files.joinToString()} is invalid:\n" + report.violations.joinToString("\n") { "rule ${it.rule}: ${it.message}" } }
            return config
        }

        fun s3ClientFor(store: S3Store, env: (String) -> String?): S3Client {
            fun Secret.value() = when (this) { is Secret.Env -> checkNotNull(env(variable)) { "environment variable $variable is not set" }; is Secret.Literal -> value }
            val credentials = checkNotNull(store.credentials) { "store ${store.name} has no credentials" }
            return S3Target.client(
                checkNotNull(store.endpoint) { "store ${store.name} has no endpoint" }, store.region, store.pathStyle,
                credentials.accessKey.value(), credentials.secretKey.value(),
                store.timeouts.connect, store.timeouts.socket, store.timeouts.apiCall,
            )
        }

        fun natsConnectionFor(channel: NatsChannelConfig, env: (String) -> String?): Connection {
            val options = Options.builder().server(checkNotNull(channel.url) { "channel ${channel.name} has no url" })
            channel.credentials?.let { secret ->
                val path = when (secret) { is Secret.Env -> checkNotNull(env(secret.variable)) { "environment variable ${secret.variable} is not set" }; is Secret.Literal -> secret.value }
                options.authHandler(Nats.credentials(path))
            }
            return Nats.connect(options.build())
        }
    }
}
