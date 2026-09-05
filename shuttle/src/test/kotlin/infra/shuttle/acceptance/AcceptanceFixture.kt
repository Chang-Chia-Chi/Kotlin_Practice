package infra.shuttle.acceptance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import infra.shuttle.core.HookPoint
import infra.shuttle.core.S3Store
import infra.shuttle.core.ShuttleConfig
import infra.shuttle.core.Transfer
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferState
import infra.shuttle.jdbi.JdbiStateStore
import infra.shuttle.jdbi.StateStoreSchema
import infra.shuttle.quarkus.NamedBeans
import infra.shuttle.quarkus.ShuttleHost
import infra.shuttle.s3.Minio
import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.HookDriver
import io.agroal.api.AgroalDataSource
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier
import io.agroal.api.security.NamePrincipal
import io.agroal.api.security.SimplePassword
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.jdbi.v3.core.ConnectionFactory
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.testcontainers.oracle.OracleContainer
import sftp.connector.testkit.EmbeddedSftpServer
import software.amazon.awssdk.services.s3.S3Client
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText

/**
 * The milestone fixture (spec 18 tier 3), shared by `M1AcceptanceTest` and `M2AcceptanceTest`: the real `ShuttleHost`
 * over the connector's embedded SSHD, the Oracle state store on Testcontainers, MinIO on Testcontainers and a loopback
 * HTTP server, started once per class. Every scenario writes spec 13.1's YAML at test scale, boots a host, and observes
 * only through the containers, the server's directories, the loopback server and the admin read operations. A crash is
 * a host closed while a pipeline is parked at a `HookDriver` point; the restart is a second host over the same
 * containers. Real time throughout, every wait bounded by `withTimeout`; the wall clock the module reads is a
 * `ClockFixture`, advanced by hand where a scenario needs time to pass (backoff, `giveUpAfter`, reconciliation).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AcceptanceFixture {
    protected class Received(val path: String, val body: JsonNode)
    // 90 s+ to report ready on a cold workstation; Testcontainers' default is 60 s
    // (`withStartupTimeoutSeconds` is the JDBC field OracleContainer's own wait strategy ignores)
    private val oracle = OracleContainer("gvenzl/oracle-free:23-slim-faststart").withStartupTimeout(Duration.ofMinutes(10))
    private lateinit var dataSource: AgroalDataSource
    protected lateinit var jdbi: Jdbi
    @Volatile protected var storeDown = false
    protected lateinit var reads: JdbiStateStore

    protected lateinit var root: Path
    protected lateinit var inbox: Path
    protected lateinit var outbound: Path
    protected lateinit var staging: Path
    protected lateinit var files: Path
    protected lateinit var server: EmbeddedSftpServer

    protected lateinit var http: HttpServer
    protected val mapper = ObjectMapper()
    protected val received = CopyOnWriteArrayList<Received>()
    protected val calls = AtomicInteger()
    /** What the loopback server answers the n-th request (1-based) with: a status and a body; tests swap it. */
    @Volatile protected var respond: (Int, String) -> Pair<Int, String> = { n, _ -> 200 to """{"requestId":"r-$n"}""" }
    protected var release = CountDownLatch(1)

    protected val clock = ClockFixture()
    protected lateinit var bucket: String
    protected lateinit var hook: HookDriver
    protected val hosts = mutableListOf<ShuttleHost>()
    protected lateinit var registry: SimpleMeterRegistry
    private var yamls = 0

    protected open val env: Map<String, String> = mapOf(
        "SFTP_USER" to USER, "SFTP_PASSWORD" to PASSWORD, "WRONG" to "not it",
        "S3_ACCESS_KEY" to Minio.user, "S3_SECRET_KEY" to Minio.password, "DOWNSTREAM_TOKEN" to "t0k3n",
    )

    @BeforeAll
    fun startEverything() {
        oracle.start()
        val agroal = AgroalDataSourceConfigurationSupplier()
        agroal.connectionPoolConfiguration().maxSize(16).connectionFactoryConfiguration()
            .jdbcUrl(oracle.jdbcUrl).principal(NamePrincipal(oracle.username)).credential(SimplePassword(oracle.password))
        dataSource = AgroalDataSource.from(agroal)
        // S16's switch: the real store over a datasource that refuses every connection while `storeDown` is set.
        jdbi = Jdbi.create(ConnectionFactory { if (storeDown) throw SQLException("state store unavailable (S16)") else dataSource.connection })
        jdbi.useHandle<Exception> { h -> StateStoreSchema.statements().forEach { h.execute(it) } }
        reads = JdbiStateStore(jdbi, Dispatchers.IO, clock)
        // Prime Oracle's shared SQL area and the connection pool: the first `find`/`seen` on a cold pool takes
        // seconds, which would outrun the poll interval and trip the connector's D40 abandon before a pipeline fetches.
        runBlocking {
            val warm = infra.shuttle.core.SourceIdentity(
                infra.shuttle.core.RouteName("warmup"), infra.shuttle.core.SourceKind.SFTP, "warmup:/", "warm", 0L, START,
            )
            reads.find(warm)
            reads.seen(warm, infra.shuttle.core.TransferKind.OBJECT)
            reads.transfers(); reads.outbox()
        }
        jdbi.useHandle<Exception> { h -> h.execute("DELETE FROM file_transfer") }

        root = Files.createTempDirectory("m1-acceptance")
        inbox = root.resolve("inbox")
        outbound = root.resolve("outbound")
        staging = Files.createDirectories(root.resolve("staging"))
        files = Files.createDirectories(root.resolve("config"))
        server = EmbeddedSftpServer.start(root, USER, PASSWORD)

        http = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { exchange ->
                val body = exchange.requestBody.readAllBytes()
                received += Received(exchange.requestURI.path, mapper.readTree(body))
                try {
                    val (status, answer) = respond(calls.incrementAndGet(), exchange.requestURI.path)
                    exchange.answer(status, answer)
                } finally {
                    exchange.close()
                }
            }
            start()
        }
    }

    @AfterAll
    fun stopEverything() {
        http.stop(0)
        server.close()
        dataSource.close()
        oracle.stop()
        root.toFile().deleteRecursively()
    }

    @BeforeEach
    fun freshScenario() {
        jdbi.useHandle<Exception> { h -> h.execute("DELETE FROM delivery_outbox"); h.execute("DELETE FROM file_transfer") }
        listOf(inbox, outbound).forEach { it.toFile().deleteRecursively(); it.createDirectories() }
        bucket = Minio.versionedBucket()
        received.clear()
        calls.set(0)
        respond = { n, _ -> 200 to """{"requestId":"r-$n"}""" }
        release = CountDownLatch(1)
        storeDown = false
        clock.set(START)
        hook = HookDriver()
    }

    @AfterEach
    fun closeHosts() {
        release.countDown()
        hosts.forEach { it.close() }
        hosts.clear()
    }

    // ---- spec 13.1 at test scale ----

    protected fun yaml(routes: String, channels: String = "", stores: String = "", readiness: String = "all-routes-down") =
        "shuttle:\n" +
            "  drainTimeout: 5s\n" +
            "  notifier: { workers: 2, batch: 10, sweepEvery: 200ms }\n" +
            "  supervision: { restartBackoff: { initial: 200ms, max: 15m }, readiness: $readiness }\n" +
            "  digest: md5\n" +
            "  objectStores:\n" +
            sftpStore("vendor", "\${SFTP_PASSWORD}") + stores +
            "    minio:\n" +
            "      s3:\n" +
            "        endpoint: ${Minio.url}\n" +
            "        region: us-east-1\n" +
            "        pathStyle: true\n" +
            "        credentials: { accessKey: \${S3_ACCESS_KEY}, secretKey: \${S3_SECRET_KEY} }\n" +
            "        timeouts: { connect: 5s, socket: 30s, apiCall: 4s }\n" +
            (if (channels.isEmpty()) "" else "  channels:\n$channels") +
            "  routes:\n" + routes

    protected fun sftpStore(name: String, password: String): String {
        val dir = Files.createDirectories(staging.resolve(name)) // rule 11: each SFTP store owns its staging dir
        return "    $name:\n" +
            "      sftp:\n" +
            "        host: ${server.host}\n" +
            "        port: ${server.port}\n" +
            "        auth: { user: \${SFTP_USER}, password: $password }\n" +
            "        hostKey: acceptAll\n" +
            "        pool: { maxSize: 8, maxConcurrentTransfers: 6 }\n" +
            "        drainTimeout: 1s\n" +
            "        cancelGrace: 500ms\n" +
            "        staging: { dir: ${dir.toString().replace('\\', '/')} }\n"
    }

    protected fun downstream(
        name: String = "downstream",
        path: String = "/api/files",
        policy: String = "{ maxAttempts: 50, giveUpAfter: 24h, backoff: { initial: 1s, max: 1s } }",
        rows: String = BODY,
    ) = "    $name:\n" +
        "      http:\n" +
        "        method: POST\n" +
        "        url: http://127.0.0.1:${http.address.port}$path\n" +
        "        auth: { bearer: \${DOWNSTREAM_TOKEN} }\n" +
        "        timeout: 2s\n" +
        "        response: { success: [200-299], retry: [408, 429, 500-599], reference: /requestId }\n" +
        "        policy: $policy\n" +
        "        body:\n" + rows

    protected fun load(text: String, beans: NamedBeans = NamedBeans.none): ShuttleConfig {
        val file = files.resolve("shuttle-${++yamls}.yaml")
        file.writeText(text)
        return ShuttleHost.load(listOf(file), env, beans)
    }

    /** The host over the real adapters: JDBI on the Oracle container, the S3 client built by the host from the YAML, started. */
    protected fun boot(text: String, beans: NamedBeans = NamedBeans.none, s3: (S3Store) -> S3Client = { ShuttleHost.s3ClientFor(it, env::get) }): ShuttleHost {
        val config = load(text, beans)
        registry = SimpleMeterRegistry()
        val io = ShuttleHost.ioDispatcher(config)
        val store = JdbiStateStore(jdbi, io, clock)
        return ShuttleHost(config, env::get, beans, store, registry, clock, s3Client = s3, hook = hook, io = io)
            .also { hosts += it; it.start() }
    }

    /** The process dies: the host closed with a pipeline parked at [point]; the row is what the ledger held at that moment. */
    protected suspend fun crash(host: ShuttleHost, point: HookPoint): TransferId {
        val id = withTimeout(TIMEOUT) { hook.awaitArrival(point) }
        host.close()
        hosts.remove(host)
        hook = HookDriver() // the recovery host runs freely: a fresh hook with nothing armed
        return id
    }

    protected suspend fun await(what: String, timeoutMillis: Long = TIMEOUT, condition: suspend () -> Boolean) {
        withTimeoutOrNull(timeoutMillis) { while (!condition()) delay(25) } ?: fail("timed out waiting for $what")
    }

    protected suspend fun awaitState(state: TransferState, timeoutMillis: Long = TIMEOUT): Transfer {
        await("a transfer in $state", timeoutMillis) { reads.transfers().any { it.state == state } }
        return reads.transfers().first { it.state == state }
    }

    protected fun seed(dir: Path, name: String, content: String = CONTENT): Path = dir.resolve(name).also { it.writeText(content) }

    protected fun counter(name: String, vararg tags: String) = registry.counter(name, *tags).count()

    protected fun HttpExchange.answer(status: Int, body: String) {
        val bytes = body.encodeToByteArray()
        sendResponseHeaders(status, bytes.size.toLong())
        responseBody.write(bytes)
    }
    /** Boot a host from a route block plus an optional channels block, wrapped in spec 13.1's shell. */
    protected fun bootR(routes: String, channels: String = "", beans: NamedBeans = NamedBeans.none) = boot(yaml(routes, channels), beans)

    /** Advances the module's clock while [block] runs, so delivery retries and reconciliation windows elapse without sleeping the poll. */
    protected suspend fun withClockTicking(block: suspend () -> Unit) {
        val scope = CoroutineScope(Dispatchers.Default)
        val ticker = scope.launch {
            while (isActive) { clock.advance(kotlin.time.Duration.parse("2s")); delay(120) }
        }
        try { block() } finally { ticker.cancelAndJoin() }
    }

    protected fun md5Hex(bytes: ByteArray) =
        java.security.MessageDigest.getInstance("MD5").digest(bytes).joinToString("") { "%02x".format(it) }

    protected companion object {
        const val USER = "etl"
        const val PASSWORD = "s3cret"
        const val CONTENT = "id,amount\n1,42\n"
        const val TIMEOUT = 60_000L
        val START: java.time.Instant = java.time.Instant.parse("2026-01-01T00:00:00Z")

        /** Spec 13.1's `downstream` body; a message parent cannot fill every row of it (see M2). */
        const val BODY =
            "          - { path: fileId,          field: TRANSFER_ID }\n" +
                "          - { path: file.name,       field: STORED_NAME }\n" +
                "          - { path: file.size,       field: TARGET_SIZE }\n" +
                "          - { path: file.md5,        field: DIGEST }\n" +
                "          - { path: location.bucket, field: TARGET_LOCATION }\n" +
                "          - { path: location.key,    field: TARGET_KEY }\n" +
                "          - { path: receivedAt,      field: SOURCE_MTIME, format: ISO_INSTANT }\n" +
                "          - { path: event,           field: EVENT }\n" +
                "          - { path: source,          value: vendor-drop }\n"
    }
}
