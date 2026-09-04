package infra.shuttle.quarkus

import infra.shuttle.core.Digest
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.S3Store
import infra.shuttle.core.ShuttleConfig
import infra.shuttle.core.TransferState
import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.InMemoryStateStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.fail
import org.mockito.Mockito
import sftp.connector.testkit.EmbeddedSftpServer
import software.amazon.awssdk.services.s3.S3Client
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.writeText

/**
 * The three seams ticket 14 left open for 17 and 18, wired: an SFTP target, a subscribed route's
 * fetcher and the staging directory of a route that fetches from S3. Plain JUnit over the
 * connector's embedded SSHD, real time, every wait bounded - `runTest` cannot drive the connector,
 * whose timeouts are real (ticket 18).
 */
class ShuttleHostM2WiringTest {

    @TempDir lateinit var remoteRoot: Path
    @TempDir lateinit var vendorStaging: Path
    @TempDir lateinit var partnerStaging: Path
    @TempDir lateinit var files: Path

    private lateinit var server: EmbeddedSftpServer
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val registry = SimpleMeterRegistry()
    private val env = mapOf("SFTP_USER" to USER, "SFTP_PASSWORD" to PASSWORD, "S3_KEY" to "k", "S3_SECRET" to "s")
    private val hosts = mutableListOf<ShuttleHost>()

    @BeforeEach
    fun startServer() {
        remoteRoot.resolve("drop").createDirectories()
        remoteRoot.resolve("landing").createDirectories()
        server = EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD)
    }

    @AfterEach
    fun stop() {
        hosts.forEach { it.close() }
        server.close()
    }

    @Test
    fun a_route_targeting_an_SFTP_store_lands_the_polled_file_on_the_partner_and_the_row_reaches_DONE() = runBlocking {
        val host = host(config(yaml(mirrorToPartner)))

        host.start()
        seed("first.csv")

        await("the row to finish") { store.transfers.singleOrNull()?.state == TransferState.DONE }
        assertEquals(CONTENT, Files.readString(remoteRoot.resolve("landing").resolve("first.csv")))
        assertEquals(listOf("first.csv"), namesUnder("landing"), "one copy, and no partial file left behind")
    }

    /**
     * Spec 5.1's other half: a subscribed route fetches through its `fetch.store` at the path read
     * from the message. Nothing here can carry a message - that needs NATS - so the seam is proven
     * where it is built: the fetcher the route's runner is handed, over a store nothing polls.
     */
    @Test
    fun the_fetcher_of_a_subscribed_route_whose_fetch_store_is_SFTP_downloads_the_path_by_stat_and_download() = runBlocking {
        seed("first.csv")
        val config = config(yaml(imagesFetchingFrom("vendor"), channels = EVENTS))
        val host = host(config)

        val staged = withTimeout(TIMEOUT) {
            host.fetcherFor(config.routes.single())("/drop/first.csv", files.resolve("staged"), DigestAlgorithm.MD5)
        }

        assertEquals("first.csv", staged.name)
        assertEquals(15L, staged.size)
        assertEquals(Digest(DigestAlgorithm.MD5, "9ede8f0bbcd1302e2b0b86693491acba"), staged.digest)
        assertEquals(CONTENT, Files.readString(staged.path))
    }

    /**
     * The other `fetch.store`: ticket 11's fetcher over the store's own client, built without asking
     * the bucket anything - a boot must not depend on the object store being reachable at step 7 - and
     * staging for it is the JVM's temp directory, because an S3 store declares no local disk.
     */
    @Test
    fun the_fetcher_of_a_route_fetching_from_S3_is_built_from_the_stores_client_without_calling_it() = runBlocking {
        val s3 = Mockito.mock(S3Client::class.java)
        val config = config(yaml(imagesFetchingFrom("minio", bucket = "bucket: images, "), channels = EVENTS))
        val host = host(config, s3Client = { s3 })

        val staging = host.stagingFor(config.routes.single()).dir

        assertNotNull(host.fetcherFor(config.routes.single()))
        Mockito.verifyNoInteractions(s3)
        assertTrue(Files.isDirectory(staging), "$staging is a directory a fetch can stage into")
        assertEquals(Path.of(System.getProperty("java.io.tmpdir"), "shuttle-staging", "minio"), staging)
    }

    /** Spec 12.3: the target connector is the host's, so shutdown leaves nothing of it on the partner. */
    @Test
    fun close_closes_the_target_connector_and_the_partner_is_left_with_no_session() = runBlocking {
        val host = host(config(yaml(mirrorToPartner)))
        host.start()
        seed("first.csv")
        await("the row to finish") { store.transfers.singleOrNull()?.state == TransferState.DONE }

        host.close()

        await("every session to be gone from the partner") { server.liveSessions == 0 }
    }

    // ---- the fixture: spec 13.1's shape at test scale ----

    /** A poll on `vendor` whose target is the partner SFTP store's `/landing` (spec 7.3). */
    private val mirrorToPartner =
        "    mirror:\n" +
            "      source: { poll: { store: vendor, directory: /drop, every: 200ms," +
            " readiness: [ { sizeStable: { checks: 1, interval: 1ms } } ], onAck: delete } }\n" +
            "      target: { store: partner, directory: /landing }\n"

    /** Spec 13.1's image-sets shape: a subscribe trigger, a fetch by path, a target on the partner. */
    private fun imagesFetchingFrom(store: String, bucket: String = "") =
        "    images:\n" +
            "      source: { subscribe: { channel: events, subject: images.ready, onAck: ack } }\n" +
            "      fetch: { store: $store, ${bucket}path: /metadata.path }\n" +
            "      target: { store: partner, directory: /landing }\n"

    private fun yaml(routes: String, channels: String = "") =
        "shuttle:\n" +
            "  drainTimeout: 5s\n" +
            "  notifier: { workers: 2, batch: 10, sweepEvery: 200ms }\n" +
            "  supervision: { restartBackoff: { initial: 200ms, max: 15m }, readiness: all-routes-down }\n" +
            "  objectStores:\n" +
            sftpStore("vendor", vendorStaging) + sftpStore("partner", partnerStaging) +
            "    minio:\n" +
            "      s3: { endpoint: http://127.0.0.1:1, credentials: { accessKey: \${S3_KEY}, secretKey: \${S3_SECRET} }," +
            " timeouts: { apiCall: 1s } }\n" +
            (if (channels.isEmpty()) "" else "  channels:\n$channels") +
            "  routes:\n" + routes

    private fun sftpStore(name: String, staging: Path) =
        "    $name:\n" +
            "      sftp:\n" +
            "        host: ${server.host}\n" +
            "        port: ${server.port}\n" +
            "        auth: { user: \${SFTP_USER}, password: \${SFTP_PASSWORD} }\n" +
            "        pool: { maxSize: 8 }\n" +
            "        drainTimeout: 1s\n" +
            "        cancelGrace: 500ms\n" +
            "        staging: { dir: $staging }\n"

    private fun config(text: String): ShuttleConfig {
        val file = files.resolve("shuttle.yaml")
        Files.writeString(file, text)
        return ShuttleHost.load(listOf(file), env, NamedBeans.none)
    }

    private fun host(config: ShuttleConfig, s3Client: (S3Store) -> S3Client = { ShuttleHost.s3ClientFor(it, env::get) }) =
        ShuttleHost(config, env::get, NamedBeans.none, store, StoreReads({ store.transfers }, { store.outbox }), registry, clock, s3Client = s3Client)
            .also { hosts += it }

    private fun seed(name: String) = remoteRoot.resolve("drop").resolve(name).also { it.writeText(CONTENT) }

    private fun namesUnder(directory: String) = remoteRoot.resolve(directory).listDirectoryEntries().map { it.name }.sorted()

    private suspend fun await(what: String, condition: () -> Boolean) {
        withTimeoutOrNull(TIMEOUT) { while (!condition()) delay(20) } ?: fail("timed out waiting for $what")
    }

    private companion object {
        const val USER = "shuttle"
        const val PASSWORD = "s3cret"
        const val CONTENT = "id,amount\n1,42\n"
        const val TIMEOUT = 20_000L
        const val EVENTS = "    events:\n      nats: { url: nats://127.0.0.1:1 }\n"
    }
}
