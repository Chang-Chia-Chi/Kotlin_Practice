package infra.shuttle.quarkus

import infra.shuttle.core.DeliveryState
import infra.shuttle.core.Hook
import infra.shuttle.core.HookPoint
import infra.shuttle.core.ShuttleConfig
import infra.shuttle.core.ShuttleMetrics
import infra.shuttle.core.TransferState
import infra.shuttle.jdbi.JdbiStateStore
import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.HookDriver
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import sftp.connector.testkit.EmbeddedSftpServer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import com.sun.net.httpserver.HttpServer
import infra.shuttle.core.DeliveryId
import infra.shuttle.core.TransferId
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import kotlin.io.path.createDirectories
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText

/**
 * Spec 12.1 and 12.3 through the real host over the connector's embedded SSHD, the in-memory state store and
 * target, and a loopback HTTP server: real time, every wait bounded by `withTimeout`, no sleep.
 */
class ShuttleHostTest {
    @TempDir lateinit var remoteRoot: Path
    @TempDir lateinit var staging: Path
    @TempDir lateinit var files: Path
    @TempDir lateinit var badStaging: Path

    private lateinit var server: EmbeddedSftpServer
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val target = InMemoryTarget("landing")
    private val registry = SimpleMeterRegistry()
    private val env = mapOf("SFTP_USER" to USER, "SFTP_PASSWORD" to PASSWORD, "WRONG" to "not it", "S3_KEY" to "k", "S3_SECRET" to "s")
    private val hosts = mutableListOf<ShuttleHost>()
    private var http: HttpServer? = null
    private val release = CountDownLatch(1)

    @BeforeEach
    fun startServer() {
        remoteRoot.resolve("drop").createDirectories()
        server = EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD)
    }

    @AfterEach
    fun stop() {
        hosts.forEach { it.close() }
        release.countDown()
        http?.stop(0)
        server.close()
    }

    /** Spec 13.1's shape with every timeout shrunk to test scale; [routes], [channels] and [stores] are pre-indented blocks. */
    private fun yaml(routes: String, channels: String = "", stores: String = "", readiness: String = "all-routes-down", backoff: String = "200ms") =
        "shuttle:\n" +
            "  drainTimeout: 5s\n" +
            "  notifier: { workers: 2, batch: 10, sweepEvery: 200ms }\n" +
            "  supervision: { restartBackoff: { initial: $backoff, max: 15m }, readiness: $readiness }\n" +
            "  objectStores:\n" +
            sftpStore("vendor", "\${SFTP_PASSWORD}", staging) + stores +
            "    minio:\n" +
            "      s3: { endpoint: http://127.0.0.1:1, credentials: { accessKey: \${S3_KEY}, secretKey: \${S3_SECRET} }, timeouts: { apiCall: 1s } }\n" +
            (if (channels.isEmpty()) "" else "  channels:\n$channels") +
            "  routes:\n" + routes

    private fun sftpStore(name: String, password: String, dir: Path) =
        "    $name:\n" +
            "      sftp:\n" +
            "        host: ${server.host}\n" +
            "        port: ${server.port}\n" +
            "        auth: { user: \${SFTP_USER}, password: $password }\n" +
            "        pool: { maxSize: 8 }\n" +
            "        drainTimeout: 1s\n" +
            "        cancelGrace: 500ms\n" +
            "        staging: { dir: $dir }\n"

    private fun route(name: String = "mirror", store: String = "vendor", onAck: String = "delete", notify: String = "") =
        "    $name:\n" +
            "      source: { poll: { store: $store, directory: /drop, every: 200ms, readiness: [ { sizeStable: { checks: 1, interval: 1ms } } ], onAck: $onAck } }\n" +
            "      target: { store: minio, bucket: landing }\n" + notify

    private fun config(text: String): ShuttleConfig {
        val file = files.resolve("shuttle.yaml")
        Files.writeString(file, text)
        return ShuttleHost.load(listOf(file), env, NamedBeans.none)
    }

    private fun host(config: ShuttleConfig, registry: SimpleMeterRegistry = this.registry, hook: Hook = Hook.None) =
        ShuttleHost(config, env::get, NamedBeans.none, store, StoreReads({ store.transfers }, { store.outbox }), registry, clock, targets = mapOf("minio" to target), hook = hook)
            .also { hosts += it }

    private suspend fun await(what: String, timeoutMillis: Long = TIMEOUT, condition: () -> Boolean) {
        withTimeoutOrNull(timeoutMillis) { while (!condition()) delay(20) } ?: fail("timed out waiting for $what")
    }

    private fun restarts(route: String, registry: SimpleMeterRegistry = this.registry) = registry.counter(ShuttleMetrics.ROUTE_RESTARTS, "route", route).count()

    private fun seed(name: String) = remoteRoot.resolve("drop").resolve(name).also { it.writeText("id,amount\n1,42\n") }

    /** A loopback server that records each request and never answers it until [release] is counted down, which the teardown does. */
    private fun stalledServer(): Pair<Int, CountDownLatch> {
        val received = CountDownLatch(1)
        http = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { exchange -> received.countDown(); release.await(); exchange.close() }
            start()
        }
        return http!!.address.port to received
    }

    private fun downstream(port: Int) =
        "    downstream:\n      http: { url: http://127.0.0.1:$port/api, timeout: 4s, response: { success: [200-299], retry: [500-599] } }\n"

    @Test
    fun the_host_boots_with_one_mirror_route_and_becomes_ready() = runBlocking {
        val host = host(config(yaml(route())))

        host.start()

        await("the route up") { host.ready() }
    }

    @Test
    fun I12_close_returns_within_drainTimeout_with_a_delivery_parked_and_PENDING_rows_stay_PENDING() = runBlocking {
        val (port, received) = stalledServer()
        val host = host(config(yaml(route(notify = "      notify: [ { on: acked, channel: downstream } ]\n"), channels = downstream(port))))
        host.start()
        seed("first.csv")
        await("the delivery to be in flight at the stalled server") { received.count == 0L }
        assertEquals(DeliveryState.PENDING, store.outbox.single().state)

        val started = System.nanoTime()
        host.close()
        val elapsedMillis = (System.nanoTime() - started) / 1_000_000

        assertTrue(elapsedMillis < 5_000, "close took $elapsedMillis ms, drainTimeout is 5 s")
        assertEquals(DeliveryState.PENDING, store.outbox.single().state, "the parked delivery was neither delivered nor failed")
        assertEquals(TransferState.ACKED, store.transfers.single().state)
        assertFalse(host.ready(), "readiness went false first")
    }

    @Test
    fun S15_shutdown_during_store_leaves_the_row_PROCESSED_and_staging_is_empty_at_the_next_start() = runBlocking {
        val hook = HookDriver().apply { pauseAt(HookPoint.afterProcess) }
        val host = host(config(yaml(route())), hook = hook)
        host.start()
        seed("first.csv")
        withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterProcess) }
        assertEquals(TransferState.PROCESSED, store.transfers.single().state)

        host.close()

        assertEquals(TransferState.PROCESSED, store.transfers.single().state, "shutdown inside the store stage writes nothing")
        assertTrue(target.keys.isEmpty(), "nothing stored")
        staging.resolve("stray").writeText("left over")
        val next = host(config(yaml(route())))
        next.start()
        assertEquals(emptyList<Path>(), staging.listDirectoryEntries(), "D17: staging emptied at boot")
        await("the row to finish on the next start") { store.transfers.single().state == TransferState.DONE }
    }

    @Test
    fun S18_a_wrong_password_leaves_the_route_down_and_restarted_with_backoff_and_the_process_alive() = runBlocking {
        val host = host(config(yaml(route(store = "bad"), stores = sftpStore("bad", "\${WRONG}", badStaging))))

        host.start()

        // The second restart is counted the instant the second run dies, at the start of its wait: the gauge is 0 there.
        await("supervised restarts") { restarts("mirror") >= 2.0 }
        assertFalse(host.ready(), "all-routes-down with the only route down")
        assertTrue(server.authAttempts >= 2, "each restart tried the password again")
        host.close()
    }

    @Test
    fun readiness_follows_the_configured_rule_with_one_route_up_and_one_down() = runBlocking {
        val routes = route() + route(name = "dead", store = "bad")
        val bad = sftpStore("bad", "\${WRONG}", badStaging)
        val allRoutesDown = host(config(yaml(routes, stores = bad, backoff = "5s")))
        allRoutesDown.start()
        await("mirror up") { allRoutesDown.ready() }
        await("dead route down") { restarts("mirror" /* not restarted */) == 0.0 && restarts("dead") >= 1.0 }
        assertTrue(allRoutesDown.ready(), "all-routes-down: a partially healthy pod keeps serving")
        allRoutesDown.close()

        val other = SimpleMeterRegistry()
        val anyRouteDown = host(config(yaml(routes, stores = bad, readiness = "any-route-down", backoff = "5s")), registry = other)
        anyRouteDown.start()
        await("dead route in its backoff wait") { restarts("dead", other) >= 1.0 }
        assertFalse(anyRouteDown.ready(), "any-route-down: one dead route makes the pod unready")
    }

    @Test
    fun a_boot_with_a_missing_table_fails_naming_the_DDL() {
        val jdbi = Jdbi.create("jdbc:h2:mem:shuttle-${System.nanoTime()};DB_CLOSE_DELAY=-1")
        val empty = JdbiStateStore(jdbi, Dispatchers.IO, clock)
        val host = ShuttleHost(config(yaml(route())), env::get, NamedBeans.none, empty, StoreReads({ empty.transfers() }, { empty.outbox() }), registry, clock, targets = mapOf("minio" to target))

        val failure = assertThrows(IllegalStateException::class.java) { host.start() }

        assertTrue(failure.message!!.contains("StateStoreSchema.DDL"), failure.message)
        assertTrue(failure.message!!.contains("CREATE TABLE file_transfer"), "the DDL itself is in the message")
    }

    @Test
    fun a_boot_with_a_missing_bucket_fails_naming_the_bucket() {
        val s3 = Mockito.mock(S3Client::class.java)
        Mockito.`when`(s3.headBucket(any(HeadBucketRequest::class.java))).thenThrow(NoSuchBucketException.builder().message("no").build())
        val host = ShuttleHost(config(yaml(route())), env::get, NamedBeans.none, store, StoreReads({ store.transfers }, { store.outbox }), registry, clock, s3Client = { s3 })

        val failure = assertThrows(IllegalStateException::class.java) { host.start() }

        assertTrue(failure.message!!.contains("bucket landing"), failure.message)
        Mockito.verify(s3, Mockito.never()).putObject(any(software.amazon.awssdk.services.s3.model.PutObjectRequest::class.java), any(software.amazon.awssdk.core.sync.RequestBody::class.java))
    }

    @Test
    fun S24_rule_9_ends_startup_naming_the_rule() {
        val failure = assertThrows(IllegalStateException::class.java) {
            config(yaml("    mirror:\n      source: { poll: { store: vendor, directory: /drop, every: 1h, onAck: delete } }\n      target: { store: minio, bucket: landing }\n      parallelism: 9\n"))
        }
        assertTrue(failure.message!!.contains("rule 9:"), failure.message)
    }

    @Test
    fun the_admin_operations_change_exactly_what_spec_14_1_says() = runBlocking {
        val hook = HookDriver().apply { pauseAt(HookPoint.afterLedgerStored) }
        val (port, received) = stalledServer()
        val host = host(config(yaml(route(notify = "      notify: [ { on: acked, channel: downstream } ]\n"), channels = downstream(port))), hook = hook)
        host.start()
        seed("first.csv")
        val id = withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterLedgerStored) }
        assertEquals(TransferState.STORED, store.transfer(id).state)

        // routes: up, a trigger seen, no restarts, one STORED row
        val routes = host.routes().single()
        assertEquals("mirror", routes["name"])
        assertEquals(true, routes["up"])
        assertEquals(mapOf("STORED" to 1), routes["counts"])
        assertEquals(0L, routes["restarts"])

        // manual ack: STORED to ACKED with the route's acked delivery, then the notifier is woken
        assertEquals(ShuttleHost.Outcome.WRONG_STATE, host.redrive(id), "a STORED row is not re-drivable")
        assertEquals(ShuttleHost.Outcome.DONE, host.ack(id))
        assertEquals(TransferState.ACKED, store.transfer(id).state)
        val delivery = store.outbox.single()
        assertEquals(ShuttleHost.Outcome.WRONG_STATE, host.ack(id), "acked twice is refused")

        // the delivery parks in the stalled server, so the row is the operator's alone: fail it by hand and re-drive it
        await("the delivery to be in flight") { received.count == 0L }
        assertEquals(ShuttleHost.Outcome.WRONG_STATE, host.redriveDelivery(delivery.id), "a PENDING delivery is not re-drivable")
        store.deliveryFailed(delivery.id, "400", "operator test")
        assertEquals(ShuttleHost.Outcome.DONE, host.redriveDelivery(delivery.id))
        assertEquals(DeliveryState.PENDING, store.outbox.single().state)
        assertEquals(ShuttleHost.Outcome.NOT_FOUND, host.redriveDelivery(DeliveryId(99)))

        // listing: one parent row with its deliveries
        val listed = host.transfers(route = "mirror", state = null, limit = 10).single()
        assertEquals(id.value, listed["id"])
        assertEquals(emptyList<Any>(), listed["children"])
        assertEquals(emptyList<Any>(), host.transfers(route = "other", state = null, limit = 10))
        assertEquals("acked", host.deliveries(id)!!.single()["event"])
        assertEquals(null, host.deliveries(TransferId(99)))

        // redrive: only from REJECTED or FAILED, back to SEEN
        store.rejected(id, "by hand")
        assertEquals(ShuttleHost.Outcome.DONE, host.redrive(id))
        assertEquals(TransferState.SEEN, store.transfer(id).state)
        assertEquals(ShuttleHost.Outcome.NOT_FOUND, host.redrive(TransferId(99)))

        // restart: the route's run is cut short and counted
        assertEquals(true, host.restart("mirror"))
        assertEquals(false, host.restart("nobody"))
        await("the restart to be counted") { restarts("mirror") == 1.0 }
        await("the route back up") { host.ready() }
    }

    private companion object {
        const val USER = "etl"
        const val PASSWORD = "s3cret"
        const val TIMEOUT = 30_000L
    }
}
