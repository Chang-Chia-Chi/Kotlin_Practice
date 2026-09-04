package infra.shuttle.quarkus

import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.StateStore
import infra.shuttle.core.TransferState
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import jakarta.enterprise.inject.Produces
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import sftp.connector.testkit.EmbeddedSftpServer
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText

/**
 * The embedded SSHD and the YAML the host boots from, made before Quarkus reads its configuration: the server's
 * port is the operating system's choice and the host starts inside boot. Paths are written with forward slashes
 * because SmallRye Config's list converter reads a backslash as an escape.
 */
class HostResource : QuarkusTestResourceLifecycleManager {
    override fun start(): Map<String, String> {
        root = Files.createTempDirectory("shuttle-quarkus")
        root.resolve("drop").createDirectories()
        val staging = Files.createDirectory(root.resolve("staging"))
        server = EmbeddedSftpServer.start(root, USER, PASSWORD)
        val yaml = root.resolve("shuttle.yaml")
        yaml.writeText(
            "shuttle:\n" +
                "  drainTimeout: 5s\n" +
                "  supervision: { restartBackoff: { initial: 200ms, max: 15m }, readiness: all-routes-down }\n" +
                "  objectStores:\n" +
                "    vendor:\n" +
                "      sftp: { host: ${server.host}, port: ${server.port}, auth: { user: \${SFTP_USER}, password: \${SFTP_PASSWORD} }, drainTimeout: 1s, cancelGrace: 500ms, staging: { dir: $staging } }\n" +
                "    minio:\n" +
                "      s3: { endpoint: http://127.0.0.1:1, credentials: { accessKey: \${S3_KEY}, secretKey: \${S3_SECRET} }, timeouts: { apiCall: 1s } }\n" +
                "  routes:\n" +
                "    mirror:\n" +
                "      source: { poll: { store: vendor, directory: /drop, every: 200ms, readiness: [ { sizeStable: { checks: 1, interval: 1ms } } ], onAck: delete } }\n" +
                "      target: { store: minio, bucket: landing }\n",
        )
        return mapOf(
            "shuttle.config" to yaml.toString().replace('\\', '/'),
            // What the YAML's `${VAR}` references resolve from: config overrides are what a test has instead of an environment.
            "SFTP_USER" to USER, "SFTP_PASSWORD" to PASSWORD, "S3_KEY" to "k", "S3_SECRET" to "s",
        )
    }

    override fun stop() {
        server.close()
    }

    companion object {
        const val USER = "etl"
        const val PASSWORD = "s3cret"
        lateinit var root: Path
        lateinit var server: EmbeddedSftpServer
    }
}

/** The test kit's store and target as beans, which `ShuttleLifecycle` picks over the datasource and the S3 client. */
@Singleton
class TestKitBeans {
    @Produces @Singleton fun store(clock: Clock): StateStore = InMemoryStateStore(clock)
    @Produces @Singleton fun reads(store: StateStore): StoreReads = (store as InMemoryStateStore).let { StoreReads({ it.transfers }, { it.outbox }) }
    @Produces @Singleton @Named("minio") fun target(): ObjectStoreTarget = InMemoryTarget("landing")
}

/** Spec 14.1 and 14.2 over HTTP: the seven endpoints under the role, readiness at the conventional path, the meters in the scrape. */
@QuarkusTest
@WithTestResource(HostResource::class)
class ShuttleQuarkusTest {
    @Inject lateinit var lifecycle: ShuttleLifecycle
    @Inject lateinit var store: StateStore
    @Inject lateinit var registry: MeterRegistry

    private fun await(what: String, condition: () -> Boolean) = runBlocking {
        withTimeoutOrNull(30_000) { while (!condition()) delay(20) } ?: fail("timed out waiting for $what")
    }

    @Test
    fun readiness_at_the_conventional_path_is_UP_once_the_route_is_up_and_the_meters_are_in_the_scrape() {
        await("the route up") { lifecycle.ready() }
        // Agroal registers its own datasource check beside this one; the host's is found by name.
        val ready = given().get("/q/health/ready").then().statusCode(200).extract()
        assertEquals("UP", ready.path<String>("status"))
        assertTrue("shuttle-routes" in ready.path<List<String>>("checks.name"), "the host's check is named in ${ready.asString()}")
        assertNotNull(registry.find("shuttle_route_up").tag("route", "mirror").gauge(), "the supervisor's gauge is on the host registry")
        val scrape = given().get("/q/metrics").then().statusCode(200).extract().asString()
        assertTrue("shuttle_route_up" in scrape, "the gauge is in the scrape")
    }

    @Test
    @TestSecurity(user = "ops", roles = ["shuttle-admin"])
    fun every_admin_endpoint_answers_under_the_role_and_changes_what_it_says() {
        await("the route up") { lifecycle.ready() }
        HostResource.root.resolve("drop/one.csv").writeText("a,b\n")
        val memory = store as InMemoryStateStore
        await("the file to finish") { memory.transfers.any { it.state == TransferState.DONE } }
        val id = memory.transfers.first().id

        val routes = given().get("/admin/shuttle/routes").then().statusCode(200).extract()
        assertEquals("mirror", routes.path<String>("[0].name"))
        assertEquals(true, routes.path<Boolean>("[0].up"))
        assertEquals(1, routes.path<Int>("[0].counts.DONE"))
        val transfers = given().get("/admin/shuttle/transfers?route=mirror&state=done&limit=5").then().statusCode(200).extract()
        assertEquals(id.value.toInt(), transfers.path<Int>("[0].id"))
        assertEquals("DONE", transfers.path<String>("[0].state"))
        assertEquals(0, given().get("/admin/shuttle/transfers/${id.value}/deliveries").then().statusCode(200).extract().path<Int>("size()"))
        given().get("/admin/shuttle/transfers/99/deliveries").then().statusCode(404)

        given().post("/admin/shuttle/transfers/${id.value}/redrive").then().statusCode(409)
        runBlocking { memory.rejected(id, "by hand") }
        given().post("/admin/shuttle/transfers/${id.value}/redrive").then().statusCode(200)
        assertEquals(TransferState.SEEN, memory.transfer(id).state)

        given().post("/admin/shuttle/transfers/${id.value}/ack").then().statusCode(409)
        given().post("/admin/shuttle/transfers/99/ack").then().statusCode(404)
        given().post("/admin/shuttle/deliveries/99/redrive").then().statusCode(404)

        given().post("/admin/shuttle/routes/mirror/restart").then().statusCode(200)
        given().post("/admin/shuttle/routes/nobody/restart").then().statusCode(404)
        await("the restart to be counted") { (registry.find("shuttle_route_restarts_total").tag("route", "mirror").counter()?.count() ?: 0.0) >= 1.0 }
        await("the route back up") { lifecycle.ready() }
    }

    @Test
    fun an_anonymous_caller_is_refused_on_every_endpoint() {
        given().get("/admin/shuttle/routes").then().statusCode(401)
        given().get("/admin/shuttle/transfers").then().statusCode(401)
        given().get("/admin/shuttle/transfers/1/deliveries").then().statusCode(401)
        given().post("/admin/shuttle/transfers/1/redrive").then().statusCode(401)
        given().post("/admin/shuttle/transfers/1/ack").then().statusCode(401)
        given().post("/admin/shuttle/deliveries/1/redrive").then().statusCode(401)
        given().post("/admin/shuttle/routes/mirror/restart").then().statusCode(401)
    }

    @Test
    @TestSecurity(user = "someone", roles = ["reader"])
    fun a_caller_without_the_admin_role_is_refused() {
        given().get("/admin/shuttle/routes").then().statusCode(403)
        given().post("/admin/shuttle/routes/mirror/restart").then().statusCode(403)
    }
}
