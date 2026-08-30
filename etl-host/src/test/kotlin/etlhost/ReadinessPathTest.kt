package etlhost

import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import java.nio.file.Path
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Test

/**
 * The same host against a source whose tables exist and are **empty**.
 *
 * `verify.nonEmpty` is always on (snapshotcache spec 8.2, D13 - publishing an empty dataset is the
 * most expensive failure there is), so the startup refresh reads a table, fails the gate, and
 * nothing publishes. That is the shape this test needs: a host that boots not-ready and can be made
 * ready afterwards, in one instance, without a restart.
 */
class EmptySourceFixture : HostFixture() {

    override fun startSource(root: Path): Map<String, String> {
        val url = url(root, "empty-source.db")
        connect(url).use { source ->
            source.createStatement().use { st ->
                st.execute("CREATE TABLE lot (id BIGINT, lot_id VARCHAR, qty DECIMAL(18,3), site VARCHAR)")
                st.execute("CREATE TABLE equipment (id BIGINT, tool_id VARCHAR, state VARCHAR)")
            }
        }
        return mapOf("etl-host.source.url" to url)
    }
}

/**
 * **SimpleEtl spec 8.6's readiness row, at the path a manifest actually probes.**
 *
 * `/health/ready` is this host's own resource and has always worked. `/q/health/ready` is where a
 * stock Quarkus deployment manifest points its `readinessProbe`, and before `quarkus-smallrye-health`
 * was on the classpath it answered **404** - so the probe failed forever, the pod never joined the
 * service, and the application behind it was running perfectly the whole time. No test in this
 * module could see that, because no test probed the conventional path. This one does.
 *
 * One method rather than three, because the assertions *are* a sequence: DOWN, then UP, then DOWN
 * again, in one instance. The second transition is the one a stored flag could never make - readiness
 * was written once at the end of startup and never again, so a host whose first refresh failed
 * stayed not-ready through every later tick that published perfectly well. [EtlHost.readinessState]
 * asks the cache instead, and this is that change measured rather than argued.
 *
 * Its own Quarkus instance, forced by its own test resource, because it ends by shutting the host
 * down.
 */
@QuarkusTest
@WithTestResource(EmptySourceFixture::class)
class ReadinessPathTest {

    @Inject
    lateinit var managed: ManagedSnapshotCache

    @Inject
    lateinit var config: HostConfig

    @Inject
    lateinit var host: EtlHost

    @Test
    fun `the conventional path is DOWN before the first generation, UP after it, and DOWN at shutdown`() {
        // 1. Booted, serving, and correctly not ready. Both paths agree, and the SmallRye body
        //    carries the same word the hand-rolled one does, so an operator reading either learns
        //    *which* not-ready state this is.
        given().get("/q/health/ready")
            .then()
            .statusCode(503)
            .body("status", equalTo("DOWN"))
            .body("checks[0].name", equalTo("snapshot-cache"))
            .body("checks[0].data.state", equalTo("awaiting-first-generation"))
        given().get("/health/ready")
            .then().statusCode(503).body("state", equalTo("awaiting-first-generation"))

        // 2. The source gets its rows and a later refresh publishes - the tick's job, driven here
        //    directly because that Quarkus fires an @Scheduled method is Quarkus's property.
        seedSource()
        host.groups.forEach { managed.admin.triggerRefresh(it) }

        given().get("/q/health/ready")
            .then()
            .statusCode(200)
            .body("status", equalTo("UP"))
            .body("checks[0].data.state", equalTo(EtlHost.READY))
        given().get("/health/ready").then().statusCode(200).body("state", equalTo(EtlHost.READY))

        // 3. And down again the moment shutdown begins, before close() runs - so the probe pulls
        //    the pod out of the service before the 409s and 503s start.
        host.onStop(ShutdownEvent())

        given().get("/q/health/ready")
            .then()
            .statusCode(503)
            .body("status", equalTo("DOWN"))
            .body("checks[0].data.state", equalTo("shutting-down"))
    }

    private fun seedSource() = HostFixture.connect(config.sourceUrl).use { source ->
        source.createStatement().use { st ->
            st.execute("INSERT INTO lot SELECT i, 'L' || i, i * 1.5, 'F11' FROM range(1, ${HostFixture.ROWS}) t(i)")
            st.execute("INSERT INTO equipment SELECT i, 'T' || i, 'UP' FROM range(1, ${HostFixture.TOOLS}) t(i)")
        }
    }
}
