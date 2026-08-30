package etlhost

import io.quarkus.runtime.ShutdownEvent
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.everyItem
import org.junit.jupiter.api.Test

/**
 * Its own Quarkus instance, because this test ends the host. A shutdown run inside the shared
 * instance would take every later test class down with it.
 */
class IsolatedShutdown : QuarkusTestProfile {
    override fun getConfigOverrides() = mapOf("etl-host.cache.serving-memory-limit" to "512MB")
}

/**
 * **`composed-host-example`'s M3, moved from a nine-line probe into a real HTTP surface** - and the
 * one ordering that cannot lie.
 *
 * `TriggerResult.AlreadyRunning` answers both "that task is running" and "this wiring is closed and
 * nothing will ever run again". SimpleEtl spec 11.2 declined a fifth sealed case on one claim: *the
 * host does not need the framework to tell them apart, because the host is the one that called
 * `close()`*. This is that claim executed against a resource an operator can actually curl.
 *
 * The flag goes up **before** `wired.close()`, never after. Reversed, there is a window in which a
 * cancelled runner refuses a trigger while readiness still says "ready" and the trigger still says
 * "409, retry later" - the one wrong answer of the four, given to a caller that will never be
 * served.
 *
 * One method rather than several, because the assertions *are* a sequence and JUnit does not
 * promise method order.
 */
@QuarkusTest
@WithTestResource(HostFixture::class)
@TestProfile(IsolatedShutdown::class)
class ShutdownSequenceTest {

    @Inject
    lateinit var host: EtlHost

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `shutdown turns busy into gone, and a closed wiring is not a bad-file error`() {
        given().get("/health/ready").then().statusCode(200).body("state", equalTo("ready"))

        host.onStop(ShutdownEvent())

        // Readiness first, so the load balancer stops sending work before the 503s start.
        given().get("/health/ready").then().statusCode(503).body("state", equalTo("shutting-down"))

        // The framework's answer is unchanged - AlreadyRunning - and the host's flag is what turns
        // it into "gone, retry elsewhere" instead of "busy, retry later".
        given().post("/admin/etl/tasks/${HostFixture.TASK}/runs").then().statusCode(503)

        /*
         * The case a depth sweep caught, and the one a naive mapping gets wrong in the worst place.
         * `close` is terminal, so `TaskAdmin.reload` comes back with a ValidationReport too - one
         * error, `file` reading `<wiring>` rather than a task file. Rendered as 400 that tells an
         * operator, mid-shutdown, that their YAML is badly authored: they go and read files that
         * are fine while the pod disappears underneath them.
         */
        given().post("/admin/etl/reload")
            .then()
            .statusCode(503)
            .body("state", equalTo("shutting-down"))

        /*
         * And the listing still answers, which is the whole reason `close` leaves the definitions in
         * place (spec 8.6's last row). The pre-E16 workaround - reloading an empty directory -
         * blanked this view at exactly the moment an operator is watching a shutdown.
         */
        given().get("/admin/etl/tasks")
            .then()
            .statusCode(200)
            .body("scheduled", everyItem(equalTo(false)))
    }
}
