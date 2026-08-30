package etlhost

import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.hasItem
import org.hamcrest.Matchers.notNullValue
import org.junit.jupiter.api.Test

/**
 * **SimpleEtl spec 8.6, rows 3 and 4**: the HTTP mapping and the role check, which the framework
 * deliberately does not own.
 *
 * `TriggerResult` is sealed so that this mapping is exhaustive, and all four cases are asserted
 * here against a running host rather than against a double: 202 accepted, 409 busy, 404 unknown,
 * 400 disabled. `TaskAdmin` authorises nothing and records the identity it is handed, so the role
 * check exists only if the host wrote it - which is why the deny half matters as much as the allow.
 */
@QuarkusTest
@WithTestResource(HostFixture::class)
class AdminResourceTest {

    @Inject
    lateinit var listener: RecordingListener

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `the task listing reports both task files`() {
        given().get("/admin/etl/tasks")
            .then()
            .statusCode(200)
            .body("name", hasItem(HostFixture.TASK))
            .body("name", hasItem(HostFixture.DISABLED_TASK))
    }

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `triggering a known task is 202 with a runId, and a second trigger while it runs is 409`() {
        val release = listener.holdNextRunOf(HostFixture.TASK)
        val ended = listener.latch(HostFixture.TASK)
        try {
            given().post("/admin/etl/tasks/${HostFixture.TASK}/runs")
                .then().statusCode(202).body("runId", notNullValue())

            // Deterministic, not a race: TaskRunner claims its AtomicBoolean on the triggering
            // thread inside submit, so "already running" is true the instant 202 was written.
            given().post("/admin/etl/tasks/${HostFixture.TASK}/runs")
                .then().statusCode(409)
        } finally {
            release.countDown()
        }
        assertThat(ended.await(60, TimeUnit.SECONDS)).isTrue()
    }

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `triggering an unknown task is 404`() {
        given().post("/admin/etl/tasks/no-such-task/runs").then().statusCode(404)
    }

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `triggering a disabled task is 400`() {
        given().post("/admin/etl/tasks/${HostFixture.DISABLED_TASK}/runs").then().statusCode(400)
    }

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `reload re-reads the task directory`() {
        given().post("/admin/etl/reload").then().statusCode(200).body("tasks", equalTo(2))
    }

    /** snapshotcache spec 12.7, and the readiness proof that a generation published at startup. */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `the snapshot endpoint reports the current generation and its row counts`() {
        given().get("/admin/etl/snapshot/${HostFixture.GROUP}")
            .then()
            .statusCode(200)
            .body("current.generation", notNullValue())
            .body("current.rowCounts.${HostFixture.GROUP}", equalTo(HostFixture.ROWS - 1))
    }

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `an unknown group is 404 rather than an empty state`() {
        given().get("/admin/etl/snapshot/nope").then().statusCode(404)
    }

    /**
     * The deny half of row 4. **Without this the row is untested**: every assertion above runs as a
     * caller who already has the role, and would pass just as green on a resource carrying no
     * annotation at all - which is the row's symptom, "an unauthenticated caller can trigger any
     * task", exactly.
     */
    @Test
    fun `an anonymous caller is refused, on every endpoint`() {
        given().get("/admin/etl/tasks").then().statusCode(401)
        given().post("/admin/etl/tasks/${HostFixture.TASK}/runs").then().statusCode(401)
        given().post("/admin/etl/reload").then().statusCode(401)
        given().get("/admin/etl/snapshot/${HostFixture.GROUP}").then().statusCode(401)
    }

    /** Authenticated but not an operator: 403, not 401, and still not a run. */
    @Test
    @TestSecurity(user = "someone", roles = ["reader"])
    fun `a caller without the etl-admin role is refused`() {
        given().post("/admin/etl/tasks/${HostFixture.TASK}/runs").then().statusCode(403)
    }

    /**
     * Readiness is the one endpoint outside the role check, because a kubelet carries no bearer
     * token: behind `@RolesAllowed` it answers 401 forever and the pod never joins the service.
     */
    @Test
    fun `readiness answers ready without a role, once a generation has published`() {
        given().get("/health/ready").then().statusCode(200).body("state", equalTo("ready"))
    }
}
