package etlhost

import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.notNullValue
import org.junit.jupiter.api.Test

/**
 * **SimpleEtl spec 8.6's auth row, exercised the way production exercises it** - a Basic credential
 * on the wire, an identity provider that checks it, and `@RolesAllowed` deciding against the
 * `SecurityIdentity` that came back.
 *
 * Every other security assertion in this module carries `@TestSecurity`, which synthesises an
 * identity and never goes near a provider. That is the right tool for asserting the *mapping* - and
 * it is exactly why eight phases of green suite sat on top of a host that configured no identity
 * provider at all and answered **403 to everyone, forever**, on a real boot. A `@TestSecurity` test
 * cannot fail for that reason, so this class does not use it.
 *
 * The three cases are the three answers the mechanism has to be able to give, and only the first
 * of them is reachable at all without a provider:
 *
 * - a credential the provider knows, carrying `etl-admin` -> the run is accepted;
 * - a credential the provider rejects -> **401**, a challenge the caller can answer;
 * - a credential the provider accepts, carrying some other role -> **403**, a refusal it cannot.
 *
 * The passwords are `application.properties`' placeholders, read from the same config the
 * application reads, so a deployment that overrides `ETL_ADMIN_PASSWORD` does not break this test -
 * and a deployment that swaps the whole mechanism for OIDC is expected to replace this class.
 */
@QuarkusTest
@WithTestResource(HostFixture::class)
class RealCredentialTest {

    @Inject
    lateinit var listener: RecordingListener

    @Test
    fun `a real credential carrying etl-admin triggers a run`() {
        val ended = listener.latch(HostFixture.TASK)
        given().auth().preemptive().basic(ADMIN, PASSWORD)
            .post("/admin/etl/tasks/${HostFixture.TASK}/runs")
            .then()
            .statusCode(202)
            .body("runId", notNullValue())

        // Awaited rather than left in flight: the next test class to trigger this task would
        // otherwise get 409 from a run this one started.
        assertThat(ended.await(60, TimeUnit.SECONDS)).isTrue()
    }

    @Test
    fun `the same user with the wrong password is 401, not 403`() {
        given().auth().preemptive().basic(ADMIN, "not-the-password")
            .post("/admin/etl/tasks/${HostFixture.TASK}/runs")
            .then().statusCode(401)
    }

    @Test
    fun `an unknown user is 401`() {
        given().auth().preemptive().basic("nobody", PASSWORD)
            .get("/admin/etl/tasks")
            .then().statusCode(401)
    }

    /**
     * The case that separates "authentication is wired" from "authorisation is wired". This caller
     * is who they say they are and still may not trigger anything - which is what `@RolesAllowed`
     * is for, and what a host with no provider can never demonstrate because nobody ever gets far
     * enough to be refused for the right reason.
     */
    @Test
    fun `a real credential without the etl-admin role is 403`() {
        given().auth().preemptive().basic(READER, PASSWORD)
            .post("/admin/etl/tasks/${HostFixture.TASK}/runs")
            .then().statusCode(403)

        given().auth().preemptive().basic(READER, PASSWORD)
            .get("/admin/etl/tasks")
            .then().statusCode(403)
    }

    /** Readiness stays outside the mechanism entirely - a kubelet carries no credential. */
    @Test
    fun `both readiness paths answer without any credential at all`() {
        given().get("/health/ready").then().statusCode(200).body("state", equalTo(EtlHost.READY))
        given().get("/q/health/ready").then().statusCode(200).body("status", equalTo("UP"))
    }

    private companion object {
        const val ADMIN = "etl-admin"
        const val READER = "etl-reader"
        const val PASSWORD = "placeholder-change-me"
    }
}
