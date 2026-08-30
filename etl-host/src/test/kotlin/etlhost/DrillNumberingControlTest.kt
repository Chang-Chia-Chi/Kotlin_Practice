package etlhost

import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import org.junit.jupiter.api.Test

/** Control for drill 4: the same host with NO leftover files, so the baseline number is measured. */
@QuarkusTest
@WithTestResource(HostFixture::class)
class DrillNumberingControlTest {

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `what generation number does a clean first refresh produce`() {
        val body = given().get("/admin/etl/snapshot/${HostFixture.GROUP}")
            .then().statusCode(200).extract().body().asString()
        println("=== DRILL 4 CONTROL clean-boot snapshot ===")
        println(body)
        println("=== end ===")
    }
}
