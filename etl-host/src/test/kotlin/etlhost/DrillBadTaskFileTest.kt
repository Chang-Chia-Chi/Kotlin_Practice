package etlhost

import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Day-2 drill 1: an operator pushes a broken task file and reloads.
 *
 * Its own fixture subclass, so it gets its own Quarkus instance and its own task directory - the
 * shared instance's directory is read by `AdminResourceTest`, which asserts a task count.
 */
class BadFileFixture : HostFixture() {
    override fun start(): Map<String, String> = super.start().also { taskDirectory = Path.of(it.getValue(TASK_DIR_KEY)) }

    companion object {
        const val TASK_DIR_KEY = "etl-host.etl.task-directory"

        /** Set by [start] so the test can push a file into the directory the host is actually reading. */
        lateinit var taskDirectory: Path
    }
}

@QuarkusTest
@WithTestResource(BadFileFixture::class)
class DrillBadTaskFileTest {

    /**
     * Three defects in one file, because the question is not "is it 400" but "can an operator fix
     * the file from the response": an unknown step type, an unknown cache name, and an unknown
     * datasource. A report that names one of three sends an operator round the loop three times.
     */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `a broken task file is 400, and the report names the file, the step and the cause`() {
        val broken = BadFileFixture.taskDirectory.resolve("broken.yaml")
        Files.writeString(
            broken,
            """
            name: broken
            schedule:
              cron: "0 0 * * * ?"
            phases:
              - name: load
                steps:
                  - name: bad-cache
                    type: cacheCopy
                    cache: no-such-group
                    sql: select id from no-such-group
                    output: x
                  - name: bad-datasource
                    type: materialize
                    datasource: no-such-datasource
                    output: y
                    sql: select 1
            """.trimIndent(),
        )
        try {
            val body = given().post("/admin/etl/reload").then().statusCode(400).extract().body().asString()
            println("=== DRILL 1 reload 400 body ===\n$body\n=== end ===")

            assertThat(body).contains("broken.yaml")
            assertThat(body).contains("no-such-group")
            assertThat(body).contains("no-such-datasource")
            // The step name is what an operator greps the file for.
            assertThat(body).contains("bad-cache")
            assertThat(body).contains("bad-datasource")
        } finally {
            Files.deleteIfExists(broken)
        }
    }

    /** Spec 8.5's atomicity, from the operator's side: a rejected reload leaves the old list serving. */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `a rejected reload changes nothing`() {
        val broken = BadFileFixture.taskDirectory.resolve("broken2.yaml")
        Files.writeString(broken, "name: broken2\nphases: []\n")
        try {
            given().post("/admin/etl/reload").then().statusCode(400)
            given().get("/admin/etl/tasks").then().statusCode(200).body("size()", org.hamcrest.Matchers.equalTo(2))
        } finally {
            Files.deleteIfExists(broken)
        }
    }

    /** A file that is not YAML at all - the parser's error, not the validator's. */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `an unparseable file is also 400 and names itself`() {
        val broken = BadFileFixture.taskDirectory.resolve("garbage.yaml")
        Files.writeString(broken, "name: [unclosed\n  : : :\n")
        try {
            val body = given().post("/admin/etl/reload").then().statusCode(400).extract().body().asString()
            println("=== DRILL 1b unparseable body ===\n$body\n=== end ===")
            assertThat(body).contains("garbage.yaml")
        } finally {
            Files.deleteIfExists(broken)
        }
    }
}
