package etlhost

import infra.snapshotcache.api.GroupId
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.hasItem
import org.junit.jupiter.api.Test

/**
 * The extension measurement: a **second cache group and a second task consuming it**, end to end.
 *
 * The group itself is one line of `application.properties` - its store directory, its
 * `CacheBinding` and the cache name a task file may reference are all derived from that one map,
 * which is spec 8.6's two "by construction" rows doing their job. What that line does *not* create
 * is the source table behind it (added to both fixtures) or the task file that reads it (below).
 *
 * The task file lives here rather than in [HostFixture] deliberately: adding it there would make
 * the shared instance report three tasks, and two earlier tests assert a task count of two. The
 * house rule against modifying an earlier phase's tests is what puts this file in its own fixture,
 * and it is worth noticing that a count assertion is what a third task collides with.
 */
class SecondGroupFixture : HostFixture() {

    override fun start(): Map<String, String> {
        val overrides = super.start()
        Files.writeString(
            Path.of(overrides.getValue("etl-host.etl.task-directory")).resolve("$TASK_2.yaml"),
            """
            name: $TASK_2
            schedule:
              cron: "0 30 * * * ?"
            phases:
              - name: load
                steps:
                  - name: copy-equipment
                    type: cacheCopy
                    cache: $GROUP_2
                    sql: select id, tool_id, state from $GROUP_2
                    output: equipment_cache
                  - name: summarise
                    type: materialize
                    datasource: scratch
                    output: uptime
                    sql: select state, count(*) as tools from equipment_cache group by state
                  - name: publish
                    type: pipe
                    source:
                      datasource: scratch
                      sql: select state, tools from uptime
                    target:
                      datasource: report
                      table: equipment_state
            """.trimIndent(),
        )
        return overrides
    }
}

@QuarkusTest
@WithTestResource(SecondGroupFixture::class)
class SecondGroupTest {

    @Inject
    lateinit var managed: ManagedSnapshotCache

    @Inject
    lateinit var config: HostConfig

    @Inject
    lateinit var listener: RecordingListener

    /** Both groups refresh at startup, and readiness waits for both - not for whichever is first. */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `both groups publish a generation and the host is ready`() {
        given().get("/health/ready").then().statusCode(200).body("state", equalTo("ready"))

        listOf(HostFixture.GROUP to HostFixture.ROWS - 1, HostFixture.GROUP_2 to HostFixture.TOOLS - 1)
            .forEach { (group, rows) ->
                given().get("/admin/etl/snapshot/$group")
                    .then().statusCode(200)
                    .body("current.rowCounts.$group", equalTo(rows))
            }

        // Spec 5.4's derived store directory: one per group, which is what makes generation
        // numbering restarting at 1 per group safe.
        assertThat(config.storagePath.resolve(HostFixture.GROUP_2)).isDirectory()
    }

    /** The task the group exists for: cache -> scratch -> target, and the rows really land. */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `the second task runs off the second group and its rows land in the target`() {
        given().get("/admin/etl/tasks").then().statusCode(200).body("name", hasItem(HostFixture.TASK_2))

        val ended = listener.latch(HostFixture.TASK_2)
        given().post("/admin/etl/tasks/${HostFixture.TASK_2}/runs").then().statusCode(202)
        assertThat(ended.await(60, TimeUnit.SECONDS))
            .withFailMessage("the second task never finished")
            .isTrue()

        assertThat(uptimeRows())
            .withFailMessage("no rows reached report.equipment_state, so the second group is wired but dead")
            .containsExactly("DOWN" to 20L, "UP" to 20L)
    }

    /** The two groups are independent: the second task's copy pins only its own generation. */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `neither group is left pinned by the other groups task`() {
        listOf(HostFixture.GROUP, HostFixture.GROUP_2).forEach { group ->
            assertThat(managed.admin.liveGenerations(GroupId(group)))
                .allSatisfy { assertThat(it.refCount).isZero() }
        }
    }

    private fun uptimeRows(): List<Pair<String, Long>> =
        HostFixture.connect(config.targetUrl).use { report ->
            report.createStatement().use { st ->
                st.executeQuery("select state, tools from equipment_state order by state").use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1) to rs.getLong(2)) }
                }
            }
        }
}
