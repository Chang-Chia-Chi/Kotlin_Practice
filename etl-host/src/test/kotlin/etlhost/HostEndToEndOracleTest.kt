package etlhost

import infra.snapshotcache.api.GroupId
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import java.nio.file.Path
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.oracle.OracleContainer

/**
 * The same host, with its DuckDB source swapped for a real Oracle. Nothing else changes - not the
 * SQL behind the group, not `JdbcGenerationSource`, not the task file, not a single producer.
 */
class OracleSource : HostFixture() {

    private lateinit var container: OracleContainer

    override fun startSource(root: Path): Map<String, String> {
        container = OracleContainer("gvenzl/oracle-free:slim-faststart")
        container.start()
        DriverManager.getConnection(container.jdbcUrl, container.username, container.password).use { oracle ->
            oracle.createStatement().use { st ->
                // NUMBER(18) declared rather than left to an expression: an uncast expression
                // reports precision 0 and AUTO DDL rejects it at writer open (SimpleEtl spec 4.4).
                st.execute(
                    "create table lot (id number(18), lot_id varchar2(40), qty number(18,3), site varchar2(8))",
                )
                st.execute(
                    "insert into lot select level, 'L' || level, level * 1.5, " +
                        "case when mod(level, 2) = 0 then 'F12' else 'F11' end " +
                        "from dual connect by level <= ${ROWS - 1}",
                )
                // No explicit commit: DriverManager hands back an auto-commit connection and
                // Oracle's driver throws rather than no-ops if you commit one.
            }
        }
        return mapOf(
            "etl-host.source.url" to container.jdbcUrl,
            "etl-host.source.username" to container.username,
            "etl-host.source.password" to container.password,
        )
    }

    override fun stop() {
        if (this::container.isInitialized) container.stop()
    }
}

class OracleProfile : QuarkusTestProfile {
    override fun getConfigOverrides() = mapOf("etl-host.cache.serving-memory-limit" to "768MB")
}

/**
 * **The whole chain, once, on the real thing**: Oracle -> `GenerationSource` -> a verified,
 * published generation -> a shape-D task triggered over HTTP -> rows in the target -> the
 * generation reclaimed.
 *
 * The last link is the point. SimpleEtl spec 8.6 called it **"not testable in this repository"**,
 * and it was right for the reason it gave: reclamation lives in `DefaultSnapshotCache`, which is
 * `internal` to the cache module, so SimpleEtl's own tests use a double implementing the public
 * interface and a double cannot leak a generation. The claim was never about difficulty - it was
 * about there being no module that owned a real cache *and* ran a real task. This one does.
 *
 * A step that held or referenced a generation past its copy would stall refreshing until the live
 * count reached K and spec 6.1 paused it, and no green suite anywhere would have said so.
 *
 * Excluded by default (`@Tag("oracle")`), the same convention SimpleEtl and snapshotcache use for
 * their Testcontainers classes. `mvn -pl etl-host test -Dgroups=oracle`.
 */
@QuarkusTest
@QuarkusTestResource(OracleSource::class)
@TestProfile(OracleProfile::class)
@Tag("oracle")
class HostEndToEndOracleTest {

    @Inject
    lateinit var managed: ManagedSnapshotCache

    @Inject
    lateinit var config: HostConfig

    @Inject
    lateinit var listener: RecordingListener

    private val group = GroupId(HostFixture.GROUP)

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `oracle to generation to task to target, and the generation is then reclaimable`() {
        // 1. The startup refresh read Oracle, the verify gate passed it, and readiness flipped.
        given().get("/health/ready").then().statusCode(200).body("state", equalTo("ready"))
        val first = requireNotNull(managed.cache.currentInfo(group)) { "no generation published" }
        assertThat(first.rowCounts[HostFixture.GROUP]).isEqualTo((HostFixture.ROWS - 1).toLong())

        // 2. A shape-D run, triggered the way an operator triggers one.
        val ended = listener.latch(HostFixture.TASK)
        given().post("/admin/etl/tasks/${HostFixture.TASK}/runs").then().statusCode(202)
        assertThat(ended.await(120, TimeUnit.SECONDS)).isTrue()

        // 3. The rows crossed both frameworks: Oracle -> generation -> scratch -> target.
        assertThat(summaryRows()).containsExactly("F11" to 250L, "F12" to 250L)

        // 4. **The row spec 8.6 called untestable.** The lease is released the instant `copyOut`
        //    returns - not at the end of the run, not at the end of the phase - so by now the
        //    generation the copy read is pinned by nobody.
        val held = managed.admin.liveGenerations(group).single { it.generation == first.generation }
        assertThat(held.refCount)
            .withFailMessage(
                "generation %d still has refCount %d after the run finished. A step that holds or " +
                    "references a generation stalls refreshing until the live count reaches K.",
                first.generation, held.refCount,
            )
            .isZero()
        assertThat(held.leases).isEmpty()

        // 5. A successor publishes, the old generation stops being current, and GC takes it.
        managed.admin.triggerRefresh(group)
        val second = requireNotNull(managed.cache.currentInfo(group))
        assertThat(second.generation).isGreaterThan(first.generation)
        managed.admin.gc(group)

        assertThat(managed.admin.liveGenerations(group).map { it.generation })
            .doesNotContain(first.generation)
        assertThat(generationFile(first.generation))
            .withFailMessage("the reclaimed generation's file is still on disk - DETACH or delete did not happen")
            .doesNotExist()
    }

    /** One generation = one file (D1), so reclamation is observable as a missing file. */
    private fun generationFile(generation: Long): Path = config.storagePath
        .resolve(HostFixture.GROUP)
        .resolve("gen_%010d.db".format(generation))

    private fun summaryRows(): List<Pair<String, Long>> =
        HostFixture.connect(config.targetUrl).use { report ->
            report.createStatement().use { st ->
                st.executeQuery("select site, lots from wip_summary order by site").use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1) to rs.getLong(2)) }
                }
            }
        }
}
