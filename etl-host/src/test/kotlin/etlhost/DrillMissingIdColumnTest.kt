package etlhost

import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import jakarta.inject.Inject
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.Test

/**
 * The same host, with one character of configuration changed: the group's SQL no longer projects
 * `id`. `VerifyConfig.keyUnique` defaults to true, so the gate fails and nothing ever publishes.
 *
 * This is the failure both specs single out as the one whose cause and symptom sit two systems
 * apart (snapshotcache spec 5.4's `openSnapshotCache` KDoc, and `HostConfig.groupSql`). Drills 3
 * and 5 are the same boot because they are the same incident: the gate fails, no generation
 * publishes, and the first task to fire spends the whole wait budget before failing.
 */
class NoIdColumnFixture : HostFixture() {
    override fun start(): Map<String, String> =
        super.start() + ("etl-host.cache.sql.${GROUP}" to "select lot_id, qty, site from lot")
}

@QuarkusTest
@WithTestResource(NoIdColumnFixture::class)
class DrillMissingIdColumnTest {

    @Inject
    lateinit var managed: ManagedSnapshotCache

    @Inject
    lateinit var registry: PrometheusMeterRegistry

    @Inject
    lateinit var listener: RecordingListener

    /**
     * Drill 3. Four places an operator looks, in the order they look: readiness, the refresh
     * outcome the startup log line renders, the metric, and the admin endpoint.
     */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `the verify gate failure is visible at readiness, in the refresh detail, and on the scrape`() {
        // 1. The probe. This is the first thing that says anything is wrong.
        given().get("/health/ready").then().statusCode(503).body("state", equalTo("awaiting-first-generation"))

        // 2. The refresh outcome - the value EtlHost renders into "startup refresh of group wip: ...".
        val outcome = managed.admin.triggerRefresh(GroupId(HostFixture.GROUP))
        println("=== DRILL 3 RefreshOutcome ===\nresult=${outcome.result}\ndetail=${outcome.detail}\n=== end ===")
        assertThat(outcome.result).isNotEqualTo(RefreshResult.SUCCESS)
        assertThat(outcome.detail)
            .withFailMessage(
                "the refresh detail is what the host's one startup log line renders; if it does not " +
                    "name the failing rule, the operator's only lead is a 503. detail was: %s",
                outcome.detail,
            )
            .isNotNull()

        // 3. The metric. Spec 12's verify counter carries the failing rule as a label.
        val verify = registry.scrape().lines().filter { it.startsWith("snapshot_verify_failed_total{") }
        println("=== DRILL 3 verify series ===\n${verify.joinToString("\n")}\n=== end ===")
        assertThat(verify)
            .withFailMessage("no snapshot_verify_failed_total series; the gate failure is invisible to a scrape")
            .isNotEmpty()

        // 4. The admin endpoint, which is where an operator goes after the probe.
        given().get("/admin/etl/snapshot/${HostFixture.GROUP}")
            .then().statusCode(200).body("current", nullValue())
    }

    /**
     * Drill 5. A task triggered before any generation exists.
     *
     * SimpleEtl spec 8.6's note on 3.6 makes two claims a host has to size for: the step pins its
     * dispatcher for the *whole* wait budget, and it fails with `TIMEOUT` rather than `NOT_READY`.
     * Both are measured here against the production default budget (PT30S), not a shortened one -
     * the duration is the point.
     */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `a task fired before the first generation burns the whole wait budget and fails TIMEOUT`() {
        val ended = listener.latch(HostFixture.TASK)
        val started = System.nanoTime()
        val runId = given().post("/admin/etl/tasks/${HostFixture.TASK}/runs")
            .then().statusCode(202).extract().path<String>("runId")

        assertThat(ended.await(90, TimeUnit.SECONDS))
            .withFailMessage("the run never ended within 90s of a 30s wait budget")
            .isTrue()
        val elapsedMs = (System.nanoTime() - started) / 1_000_000

        val body = given().get("/admin/etl/tasks/${HostFixture.TASK}/runs/$runId")
            .then().statusCode(200).extract().body().asString()
        println("=== DRILL 5 elapsed=${elapsedMs}ms run body ===\n$body\n=== end ===")

        assertThat(body.lowercase()).contains("failed")
        // The wait budget was actually spent, not short-circuited.
        assertThat(elapsedMs)
            .withFailMessage("expected the full 30s budget to be spent, was %d ms", elapsedMs)
            .isGreaterThan(25_000)

        // The reason an alert or runbook has to match on. NOT_READY is only what a zero budget
        // produces, and the framework never passes zero.
        assertThat(body.lowercase())
            .withFailMessage(
                "the operator-facing run record does not carry the word 'timeout'; spec 8.6's note " +
                    "on 3.6 says this is the reason a runbook must match. Body was:%n%s",
                body,
            )
            .contains("timeout")

        // And the same fact on the scrape, under the cache's own label.
        val unavailable = registry.scrape().lines().filter { it.startsWith("snapshot_acquire_unavailable_total{") }
        println("=== DRILL 5 acquire_unavailable series ===\n${unavailable.joinToString("\n")}\n=== end ===")
        assertThat(unavailable.joinToString("\n")).contains("""reason="timeout"""")
    }
}

