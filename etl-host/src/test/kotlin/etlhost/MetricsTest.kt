package etlhost

import io.micrometer.prometheus.PrometheusMeterRegistry
import io.quarkus.test.common.QuarkusTestResource
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The two metric bindings, read the way an operator reads them: off a Prometheus scrape.
 *
 * A `MeterRegistry` assertion would pass on a registry nothing exports. The scrape text is the
 * contract - both specs fix the metric *names*, and a name is only real once something renders it.
 */
@QuarkusTest
@QuarkusTestResource(HostFixture::class)
class MetricsTest {

    @Inject
    lateinit var registry: PrometheusMeterRegistry

    /**
     * **SimpleEtl spec 8.6's seeding row, finally exercised by a real registry in a real host.**
     *
     * A task that has never run registers no meter, so `etl_task_runs_total{outcome="succeeded"}
     * == 0` matches *nothing* - the alert does not fire and does not error, which is the normal
     * state of every task after every deploy. `seed` pre-registers the four series a task name
     * determines (`TriggerSource` and `Outcome` are both closed two-valued enums).
     *
     * `archive-old` is used rather than `wip-summary` on purpose: it is `enabled: false`, so no test
     * in this module can have run it, and its series exist only because something seeded them.
     *
     * **This host contains no `seed` call site.** `EtlWiring` takes `onTasksLoaded` and the
     * framework invokes it after the initial load and after every reload, so the assertion below is
     * about the framework doing it - which is exactly what today's deepening of that seam was for.
     */
    @Test
    fun `a task that has never run already has its four zero series`() {
        val series = scrape().lines().filter {
            it.startsWith("etl_task_runs_total{") && it.contains("""task="${HostFixture.DISABLED_TASK}"""")
        }

        assertThat(series)
            .withFailMessage(
                "expected 4 seeded series for a never-run task, got:%n%s%n%nAn absent series is the " +
                    "silent failure spec 9.3 describes: a counter alert written against it matches " +
                    "nothing at all rather than reporting zero.",
                series.joinToString("\n"),
            )
            .hasSize(4)
        assertThat(series).allSatisfy { assertThat(it).endsWith(" 0.0") }
        assertThat(series.joinToString("\n"))
            .contains("""outcome="succeeded"""")
            .contains("""outcome="failed"""")
            .contains("""trigger="api"""")
            .contains("""trigger="schedule"""")
    }

    /**
     * snapshotcache spec 12's gauges, which the framework explicitly does not own: "gauges are NOT
     * events. Callers poll them." [CacheMetrics.bind] is the host taking that job, and these are the
     * names an alert rule is written against, verbatim.
     *
     * `snapshot_live_generations` is checked for a value as well as a name - spec 12.3 calls it "the
     * most important leak indicator", and a gauge wired to a supplier that never runs reads 0 while
     * looking perfectly present on a scrape.
     */
    @Test
    fun `the cache gauges are exported under spec 12's names, with real values`() {
        val scrape = scrape()
        val group = """group="${HostFixture.GROUP}""""

        listOf(
            "snapshot_current_generation",
            "snapshot_data_as_of_seconds",
            "snapshot_published_at_seconds",
            "snapshot_last_success_seconds",
            "snapshot_live_generations",
            "snapshot_active_leases",
            "snapshot_db_file_bytes",
            "snapshot_rows",
        ).forEach { name ->
            assertThat(scrape.lines().filter { it.startsWith("$name{") && it.contains(group) })
                .withFailMessage("no scraped series named %s for %s", name, group)
                .isNotEmpty()
        }

        assertThat(value(scrape, "snapshot_live_generations"))
            .withFailMessage("the leak indicator reads 0, which means the gauge is not polling anything")
            .isGreaterThanOrEqualTo(1.0)
        assertThat(value(scrape, "snapshot_rows"))
            .isEqualTo((HostFixture.ROWS - 1).toDouble())
        // Spec 12.1: an ABSOLUTE Unix-seconds value, not an age, so `time() - x > threshold` is
        // editable without a deploy. An age would read as a small number here.
        assertThat(value(scrape, "snapshot_data_as_of_seconds"))
            .isGreaterThan(1_700_000_000.0)
    }

    /** Spec 12.5: `generation` increases monotonically and would grow the series count forever. */
    @Test
    fun `no snapshot metric carries a generation label`() {
        assertThat(scrape().lines().filter { it.startsWith("snapshot_") && it.contains("generation=") })
            .isEmpty()
    }

    /** Spec 12.2's counter, whose label values are `RefreshResult.name.lowercase()` verbatim. */
    @Test
    fun `the refresh counter records the startup round under its spec 12_2 label`() {
        assertThat(scrape())
            .contains("""snapshot_refresh_total{group="${HostFixture.GROUP}",result="success",}""")
    }

    private fun scrape() = registry.scrape()

    private fun value(scrape: String, name: String): Double =
        scrape.lines().first { it.startsWith("$name{") }.substringAfterLast(' ').toDouble()
}
