package etlhost

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Everything the host reads from disk, created before Quarkus boots.
 *
 * A [QuarkusTestResourceLifecycleManager] rather than a `@BeforeAll`, because the host's
 * `StartupEvent` observer runs the first refresh *during* boot: a fixture that ran after the app
 * started would be seeding a source the cache had already read as empty, and `verify.nonEmpty`
 * would have failed the first generation. `start()` returns the config overrides, which is the one
 * hook that is guaranteed to run first.
 *
 * The source is a DuckDB file rather than Oracle on purpose. It exercises the identical production
 * path - `JdbcGenerationSource`, `RowPipe`, `DuckDbTableWriter` - in a second rather than in two and
 * a half minutes, and [HostEndToEndOracleTest] runs the same host against a real Oracle so the
 * substitution is stated rather than assumed.
 */
open class HostFixture : QuarkusTestResourceLifecycleManager {

    private lateinit var root: Path

    override fun start(): Map<String, String> {
        root = Files.createTempDirectory("etl-host-test")
        val tasks = Files.createDirectories(root.resolve("tasks"))
        writeTaskFiles(tasks)
        seedSource(url(root, "source.db"))
        createTarget(url(root, "report.db"))
        return buildMap {
            put("etl-host.cache.storage-path", root.resolve("cache").toString())
            put("etl-host.cache.temp-directory", root.resolve("tmp").toString())
            put("etl-host.etl.task-directory", tasks.toString())
            put("etl-host.etl.scratch-directory", root.resolve("scratch").toString())
            put("etl-host.etl.target-url", url(root, "report.db"))
            // Long enough that the tick never fires inside a test. The tick's logic is called
            // directly where it is under test; that Quarkus fires an @Scheduled method is
            // Quarkus's property, not this host's.
            put("etl-host.cache.refresh-interval", "PT30M")
            putAll(sourceOverrides(root))
        }
    }

    /** Overridden by the Oracle fixture, which points the same host at a container instead. */
    protected open fun sourceOverrides(root: Path): Map<String, String> =
        mapOf("etl-host.source.url" to url(root, "source.db"))

    protected open fun seedSource(url: String) {
        connect(url).use { source ->
            source.createStatement().use { st ->
                st.execute("CREATE TABLE lot (id BIGINT, lot_id VARCHAR, qty DECIMAL(18,3), site VARCHAR)")
                st.execute(
                    "INSERT INTO lot SELECT i, 'L' || i, i * 1.5, " +
                        "CASE WHEN i % 2 = 0 THEN 'F12' ELSE 'F11' END FROM range(1, $ROWS) t(i)",
                )
            }
        }
    }

    private fun createTarget(url: String) = connect(url).use {
        it.createStatement().use { st ->
            st.execute("CREATE TABLE wip_summary (site VARCHAR, lots BIGINT, total_qty DECIMAL(38,3))")
        }
    }

    override fun stop() = Unit

    companion object {
        const val GROUP = "wip"
        const val TASK = "wip-summary"
        const val DISABLED_TASK = "archive-old"

        /** 500 rows, split across two sites, so the summary has two rows to assert on. */
        const val ROWS = 501

        /**
         * The explicit `Class.forName` is not superstition. A test resource runs before the
         * application classloader exists, on a loader where `ServiceLoader` has not yet registered
         * the DuckDB driver, and `DriverManager` answers "No suitable driver found" rather than
         * anything that names the real cause.
         */
        fun connect(url: String): Connection {
            Class.forName("org.duckdb.DuckDBDriver")
            return DriverManager.getConnection(url)
        }

        fun url(root: Path, name: String) = "jdbc:duckdb:" + root.resolve(name).toString().replace('\\', '/')

        /**
         * Spec 2.4's shape D - `cacheCopy` into scratch, `materialize`, `pipe` out - which is the
         * only shape that crosses both frameworks, plus one disabled task so `AdminResource`'s 400
         * case has something to answer for.
         *
         * The cron is hourly. A test that needed a firing registers its own through
         * `QuarkusCronScheduler`; a task file that fired every second would run the whole pipeline
         * underneath every other test in the module.
         */
        fun writeTaskFiles(directory: Path) {
            Files.writeString(
                directory.resolve("$TASK.yaml"),
                """
                name: $TASK
                schedule:
                  cron: "0 0 * * * ?"
                phases:
                  - name: load
                    steps:
                      - name: copy-wip
                        type: cacheCopy
                        cache: $GROUP
                        sql: select id, lot_id, qty, site from $GROUP
                        output: wip_cache
                      - name: summarise
                        type: materialize
                        datasource: scratch
                        output: summary
                        sql: select site, count(*) as lots, sum(qty) as total_qty from wip_cache group by site
                      - name: publish
                        type: pipe
                        source:
                          datasource: scratch
                          sql: select site, lots, total_qty from summary
                        target:
                          datasource: report
                          table: wip_summary
                """.trimIndent(),
            )
            Files.writeString(
                directory.resolve("$DISABLED_TASK.yaml"),
                """
                name: $DISABLED_TASK
                enabled: false
                phases:
                  - name: load
                    steps:
                      - name: noop
                        type: sql
                        datasource: report
                        statements:
                          - "select 1"
                """.trimIndent(),
            )
        }
    }
}
