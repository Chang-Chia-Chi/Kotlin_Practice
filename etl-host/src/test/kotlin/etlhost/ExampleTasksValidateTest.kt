package etlhost

import infra.etl.task.LoadResult
import infra.etl.task.TaskFileLoader
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The copy-me file stays copyable.
 *
 * `example-tasks/` exists so an adopter never has to dig YAML out of test code - and it has
 * already shipped broken once: the P9 operator round promoted the fixture task verbatim,
 * Kotlin string templates and all, and nothing noticed until a real boot failed naming it.
 * The soak round then found its publish step appending without bound, and the fix again
 * changed a file no test loads. This closes that: every file in the directory must pass the
 * same spec 10 validation a booting host runs, against the staging wiring's own name sets.
 *
 * No Quarkus, no containers - `TaskFileLoader` alone, so it runs in the default suite.
 */
class ExampleTasksValidateTest {

    @Test
    fun everyExampleTaskLoadsCleanAgainstTheStagingNameSets() {
        // The loader's rule-6 syntax check opens `jdbc:duckdb:`, and under the Quarkus test
        // classloader DriverManager sees no service-loader driver until the class is touched -
        // order-dependent, so this test passed alone and errored in the full suite. Same trap
        // and same fix the staging round recorded for its Oracle init.
        Class.forName("org.duckdb.DuckDBDriver")
        val directory = Path.of("example-tasks")
        assertTrue(java.nio.file.Files.isDirectory(directory)) { "example-tasks/ is missing" }

        val loader = TaskFileLoader(
            datasources = setOf("report"),
            hooks = emptySet(),
            caches = setOf("wip"),
        )
        val result = loader.load(directory)

        assertTrue(result is LoadResult.Loaded) {
            "the copy-me file does not validate; a host booting on it fails by name:\n" +
                (result as LoadResult.Invalid).report.errors.joinToString("\n") {
                    "  ${it.file} / ${it.step}: ${it.message}"
                }
        }
        assertTrue((result as LoadResult.Loaded).tasks.isNotEmpty()) {
            "example-tasks/ holds no task at all"
        }
    }
}
