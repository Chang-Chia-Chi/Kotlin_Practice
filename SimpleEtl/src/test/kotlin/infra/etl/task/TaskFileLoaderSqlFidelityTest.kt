package infra.etl.task

import infra.etl.TaskFiles.loadOne
import infra.etl.task.MaterializeStep
import infra.etl.task.SqlStep
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * Done-when: "SQL containing `${...}` and multi-line SQL survive loading unchanged, proving the
 * Quarkus-config path was correctly avoided."
 *
 * There are three reasons task files are not read through Quarkus configuration, and the
 * one with teeth is that config performs property expansion. A `${SITE}` inside a SQL string
 * literal would either be substituted with something else or fail the build outright, and the
 * author would find out at 03:00 with a query that no longer says what the file says.
 *
 * The assertion is byte equality against a string this file builds line by line, not a
 * `contains` and not a non-empty check: an expanded, re-indented or newline-normalised SQL text
 * would still be non-empty, would still contain `select`, and would still run - differently.
 *
 * The expected value and the YAML are generated from the same list of lines, so the two cannot
 * drift apart while looking correct. The YAML uses a `|-` block scalar, whose semantics are
 * exact: strip the block's own indentation from every line, join with a single newline, and
 * chomp the trailing one. Anything the loader does to the text beyond that shows up here.
 */
class TaskFileLoaderSqlFidelityTest {

    @TempDir
    lateinit var root: Path

    private val sqlLines = listOf(
        "select lot_id,",
        "       '\${SITE}' as site,",
        "       count(*) as n",
        "from wip_stg",
        "group by 1, 2",
    )

    private val statementLines = listOf(
        "select '\${BUILD_ID}' as build,",
        "       1 as one",
    )

    private val expectedSql = sqlLines.joinToString("\n")

    private val expectedStatement = statementLines.joinToString("\n")

    private val yaml = buildString {
        append("name: sql-fidelity\n")
        append("phases:\n")
        append("  - name: only\n")
        append("    steps:\n")
        append("      - name: build-summary\n")
        append("        type: materialize\n")
        append("        datasource: scratch\n")
        append("        output: summary\n")
        append("        sql: |-\n")
        sqlLines.forEach { append("          ").append(it).append("\n") }
        append("      - name: annotate\n")
        append("        type: sql\n")
        append("        datasource: scratch\n")
        append("        statements:\n")
        append("          - |-\n")
        statementLines.forEach { append("            ").append(it).append("\n") }
    }

    @Test
    fun multiLineSqlWithAPropertyPlaceholderArrivesByteIdentical() {
        val task = loadOne(root, yaml).single()
        val step = task.phases.single().steps.first() as MaterializeStep

        assertEquals(expectedSql, step.sql) {
            "SQL is read from the file, never through a config layer that expands it"
        }
    }

    /**
     * The same for the `statements` list, which is a different deserialisation path: a list of
     * scalars rather than one scalar, and the only place a task file carries SQL the framework
     * hands to the driver verbatim without ever wrapping it.
     */
    @Test
    fun aMultiLineStatementInASqlStepArrivesByteIdentical() {
        val task = loadOne(root, yaml).single()
        val step = task.phases.single().steps.last() as SqlStep

        assertEquals(listOf(expectedStatement), step.statements)
    }

    /**
     * Named separately from the equality assertions above because it is the one that fails
     * loudly for the right reason. If a future wiring change routes task files through Quarkus
     * config, `${SITE}` becomes an unresolved-property build failure or a substituted value, and
     * a reader of that failure should not have to work out which of the equality assertions
     * cared about the dollar sign.
     */
    @Test
    fun thePropertyPlaceholderIsNotExpanded() {
        val task = loadOne(root, yaml).single()
        val step = task.phases.single().steps.first() as MaterializeStep

        assertAll(
            {
                assertTrue("\${SITE}" in step.sql) {
                    "the property placeholder was expanded; SQL was: ${step.sql}"
                }
            },
            {
                assertEquals(5, step.sql.lines().size) {
                    "the block scalar's five lines, neither folded into one nor re-indented; " +
                        "was ${step.sql.lines()}"
                }
            },
        )
    }
}
