package infra.etl

import infra.etl.TaskFiles.VALID
import infra.etl.TaskFiles.VALID_REQUIRED
import infra.etl.TaskFiles.assertRejects
import infra.etl.TaskFiles.dirOf
import infra.etl.TaskFiles.dropLine
import infra.etl.TaskFiles.edit
import infra.etl.TaskFiles.load
import infra.etl.TaskFiles.loadOne
import infra.etl.TaskFiles.minimal
import infra.etl.TaskFiles.orderedVars
import infra.etl.task.MaterializeFormat
import infra.etl.task.MaterializeStep
import infra.etl.task.PipeStep
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

/**
 * The eighteen validation rules of spec 10, one deliberately broken file each.
 *
 * Every file here is a baseline from `TaskFiles` with a single edit, and every baseline is
 * asserted to load in `TaskFileLoaderValidTest`. That pairing is the whole design: on its own,
 * "this file was rejected" is satisfied by a loader that rejects everything, and eighteen such
 * assertions are eighteen copies of the same vacuous test. One rule away from a sibling that
 * loads is what makes each rejection attributable to the rule under test.
 *
 * Each assertion goes through [assertRejects], which checks the structured `file` and `step`
 * fields rather than searching the message text for them, and which checks `line` only for
 * plausibility - it is nullable by contract, so asserting a number would test a guess.
 *
 * **Rule 15 is tested over what a task file states, not over what it discovers.** A table's
 * declared column types are unreachable at startup - measured three ways on duckdb_jdbc 1.1.3 -
 * so that half is P2's, at writer open. `transform.addColumns` states its types in the YAML, so
 * that half is tested here.
 */
class TaskFileLoaderRulesTest {

    @TempDir
    lateinit var root: Path

    // --- rule 1: YAML parses and deserialises; unknown fields rejected ------------------

    @Test
    fun rule01YamlThatDoesNotParseIsRejected() {
        val yaml = edit(minimal("bad-yaml"), "  - name: only", "\t- name: only")

        val loaded = loadOne(root, yaml)

        assertThat(loaded.tasks).isNull()
        assertThat(loaded.errors).isNotEmpty()
        assertThat(loaded.errors).allSatisfy { assertThat(it.file).contains("task.yaml") }
    }

    /**
     * Done-when: "an unknown YAML field is rejected rather than ignored". The message has to
     * name the field - a loader that rejected the file for any other reason would otherwise
     * pass this test, and so would one whose message said only "invalid file".
     */
    @Test
    fun rule01AnUnknownTaskFieldIsRejectedAndNamed() {
        val yaml = edit(VALID, "chunkSize: 5000", "chunkSize: 5000\nchunkSizeTypo: 10")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = null, "chunkSizeTypo")
    }

    @Test
    fun rule01AnUnknownStepFieldIsRejectedAndAttributedToItsStep() {
        val yaml = edit(
            VALID,
            "        type: materialize",
            "        type: materialize\n        outputt: summary",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "build-summary", "outputt")
    }

    // --- rule 2: name unique across files and matching the allowed pattern --------------

    @Test
    fun rule02ANameOutsideTheAllowedPatternIsRejected() {
        val yaml = edit(VALID, "name: wip-summary", "name: WIP_Summary")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = null, "WIP_Summary")
    }

    @Test
    fun rule02TheSameTaskNameInTwoFilesIsRejected() {
        val loaded = load(dirOf(root, "a.yaml" to VALID, "b.yaml" to VALID))

        assertThat(loaded.tasks).isNull()
        assertThat(loaded.errors).isNotEmpty()
        assertThat(loaded.errors).allSatisfy {
            assertThat(it.file).containsAnyOf("a.yaml", "b.yaml")
        }
        assertThat(loaded.errors)
            .describedAs("the report must name the duplicated task, not just say 'duplicate'")
            .anySatisfy { assertThat(it.message).contains("wip-summary") }
    }

    // --- rule 3: every referenced datasource exists -------------------------------------

    @Test
    fun rule03AnUnknownDatasourceIsRejected() {
        val yaml = edit(VALID, "          datasource: oracle_mes", "          datasource: oracle_typo")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-wip", "oracle_typo")
    }

    // --- rule 4: every transform.bean resolves ------------------------------------------

    @Test
    fun rule04AnUnknownTransformBeanIsRejected() {
        val yaml = edit(VALID, "bean: wipEnricher", "bean: noSuchBean")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-wip", "noSuchBean")
    }

    // --- rule 5: every hook name exists in the registry ---------------------------------

    @Test
    fun rule05AnUnknownHookNameIsRejected() {
        val yaml = edit(VALID, "onSuccess: notify-downstream", "onSuccess: no-such-hook")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = null, "no-such-hook")
    }

    // --- rule 6: every SQL text parses ---------------------------------------------------

    /**
     * The SQL is DuckDB's own dialect on the `scratch` datasource, deliberately: a parse check
     * built on the one engine this module has on its classpath can be held to this file, and
     * could not fairly be held to the Oracle MERGE in the publish step.
     */
    @Test
    fun rule06MalformedSqlIsRejected() {
        val yaml = edit(
            VALID,
            "sql: \"select lot_id, sum(qty) as qty from wip_stg group by 1\"",
            "sql: \"select lot_id, sum(qty as qty from wip_stg group by\"",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "build-summary")
    }

    @Test
    fun rule06EmptySqlIsRejected() {
        val yaml = edit(VALID, "sql: \"select lot_id, qty from summary\"", "sql: \"\"")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "publish-summary")
    }

    // --- rule 7: every :name resolves, in step order -------------------------------------

    @Test
    fun rule07AnUnknownBindNameIsRejected() {
        val yaml = edit(
            VALID,
            "from wip_stg group by 1",
            "from wip_stg where site = :nosuchvar group by 1",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "build-summary", "nosuchvar")
    }

    /**
     * The ordering half. The variable is exported by a step in the same phase, so a rule that
     * only asked "is this name defined anywhere in the file" would accept it.
     */
    @Test
    fun rule07AVariableUsedBeforeItsExportIsRejected() {
        assertRejects(
            loadOne(root, orderedVars(exportFirst = false)),
            file = "task.yaml",
            step = "use-watermark",
            "lastTs",
        )
    }

    // --- rule 8: no variable defined twice, no literal var with a null value -------------

    @Test
    fun rule08AVariableDefinedTwiceIsRejected() {
        val yaml = edit(
            VALID,
            "  - name: siteCode\n    value: \"F12\"",
            "  - name: siteCode\n    value: \"F12\"\n  - name: siteCode\n    value: \"F13\"",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = null, "siteCode")
    }

    /**
     * `LiteralVar`'s own constructor already rejects a null value with `require`, so a loader
     * that hands the YAML straight to the model throws instead of reporting. Startup validation
     * has to produce a `ValidationReport` naming the file, which is what this asserts: the
     * adapter in `TaskFiles.load` does not catch anything, so an escaping exception fails here.
     */
    @Test
    fun rule08ALiteralVarWithANullValueIsRejected() {
        val yaml = edit(VALID, "value: \"F12\"", "value: null")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = null, "siteCode")
    }

    // --- rule 9: dataset names unique within the task ------------------------------------

    @Test
    fun rule09TwoDatasetsWithTheSameNameAreRejected() {
        val yaml = edit(VALID, "output: summary", "output: wip_stg")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "build-summary", "wip_stg")
    }

    // --- rule 10: exactly one of target.table or target.sql -------------------------------

    @Test
    fun rule10ATargetWithBothTableAndSqlIsRejected() {
        val yaml = edit(
            VALID,
            "          datasource: report_oracle\n          sql: \"merge",
            "          datasource: report_oracle\n          table: wip_summary\n          sql: \"merge",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "publish-summary", "target")
    }

    @Test
    fun rule10ATargetWithNeitherTableNorSqlIsRejected() {
        val yaml = dropLine(VALID, "merge into")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "publish-summary", "target")
    }

    // --- rule 11: target.sql not on a DuckDB datasource -----------------------------------

    @Test
    fun rule11AStatementTargetOnScratchIsRejected() {
        val yaml = edit(VALID, "          datasource: report_oracle", "          datasource: scratch")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "publish-summary", "scratch")
    }

    // --- rule 12: retries > 0 on a non-scratch target requires idempotent: true ------------

    @Test
    fun rule12RetriesOnANonScratchTargetWithoutIdempotentAreRejected() {
        val yaml = dropLine(VALID, "idempotent: true")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "publish-summary", "idempotent")
    }

    // --- rule 13: format: PARQUET only on materialize ---------------------------------------

    @Test
    fun rule13ParquetOnAStepThatIsNotMaterializeIsRejected() {
        val yaml = edit(
            VALID,
            "        type: sql\n        datasource: scratch\n        statements:",
            "        type: sql\n        datasource: scratch\n        format: PARQUET\n        statements:",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "index-staging", "format")
    }

    /**
     * The other half of rule 13, and the reason the rejection above means something: a loader
     * that rejected `format` wherever it appeared would pass that test and break every parquet
     * task in the fleet.
     */
    @Test
    fun rule13ParquetIsAcceptedOnMaterialize() {
        val yaml = edit(VALID, "format: TABLE", "format: PARQUET")

        val step = loadOne(root, yaml).single()
            .phases.flatMap { it.steps }.single { it.name == "build-summary" } as MaterializeStep

        assertThat(step.format).isEqualTo(MaterializeFormat.PARQUET)
    }

    // --- rule 14: createTable AUTO only on scratch, and not with an undeclared transform ----

    /**
     * Two edits, not one, and deliberately: moving the target off scratch while leaving
     * `retries: 3` would also trip rule 12, and a file two rules away cannot attribute its
     * rejection to either of them.
     */
    @Test
    fun rule14AutoOnANonScratchTargetIsRejected() {
        val moved = edit(
            VALID,
            "          datasource: scratch\n          table: wip_stg",
            "          datasource: report_oracle\n          table: wip_stg",
        )
        val yaml = edit(moved, "          createTable: AUTO\n        retries: 3", "          createTable: AUTO\n        retries: 0")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-wip", "AUTO")
    }

    @Test
    fun rule14AutoWithATransformThatDeclaresNoAddedColumnsIsRejected() {
        val yaml = edit(
            VALID,
            "          addColumns:\n            - name: row_hash\n              type: VARCHAR\n",
            "",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-wip", "addColumns")
    }

    // --- rule 15: DuckDB target column types ------------------------------------------------

    /**
     * The half of rule 15 startup can reach: a column type the task file *states*.
     *
     * A table's declared types are unreachable at load - under REQUIRED they live in a catalog
     * the run creates, under AUTO they come from result set metadata - and duckdb_jdbc 1.1.3
     * offers no parse-to-AST path for DDL, so that half is enforced at writer open and is
     * covered by P2's `DuckDbTableWriterRequiredTest`. `transform.addColumns` is different: the
     * author writes the type in the YAML, so nothing has to be discovered to check it.
     *
     * DATE, BLOB and TIMESTAMP WITH TIME ZONE are rejected whether nullable or not - the DATE
     * truncation of 4.6 is silent and does not depend on nullability, and neither has an
     * appender path at all. BOOLEAN and DOUBLE are rejected only when nullable: their only
     * `append` overloads are primitive.
     */
    @ParameterizedTest(name = "{0} nullable={1}")
    @CsvSource(
        "DATE, true",
        "DATE, false",
        "BLOB, true",
        "TIMESTAMP WITH TIME ZONE, true",
        "BOOLEAN, true",
        "DOUBLE, true",
    )
    fun rule15ADeclaredAddColumnTypeWithNoWritePathIsRejected(type: String, nullable: Boolean) {
        val yaml = TaskFiles.withAddColumn(type, nullable)

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-wip", "row_hash")
    }

    /**
     * The pairing, and it carries more than rule 13's did. `VARCHAR` alone would leave a loader
     * that rejects every `addColumns` entry except VARCHAR looking correct; `BIGINT` nullable is
     * S3's ruling, and `BOOLEAN` **not** nullable is the one that proves the nullability
     * qualifier is read rather than the type name being blanket-matched - a loader that rejects
     * BOOLEAN on sight passes all six rejection cases above.
     */
    @ParameterizedTest(name = "{0} nullable={1}")
    @CsvSource(
        "VARCHAR, true",
        "BIGINT, true",
        "BOOLEAN, false",
    )
    fun rule15ADeclaredAddColumnTypeWithAWritePathIsAccepted(type: String, nullable: Boolean) {
        val yaml = TaskFiles.withAddColumn(type, nullable)

        val step = loadOne(root, yaml).single()
            .phases.flatMap { it.steps }.single { it.name == "load-wip" } as PipeStep

        assertThat(step.addColumns.single().name).isEqualTo("row_hash")
    }

    /**
     * Rule 14's DECIMAL clause, not rule 15's, but the same shape of defect: `precision` defaults
     * to 0, `DECIMAL(0,0)` is outside `1 <= p <= 38`, and DuckDB rejects it at parse time. The
     * value is stated in the YAML, so nothing has to be discovered to catch it at startup, and
     * today it is caught at writer open instead - one step into a run rather than at boot.
     */
    @Test
    fun rule14ADeclaredDecimalAddColumnWithNoPrecisionIsRejected() {
        val yaml = TaskFiles.withAddColumn("DECIMAL", nullable = true)

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-wip", "row_hash")
    }

    // --- rule 16: cron expression valid when present ----------------------------------------

    @Test
    fun rule16AnInvalidCronExpressionIsRejected() {
        val yaml = edit(VALID, "cron: \"0 */10 * * * ?\"", "cron: \"not-a-cron\"")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = null, "cron")
    }

    // --- rule 17: each step's field set matches its declared type exactly --------------------

    /**
     * `statements` is a real field of a real step type, just not of this one, which is what
     * separates rule 17 from rule 1: the key is known to the schema and wrong for the step.
     */
    @Test
    fun rule17AFieldBelongingToAnotherStepTypeIsRejected() {
        val yaml = edit(
            VALID,
            "        output: summary",
            "        output: summary\n        statements:\n          - \"select 1\"",
        )

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "build-summary", "statements")
    }

    // --- rule 18: scratch + createTable REQUIRED + retries > 0 -------------------------------

    @Test
    fun rule18RetriesOnARequiredScratchTargetAreRejected() {
        val yaml = edit(VALID_REQUIRED, "        retries: 0", "        retries: 2")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-required", "retries")
    }

    /**
     * The shape that actually reaches production. Spec 5.3 defaults `retries` to 3 for any
     * scratch target, REQUIRED included, so the hazard arrives on a default the author never
     * wrote - which is how it survived until P5's review found it.
     */
    @Test
    fun rule18OmittedRetriesOnARequiredScratchTargetIsStillRejected() {
        val yaml = dropLine(VALID_REQUIRED, "retries: 0")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "load-required", "retries")
    }
}
