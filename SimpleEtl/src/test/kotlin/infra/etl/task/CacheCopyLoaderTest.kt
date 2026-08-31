package infra.etl.task

import infra.etl.TaskFiles
import infra.etl.TaskFiles.VALID_CACHE_COPY
import infra.etl.TaskFiles.assertRejects
import infra.etl.TaskFiles.edit
import infra.etl.TaskFiles.loadOne
import infra.etl.TaskFiles.loadOneWithCaches
import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/**
 * P9, criterion 9: **the `cacheCopy` YAML form** - the step type P6 deliberately left out of the
 * schema ("no YAML schema by design; P9's") - and the three validation rules added with it: 19, 20
 * and 21.
 *
 * Every file here is [VALID_CACHE_COPY] with a single [edit], and that baseline is asserted to load
 * in the first test. The pairing is P6's rule and it is what makes each rejection attributable:
 * standing alone, "this file was rejected" is satisfied by a loader that rejects everything, and
 * five such assertions are five copies of the same vacuous test.
 *
 * ### Why rules 19 and 20 are startup rules rather than runtime ones
 *
 * Both could have been runtime `require`s in the executor, and both would then boot green and kill
 * a task thirty minutes in - the exact failure startup validation exists to prevent. A `:name` in
 * cache SQL cannot be bound at all (`CopyOutSpec.sql` is a plain `String`), and a `retries` above
 * zero can never fire (the transient classification is JDBC-shaped and a local DuckDB copy raises
 * none of it), so neither is a condition that might come good on the day. The runtime guard stays
 * for definitions built in code, which is the other entry point to a definition and has no loader
 * in front of it; that half is `CacheCopyStepTest`'s.
 *
 * ### The asymmetry rule 20 is written around
 *
 * `CacheCopyStep.retries` defaults to **3** in the programmatic model, frozen since P5 because
 * every other scratch-targeted step does. The **YAML** default is **0**. Had the loader inherited
 * the model's default, every task file that omits `retries` would fail rule 20 on a value nobody
 * wrote - so rule 20 tests the *stated* value, and the omitted case is asserted to resolve to 0 in
 * the first test rather than left to be discovered.
 */
class CacheCopyLoaderTest {

    @TempDir
    lateinit var root: Path

    private fun dir(name: String): Path = Files.createDirectories(root.resolve(name))

    private fun cacheStepOf(loaded: TaskFiles.Loaded): CacheCopyStep =
        loaded.single().phases.first().steps.single() as CacheCopyStep

    /**
     * The baseline loads, and its `cacheCopy` step arrives with its four declared fields intact
     * and `retries` resolved to 0.
     *
     * The `sql` is asserted **verbatim**. It is the one string in a task file that reaches a
     * foreign DuckDB instance with no binding channel at all, so a loader that trimmed, re-quoted
     * or round-tripped it through its parser would change what the cache executes while every
     * other assertion in this file stayed green.
     */
    @Test
    fun aCacheCopyFileLoadsWithTheFourFieldsOfSpec36AndRetriesResolvedToZero() {
        val loaded = loadOne(root, VALID_CACHE_COPY)
        val step = cacheStepOf(loaded)

        assertAll(
            { assertEquals("copy-wip", step.name) },
            { assertEquals("wip_cache", step.cache) { "the host-bound name, not a datasource" } },
            { assertEquals("select lot_id, qty from wip where site = 'F12'", step.sql) },
            { assertEquals("wip_cache", step.output) { "a scratch dataset, with spec 5.5's suffix and view" } },
            {
                assertEquals(0, step.retries) {
                    "the YAML default for this step type is 0. Inheriting the model's 3 would make every " +
                        "file that omits retries fail rule 20 on a value its author never wrote"
                }
            },
        )
    }

    // --- rule 21: every cache name exists in the host-supplied binding set ------------------

    /**
     * Rule 21, the exact analogue of rule 3 for datasources, asserted as a **pair against one
     * loader**: `wip_cache` is registered and loads, `typo_cache` is not and is rejected.
     *
     * The pair is mandatory rather than tidy. With an empty `caches` set every name is unknown, so
     * a lone rejection passes against a loader that has no notion of a cache at all - which is
     * precisely how a rule can ship enforcing nothing.
     */
    @Test
    fun rule21AnUnregisteredCacheNameIsRejectedWhileTheRegisteredSiblingLoads() {
        val onlyWipCache = setOf("wip_cache")

        val accepted = loadOneWithCaches(dir("registered"), VALID_CACHE_COPY, onlyWipCache)
        val rejected = loadOneWithCaches(
            dir("unregistered"),
            edit(VALID_CACHE_COPY, "cache: wip_cache", "cache: typo_cache"),
            onlyWipCache,
        )

        assertAll(
            {
                assertEquals("wip_cache", cacheStepOf(accepted).cache) {
                    "the registered name must load, or the rejection below proves nothing: " +
                        "${accepted.errors.map { it.message }}"
                }
            },
            { assertRejects(rejected, file = "task.yaml", step = "copy-wip", "typo_cache", "wip_cache") },
        )
    }

    // --- rule 19: cacheCopy SQL binds no variable -------------------------------------------

    /**
     * Rule 19, with the accepted counterpart that makes it mean what it says.
     *
     * A test that only rejects `:siteCode` passes against `require(!sql.contains(":"))` - and that
     * implementation then rejects `qty::varchar` and `'a:b'`, both legal DuckDB, both correctly
     * skipped by JDBI's own parser, and both silently unusable in every task file that needs a
     * cast. So the same test loads a file carrying each of them.
     *
     * The rejected file names a **registered** cache, because unknown-cache is checked first and a
     * file failing that never reaches the rule under test.
     */
    @Test
    fun rule19CacheSqlBindingAVariableIsRejectedWhileACastAndAColonInALiteralLoad() {
        val bound = edit(
            VALID_CACHE_COPY,
            "sql: \"select lot_id, qty from wip where site = 'F12'\"",
            "sql: \"select lot_id, qty from wip where site = :siteCode\"",
        )
        val punctuated = edit(
            VALID_CACHE_COPY,
            "sql: \"select lot_id, qty from wip where site = 'F12'\"",
            "sql: \"select lot_id, qty::varchar as qty_text, 'a:b' as tag from wip\"",
        )

        val accepted = loadOne(dir("punctuated"), punctuated)

        assertAll(
            { assertRejects(loadOne(dir("bound"), bound), file = "task.yaml", step = "copy-wip", "siteCode") },
            {
                assertEquals(
                    "select lot_id, qty::varchar as qty_text, 'a:b' as tag from wip",
                    cacheStepOf(accepted).sql,
                ) {
                    "a cast and a colon inside a string literal are not variables; errors were " +
                        "${accepted.errors.map { it.message }}"
                }
            },
        )
    }

    // --- rule 20: a stated retries above zero is rejected ------------------------------------

    /**
     * Rule 20. The knob can never fire, so it is refused rather than accepted and ignored - the
     * treatment rules 12 and 18 already set the precedent for.
     *
     * The message must name `retries`: a file rejected for any other reason would otherwise
     * satisfy this test, and so would one whose report said only "invalid step".
     */
    @Test
    fun rule20AStatedNonZeroRetriesOnACacheCopyStepIsRejected() {
        val yaml = edit(VALID_CACHE_COPY, "output: wip_cache", "output: wip_cache\n        retries: 2")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "copy-wip", "retries")
    }

    // --- rule 9: the output is an ordinary scratch dataset name -------------------------------

    /**
     * Rule 9. A `cacheCopy` `output` shares one namespace with every other dataset the task
     * produces, so the `materialize` step below cannot quietly reuse the name.
     *
     * The error is attributed to `build-summary`, the **second** producer, and that is what makes
     * this test load-bearing: it can only fire if the loader registered the `cacheCopy` output in
     * the first place. A loader that ignored cache outputs would let this file through.
     */
    @Test
    fun rule09ACacheCopyOutputSharesTheDatasetNamespaceWithEveryOtherStep() {
        val yaml = edit(VALID_CACHE_COPY, "output: summary", "output: wip_cache")

        assertRejects(loadOne(root, yaml), file = "task.yaml", step = "build-summary", "wip_cache")
    }

    /**
     * The dataset-name character check, which `datasetIdentifier` exists for and which is not one
     * of the numbered validation rules.
     *
     * A dataset name arrives from a file and becomes a SQL identifier that no prepared statement
     * can parameterise, and for `cacheCopy` it also becomes the `targetTable` a *foreign* DuckDB
     * instance quotes into its own `CREATE TABLE`. Checking it at load is the only place that is
     * cheap and the only place that is early.
     */
    @Test
    fun aCacheCopyOutputThatIsNotAnIdentifierIsRejected() {
        val yaml = edit(VALID_CACHE_COPY, "output: wip_cache", "output: wip cache")

        val loaded = loadOne(root, yaml)

        assertAll(
            { assertRejects(loaded, file = "task.yaml", step = "copy-wip", "wip cache") },
            {
                assertTrue(loaded.errors.any { "5.5" in it.message }) {
                    "the message should point at the rule it enforces; messages were " +
                        "${loaded.errors.map { it.message }}"
                }
            },
        )
    }
}
