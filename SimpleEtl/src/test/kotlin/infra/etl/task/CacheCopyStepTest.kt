package infra.etl.task

import infra.etl.Etl
import infra.etl.EventTrace
import infra.etl.FakeSnapshotCache
import infra.etl.ListenerCall
import infra.etl.TaskHarness
import infra.etl.micrometer.MicrometerTaskMetrics
import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.NotReadyException
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.nio.file.Path
import java.sql.Connection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.io.TempDir

/** The row counter series, named once so a typo in the test cannot agree with a typo in the code. */
private const val ROWS = "etl_step_rows_total"

/**
 * The generation holds **9** rows and every step's SQL selects **7** of them.
 *
 * Two numbers under test are never allowed to be equal - P8b's `read`/`written` lesson - and here
 * they carry the whole of "a *subset* was copied": with 9 and 7 apart, an implementation that
 * copied the table wholesale, or one that copied nothing and let a later step read the generation
 * directly, fails. `where lot_id < 7` is a filter the generation itself must evaluate, because
 * the copy runs **inside the cache's own DuckDB instance** and no row crosses the JVM.
 */
private const val GENERATION_ROWS = 9
private const val SUBSET_SQL = "select lot_id, qty from wip where lot_id < 7"
private const val SUBSET_ROWS = 7L

/**
 * P9: **the `cacheCopy` executor**, the fifth step type and the reason this framework
 * depends on `snapshotcache` at all.
 *
 * Every run here goes through a [FakeSnapshotCache], which performs the real ATTACH / USE / CTAS /
 * USE / DETACH against a genuine second DuckDB file. `DefaultSnapshotCache` is `internal` to its
 * own module, so a real cache is out of reach from here by construction; the integration belongs
 * to the host's own wiring obligations and is recorded there rather than faked here.
 *
 * ### What each of these tests is really guarding against
 *
 * The wrong implementations are specific and none of them is exotic:
 *
 * - **A `Snapshot` held across steps.** `copyOut` owns the lease lifecycle, which is why the step
 *   type is defined over it and not `acquire`; a task holding a lease for thirty minutes stalls
 *   every refresh of the cache, with the cause in a different system from the symptom. The double
 *   throws an `Error` from `acquire` and `withSnapshot`, so a run that took a lease dies rather
 *   than passing.
 * - **A view over the attached generation** instead of a materialised copy. The double deletes the
 *   generation file inside `copyOut`'s own `finally`, and a later step then reads the dataset.
 * - **A row count that means "rows the database touched".** `StepResult` is 0 / 0 like every
 *   non-pipe step: `etl_step_rows_total{direction}` is one counter series across all step types,
 *   and a `rowsCopied` reported here would make it mean three different things. `rowsCopied` is
 *   lineage, and lineage goes in the log line beside `generation` and `dataAsOf`.
 * - **The `NotImplementedError` path left behind.** Its stub was an `Error`, which the step loop's
 *   `catch (Exception)` misses, so today a cache-copy step produces no `onStepEnd` and no
 *   `stepEnded` metric at all. `theListenerAndMetricSeamsBothSeeACacheCopyStep` is what says that
 *   is over.
 *
 * Nothing here sleeps, and the one retry assertion reads the harness's recorded backoff rather
 * than elapsed time.
 */
class CacheCopyStepTest {

    @TempDir
    lateinit var root: Path

    /** What one probe file was asked, in one open, so a run is read back exactly once. */
    private data class Observed(val rows: Long, val tables: List<String?>, val viewSql: String?)

    private fun seededCache(name: String = "wip-42"): FakeSnapshotCache =
        FakeSnapshotCache(root.resolve("generations").resolve("$name.duckdb"))
            .also { it.seed("wip", rows = GENERATION_ROWS) }

    /**
     * Criteria 1 and 5. **A subset copies out of a generation into scratch**, under the
     * attempt-suffixed physical name with a stable view over it, and the engine reaches the
     * generation through `copyOut` and nothing else.
     *
     * Criterion 5 is asserted in this run rather than in one of its own: standing alone, "acquire
     * was never called" passes for any run that never reached the cache, including one whose
     * executor is still a stub.
     *
     * `targetTable` is asserted **bare**. `DuckDbGenerationStore.copyOut` quotes the name itself,
     * while `materialize` quotes at its own call site, so an executor that mirrored `materialize`
     * literally would hand over `"wip_cache__a1"` and create a table whose name contains the
     * quotes - a table every later step then fails to find, for a reason the message would not
     * explain.
     */
    @Test
    fun aSubsetOfAGenerationIsCopiedIntoScratchUnderItsAttemptSuffixedNameAndStableView() {
        TaskHarness(root).use { harness ->
            val cache = seededCache()
            harness.cache("wip_cache", cache, group = "wip")
            val probe = harness.probeFile("subset")

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-cache-copy",
                    Etl.phase("copy", Etl.cacheCopy("copy-wip", "wip_cache", SUBSET_SQL, "wip_cache")),
                    Etl.phase("observe", Etl.probeScratch("probe", probe, "wip_cache")),
                ),
            )

            val observed = harness.readProbe(probe) { read(it, "wip_cache") }
            val spec = cache.copyOuts.single()

            assertAll(
                {
                    assertEquals(SUBSET_ROWS, observed.rows) {
                        "the stable view 'wip_cache' must resolve to the copied subset"
                    }
                },
                {
                    assertEquals(
                        GENERATION_ROWS.toLong(),
                        cache.readGeneration { Etl.longAt(it, "select count(*) from wip") },
                    ) { "the generation must still hold every row, so $SUBSET_ROWS is a subset and not the lot" }
                },
                {
                    assertTrue("wip_cache__a1" in observed.tables) {
                        "spec 5.5 is unconditional: the physical table carries the attempt suffix. " +
                            "scratch held ${observed.tables}"
                    }
                },
                {
                    assertTrue(observed.viewSql?.contains("wip_cache__a1") == true) {
                        "the stable view must point at attempt 1's table; its definition was ${observed.viewSql}"
                    }
                },
                {
                    assertEquals("wip_cache__a1", spec.targetTable) {
                        "targetTable is passed unquoted - the store quotes it itself"
                    }
                },
                { assertEquals(SUBSET_SQL, spec.sql) { "the step's SQL reaches the cache verbatim" } },
                { assertEquals(listOf(GroupId("wip")), cache.groups) { "copyOut takes the group, not the name" } },
                {
                    assertEquals(0, cache.acquireCalls.get() + cache.withSnapshotCalls.get()) {
                        "the framework must never take a lease of its own (spec 7.3, contract 2.2)"
                    }
                },
                {
                    assertEquals(0, cache.currentInfoCalls.get()) {
                        "currentInfo would be the engine deciding for itself what copyOut decides"
                    }
                },
            )
        }
    }

    /**
     * Criterion 3. **The lease is the cache's and it is gone by the time the next step runs.**
     *
     * A counting double cannot express this - the engine never calls `acquire`, so "releases equals
     * acquires" is `0 == 0` and passes against an engine with no cache executor in it. The
     * observable property is destructive instead: the double deletes the generation file inside
     * `copyOut`'s own `finally`, which is precisely where the real cache becomes free to reclaim
     * it, and a later `materialize` step then reads the copied dataset. If the executor had left a
     * view over an attached generation rather than materialising a table, that read finds nothing.
     */
    @Test
    fun theGenerationIsReclaimableTheMomentTheStepEndsAndALaterStepStillReadsTheCopy() {
        TaskHarness(root).use { harness ->
            val cache = seededCache().also { it.deleteGenerationInsideCopyOut = true }
            harness.cache("wip_cache", cache)
            val probe = harness.probeFile("reclaimed")

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-cache-copy",
                    Etl.phase("copy", Etl.cacheCopy("copy-wip", "wip_cache", SUBSET_SQL, "wip_cache")),
                    Etl.phase(
                        "build",
                        Etl.materialize("build-summary", output = "summary", sql = "select * from wip_cache"),
                    ),
                    Etl.phase("observe", Etl.probeScratch("probe", probe, "summary")),
                ),
            )

            val observed = harness.readProbe(probe) { read(it, "summary") }

            assertAll(
                {
                    assertFalse(cache.generationExists()) {
                        "the double deletes the generation inside copyOut's finally; the file is still at " +
                            "${cache.generationFile}, so the step never let the lease go"
                    }
                },
                {
                    assertEquals(SUBSET_ROWS, observed.rows) {
                        "a later step read $SUBSET_ROWS rows out of scratch with the generation already " +
                            "deleted - a view over the attached generation would have read nothing"
                    }
                },
                {
                    assertEquals(1, cache.copyOuts.size) {
                        "one copy per attempt, and this run had one attempt; specs were ${cache.copyOuts}"
                    }
                },
                {
                    assertEquals(0, cache.acquireCalls.get() + cache.withSnapshotCalls.get()) {
                        "no lease is held across steps"
                    }
                },
                { assertEquals(0, cache.currentInfoCalls.get()) { "and no generation is inspected behind copyOut" } },
            )
        }
    }

    /**
     * Criteria 2 and 8. **`StepResult` is 0 / 0, and both observation seams see the step.**
     *
     * The pair is asserted as one equality. Asserted as two, an executor that reported
     * `rowsWritten = rowsCopied` would fail only the second and a reader of the failure would be
     * told half the story; and `rowsRead == 0` alone proves nothing at all, because it is the
     * constant every non-pipe executor already returns. What makes 0 / 0 non-vacuous here is the
     * row count read back out of scratch in the same assertion block: an executor that did nothing
     * reports the same pair and copies no rows.
     *
     * The no-row-through-the-JVM property itself is **structural**, not observed: `copyOut(group,
     * spec)` is the API's only channel to a generation, and the throwing double above is what says
     * no other channel was used.
     *
     * This is also the test that closes the inherited `NotImplementedError` trap. That stub was an
     * `Error`, so the step loop's `catch (Exception)` missed it and neither `onStepEnd` nor
     * `stepEnded` ever fired for a cache-copy step. `MicrometerTaskMetrics` is used rather than a
     * recording double because the criterion names the **series**, and measured on micrometer
     * 1.14.2 `increment(0.0)` registers a counter that reads back 0.0 - a step that moved no row
     * still gets its series, which is what stops a hole appearing in a dashboard exactly where an
     * operator looks first.
     */
    @Test
    fun theListenerAndMetricSeamsBothSeeACacheCopyStepReportingNoRowsThroughTheJvm() {
        TaskHarness(root).use { harness ->
            val cache = seededCache()
            harness.cache("wip_cache", cache)
            val registry = SimpleMeterRegistry()
            val listener = EventTrace().listener()
            harness.listener = listener
            harness.metrics = MicrometerTaskMetrics(registry)
            val probe = harness.probeFile("rows")

            harness.runExpectingSuccess(
                Etl.task(
                    "wip-cache-copy",
                    Etl.phase("copy", Etl.cacheCopy("copy-wip", "wip_cache", SUBSET_SQL, "wip_cache")),
                    Etl.phase("observe", Etl.probeScratch("probe", probe, "wip_cache")),
                ),
            )

            val result = listener.result("copy-wip")
            val written = registry.find(ROWS)
                .tags("task", "wip-cache-copy", "phase", "copy", "step", "copy-wip", "direction", "written")
                .counter()

            assertAll(
                {
                    assertEquals(0L to 0L, result.rowsRead to result.rowsWritten) {
                        "no row passes through the JVM in a cache copy, and rowsCopied is lineage rather " +
                            "than throughput (contract 2.4)"
                    }
                },
                {
                    assertEquals(
                        SUBSET_ROWS,
                        harness.readProbe(probe) { read(it, "wip_cache") }.rows,
                    ) { "so the 0 / 0 above is the reading of a step that really copied, not of a no-op" }
                },
                {
                    assertTrue(listener.stepStarts.any { it.step == "copy-wip" }) {
                        "onStepStart never fired; the listener saw ${listener.stepStarts.map { it.step }}"
                    }
                },
                {
                    assertTrue(ListenerCall.STEP_END in listener.calls) {
                        "onStepEnd never fired - the Error path is still there; calls were ${listener.calls}"
                    }
                },
                {
                    assertNotNull(written) {
                        "$ROWS{direction=written} was never registered for this step; the registry held " +
                            "${registry.meters.map { it.id }}"
                    }
                },
                { assertEquals(0.0, written?.count() ?: -1.0) { "and it reads 0, matching the StepResult" } },
                {
                    assertEquals(
                        setOf(
                            Tag.of("task", "wip-cache-copy"), Tag.of("phase", "copy"),
                            Tag.of("step", "copy-wip"), Tag.of("direction", "written"),
                        ),
                        written?.id?.tags?.toSet(),
                    ) { "the label set a host will scrape" }
                },
            )
        }
    }

    /**
     * Criterion 4. **No generation available fails the step at once and never retries.**
     *
     * `NotReadyException` is a plain `RuntimeException`, so the step loop sees it and the run ends
     * as an ordinary failure - which is also what makes the listener and metric call sites fire on
     * this path. It is deliberately **not** transient, because that classification is JDBC-shaped:
     * `copyOut`'s own `waitBudget` is the waiting mechanism, and retrying a cache that has nothing
     * to hand out only turns a fast failure into a slow one.
     *
     * The step states `retries = 3`, and that is asserted rather than assumed: "nothing was
     * retried" is worth nothing against a step that was never allowed a second attempt. It was
     * `CacheCopyStep`'s declared default until E10 made the field `Int?` and both paths resolve an
     * unstated one to 0 (rule 20), so the 3 this criterion needs is now written down.
     * `delaysMillis` is the harness's record of what the engine *asked* the sleeper for, so this
     * assertion costs no wall time.
     *
     * The message is checked for the **group**. `NotReadyException` names the group and not the
     * step - it is thrown by a cache that has never heard of this task - and asserting otherwise
     * would pin a wrapper the contract does not ask for.
     */
    @Test
    fun aCacheWithNoGenerationFailsTheStepImmediatelyAndIsNeverRetried() {
        TaskHarness(root).use { harness ->
            val cache = seededCache()
            cache.failure = { group -> NotReadyException(group, AcquireUnavailableReason.NOT_READY) }
            harness.cache("wip_cache", cache, group = "wip")
            val step = Etl.cacheCopy("copy-wip", "wip_cache", SUBSET_SQL, "wip_cache", retries = 3)

            val outcome = harness.run(Etl.task("wip-cache-copy", Etl.phase("copy", step)))

            assertAll(
                { assertEquals(3, step.retries) { "the no-retry assertion below needs retries in play" } },
                { assertEquals(Outcome.FAILED, outcome.outcome) { "the run failed with ${outcome.failure}" } },
                {
                    assertInstanceOf(NotReadyException::class.java, outcome.failure) {
                        "TaskOutcome.failure must carry the cache's own exception, not a re-wrap: was " +
                            "${outcome.failure}"
                    }
                },
                {
                    assertTrue(outcome.failure?.message?.contains("wip") == true) {
                        "the message names the group it could not serve; was ${outcome.failure?.message}"
                    }
                },
                {
                    assertTrue(harness.delaysMillis.isEmpty()) {
                        "a NotReadyException is not transient under spec 5.3, so no backoff may be " +
                            "requested; the engine asked for ${harness.delaysMillis}"
                    }
                },
                {
                    assertEquals(1, cache.copyOuts.size) {
                        "exactly one attempt reached the cache; it saw ${cache.copyOuts.size}"
                    }
                },
            )
        }
    }

    /**
     * Criterion 6. **Cache SQL binding a variable is rejected; a cast and a colon in a literal are
     * not.**
     *
     * `CopyOutSpec.sql` is a plain `String` with no binding channel, and interpolating a task
     * variable into it would be the injection path the rest of the engine refuses. So the step
     * fails, naming itself, and the author's workaround is to copy the wider subset and filter in
     * the following `materialize`.
     *
     * The accepted half is not decoration. A test that only rejects `:siteCode` passes against
     * `require(!sql.contains(":"))`, which would then reject `qty::varchar` and `'a:b'` - both
     * legal DuckDB, both skipped correctly by JDBI's own parser, and both silently unusable in
     * every task file that needs a cast. `siteCode` is a **defined** task variable here, so the
     * rejection cannot be the "used before its export" error wearing a different hat, and the
     * cache name is a **registered** one, so the step reaches the check under test rather than
     * dying on an unknown cache first.
     */
    @Test
    fun cacheSqlBindingAVariableIsRejectedWhileACastAndAColonInsideALiteralAreNot() {
        TaskHarness(root).use { harness ->
            val cache = seededCache()
            harness.cache("wip_cache", cache)
            val probe = harness.probeFile("literals")

            val rejected = harness.run(
                Etl.task(
                    "bound-cache-sql",
                    Etl.phase(
                        "copy",
                        Etl.cacheCopy(
                            "copy-wip", "wip_cache",
                            "select lot_id, qty from wip where site = :siteCode", "wip_cache",
                        ),
                    ),
                    vars = listOf(Etl.literal("siteCode", "F12")),
                ),
            )

            harness.runExpectingSuccess(
                Etl.task(
                    "cast-cache-sql",
                    Etl.phase(
                        "copy",
                        Etl.cacheCopy(
                            "copy-wip", "wip_cache",
                            "select lot_id, qty::varchar as qty_text, 'a:b' as tag from wip where lot_id < 7",
                            "wip_cache",
                        ),
                    ),
                    Etl.phase("observe", Etl.probeScratch("probe", probe, "wip_cache")),
                ),
            )

            assertAll(
                { assertEquals(Outcome.FAILED, rejected.outcome) { "cache SQL takes no variables (spec 3.6)" } },
                {
                    assertInstanceOf(IllegalArgumentException::class.java, rejected.failure) {
                        "the rejection is a plain argument error, not a driver failure: ${rejected.failure}"
                    }
                },
                {
                    assertTrue(rejected.failure?.message?.contains("copy-wip") == true) {
                        "the message must name the step; was ${rejected.failure?.message}"
                    }
                },
                {
                    assertTrue(cache.copyOuts.single().sql.contains("qty_text")) {
                        "only the accepted SQL reached the cache; it was handed ${cache.copyOuts.map { it.sql }}"
                    }
                },
                {
                    assertEquals(SUBSET_ROWS, harness.readProbe(probe) { read(it, "wip_cache") }.rows) {
                        "a cast and a colon inside a string literal are legal SQL and must copy"
                    }
                },
                {
                    assertTrue(harness.readProbe(probe) { columns(it, "wip_cache") }.contains("qty_text")) {
                        "the cast column reached scratch, so the SQL was not mangled on its way over"
                    }
                },
            )
        }
    }

    /**
     * Criterion 7. **An unknown `cache` name fails the step naming the configured names**, and a
     * registered sibling in the same harness still runs.
     *
     * The pair is mandatory. With an empty binding map every name is unknown, so a lone rejection
     * passes against an engine that has no cache resolution at all - the P8b lesson that the
     * loader learned for rule 3 and the engine had not yet been asked.
     */
    @Test
    fun anUnknownCacheNameFailsTheStepNamingTheConfiguredOnesWhileTheRegisteredNameRuns() {
        TaskHarness(root).use { harness ->
            val cache = seededCache()
            harness.cache("wip_cache", cache)
            val probe = harness.probeFile("known")

            val rejected = harness.run(
                Etl.task(
                    "typo-cache",
                    Etl.phase("copy", Etl.cacheCopy("copy-wip", "typo_cache", SUBSET_SQL, "wip_cache")),
                ),
            )

            harness.runExpectingSuccess(
                Etl.task(
                    "known-cache",
                    Etl.phase("copy", Etl.cacheCopy("copy-wip", "wip_cache", SUBSET_SQL, "wip_cache")),
                    Etl.phase("observe", Etl.probeScratch("probe", probe, "wip_cache")),
                ),
            )

            val message = rejected.failure?.message
            assertAll(
                { assertEquals(Outcome.FAILED, rejected.outcome) { "an unknown cache name is not a no-op" } },
                {
                    assertInstanceOf(IllegalArgumentException::class.java, rejected.failure) {
                        "the same shape jdbi(datasource) already uses: ${rejected.failure}"
                    }
                },
                { assertTrue(message?.contains("copy-wip") == true) { "the message names the step; was $message" } },
                { assertTrue(message?.contains("typo_cache") == true) { "and the name it could not find" } },
                {
                    assertTrue(message?.contains("wip_cache") == true) {
                        "and the configured names, or an operator cannot see the typo; was $message"
                    }
                },
                {
                    assertEquals(SUBSET_ROWS, harness.readProbe(probe) { read(it, "wip_cache") }.rows) {
                        "the registered sibling still copies, so the rejection is attributable to the name"
                    }
                },
            )
        }
    }

    private fun read(probeDb: Connection, dataset: String): Observed = Observed(
        rows = Etl.longAt(probeDb, "select count(*) from $dataset"),
        tables = Etl.strings(probeDb, "select table_name from probe_tables"),
        viewSql = Etl.strings(probeDb, "select sql from probe_views where view_name = '$dataset'").firstOrNull(),
    )

    private fun columns(probeDb: Connection, dataset: String): List<String?> = Etl.strings(
        probeDb,
        "select column_name from information_schema.columns where table_name = '$dataset'",
    )
}
