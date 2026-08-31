package infra.snapshotcache.core

import infra.snapshotcache.api.RefreshOutcome
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.RowCountDeltaConfig
import infra.snapshotcache.api.VerifyConfig
import infra.snapshotcache.spi.Candidate
import infra.snapshotcache.spi.GenerationStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.Connection

/**
 * P4b: the deferred second half of P4 (its pre-authorized split).
 *
 * Built-in verify-rule behavior, each failing rule asserted to
 * carry its exact published label and a non-generic detail, abort the round
 * with current untouched and the candidate cleaned, and stay recoverable. Plus the two
 * RefreshCycle follow-ups: candidate-connection()-throws -> DISK_ERROR + emergency GC,
 * and the round-entry shutdown short-circuit.
 *
 * `non_empty` and `readable` have no disable knob by construction (computed vals; P0's
 * config test pins that), so their "cannot be disabled" flag needs no runtime case here.
 *
 * SQL is answered by QueryScript heuristics/patterns, never exact strings: the
 * engineer's formulations are FREE. No sleeps. AccountingFixture is
 * registered by the shared base with its suppliers wired to the registry.
 */
internal class VerifyRulesTest : RefreshCycleTestBase() {

    // ------------------------------------------------------------------ shared assertions

    /** Failing rule: exact label, non-generic detail, event fired, no generation on the outcome. */
    private fun assertVerifyRejected(out: RefreshOutcome, rule: String, vararg detailBits: String) {
        assertThat(out.result).isEqualTo(RefreshResult.VERIFY_FAILED)
        assertThat(out.generation).describedAs("generation only on SUCCESS").isNull()
        assertThat(out.detail).describedAs("outcome detail names the failing rule").contains(rule)
        val (eventRule, eventDetail) = events.verifyFailures.last()
        assertThat(eventRule).describedAs("exact spec 8.1 / 12.2 rule label").isEqualTo(rule)
        assertThat(eventDetail)
            .describedAs("never just 'verification failed' (spec 8.5)")
            .isNotBlank()
            .isNotEqualToIgnoringCase("verification failed")
        for (bit in detailBits) {
            assertThat(eventDetail).describedAs("detail must name the offender").contains(bit)
        }
        assertThat(events.finished.last()).isEqualTo(RefreshResult.VERIFY_FAILED to null)
    }

    /** After an aborted round: current untouched, candidate closed + deleted, no leaked connections (I7 shape). */
    private fun assertCandidateCleaned(expectedCurrent: Long?) {
        assertThat(registry.current()).describedAs("current pointer untouched").isEqualTo(expectedCurrent)
        val survivors = setOfNotNull(expectedCurrent)
        assertThat(store.generationsOnDisk()).describedAs("candidate file deleted").isEqualTo(survivors)
        assertThat(store.openedGenerations()).describedAs("candidate detached").isEqualTo(survivors)
        assertThat(store.tracker.unclosed()).describedAs("no leaked connections").isEmpty()
    }

    // ------------------------------------------------------------------ non_empty

    @Test
    fun nonEmpty_zeroRowTable_failsNamingTheTable_currentUntouched_nextRoundSucceeds() {
        val c = cycle()
        runSuccess(c) // gen 1, healthy

        script.tables["t_b"] = 0L
        assertVerifyRejected(c.runOnce(), "non_empty", "t_b")
        assertCandidateCleaned(1L)

        script.tables["t_b"] = 20L
        assertThat(runSuccess(c)).describedAs("return to a usable state (spec 17.8)").isEqualTo(3L)
    }

    @Test
    fun nonEmpty_candidateWithNoTablesAtAll_failsOnColdStart_nothingPublished() {
        script.tables.clear()
        assertVerifyRejected(cycle().runOnce(), "non_empty")
        assertCandidateCleaned(null)

        script.tables["t_a"] = 10L
        script.tables["t_b"] = 20L
        assertThat(runSuccess(cycle())).isEqualTo(2L)
    }

    // ------------------------------------------------------------------ key_unique

    @Test
    fun keyUnique_duplicateIds_failNamingTheTable_disablingTheKnobLetsTheSameDataPublish() {
        val c = cycle()
        runSuccess(c) // gen 1

        script.duplicateKeys["t_a"] = 3L
        assertVerifyRejected(c.runOnce(), "key_unique", "t_a")
        assertCandidateCleaned(1L)

        // Config gating: `verify.keyUnique = false` skips the rule; the duplicates publish.
        val relaxed = cycle(cfg = config.copy(verify = VerifyConfig(keyUnique = false)))
        assertThat(runSuccess(relaxed)).isEqualTo(3L)
        assertThat(registry.currentInfo()!!.rowCounts).isEqualTo(mapOf("t_a" to 10L, "t_b" to 20L))
    }

    // ------------------------------------------------------------------ required_non_null

    @Test
    fun requiredNonNull_tableQualifiedEntry_checksOnlyThatTable() {
        // NULLs live in t_b (QueryScript keys are matched as SQL substrings, so the
        // table name scopes the answer to that table's IS NULL probe).
        script.nullCounts["t_b"] = 4L

        // An entry qualified to t_a must not look at t_b: the round publishes.
        val checkOther = cycle(cfg = config.copy(verify = VerifyConfig(requiredNonNull = listOf("t_a.customer_id"))))
        runSuccess(checkOther) // gen 1

        // The same data with the entry qualified to t_b fails, naming table and column.
        val checkHit = cycle(cfg = config.copy(verify = VerifyConfig(requiredNonNull = listOf("t_b.customer_id"))))
        assertVerifyRejected(checkHit.runOnce(), "required_non_null", "t_b", "customer_id")
        assertCandidateCleaned(1L)
    }

    @Test
    fun requiredNonNull_bareColumnEntry_checksEveryTable() {
        val c = cycle(cfg = config.copy(verify = VerifyConfig(requiredNonNull = listOf("customer_id"))))

        script.nullCounts["t_a"] = 1L
        assertVerifyRejected(c.runOnce(), "required_non_null", "t_a", "customer_id")
        assertCandidateCleaned(null)

        script.nullCounts.clear()
        script.nullCounts["t_b"] = 2L
        assertVerifyRejected(c.runOnce(), "required_non_null", "t_b", "customer_id")
        assertCandidateCleaned(null)

        script.nullCounts.clear()
        assertThat(runSuccess(c)).isEqualTo(3L)
    }

    @Test
    fun requiredNonNull_defaultEmptyColumnList_nullsPublish() {
        // Config gating: the column list defaults to empty (deployment config fills it later),
        // so NULLs anywhere pass the default gate.
        script.nullCounts["t_a"] = 5L
        script.nullCounts["t_b"] = 5L
        runSuccess(cycle())
    }

    // ------------------------------------------------------------------ row_count_delta

    @Test
    fun rowCountDelta_defaultOff_wildDeltasPublish_rowCountsFromVerifyLandInCurrentInfo() {
        val c = cycle()
        runSuccess(c) // gen 1: t_a=10, t_b=20

        script.tables["t_a"] = 1L // -90%
        script.tables["t_b"] = 200L // +900%
        assertThat(runSuccess(c)).describedAs("off by default: observe, never gate (D14)").isEqualTo(2L)
        assertThat(registry.currentInfo()!!.rowCounts)
            .describedAs("verify-stage counts become GenerationInfo.rowCounts")
            .isEqualTo(mapOf("t_a" to 1L, "t_b" to 200L))
    }

    @Test
    fun rowCountDelta_enabled_decreaseBeyondItsRatioFails_whileALargerIncreaseIsFine() {
        val c = cycle(
            cfg = config.copy(verify = VerifyConfig(rowCountDelta = RowCountDeltaConfig(enabled = true))),
        )
        runSuccess(c) // gen 1: cold start has no previous, so the enabled gate is skipped

        // t_a -30% trips maxDecreaseRatio=0.20; t_b +25% is well under maxIncreaseRatio=1.00.
        script.tables["t_a"] = 7L
        script.tables["t_b"] = 25L
        assertVerifyRejected(c.runOnce(), "row_count_delta", "t_a")
        assertCandidateCleaned(1L)

        // Same +25% increase alone publishes: the decrease was the sole offender.
        script.tables["t_a"] = 10L
        assertThat(runSuccess(c)).isEqualTo(3L)
    }

    @Test
    fun rowCountDelta_enabled_increaseBeyondItsRatioFails_whileASmallDecreaseIsFine() {
        val c = cycle(
            cfg = config.copy(verify = VerifyConfig(rowCountDelta = RowCountDeltaConfig(enabled = true))),
        )
        runSuccess(c) // gen 1: t_a=10, t_b=20

        // t_b +125% trips maxIncreaseRatio=1.00; t_a -10% is under maxDecreaseRatio=0.20.
        // A 95%-magnitude move is a pass upward but (per the sibling test) a 30% move
        // already fails downward: the two directions are gated by separate ratios.
        script.tables["t_a"] = 9L
        script.tables["t_b"] = 45L
        assertVerifyRejected(c.runOnce(), "row_count_delta", "t_b")
        assertCandidateCleaned(1L)

        script.tables["t_b"] = 39L // +95% vs the still-current gen 1
        assertThat(runSuccess(c)).isEqualTo(3L)
    }

    // ------------------------------------------------------------------ discovery

    @Test
    fun discovery_listsBaseTablesOnly_unionViewWithDuplicateIdsIsExemptFromKeyUnique() {
        // Catalog stub: a discovery query that filters to base tables sees t_a/t_b; one
        // that does not also sees the t_unified union view (heuristic filter detection,
        // not exact SQL). The view unions both tables, so its ids are duplicated: were
        // it discovered, key_unique (30 vs 25) would reject the candidate and the
        // rowCounts assertion below would see a t_unified entry.
        script.onPattern("information_schema|show\\s+tables|duckdb_tables|pragma") { sql ->
            if (Regex("base[\\s_]*table", RegexOption.IGNORE_CASE).containsMatchIn(sql)) {
                script.tableListRows()
            } else {
                StubRows(
                    listOf("table_name", "name"),
                    (script.tables.keys + "t_unified").map { listOf<Any?>(it, it) },
                )
            }
        }
        script.onPattern("t_unified") { sql ->
            StubRows.single(if ("distinct" in sql.lowercase()) 25L else 30L)
        }

        runSuccess(cycle())
        assertThat(registry.currentInfo()!!.rowCounts)
            .describedAs("only BASE TABLEs are verified and counted; the view is exempt")
            .isEqualTo(mapOf("t_a" to 10L, "t_b" to 20L))
    }

    // ------------------------------------------------------------------ candidate connection() throws (disk row; lead ruling 5)

    /**
     * [InMemoryGenerationStore.failOnGen] cannot script this seam - the store call
     * succeeds and the returned [Candidate]'s connection() is what fails - so the store
     * is wrapped: delegate everything, refuse the write connection on demand.
     */
    private class CandidateConnectionRefusingStore(
        private val delegate: GenerationStore,
    ) : GenerationStore by delegate {
        @Volatile
        var refuseConnections = false

        override fun createCandidate(gen: Long): Candidate {
            val real = delegate.createCandidate(gen)
            return object : Candidate {
                override val generation: Long = real.generation
                override fun connection(): Connection {
                    if (refuseConnections) throw RuntimeException("no space left on device: cannot open write connection")
                    return real.connection()
                }

                override fun close() = real.close()
            }
        }
    }

    @Test
    fun candidateConnectionThrows_classifiedDiskError_runsEmergencyGc_nextRoundSucceeds() {
        val wrapped = CandidateConnectionRefusingStore(stubStore)
        val c = RefreshCycle(
            group = group,
            registry = registry,
            store = wrapped,
            source = source,
            config = config,
            events = events,
            checks = emptyList(),
            clock = clock,
            hooks = hooks,
        )
        runSuccess(c) // gen 1

        // A reclaimable non-current generation for the emergency GC to find (as in the
        // P4 disk-error test): lease gen 1 across the next round, then release.
        val lease = checkNotNull(registry.tryAcquire("holder"))
        runSuccess(c) // gen 2
        registry.release(lease)
        assertThat(store.generationsOnDisk()).contains(1L, 2L)

        wrapped.refuseConnections = true
        val out = c.runOnce()

        assertThat(out.result)
            .describedAs("the candidate is a store product: its connection failing is a disk problem")
            .isEqualTo(RefreshResult.DISK_ERROR)
        assertThat(out.generation).isNull()
        assertThat(out.detail).describedAs("detail names the failure").contains("no space left")
        assertThat(events.finished.last()).isEqualTo(RefreshResult.DISK_ERROR to null)
        assertThat(events.reclaimedGens).describedAs("emergency GC ran during the failed round").contains(1L)
        assertThat(store.generationsOnDisk()).describedAs("candidate deleted, stale gen collected").containsExactly(2L)
        assertThat(registry.current()).describedAs("current keeps serving").isEqualTo(2L)
        assertThat(store.tracker.unclosed()).isEmpty()

        wrapped.refuseConnections = false
        assertThat(runSuccess(c)).describedAs("return to a usable state (spec 17.8)").isEqualTo(4L)
    }

    // ------------------------------------------------------------------ round-entry shutdown short-circuit

    @Test
    fun shutdownBeforeRoundEntry_returnsShutdownAborted_withZeroStoreCalls() {
        registry.beginShutdown()

        val out = cycle().runOnce()

        assertThat(out.result).isEqualTo(RefreshResult.SHUTDOWN_ABORTED)
        assertThat(out.generation).isNull()
        assertThat(store.calls())
            .describedAs("the short-circuit performs no store I/O at all")
            .isEmpty()
        assertThat(events.finished).containsExactly(RefreshResult.SHUTDOWN_ABORTED to null)
        assertThat(registry.current()).describedAs("nothing was published").isNull()
    }
}
