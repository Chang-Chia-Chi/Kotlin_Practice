package infra.snapshotcache.duckdb

import infra.snapshotcache.api.VerifyConfig
import infra.snapshotcache.spi.GateOutcome
import infra.snapshotcache.spi.VerifyGate
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * Adapter-level regression tests for the 2026-08-28 code-review fix pass, on real DuckDB
 * 1.1.3: idempotent detach (H1), identifier quoting in the verify gate (H3), and the
 * pruning of closed connections from the per-generation tracking list (M3).
 */
internal class DuckDbStoreReviewFixTest {

    @TempDir
    lateinit var groupDir: Path

    @TempDir
    lateinit var tempDir: Path

    private lateinit var store: DuckDbGenerationStore

    @BeforeEach
    fun openStore() {
        store = DuckDbGenerationStore(groupDir, tempDir, "500MB")
    }

    @AfterEach
    fun closeStore() {
        store.close()
    }

    /** Builds, promotes and returns [gen] with one table per entry of [tables] (name to DDL rows). */
    private fun buildAndPromote(gen: Long, tables: Map<String, String>) {
        store.createCandidate(gen).use { candidate ->
            candidate.connection().createStatement().use { st ->
                for ((name, rows) in tables) {
                    st.execute("""CREATE TABLE "$name" (id INTEGER, v VARCHAR)""")
                    st.execute("""INSERT INTO "$name" VALUES $rows""")
                }
            }
        }
        store.promote(gen)
    }

    // ------------------------------------------------------------------ H1

    @Test
    fun close_isIdempotent_soAReclaimRetryAfterAFailedDeleteCanFinish() {
        buildAndPromote(1, mapOf("t_a" to "(1, 'one'), (2, 'two')"))
        store.open(1)
        store.close(1)

        // The reclaim pass retries close + delete as one unit after a transient delete
        // failure. A second DETACH is a catalog error, so close must no-op instead.
        assertThatCode { store.close(1) }.doesNotThrowAnyException()
        assertThatCode { store.delete(1) }.doesNotThrowAnyException()
        assertThat(store.listOnDisk()).isEmpty()

        // Never-opened is the same no-op: reclaim never reaches it, and the abort path
        // asks only after a successful open.
        assertThatCode { store.close(99) }.doesNotThrowAnyException()
    }

    // ------------------------------------------------------------------ H3

    @Test
    fun verifyGate_reservedWordAndMixedCaseTableNames_pass_insteadOfFailingEveryRound() {
        // `order` is a reserved word and `MixedCase` only resolves quoted: unquoted, both
        // are parse errors that the gate would report as bad data, wedging every round.
        buildAndPromote(
            1,
            mapOf(
                "order" to "(1, 'one'), (2, 'two')",
                "MixedCase" to "(1, 'one'), (2, 'two'), (3, 'three')",
            ),
        )
        val opened = store.open(1)

        val verdict = VerifyGate(VerifyConfig(requiredNonNull = listOf("v")), emptyList())
            .verify(opened, previous = null)

        assertThat(verdict).isInstanceOf(GateOutcome.Passed::class.java)
        assertThat((verdict as GateOutcome.Passed).rowCounts)
            .isEqualTo(mapOf("order" to 2L, "MixedCase" to 3L))
    }

    // ------------------------------------------------------------------ M3

    @Test
    fun issuingConnectionsOnALongLivedGeneration_doesNotGrowTheTrackingListWithoutBound() {
        buildAndPromote(1, mapOf("t_a" to "(1, 'one')"))
        val opened = store.open(1)

        // The shape of a generation that stays current for hours while consumers acquire
        // per request: before the fix every connection stayed listed after being closed.
        repeat(50) { opened.connection().close() }

        assertThat(store.openIssuedConnections()).isZero()
        assertThat(store.trackedConnections(1))
            .describedAs("closed entries are pruned on append, so at most the last one lingers")
            .isLessThanOrEqualTo(1)
    }
}
