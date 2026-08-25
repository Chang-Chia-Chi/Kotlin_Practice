package infra.snapshotcache.testkit

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * P2 self-tests for [AccountingFixture], including the plan P2 acceptance criterion: the
 * fixture demonstrably FAILS on a seeded leak, naming the exact generation and operation.
 *
 * The class-level extension exercises the automatic afterEach path on every green test;
 * seeded-leak tests use a local fixture so their deliberate violations stay contained.
 */
class AccountingFixtureTest {

    @JvmField
    @RegisterExtension
    val acc = AccountingFixture()

    private val store get() = acc.store

    private fun AccountingFixture.publishGen(gen: Long): infra.snapshotcache.spi.OpenGeneration {
        this.store.createCandidate(gen).close()
        this.store.promote(gen)
        return this.store.open(gen)
    }

    // ------------------------------------------------------------------ green paths

    @Test
    fun cleanFullLifecycle_passes() {
        acc.publishGen(1)
        store.close(1)
        store.delete(1)
        assertThatCode { acc.verify() }.doesNotThrowAnyException()
        // afterEach re-verifies automatically after this test returns.
    }

    @Test
    fun liveCurrentGeneration_passesViaRegisteredSupplier() {
        acc.publishGen(1)
        acc.currentGeneration = { 1L }
    }

    @Test
    fun leasedNonCurrentGeneration_passesViaRefcountSupplier() {
        acc.publishGen(1)
        acc.publishGen(2)
        acc.currentGeneration = { 2L }
        acc.refCounts = { mapOf(1L to 1) }
    }

    @Test
    fun scriptedFailedCalls_doNotCountInTheEquations() {
        store.failOnGen(StoreOp.PROMOTE, 1)
        store.createCandidate(1).close()
        assertThatThrownBy { store.promote(1) }.isInstanceOf(ScriptedFailureException::class.java)
        store.delete(1) // the abort path a failed promote forces; equation 1 balances
        assertThatCode { acc.verify() }.doesNotThrowAnyException()
    }

    // ------------------------------------------------------------------ seeded leaks (plan P2 acceptance)

    @Test
    fun seededCandidateLeak_failsEquation1_namingGenerationAndOperation() {
        val local = AccountingFixture()
        local.store.createCandidate(7).close() // never promoted, never deleted

        assertThatThrownBy { local.verify() }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("equation 1")
            .hasMessageContaining("createCandidate leaked")
            .hasMessageContaining("[7]")
    }

    @Test
    fun seededOpenLeak_failsEquation3_namingGeneration() {
        val local = AccountingFixture()
        local.publishGen(9) // opened, but neither current nor leased at test end

        assertThatThrownBy { local.verify() }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("equation 3")
            .hasMessageContaining("close leaked")
            .hasMessageContaining("[9]")
    }

    @Test
    fun seededUndeletedFile_failsEquation4_namingGeneration() {
        val local = AccountingFixture()
        local.publishGen(4)
        local.store.close(4) // detached, but the file was never deleted

        assertThatThrownBy { local.verify() }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("equation 4")
            .hasMessageContaining("delete leaked")
            .hasMessageContaining("[4]")
    }

    @Test
    fun claimedCurrentThatWasNeverOpened_failsEquation3() {
        val local = AccountingFixture()
        local.currentGeneration = { 5L }

        assertThatThrownBy { local.verify() }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("equation 3")
            .hasMessageContaining("never opened")
            .hasMessageContaining("[5]")
    }

    @Test
    fun seededConnectionLeak_failsWithCreationStackPinpointingThisTest() {
        val local = AccountingFixture()
        val opened = local.publishGen(3)
        opened.connection() // issued here, never closed
        local.currentGeneration = { 3L }

        assertThatThrownBy { local.verify() }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("unclosed connection")
            .hasMessageContaining("read connection, gen=3")
            .hasMessageContaining("AccountingFixtureTest") // the creation stack names the leaking line
    }

    @Test
    fun afterEach_runsVerify_soLeaksFailTheTestAutomatically() {
        val local = AccountingFixture()
        local.store.createCandidate(2).close()

        assertThatThrownBy { local.afterEach(null) }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("equation 1")
    }
}
