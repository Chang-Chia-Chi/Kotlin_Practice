package infra.snapshotcache.testkit

import infra.snapshotcache.api.CopyOutSpec
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * P2 self-tests for [InMemoryGenerationStore]: call recording with arguments and order,
 * strict lifecycle guards, and the two scripted-failure forms of plan P2. Deterministic;
 * no sleeps.
 */
class InMemoryGenerationStoreTest {

    private val store = InMemoryGenerationStore()

    private fun buildPromoted(gen: Long) {
        store.createCandidate(gen).close()
        store.promote(gen)
    }

    @Test
    fun happyLifecycle_recordsEveryCallWithArgumentsInOrder() {
        val candidate = store.createCandidate(1)
        assertThat(candidate.generation).isEqualTo(1)
        candidate.close()
        store.promote(1)
        val open = store.open(1)
        assertThat(open.generation).isEqualTo(1)
        assertThat(open.fileBytes()).isEqualTo(100)
        assertThat(store.listOnDisk()).containsExactly(1L)
        store.close(1)
        store.delete(1)

        assertThat(store.calls().map { it.op }).containsExactly(
            StoreOp.CREATE_CANDIDATE, StoreOp.PROMOTE, StoreOp.OPEN,
            StoreOp.LIST_ON_DISK, StoreOp.CLOSE, StoreOp.DELETE,
        )
        assertThat(store.calls().map { it.gen }).containsExactly(1L, 1L, 1L, null, 1L, 1L)
        assertThat(store.calls().last().detail).isEqualTo("promoted")
        assertThat(store.generationsOnDisk()).isEmpty()
        assertThat(store.openedGenerations()).isEmpty()
    }

    @Test
    fun guards_rejectOutOfOrderTransitions() {
        assertThatThrownBy { store.promote(9) }.isInstanceOf(IllegalStateException::class.java)
        assertThatThrownBy { store.open(9) }.isInstanceOf(IllegalStateException::class.java)
        assertThatThrownBy { store.close(9) }.isInstanceOf(IllegalStateException::class.java)
        assertThatThrownBy { store.delete(9) }.isInstanceOf(IllegalStateException::class.java)

        store.createCandidate(1).close()
        assertThatThrownBy { store.createCandidate(1) }.isInstanceOf(IllegalStateException::class.java)
        assertThatThrownBy { store.open(1) }
            .describedAs("a candidate is not openable before promote")
            .isInstanceOf(IllegalStateException::class.java)

        store.promote(1)
        store.open(1)
        assertThatThrownBy { store.open(1) }.isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun candidateClose_idempotentNeverThrows_andClosesTheWriteConnection() {
        val candidate = store.createCandidate(5)
        val conn = candidate.connection()
        assertThat(conn.isClosed).isFalse()
        assertThatCode {
            candidate.close()
            candidate.close()
        }.doesNotThrowAnyException()
        assertThat(conn.isClosed).isTrue()

        // An abandoned candidate that never asked for a connection closes without issuing one.
        val untouched = store.createCandidate(6)
        assertThatCode { untouched.close() }.doesNotThrowAnyException()
        assertThat(store.tracker.unclosed()).isEmpty()
    }

    @Test
    fun scriptedFailure_nthClose_throwsOnceThenIsSpent() {
        (1L..3L).forEach { buildPromoted(it); store.open(it) }
        store.failOnNth(StoreOp.CLOSE, 2)

        store.close(1)
        assertThatThrownBy { store.close(2) }.isInstanceOf(ScriptedFailureException::class.java)

        assertThat(store.openedGenerations()).describedAs("failed close mutates nothing").contains(2L)
        val failed = store.calls().single { it.failed }
        assertThat(failed.op).isEqualTo(StoreOp.CLOSE)
        assertThat(failed.gen).isEqualTo(2L)

        assertThatCode { store.close(2) }.describedAs("one-shot: the retry succeeds").doesNotThrowAnyException()
        store.close(3)
    }

    @Test
    fun scriptedFailure_promoteOfSpecificGen_throwsOnceThenIsSpent() {
        store.createCandidate(1).close()
        store.createCandidate(2).close()
        store.failOnGen(StoreOp.PROMOTE, 2)

        store.promote(1)
        assertThatThrownBy { store.promote(2) }.isInstanceOf(ScriptedFailureException::class.java)
        assertThat(store.listOnDisk()).describedAs("failed promote leaves the candidate").containsExactly(1L, 2L)
        assertThatCode { store.promote(2) }.doesNotThrowAnyException()
    }

    @Test
    fun delete_ofCandidate_isRecordedAsCandidateDelete() {
        store.createCandidate(7).close()
        store.delete(7)
        assertThat(store.calls().last()).isEqualTo(StoreCall(StoreOp.DELETE, 7, detail = "candidate"))
        assertThat(store.generationsOnDisk()).isEmpty()
    }

    @Test
    fun copyOut_recordsGenerationAndTargetTable_returnsConfiguredRows() {
        buildPromoted(1)
        val opened = store.open(1)
        store.copyOutRows = 42
        val target = ConnectionTracker().issue("copy-out target").connection

        val rows = store.copyOut(opened, CopyOutSpec("SELECT * FROM t_unified", "staging", target))

        assertThat(rows).isEqualTo(42)
        assertThat(store.calls().last()).isEqualTo(StoreCall(StoreOp.COPY_OUT, 1, detail = "staging"))
    }

    @Test
    fun trackedConnection_supportsOnlyCloseAndIsClosed() {
        buildPromoted(1)
        val conn = store.open(1).connection()
        assertThat(conn.isClosed).isFalse()
        assertThatThrownBy { conn.createStatement() }.isInstanceOf(UnsupportedOperationException::class.java)
        conn.close()
        assertThat(conn.isClosed).isTrue()
        assertThat(store.tracker.unclosed()).isEmpty()
    }
}
