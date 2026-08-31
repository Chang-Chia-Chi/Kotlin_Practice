package infra.snapshotcache.testkit

import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.spi.Candidate
import infra.snapshotcache.spi.GenerationStore
import infra.snapshotcache.spi.OpenGeneration
import java.sql.Connection

enum class StoreOp { CREATE_CANDIDATE, PROMOTE, OPEN, CLOSE, DELETE, LIST_ON_DISK, COPY_OUT }

/** One recorded store call. [failed] marks a scripted failure; state was not mutated for it. */
data class StoreCall(
    val op: StoreOp,
    val gen: Long?,
    val detail: String? = null,
    val failed: Boolean = false,
)

/** Thrown by a matching failure script, e.g. "the 3rd close throws". */
class ScriptedFailureException(message: String) : RuntimeException(message)

/**
 * In-memory [GenerationStore] fake: records every call with arguments and
 * order, models the on-disk candidate/promoted/opened states with strict transition
 * guards, and supports directly-scripted one-shot failures - a general-purpose mocking
 * layer is on the do-not-build list.
 *
 * Recording is thread-safe under one monitor for the P5 stress suite; determinism comes
 * from the callers' interleaving control, never from timing here.
 */
class InMemoryGenerationStore : GenerationStore {

    private enum class FileState { CANDIDATE, PROMOTED }
    private data class Script(val op: StoreOp, val nth: Int?, val gen: Long?)

    private val lock = Any()
    private val calls = mutableListOf<StoreCall>()
    private val files = mutableMapOf<Long, FileState>()
    private val opened = mutableSetOf<Long>()
    private val detached = mutableSetOf<Long>()
    private val scripts = mutableListOf<Script>()
    private val attempts = mutableMapOf<StoreOp, Int>()

    val tracker = ConnectionTracker()

    /** Returned by [copyOut]; set by tests that assert row counts. */
    var copyOutRows: Long = 0

    /** Returned by [OpenGeneration.fileBytes]. */
    var fileBytesPerGeneration: Long = 100

    // ------------------------------------------------------------------ failure scripting

    /** The [nth] call of [op] (1-based, counting attempts) throws once; the script is then spent. */
    fun failOnNth(op: StoreOp, nth: Int) {
        synchronized(lock) { scripts += Script(op, nth, null) }
    }

    /** The next call of [op] for generation [gen] throws once; the script is then spent. */
    fun failOnGen(op: StoreOp, gen: Long) {
        synchronized(lock) { scripts += Script(op, null, gen) }
    }

    // ------------------------------------------------------------------ recorded state

    fun calls(): List<StoreCall> = synchronized(lock) { calls.toList() }

    fun openedGenerations(): Set<Long> = synchronized(lock) { opened.toSet() }

    /** Every generation with a file, candidate or promoted. */
    fun generationsOnDisk(): Set<Long> = synchronized(lock) { files.keys.toSet() }

    // ------------------------------------------------------------------ GenerationStore

    override fun createCandidate(gen: Long): Candidate {
        synchronized(lock) {
            check(gen !in files) { "createCandidate($gen): file already exists" }
            attemptAndMaybeFail(StoreOp.CREATE_CANDIDATE, gen)
            files[gen] = FileState.CANDIDATE
            calls += StoreCall(StoreOp.CREATE_CANDIDATE, gen)
        }
        return FakeCandidate(gen)
    }

    override fun promote(gen: Long) {
        synchronized(lock) {
            check(files[gen] == FileState.CANDIDATE) { "promote($gen): no candidate on disk (state=${files[gen]})" }
            attemptAndMaybeFail(StoreOp.PROMOTE, gen)
            files[gen] = FileState.PROMOTED
            calls += StoreCall(StoreOp.PROMOTE, gen)
        }
    }

    override fun open(gen: Long): OpenGeneration {
        synchronized(lock) {
            check(files[gen] == FileState.PROMOTED) { "open($gen): not a promoted file (state=${files[gen]})" }
            check(gen !in opened) { "open($gen): already opened" }
            attemptAndMaybeFail(StoreOp.OPEN, gen)
            opened += gen
            detached -= gen
            calls += StoreCall(StoreOp.OPEN, gen)
        }
        return FakeOpenGeneration(gen)
    }

    override fun close(gen: Long) {
        synchronized(lock) {
            // Idempotent per the SPI contract: a second detach of an already-detached
            // generation is a no-op, so reclaim can retry close + delete as one unit.
            // Never-opened is still a guard violation, not an idempotent call.
            if (gen in detached) return
            check(gen in opened) { "close($gen): not opened" }
            attemptAndMaybeFail(StoreOp.CLOSE, gen)
            opened -= gen
            detached += gen
            calls += StoreCall(StoreOp.CLOSE, gen)
        }
    }

    override fun delete(gen: Long) {
        synchronized(lock) {
            val state = checkNotNull(files[gen]) { "delete($gen): no file on disk" }
            attemptAndMaybeFail(StoreOp.DELETE, gen)
            files.remove(gen)
            detached -= gen
            calls += StoreCall(
                StoreOp.DELETE,
                gen,
                detail = if (state == FileState.CANDIDATE) "candidate" else "promoted",
            )
        }
    }

    override fun listOnDisk(): List<Long> = synchronized(lock) {
        attemptAndMaybeFail(StoreOp.LIST_ON_DISK, null)
        calls += StoreCall(StoreOp.LIST_ON_DISK, null)
        files.keys.sorted()
    }

    override fun copyOut(opened: OpenGeneration, spec: CopyOutSpec): Long = synchronized(lock) {
        attemptAndMaybeFail(StoreOp.COPY_OUT, opened.generation)
        calls += StoreCall(StoreOp.COPY_OUT, opened.generation, detail = spec.targetTable)
        copyOutRows
    }

    /** Counts the attempt; on a script match, records the failed call, spends the script, throws. */
    private fun attemptAndMaybeFail(op: StoreOp, gen: Long?) {
        val n = (attempts[op] ?: 0) + 1
        attempts[op] = n
        val hit = scripts.firstOrNull {
            it.op == op && (it.nth == null || it.nth == n) && (it.gen == null || it.gen == gen)
        } ?: return
        scripts.remove(hit)
        calls += StoreCall(op, gen, detail = "scripted", failed = true)
        throw ScriptedFailureException("scripted failure: $op(gen=$gen), attempt #$n")
    }

    private inner class FakeCandidate(override val generation: Long) : Candidate {
        private val write = lazy { tracker.issue("write connection, candidate gen=$generation") }

        override fun connection(): Connection = write.value.connection

        /** Idempotent and never throws, even when abandoned (P0 progress note). */
        override fun close() {
            if (write.isInitialized()) write.value.connection.close()
        }
    }

    private inner class FakeOpenGeneration(override val generation: Long) : OpenGeneration {
        override fun connection(): Connection = tracker.issue("read connection, gen=$generation").connection

        override fun fileBytes(): Long = fileBytesPerGeneration
    }
}
