package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import io.microraft.statemachine.StateMachine
import java.util.function.Consumer

/**
 * The one MicroRaft state machine of a CP member: it owns log time (CP spec 5) and hands each
 * committed [Command.Cp] to the primitive that answers it. The primitives never see a clock or
 * the log; they see the command and the stamp of the entry carrying it, so every member applies
 * the same entry to the same effect (C23).
 *
 * The operations are [CpOp]s wrapping [Command.Cp] values; the results are ordinary
 * [dynacache.engine.Reply]s, so the CP engine hands a committed reply straight back to the
 * caller with nothing to translate.
 */
class CpStateMachine(
    /** The term this member is in, read when it starts leading one. */
    private val currentTerm: () -> Int = { 0 },
    /** Told each term whose first entry was just applied. */
    private val onTermApplied: (Int) -> Unit = {},
) : StateMachine {

    val longs = AtomicLongStateMachine()
    val locks = FencedLockStateMachine()

    /** Log time as this member sees it: the stamp of the last applied entry. */
    @Volatile
    var lastAppliedTs: Long = 0
        private set

    /** The committed, unexpired counter at [key] at this member's applied index, or null. */
    fun valueOf(key: Key): Long? = longs.valueOf(key, lastAppliedTs)

    override fun runOperation(commitIndex: Long, operation: Any): Any? = when (operation) {
        is CpOp -> {
            lastAppliedTs = operation.ts
            when (val command = operation.command) {
                is Command.Cp.AtomicLong -> longs.apply(command, lastAppliedTs)
                is Command.Cp.FencedLock -> locks.apply(command, lastAppliedTs)
            }
        }
        is TtlTick -> {
            lastAppliedTs = operation.ts
            longs.sweep(lastAppliedTs)
            locks.sweep(lastAppliedTs)
            null
        }
        is NewTerm -> {
            onTermApplied(operation.term)
            null
        }
        else -> null // MicroRaft's own internal entries change no state.
    }

    override fun getNewTermOperation(): Any = NewTerm(currentTerm())

    /** One chunk holding everything; chunking a large state is T45's business. */
    override fun takeSnapshot(commitIndex: Long, chunkConsumer: Consumer<Any>) =
        chunkConsumer.accept(Snapshot(lastAppliedTs, longs.snapshot(), locks.snapshot()))

    override fun installSnapshot(commitIndex: Long, chunks: List<Any>) {
        chunks.forEach {
            val snapshot = it as Snapshot
            lastAppliedTs = snapshot.lastAppliedTs
            longs.restore(snapshot.counters)
            locks.restore(snapshot.locks)
        }
    }

    private data class Snapshot(
        val lastAppliedTs: Long,
        val counters: Map<Key, AtomicLongStateMachine.Counter>,
        val locks: Map<Key, FencedLockStateMachine.Lock>,
    )
}
