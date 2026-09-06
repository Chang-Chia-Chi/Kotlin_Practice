package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import io.microraft.statemachine.StateMachine
import java.util.function.Consumer

/**
 * The one MicroRaft state machine of a CP member: it owns log time (CP spec 5) and hands each
 * committed [Command.Cp] to the primitive that answers it. The primitives never see a clock or
 * the log; they see the command and the stamp of the entry carrying it, so every member applies
 * the same entry to the same effect (C23).
 *
 * A [CpPrimitive] keeps its own table and its own bytes, so everything the composite does to all
 * of them alike - the TTL sweep, the session-close cascade and the snapshot - is a loop over
 * [primitives], and only routing a command names one.
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

    private val longs = AtomicLongStateMachine()
    private val locks = FencedLockStateMachine()
    private val semaphores = SemaphoreStateMachine()
    private val latches = CountDownLatchStateMachine()
    private val references = AtomicReferenceStateMachine()
    private val sessions = SessionRegistry()

    /** Every primitive this member holds, in the order a snapshot lists them. */
    private val primitives: List<CpPrimitive> = listOf(longs, locks, semaphores, latches, references, sessions)

    /** Log time as this member sees it: the stamp of the last applied entry. */
    @Volatile
    var lastAppliedTs: Long = 0
        private set

    /**
     * What [command] answers at this member's applied index, appending nothing: the local view a
     * follower can be asked for, and the only way past the composite into a primitive's table.
     * Read verbs only - a writing verb would write here and nobody would replicate it.
     */
    fun read(command: Command.Cp): Reply = answer(command, lastAppliedTs)

    /** The committed, unexpired counter at [key] at this member's applied index, or null. */
    fun valueOf(key: Key): Long? = (read(Command.Cp.LongGet(key)) as? Reply.Integer)?.value

    override fun runOperation(commitIndex: Long, operation: Any): Any? = when (operation) {
        is CpOp -> {
            lastAppliedTs = operation.ts
            val command = operation.command
            if (command is Command.Cp.Sessioned && !sessions.isAlive(command.session)) {
                Reply.Error("NOSESSION", "session ${command.session} expired or never created")
            } else {
                answer(command, lastAppliedTs)
            }
        }
        is SessionClosed -> {
            lastAppliedTs = operation.ts
            closeSession(operation.session)
            null
        }
        is TtlTick -> {
            lastAppliedTs = operation.ts
            primitives.forEach { it.sweep(lastAppliedTs) }
            null
        }
        is NewTerm -> {
            onTermApplied(operation.term)
            null
        }
        else -> null // MicroRaft's own internal entries change no state.
    }

    /** The one place a command meets its primitive; the compiler checks that every verb has one. */
    private fun answer(command: Command.Cp, now: Long): Reply = when (command) {
        is Command.Cp.AtomicLong -> longs.apply(command, now)
        is Command.Cp.FencedLock -> locks.apply(command, now)
        is Command.Cp.Semaphore -> semaphores.apply(command)
        is Command.Cp.CountDownLatch -> latches.apply(command)
        is Command.Cp.AtomicReference -> references.apply(command, now)
        is Command.Cp.SessionClose -> { closeSession(command.session); Reply.Simple("OK") }
        is Command.Cp.Session -> sessions.apply(command, now)
        // CP spec 6.7 asks a member what it can see, which is not state the log carries.
        is Command.Cp.Introspection -> error("$command is answered locally, never replicated")
    }

    /**
     * C18: the session and everything it held go in this one entry; a second closing is a no-op.
     * Every primitive is offered the dead session, so one that holds something on a session's
     * behalf gives it back here and one that holds nothing does nothing (I15).
     */
    private fun closeSession(session: Long) {
        if (sessions.close(session)) primitives.forEach { it.releaseAllOf(session) }
    }

    /** The sessions whose timeout has run out at this member's log time; the leader closes them. */
    fun lapsedSessions(): List<Long> = sessions.lapsed(lastAppliedTs)

    override fun getNewTermOperation(): Any = NewTerm(currentTerm())

    /** Everything this member holds at its applied index, as one value two members can be compared by. */
    val state: Snapshot
        get() = Snapshot(lastAppliedTs, primitives.map { Table(it.id, it.snapshot()) })

    // ponytail: one chunk holding everything, since the whole state is a few maps; a chunk per
    // primitive is the upgrade when one primitive outgrows a message.
    override fun takeSnapshot(commitIndex: Long, chunkConsumer: Consumer<Any>) = chunkConsumer.accept(state)

    override fun installSnapshot(commitIndex: Long, chunks: List<Any>) {
        chunks.forEach { chunk ->
            val snapshot = chunk as Snapshot
            lastAppliedTs = snapshot.lastAppliedTs
            val tables = snapshot.tables.associate { it.id to it.bytes }
            primitives.forEach {
                it.restore(tables[it.id] ?: error("the snapshot carries no table for primitive ${it.id}"))
            }
        }
    }

    /** The snapshot chunk (CP spec 10.7): log time and one table per primitive; `CpWire` gives it a byte form. */
    data class Snapshot(val lastAppliedTs: Long, val tables: List<Table>)

    /**
     * One primitive's table in a snapshot: the primitive's [id] and the bytes only that primitive
     * reads. Two of these are equal when their bytes are, so comparing two members' snapshots
     * compares the state behind them.
     */
    class Table(val id: Int, val bytes: ByteArray) {
        override fun equals(other: Any?): Boolean = this === other ||
            (other is Table && id == other.id && bytes.contentEquals(other.bytes))

        override fun hashCode(): Int = 31 * id + bytes.contentHashCode()

        override fun toString(): String = "Table($id, ${bytes.size} bytes)"
    }
}
