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
    val semaphores = SemaphoreStateMachine()
    val latches = CountDownLatchStateMachine()
    val references = AtomicReferenceStateMachine()
    val sessions = SessionRegistry()

    /** Log time as this member sees it: the stamp of the last applied entry. */
    @Volatile
    var lastAppliedTs: Long = 0
        private set

    /** The committed, unexpired counter at [key] at this member's applied index, or null. */
    fun valueOf(key: Key): Long? = longs.valueOf(key, lastAppliedTs)

    override fun runOperation(commitIndex: Long, operation: Any): Any? = when (operation) {
        is CpOp -> {
            lastAppliedTs = operation.ts
            val command = operation.command
            if (command is Command.Cp.Sessioned && !sessions.isAlive(command.session)) {
                Reply.Error("NOSESSION", "session ${command.session} expired or never created")
            } else when (command) {
                is Command.Cp.AtomicLong -> longs.apply(command, lastAppliedTs)
                is Command.Cp.FencedLock -> locks.apply(command, lastAppliedTs)
                is Command.Cp.Semaphore -> semaphores.apply(command)
                is Command.Cp.CountDownLatch -> latches.apply(command)
                is Command.Cp.AtomicReference -> references.apply(command, lastAppliedTs)
                is Command.Cp.SessionClose -> { closeSession(command.session); Reply.Simple("OK") }
                is Command.Cp.Session -> sessions.apply(command, lastAppliedTs)
                // CP spec 6.7 asks a member what it can see, which is not state the log carries.
                is Command.Cp.Introspection -> error("$command is answered locally, never replicated")
            }
        }
        is SessionClosed -> {
            lastAppliedTs = operation.ts
            closeSession(operation.session)
            null
        }
        is TtlTick -> {
            lastAppliedTs = operation.ts
            longs.sweep(lastAppliedTs)
            locks.sweep(lastAppliedTs)
            references.sweep(lastAppliedTs)
            null
        }
        is NewTerm -> {
            onTermApplied(operation.term)
            null
        }
        else -> null // MicroRaft's own internal entries change no state.
    }

    /** C18: the session and everything it held go in this one entry; a second closing is a no-op. */
    private fun closeSession(session: Long) {
        if (sessions.close(session)) {
            locks.releaseAllOf(session)
            semaphores.releaseAllOf(session)
        }
    }

    /** The sessions whose timeout has run out at this member's log time; the leader closes them. */
    fun lapsedSessions(): List<Long> = sessions.lapsed(lastAppliedTs)

    override fun getNewTermOperation(): Any = NewTerm(currentTerm())

    /** Everything this member holds at its applied index, as one value two members can be compared by. */
    val state: Snapshot
        get() = Snapshot(
            lastAppliedTs,
            longs.snapshot(),
            locks.snapshot(),
            semaphores.snapshot(),
            latches.snapshot(),
            references.snapshot(),
            sessions.snapshot(),
        )

    // ponytail: one chunk holding everything, since the whole state is a few maps; a chunk per
    // primitive is the upgrade when one primitive outgrows a message.
    override fun takeSnapshot(commitIndex: Long, chunkConsumer: Consumer<Any>) = chunkConsumer.accept(state)

    override fun installSnapshot(commitIndex: Long, chunks: List<Any>) {
        chunks.forEach {
            val snapshot = it as Snapshot
            lastAppliedTs = snapshot.lastAppliedTs
            longs.restore(snapshot.counters)
            locks.restore(snapshot.locks)
            semaphores.restore(snapshot.semaphores)
            latches.restore(snapshot.latches)
            references.restore(snapshot.references)
            sessions.restore(snapshot.sessions)
        }
    }

    /** The snapshot chunk (CP spec 10.7): log time and every primitive's table; `CpWire` gives it a byte form. */
    data class Snapshot(
        val lastAppliedTs: Long,
        val counters: Map<Key, AtomicLongStateMachine.Counter>,
        val locks: Map<Key, FencedLockStateMachine.Lock>,
        val semaphores: Map<Key, SemaphoreStateMachine.Semaphore>,
        val latches: Map<Key, Int>,
        val references: Map<Key, AtomicReferenceStateMachine.Reference>,
        val sessions: SessionRegistry.State,
    )
}
