package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Reply

/**
 * The session registry (CP spec 4): which sessions are alive, and when each last spoke, in log
 * time. Ids climb from state-machine state, so every member and every leader hands out the same
 * next id. A session lapses when its timeout has run out since its last heartbeat; who notices
 * and who releases what it held is the composite's business, this only keeps the book.
 */
class SessionRegistry : CpPrimitive {

    override val id = CpPrimitive.SESSIONS

    private val sessions = HashMap<Long, Session>()
    private var lastId = 0L

    /** [command]'s session is known to be alive; the composite answered `-NOSESSION` otherwise. */
    fun apply(command: Command.Cp.Session, now: Long): Reply = when (command) {
        is Command.Cp.SessionCreate -> {
            sessions[++lastId] = Session(lastHeartbeat = now, timeoutMs = command.timeout.toMillis())
            Reply.Integer(lastId)
        }
        is Command.Cp.SessionHeartbeat -> {
            sessions[command.session] = sessions.getValue(command.session).copy(lastHeartbeat = now)
            Reply.Simple("OK")
        }
        // CLOSE is the composite's, since what the session held goes with it (C18).
        is Command.Cp.SessionClose -> error("SessionClose is applied by the composite state machine")
    }

    fun isAlive(session: Long): Boolean = session in sessions

    /** Forgets [session]; true when it was alive. */
    fun close(session: Long): Boolean = sessions.remove(session) != null

    /** The sessions whose timeout has run out at log time [now]. */
    fun lapsed(now: Long): List<Long> =
        sessions.filterValues { it.lastHeartbeat + it.timeoutMs <= now }.keys.toList()

    /** The id the next session will get, then every live session; sorted, so equal state is equal bytes. */
    override fun snapshot(): ByteArray = CpWire.bytes {
        writeLong(lastId)
        writeInt(sessions.size)
        sessions.toSortedMap().forEach { (sessionId, session) ->
            writeLong(sessionId)
            writeLong(session.lastHeartbeat)
            writeLong(session.timeoutMs)
        }
    }

    override fun restore(bytes: ByteArray) {
        val (restoredLastId, restored) = CpWire.read(bytes) {
            readLong() to List(readInt()) { readLong() to Session(readLong(), readLong()) }.toMap()
        }
        lastId = restoredLastId
        sessions.clear()
        sessions.putAll(restored)
    }

    data class Session(val lastHeartbeat: Long, val timeoutMs: Long)
}
