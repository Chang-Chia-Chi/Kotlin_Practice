package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply

/**
 * The Semaphore (CP spec 3.3): per `cp:sem:*` key, how many permits are free and how many each
 * session holds, mutated only by applying committed entries in log order. Permits are held on
 * behalf of a session, so a session's death gives them all back ([releaseAllOf], C18, I15).
 *
 * A key that was never initialised is a semaphore of no permits: acquiring from it fails and
 * draining it yields nothing, rather than being a separate kind of answer.
 */
class SemaphoreStateMachine {

    private val semaphores = HashMap<Key, Semaphore>()

    fun apply(command: Command.Cp.Semaphore): Reply {
        val semaphore = semaphores[command.key] ?: EMPTY
        return when (command) {
            // Idempotent (CP spec 3.3): an initialised semaphore keeps the permits it has, since
            // re-initialising under live holders would invent permits nobody released.
            is Command.Cp.SemInit -> {
                semaphores.putIfAbsent(command.key, Semaphore(command.permits, emptyMap()))
                Reply.Simple("OK")
            }
            is Command.Cp.SemAcquire ->
                if (command.permits > semaphore.available) {
                    Reply.Integer(0)
                } else {
                    take(command.key, semaphore, command.session, command.permits)
                    Reply.Integer(1)
                }
            is Command.Cp.SemRelease -> {
                val held = semaphore.holders[command.session] ?: 0
                if (command.permits > held) {
                    Reply.Error("ERR", "session ${command.session} holds $held permits, not ${command.permits}")
                } else {
                    semaphores[command.key] = Semaphore(
                        available = semaphore.available + command.permits,
                        holders = semaphore.holders.without(command.session, held - command.permits),
                    )
                    Reply.Simple("OK")
                }
            }
            is Command.Cp.SemAvailable -> Reply.Integer(semaphore.available.toLong())
            is Command.Cp.SemDrain -> {
                val drained = semaphore.available
                take(command.key, semaphore, command.session, drained)
                Reply.Integer(drained.toLong())
            }
        }
    }

    /** A session's death gives back every permit it holds, in the one entry that ends it (C18, I15). */
    fun releaseAllOf(session: Long) = semaphores.replaceAll { _, semaphore ->
        val held = semaphore.holders[session] ?: return@replaceAll semaphore
        Semaphore(semaphore.available + held, semaphore.holders - session)
    }

    fun snapshot(): Map<Key, Semaphore> = HashMap(semaphores)

    fun restore(state: Map<Key, Semaphore>) {
        semaphores.clear()
        semaphores.putAll(state)
    }

    /** Taking nothing writes nothing, so draining a key nobody initialised leaves no semaphore behind. */
    private fun take(key: Key, semaphore: Semaphore, session: Long, permits: Int) {
        if (permits == 0) return
        semaphores[key] = Semaphore(
            available = semaphore.available - permits,
            holders = semaphore.holders + (session to ((semaphore.holders[session] ?: 0) + permits)),
        )
    }

    /** A holder of no permits is not a holder, so the map only ever names live holders. */
    private fun Map<Long, Int>.without(session: Long, left: Int): Map<Long, Int> =
        if (left == 0) this - session else this + (session to left)

    /** One semaphore's state (CP spec 3.3): the free permits, and what each holder took. */
    data class Semaphore(val available: Int, val holders: Map<Long, Int>)

    private companion object {
        val EMPTY = Semaphore(available = 0, holders = emptyMap())
    }
}
