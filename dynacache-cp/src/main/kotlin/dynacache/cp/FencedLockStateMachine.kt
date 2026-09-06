package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.util.concurrent.ConcurrentHashMap

/**
 * The FencedLock (CP spec 3.1): per `cp:lock:*` key, a holder, a fencing token, a lease and a
 * hold count, mutated only by applying committed entries in log order. The token is state, so it
 * outlives leader changes (C17): a released lock keeps its last token and the next holder gets the
 * next one (I14). A lease is measured against log time, never a clock: it is checked on every
 * access and swept on every TTL tick.
 */
class FencedLockStateMachine {

    // ponytail: a key once locked is never forgotten, since its token counter must live on.
    // The ceiling is a very large number of distinct lock keys; the repair is a per-key
    // tombstone holding only the token.
    private val locks = ConcurrentHashMap<Key, Lock>()

    fun apply(command: Command.Cp.FencedLock, now: Long): Reply {
        val lock = locks[command.key]?.at(now) ?: FREE
        return when (command) {
            is Command.Cp.LockTry -> when (lock.owner) {
                null -> grant(command.key, Lock(command.session, lock.token + 1, now + command.ttl.toMillis(), 1))
                command.session -> grant(command.key, lock.copy(holds = lock.holds + 1))
                else -> Reply.Array(listOf(Reply.Integer(0), Reply.Integer(0)))
            }
            is Command.Cp.LockUnlock -> when {
                lock.owner != command.session || lock.token != command.token -> reentrance(lock)
                lock.holds > 1 -> { locks[command.key] = lock.copy(holds = lock.holds - 1); Reply.Integer(0) }
                else -> { locks[command.key] = lock.released(); Reply.Integer(1) }
            }
            is Command.Cp.LockRenew ->
                if (lock.owner != command.session || lock.token != command.token) reentrance(lock)
                else { locks[command.key] = lock.copy(expiresAt = now + command.ttl.toMillis()); Reply.Integer(1) }
            is Command.Cp.LockForceUnlock -> { locks[command.key] = lock.released(); Reply.Simple("OK") }
            is Command.Cp.LockState -> Reply.Array(
                listOf(
                    lock.owner?.let(Reply::Integer) ?: Reply.Bulk(null),
                    Reply.Integer(lock.token),
                    Reply.Integer(if (lock.owner == null) 0 else lock.expiresAt - now),
                    Reply.Integer(lock.holds.toLong()),
                ),
            )
        }
    }

    private fun grant(key: Key, lock: Lock): Reply {
        locks[key] = lock
        return Reply.Array(listOf(Reply.Integer(1), Reply.Integer(lock.token)))
    }

    private fun reentrance(lock: Lock) =
        Reply.Error("REENTRANCE", if (lock.owner == null) "lock is not held" else "not the holder with token ${lock.token}")

    /** A tick releases every lease that has run out, so the state matches what any access would see. */
    fun sweep(now: Long) = locks.replaceAll { _, lock -> lock.at(now) }

    fun snapshot(): Map<Key, Lock> = HashMap(locks)

    fun restore(state: Map<Key, Lock>) {
        locks.clear()
        locks.putAll(state)
    }

    /** One lock's state (CP spec 3.1). [token] is the last token issued for the key, held or not. */
    data class Lock(val owner: Long?, val token: Long, val expiresAt: Long, val holds: Int) {
        /** This lock as log time [now] sees it: released once its lease has run out. */
        fun at(now: Long): Lock = if (owner != null && expiresAt <= now) released() else this

        fun released() = Lock(owner = null, token = token, expiresAt = 0, holds = 0)
    }

    private companion object {
        val FREE = Lock(owner = null, token = 0, expiresAt = 0, holds = 0)
    }
}
