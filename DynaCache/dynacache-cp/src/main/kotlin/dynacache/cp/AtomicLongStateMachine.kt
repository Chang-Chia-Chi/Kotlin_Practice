package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.util.concurrent.ConcurrentHashMap

/**
 * The AtomicLong (CP spec 3.2): one `Long` per `cp:counter:*` key, mutated only by applying
 * committed entries in log order. A counter's TTL is measured against the log time passed in
 * with every call ([CpStateMachine] owns it); nothing here reads a clock.
 */
class AtomicLongStateMachine {

    private val counters = ConcurrentHashMap<Key, Counter>()

    /** The committed, unexpired value of [key] at log time [now], or null. */
    fun valueOf(key: Key, now: Long): Long? = live(key, now)?.value

    fun apply(command: Command.Cp.AtomicLong, now: Long): Reply = when (command) {
        is Command.Cp.LongSet -> {
            counters[command.key] = Counter(command.value, command.ttl?.let { now + it.toMillis() })
            Reply.Simple("OK")
        }
        is Command.Cp.LongGet ->
            valueOf(command.key, now)?.let(Reply::Integer) ?: Reply.Bulk(null)
        is Command.Cp.LongIncr -> added(command.key, 1, now)
        is Command.Cp.LongDecr -> added(command.key, -1, now)
        is Command.Cp.LongIncrBy -> added(command.key, command.delta, now)
        is Command.Cp.LongDecrBy -> added(command.key, -command.delta, now)
        // The read and the add are one applied entry, so no one sees a value between them (I21).
        is Command.Cp.LongGetAdd -> Reply.Integer(add(command.key, command.delta, now))
        is Command.Cp.LongCas -> {
            // A counter that was never written reads as 0, as INCR treats it (I21: the compare
            // and the swap happen in one applied entry, so no reader sees a half state).
            val current = live(command.key, now)
            if ((current?.value ?: 0L) == command.expected) {
                counters[command.key] = Counter(command.new, current?.expiresAt)
                Reply.Integer(1)
            } else {
                Reply.Integer(0)
            }
        }
        is Command.Cp.LongExpire -> retime(command.key, now + command.ttl.toMillis(), now)
        is Command.Cp.LongPersist ->
            if (live(command.key, now)?.expiresAt == null) Reply.Integer(0) else retime(command.key, null, now)
        is Command.Cp.LongTtl -> {
            val counter = live(command.key, now)
            Reply.Integer(
                when {
                    counter == null -> -2
                    counter.expiresAt == null -> -1
                    command.precision == Command.Ttl.Precision.MILLIS -> counter.expiresAt - now
                    else -> (counter.expiresAt - now + 500) / 1000
                },
            )
        }
    }

    /** A tick drops every counter whose TTL has run out. */
    fun sweep(now: Long) {
        counters.values.removeIf { it.expired(now) }
    }

    fun snapshot(): Map<Key, Counter> = HashMap(counters)

    fun restore(state: Map<Key, Counter>) {
        counters.clear()
        counters.putAll(state)
    }

    /** Gives a live counter the expiry [expiresAt]: 1 when there was a live counter to give it to, else 0. */
    private fun retime(key: Key, expiresAt: Long?, now: Long): Reply {
        val current = live(key, now) ?: return Reply.Integer(0)
        counters[key] = current.copy(expiresAt = expiresAt)
        return Reply.Integer(1)
    }

    /** INCR and friends answer the new value; GETADD answers the old one [add] returns. */
    private fun added(key: Key, delta: Long, now: Long): Reply = Reply.Integer(add(key, delta, now) + delta)

    /**
     * Adds [delta] to [key], answering what the counter held before; a counter that was never
     * written counts as 0. As in Redis, the key keeps its TTL.
     */
    private fun add(key: Key, delta: Long, now: Long): Long {
        val current = live(key, now)
        val old = current?.value ?: 0L
        counters[key] = Counter(old + delta, current?.expiresAt)
        return old
    }

    private fun live(key: Key, now: Long): Counter? = counters[key]?.takeUnless { it.expired(now) }

    /** A counter and, when it has a TTL, the log time at which it stops existing. */
    data class Counter(val value: Long, val expiresAt: Long?) {
        fun expired(now: Long) = expiresAt != null && expiresAt <= now
    }
}
