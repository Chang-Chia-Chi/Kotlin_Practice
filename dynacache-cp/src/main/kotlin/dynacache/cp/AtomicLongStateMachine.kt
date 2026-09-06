package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import io.microraft.statemachine.StateMachine
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

/**
 * The AtomicLong state machine (CP spec 3.2): one `Long` per `cp:counter:*` key, mutated only by
 * applying committed log entries, so every member reaches the same value at the same log index.
 *
 * The operations are [CpOp]s wrapping [Command.Cp] values; the results are ordinary [Reply]s, so
 * the CP engine hands a committed reply straight back to the caller with nothing to translate.
 * Time is [lastAppliedTs], the stamp of the entry applied last (CP spec 5): a counter's TTL is
 * measured against it and nothing here reads a clock.
 */
class AtomicLongStateMachine(
    /** The term this member is in, read when it starts leading one. */
    private val currentTerm: () -> Int = { 0 },
    /** Told each term whose first entry was just applied. */
    private val onTermApplied: (Int) -> Unit = {},
) : StateMachine {

    private val counters = ConcurrentHashMap<Key, Counter>()

    /** Log time as this member sees it: the stamp of the last applied entry. */
    @Volatile
    var lastAppliedTs: Long = 0
        private set

    /** The committed, unexpired value of [key] at this member's applied index, or null. */
    fun valueOf(key: Key): Long? = live(key)?.value

    override fun runOperation(commitIndex: Long, operation: Any): Any? = when (operation) {
        is CpOp -> {
            lastAppliedTs = operation.ts
            apply(operation.command)
        }
        is TtlTick -> {
            lastAppliedTs = operation.ts
            counters.values.removeIf { it.expired(lastAppliedTs) }
            null
        }
        is NewTerm -> {
            onTermApplied(operation.term)
            null
        }
        else -> null // MicroRaft's own internal entries change no state.
    }

    private fun apply(command: Command.Cp): Reply = when (command) {
        is Command.Cp.LongSet -> {
            counters[command.key] = Counter(command.value, command.ttl?.let { lastAppliedTs + it.toMillis() })
            Reply.Simple("OK")
        }
        is Command.Cp.LongGet ->
            valueOf(command.key)?.let(Reply::Integer) ?: Reply.Bulk(null)
        is Command.Cp.LongIncr -> add(command.key, 1)
        is Command.Cp.LongDecr -> add(command.key, -1)
        is Command.Cp.LongIncrBy -> add(command.key, command.delta)
        is Command.Cp.LongDecrBy -> add(command.key, -command.delta)
        is Command.Cp.LongCas -> {
            // A counter that was never written reads as 0, as INCR treats it (I21: the compare
            // and the swap happen in one applied entry, so no reader sees a half state).
            val current = live(command.key)
            if ((current?.value ?: 0L) == command.expected) {
                counters[command.key] = Counter(command.new, current?.expiresAt)
                Reply.Integer(1)
            } else {
                Reply.Integer(0)
            }
        }
        is Command.Cp.LongExpire -> retime(command.key, lastAppliedTs + command.ttl.toMillis())
        is Command.Cp.LongPersist ->
            if (live(command.key)?.expiresAt == null) Reply.Integer(0) else retime(command.key, null)
        is Command.Cp.LongTtl -> {
            val counter = live(command.key)
            Reply.Integer(
                when {
                    counter == null -> -2
                    counter.expiresAt == null -> -1
                    else -> (counter.expiresAt - lastAppliedTs + 500) / 1000
                },
            )
        }
    }

    /** Gives a live counter the expiry [expiresAt]: 1 when there was a live counter to give it to, else 0. */
    private fun retime(key: Key, expiresAt: Long?): Reply {
        val current = live(key) ?: return Reply.Integer(0)
        counters[key] = current.copy(expiresAt = expiresAt)
        return Reply.Integer(1)
    }

    /** As in Redis, INCR and friends keep the key's TTL. */
    private fun add(key: Key, delta: Long): Reply {
        val current = live(key)
        val counter = Counter((current?.value ?: 0L) + delta, current?.expiresAt)
        counters[key] = counter
        return Reply.Integer(counter.value)
    }

    private fun live(key: Key): Counter? = counters[key]?.takeUnless { it.expired(lastAppliedTs) }

    override fun getNewTermOperation(): Any = NewTerm(currentTerm())

    /** One chunk holding the whole map; chunking a large counter set is T45's business. */
    override fun takeSnapshot(commitIndex: Long, chunkConsumer: Consumer<Any>) =
        chunkConsumer.accept(Snapshot(lastAppliedTs, HashMap(counters)))

    override fun installSnapshot(commitIndex: Long, chunks: List<Any>) {
        counters.clear()
        chunks.forEach {
            val snapshot = it as Snapshot
            lastAppliedTs = snapshot.lastAppliedTs
            counters.putAll(snapshot.counters)
        }
    }

    /** A counter and, when it has a TTL, the log time at which it stops existing. */
    private data class Counter(val value: Long, val expiresAt: Long?) {
        fun expired(now: Long) = expiresAt != null && expiresAt <= now
    }

    private data class Snapshot(val lastAppliedTs: Long, val counters: Map<Key, Counter>)
}
