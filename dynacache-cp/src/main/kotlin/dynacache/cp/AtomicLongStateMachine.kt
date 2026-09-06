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
 * The operations are the [Command.Cp] values themselves; the results are ordinary [Reply]s, so
 * the CP engine hands a committed reply straight back to the caller with nothing to translate.
 */
class AtomicLongStateMachine : StateMachine {

    private val counters = ConcurrentHashMap<Key, Long>()

    /** The committed value of [key], for tests that check a member applied an entry. */
    fun valueOf(key: Key): Long? = counters[key]

    override fun runOperation(commitIndex: Long, operation: Any): Any? = when (operation) {
        is Command.Cp -> apply(operation)
        else -> null // NewTerm and MicroRaft's own internal entries change no state.
    }

    private fun apply(command: Command.Cp): Reply = when (command) {
        is Command.Cp.LongSet -> {
            counters[command.key] = command.value
            Reply.Simple("OK")
        }
        is Command.Cp.LongGet ->
            counters[command.key]?.let(Reply::Integer) ?: Reply.Bulk(null)
        is Command.Cp.LongIncr -> add(command.key, 1)
        is Command.Cp.LongDecr -> add(command.key, -1)
        is Command.Cp.LongIncrBy -> add(command.key, command.delta)
        is Command.Cp.LongDecrBy -> add(command.key, -command.delta)
        is Command.Cp.LongCas -> {
            // A counter that was never written reads as 0, as INCR treats it (I21: the compare
            // and the swap happen in one applied entry, so no reader sees a half state).
            if (counters.getOrDefault(command.key, 0L) == command.expected) {
                counters[command.key] = command.new
                Reply.Integer(1)
            } else {
                Reply.Integer(0)
            }
        }
    }

    private fun add(key: Key, delta: Long): Reply =
        Reply.Integer(counters.merge(key, delta, Long::plus)!!)

    override fun getNewTermOperation(): Any = NewTerm

    /** One chunk holding the whole map; chunking a large counter set is T45's business. */
    override fun takeSnapshot(commitIndex: Long, chunkConsumer: Consumer<Any>) =
        chunkConsumer.accept(HashMap(counters))

    @Suppress("UNCHECKED_CAST")
    override fun installSnapshot(commitIndex: Long, chunks: List<Any>) {
        counters.clear()
        chunks.forEach { counters.putAll(it as Map<Key, Long>) }
    }

    /** The no-op MicroRaft appends when a leader starts a term. */
    private data object NewTerm
}
