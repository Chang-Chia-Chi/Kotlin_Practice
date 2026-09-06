package dynacache.engine

import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

/**
 * One partition: a single-thread executor and the store only that thread touches (C1 by
 * construction, ADR 0001). Everything below [submit] runs on the partition executor.
 */
internal class Partition(id: PartitionId, private val clock: Clock) {

    private class Entry(val value: Value, val expiresAt: Instant?) {
        /** The String bytes, safe to read once the kind check in [execute] has passed. */
        val str: ByteArray get() = (value as Value.Str).bytes
    }

    private val store = HashMap<Key, Entry>()
    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "partition-${id.index}").apply { isDaemon = true }
    }

    fun submit(command: Command): CompletableFuture<Reply> =
        CompletableFuture.supplyAsync({ execute(command) }, executor)

    /** One partition's share of a fanned-out command: one task, so those keys see no interleaving. */
    fun submitAll(commands: List<Command>): CompletableFuture<List<Reply>> =
        CompletableFuture.supplyAsync({ commands.map(::execute) }, executor)

    fun close() = executor.shutdown()

    /** The clock is read exactly once per command, so a command sees one instant throughout. */
    private fun execute(command: Command): Reply {
        val now = clock.instant()
        // C13: the kind is checked in front of every branch, so a wrong-type command answers
        // without a branch ever reaching the entry it would have corrupted.
        if (command is Command.Keyed && command.needs != null) {
            val held = live(command.key, now)?.value
            if (held != null && held.kind != command.needs) return WRONG_TYPE
        }
        return when (command) {
            is Command.Fanned -> error("a partition never sees a multi-key command; ApEngine fans it out")
            is Command.Ping -> Reply.Simple("PONG")

            is Command.Get -> Reply.Bulk(live(command.key, now)?.str)
            is Command.Set -> {
                val exists = live(command.key, now) != null
                val rejected = when (command.condition) {
                    null -> false
                    Command.Set.Condition.NX -> exists
                    Command.Set.Condition.XX -> !exists
                }
                if (rejected) {
                    NIL
                } else {
                    store[command.key] = Entry(Value.Str(command.value), command.ttl?.let(now::plus))
                    OK
                }
            }
            is Command.IncrBy -> {
                val current = live(command.key, now)
                val number = current?.str?.let(::asLong)
                if (current != null && number == null) NOT_AN_INTEGER else {
                    val next = try {
                        Math.addExact(number ?: 0L, command.delta)
                    } catch (overflow: ArithmeticException) {
                        return NOT_AN_INTEGER
                    }
                    store[command.key] = Entry(Value.Str(next.toString().toByteArray()), current?.expiresAt)
                    Reply.Integer(next)
                }
            }
            is Command.Append -> {
                val current = live(command.key, now)
                val joined = (current?.str ?: EMPTY) + command.value
                store[command.key] = Entry(Value.Str(joined), current?.expiresAt)
                Reply.Integer(joined.size.toLong())
            }
            is Command.StrLen -> Reply.Integer((live(command.key, now)?.str?.size ?: 0).toLong())

            is Command.HGet -> Reply.Bulk(hash(command.key, now)?.get(fieldName(command.field)))
            is Command.HSet -> Reply.Integer(put(command.key, now, command.entries))
            is Command.HMSet -> {
                put(command.key, now, command.entries)
                OK
            }
            is Command.HDel -> {
                val fields = hash(command.key, now)
                val removed = fields?.let { command.fields.count { f -> it.remove(fieldName(f)) != null } } ?: 0
                if (fields != null && fields.isEmpty()) store.remove(command.key)
                Reply.Integer(removed.toLong())
            }
            is Command.HGetAll -> Reply.Array(
                hash(command.key, now).orEmpty().flatMap { (field, value) ->
                    listOf(Reply.Bulk(fieldBytes(field)), Reply.Bulk(value))
                },
            )
            is Command.HMGet -> {
                val fields = hash(command.key, now)
                Reply.Array(command.fields.map { Reply.Bulk(fields?.get(fieldName(it))) })
            }
            is Command.HExists ->
                if (hash(command.key, now)?.containsKey(fieldName(command.field)) == true) ONE else ZERO
            is Command.HKeys -> Reply.Array(hash(command.key, now).orEmpty().keys.map { Reply.Bulk(fieldBytes(it)) })
            is Command.HVals -> Reply.Array(hash(command.key, now).orEmpty().values.map { Reply.Bulk(it) })
            is Command.HLen -> Reply.Integer((hash(command.key, now)?.size ?: 0).toLong())

            is Command.Del -> if (live(command.key, now) == null) ZERO else {
                store.remove(command.key)
                ONE
            }
            is Command.Exists -> if (live(command.key, now) == null) ZERO else ONE
            is Command.Type -> Reply.Simple(live(command.key, now)?.value?.kind?.text ?: "none")
        }
    }

    /**
     * The entry under [key] if it is still alive at [now]; an expired one is deleted on this
     * access (spec 5.4's lazy check). A key is readable through its deadline and gone after it.
     */
    private fun live(key: Key, now: Instant): Entry? {
        val entry = store[key] ?: return null
        if (entry.expiresAt != null && now.isAfter(entry.expiresAt)) {
            store.remove(key)
            return null
        }
        return entry
    }

    /** The fields under [key], or null when the key is absent. */
    private fun hash(key: Key, now: Instant): LinkedHashMap<String, ByteArray>? =
        (live(key, now)?.value as Value.Hash?)?.fields

    /** Writes [entries] into [key]'s hash, creating it when absent; replies how many were new. */
    private fun put(key: Key, now: Instant, entries: List<Pair<ByteArray, ByteArray>>): Long {
        val fields = hash(key, now) ?: Value.Hash().also { store[key] = Entry(it, null) }.fields
        return entries.count { (field, value) -> fields.put(fieldName(field), value) == null }.toLong()
    }

    /**
     * The value as a Redis integer, or null when it is not one. Redis accepts an optional `-`
     * and decimal digits only, so a leading `+` or any padding is not an integer.
     */
    private fun asLong(value: ByteArray): Long? {
        val text = value.toString(Charsets.ISO_8859_1)
        return if (text.startsWith("+")) null else text.toLongOrNull()
    }

    private companion object {
        val EMPTY = ByteArray(0)
        val OK = Reply.Simple("OK")
        val NOT_AN_INTEGER = Reply.Error("ERR", "value is not an integer or out of range")
        val WRONG_TYPE = Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
        val NIL = Reply.Bulk(null)
        val ZERO = Reply.Integer(0)
        val ONE = Reply.Integer(1)
    }
}
