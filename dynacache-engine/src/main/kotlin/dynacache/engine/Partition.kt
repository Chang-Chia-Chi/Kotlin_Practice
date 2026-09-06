package dynacache.engine

import dynacache.engine.ds.HashTable
import java.time.Clock
import java.time.Instant
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

/**
 * One partition: a single-thread executor and the store only that thread touches (C1 by
 * construction, ADR 0001). Everything below [submit] runs on the partition executor.
 */
internal class Partition(id: PartitionId, private val clock: Clock, private val random: Random) {

    private class Entry(val value: Value, val expiresAt: Instant?) {
        /** The String bytes, safe to read once the kind check in [execute] has passed. */
        val str: ByteArray get() = (value as Value.Str).bytes

        /** The one expiry rule: a key is readable through its deadline and gone after it. */
        fun expired(now: Instant): Boolean = expiresAt != null && now.isAfter(expiresAt)
    }

    private val store = HashTable<Key, Entry>()
    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "partition-${id.index}").apply { isDaemon = true }
    }

    fun submit(command: Command): CompletableFuture<Reply> =
        CompletableFuture.supplyAsync({ execute(command) }, executor)

    /** One partition's share of a fanned-out command: one task, so those keys see no interleaving. */
    fun submitAll(commands: List<Command>): CompletableFuture<List<Reply>> =
        CompletableFuture.supplyAsync({ commands.map(::execute) }, executor)

    fun close() = executor.shutdown()

    /**
     * This partition's share of a `SCAN`: the keys its walk found from [command]'s cursor, and
     * the cursor to continue from, 0 once the walk wrapped. The engine folds the partition into
     * the cursor the client sees.
     */
    fun scan(command: Command.Scan): CompletableFuture<Pair<Long, List<Reply>>> =
        CompletableFuture.supplyAsync({
            val now = clock.instant()
            val found = ArrayList<Reply>()
            val next = walk(store, command.cursor, command.count, { _, entry -> !entry.expired(now) }) { key, _ ->
                if (matches(command.pattern, key.bytes)) found += Reply.Bulk(key.bytes)
            }
            next to found
        }, executor)

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
            is Command.Cp -> error("a partition never sees a CP command; the CP engine replicates it")
            is Command.Ping -> Reply.Simple("PONG")
            is Command.CommandTable -> Reply.Array(emptyList())

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
                    store.put(command.key, Entry(Value.Str(command.value), command.ttl?.let(now::plus)))
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
                    store.put(command.key, Entry(Value.Str(next.toString().toByteArray()), current?.expiresAt))
                    Reply.Integer(next)
                }
            }
            is Command.Append -> {
                val current = live(command.key, now)
                val joined = (current?.str ?: EMPTY) + command.value
                store.put(command.key, Entry(Value.Str(joined), current?.expiresAt))
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
                if (fields != null && fields.size == 0) store.remove(command.key)
                Reply.Integer(removed.toLong())
            }
            is Command.HGetAll -> Reply.Array(
                hash(command.key, now)?.entries().orEmpty().flatMap {
                    listOf(Reply.Bulk(fieldBytes(it.key)), Reply.Bulk(it.value))
                }.toList(),
            )
            is Command.HMGet -> {
                val fields = hash(command.key, now)
                Reply.Array(command.fields.map { Reply.Bulk(fields?.get(fieldName(it))) })
            }
            is Command.HExists ->
                if (hash(command.key, now)?.get(fieldName(command.field)) != null) ONE else ZERO
            is Command.HKeys -> Reply.Array(hash(command.key, now)?.entries().orEmpty().map { Reply.Bulk(fieldBytes(it.key)) }.toList())
            is Command.HVals -> Reply.Array(hash(command.key, now)?.entries().orEmpty().map { Reply.Bulk(it.value) }.toList())
            is Command.HLen -> Reply.Integer((hash(command.key, now)?.size ?: 0).toLong())
            is Command.HScan -> {
                val fields = hash(command.key, now) ?: return scanReply(0, emptyList())
                val found = ArrayList<Reply>()
                val next = walk(fields, command.cursor, command.count, { field, _ -> matches(command.pattern, fieldBytes(field)) }) { field, value ->
                    found += Reply.Bulk(fieldBytes(field))
                    found += Reply.Bulk(value)
                }
                scanReply(next, found)
            }
            is Command.Scan -> error("SCAN runs through Partition.scan, which hands the engine the cursor")

            is Command.Push -> {
                val items = items(command.key, now) ?: Value.List().also { store.put(command.key, Entry(it, null)) }.items
                for (value in command.values) if (command.end == Command.End.HEAD) items.addFirst(value) else items.addLast(value)
                Reply.Integer(items.size.toLong())
            }
            is Command.Pop -> {
                val items = items(command.key, now)
                if (items.isNullOrEmpty()) NIL else {
                    val popped = if (command.end == Command.End.HEAD) items.removeFirst() else items.removeLast()
                    dropIfEmpty(command.key, items)
                    Reply.Bulk(popped)
                }
            }

            is Command.LRange -> {
                val items = items(command.key, now).orEmpty()
                Reply.Array(span(command.start, command.stop, items.size).map { Reply.Bulk(items[it]) })
            }
            is Command.LLen -> Reply.Integer((items(command.key, now)?.size ?: 0).toLong())
            is Command.LIndex -> {
                val items = items(command.key, now).orEmpty()
                Reply.Bulk(items.getOrNull(at(command.index, items.size)))
            }
            is Command.LSet -> {
                val items = items(command.key, now) ?: return NO_SUCH_KEY
                val position = at(command.index, items.size)
                if (position !in items.indices) INDEX_OUT_OF_RANGE else {
                    items[position] = command.value
                    OK
                }
            }
            is Command.LRem -> {
                val items = items(command.key, now)
                val removed = items?.let { remove(it, command.count, command.value) } ?: 0
                if (items != null) dropIfEmpty(command.key, items)
                Reply.Integer(removed.toLong())
            }

            is Command.DbSize, is Command.Info -> {
                purgeExpired(now)
                Reply.Integer(store.size.toLong())
            }
            is Command.Keys -> {
                purgeExpired(now)
                Reply.Array(store.entries().map { it.key }.filter { globMatches(command.pattern, it.bytes) }.map { Reply.Bulk(it.bytes) }.toList())
            }
            is Command.RandomKey -> {
                purgeExpired(now)
                Reply.Bulk(store.randomKey(random)?.bytes)
            }
            is Command.FlushDb -> {
                store.clear()
                OK
            }

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
        val entry = store.get(key) ?: return null
        if (entry.expired(now)) {
            store.remove(key)
            return null
        }
        return entry
    }

    /** The fields under [key], or null when the key is absent. */
    private fun hash(key: Key, now: Instant): HashTable<String, ByteArray>? =
        (live(key, now)?.value as Value.Hash?)?.fields

    /**
     * The lazy check of [live], applied to the whole store at once: what a keyspace-wide command
     * sees afterwards is exactly the live keys, with no copy of the key set to filter.
     *
     * ponytail: O(n) in the keyspace, so `DBSIZE` costs a walk that Redis answers in O(1). T09's
     * wheel removes expired keys as they fall due, and then this sweep can go.
     */
    private fun purgeExpired(now: Instant) {
        store.entries().filter { it.value.expired(now) }.map { it.key }.toList().forEach(store::remove)
    }

    /**
     * Redis's `SCAN` loop: buckets are walked until at least [count] entries came out of them or
     * the walk wrapped, then [keep] filters what came out. So a call may answer few keys, or
     * none, with a cursor that is not 0; the client keeps calling until it is.
     */
    private fun <K, V> walk(table: HashTable<K, V>, cursor: Long, count: Int, keep: (K, V) -> Boolean, emit: (K, V) -> Unit): Long {
        var next = cursor
        var visited = 0
        do {
            next = table.scan(next) { key, value ->
                visited++
                if (keep(key, value)) emit(key, value)
            }
        } while (next != 0L && visited < count)
        return next
    }

    /** `MATCH`: no pattern matches everything. */
    private fun matches(pattern: ByteArray?, bytes: ByteArray): Boolean = pattern == null || globMatches(pattern, bytes)

    /** The elements under [key], or null when the key is absent. */
    private fun items(key: Key, now: Instant): ArrayDeque<ByteArray>? =
        (live(key, now)?.value as Value.List?)?.items

    /**
     * A Redis list index as a position: a negative one counts back from the tail. An index the
     * list does not reach clamps to just outside it, which every caller treats as "not there";
     * the clamp is what keeps a wire-sized index inside an `Int`.
     */
    private fun at(index: Long, size: Int): Int =
        (if (index < 0) size + index else index).coerceIn(-1L, size.toLong()).toInt()

    /**
     * Redis's `LRANGE` bounds: a negative index counts back from the tail, a start before the
     * list starts at 0 and a stop past the end stops at the last element, so nothing errors.
     */
    private fun span(start: Long, stop: Long, size: Int): IntRange {
        val from = (if (start < 0) size + start else start).coerceAtLeast(0)
        val to = (if (stop < 0) size + stop else stop).coerceAtMost(size - 1L)
        return if (from > to) IntRange.EMPTY else from.toInt()..to.toInt()
    }

    /**
     * `LREM`'s count: the matches are collected in the direction the sign asks for and dropped
     * back to front, so the positions found stay valid while they are removed.
     */
    private fun remove(items: ArrayDeque<ByteArray>, count: Long, value: ByteArray): Int {
        val order = if (count < 0) items.indices.reversed() else items.indices
        // Zero means every match, and so does any magnitude the list cannot reach; clamping to
        // the size also disarms Long.MIN_VALUE, whose absolute value does not fit in a Long.
        val everyMatch = count == 0L || count >= items.size || count <= -items.size.toLong()
        val limit = if (everyMatch) items.size else Math.abs(count).toInt()
        val doomed = order.filter { items[it].contentEquals(value) }.take(limit)
        doomed.sortedDescending().forEach(items::removeAt)
        return doomed.size
    }

    /** Redis keeps no empty aggregate: the last element taken out takes the key with it. */
    private fun dropIfEmpty(key: Key, items: ArrayDeque<ByteArray>) {
        if (items.isEmpty()) store.remove(key)
    }

    /** Writes [entries] into [key]'s hash, creating it when absent; replies how many were new. */
    private fun put(key: Key, now: Instant, entries: List<Pair<ByteArray, ByteArray>>): Long {
        val fields = hash(key, now) ?: Value.Hash().also { store.put(key, Entry(it, null)) }.fields
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

    internal companion object {
        /** The `SCAN` family's reply: the cursor as a bulk, then the array of what was found. */
        fun scanReply(cursor: Long, found: List<Reply>): Reply =
            Reply.Array(listOf(Reply.Bulk(cursor.toString().toByteArray()), Reply.Array(found)))

        val EMPTY = ByteArray(0)
        val OK = Reply.Simple("OK")
        val NO_SUCH_KEY = Reply.Error("ERR", "no such key")
        val INDEX_OUT_OF_RANGE = Reply.Error("ERR", "index out of range")
        val NOT_AN_INTEGER = Reply.Error("ERR", "value is not an integer or out of range")
        val WRONG_TYPE = Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
        val NIL = Reply.Bulk(null)
        val ZERO = Reply.Integer(0)
        val ONE = Reply.Integer(1)
    }
}
