package dynacache.engine.persist

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.time.Duration
import java.time.Instant

/*
 * The engine's one encoding of a command as bytes: an op code for the command's kind and a body of
 * its arguments, big-endian and length-prefixed as the RDB's are. Every keyed command encodes,
 * reads included, and so does every fanned one, because a command crosses the network in this form
 * as well as going into the log: the WAL entry's header carries the op code and its payload the
 * body, and a forward carries the two concatenated, op code first. `Command.Cp` is CpWire's
 * business and has no form here (C16).
 *
 * What is logged is what changed, not what was asked, and that is [whatChanged]'s decision, not
 * this encoding's. A TTL travels into the log as the absolute instant the engine settled on, never
 * as the duration the client sent, so a replay lands the same deadline however late it runs (spec
 * 5.4); that is what the `now` [CommandCodec.encode] takes is for. A forward, which the coordinator
 * has yet to decide, passes none and carries the duration as the client wrote it.
 *
 * A field the log has never needed is written only when it is not its default -- `SET`'s condition
 * and asked TTL, `ZADD`'s `CH` -- so an entry written before this codec grew to the wire's needs
 * still reads back as the log meant it.
 */

private const val NO_TTL = -1L

private const val SET: Byte = 1
private const val DEL: Byte = 2
private const val EXPIRE: Byte = 3
private const val PERSIST: Byte = 4
private const val INCR_BY: Byte = 5
private const val APPEND: Byte = 6
private const val HSET: Byte = 7
private const val HDEL: Byte = 8
private const val PUSH: Byte = 9
private const val POP: Byte = 10
private const val LSET: Byte = 11
private const val LREM: Byte = 12
private const val ZADD: Byte = 13
private const val ZREM: Byte = 14
private const val ZINCR_BY: Byte = 15
private const val FLUSH_DB: Byte = 16

// The op codes above are the log's, and their bodies are what an older log holds. Everything below
// crosses the network but is never logged: a read changes nothing, a fanned command reaches the log
// as the single-key parts it splits into, and `HMSET` is logged as the `HSET` it is.
private const val HMSET: Byte = 17
private const val GET: Byte = 18
private const val EXISTS: Byte = 19
private const val TYPE: Byte = 20
private const val TTL: Byte = 21
private const val STRLEN: Byte = 22
private const val HGET: Byte = 23
private const val HGETALL: Byte = 24
private const val HMGET: Byte = 25
private const val HEXISTS: Byte = 26
private const val HKEYS: Byte = 27
private const val HVALS: Byte = 28
private const val HLEN: Byte = 29
private const val HSCAN: Byte = 30
private const val LRANGE: Byte = 31
private const val LLEN: Byte = 32
private const val LINDEX: Byte = 33
private const val ZSCORE: Byte = 34
private const val ZCARD: Byte = 35
private const val ZRANGE: Byte = 36
private const val ZRANK: Byte = 37
private const val ZRANGE_BY_SCORE: Byte = 38
private const val ZSCAN: Byte = 39
private const val MGET: Byte = 40
private const val MSET: Byte = 41
private const val DEL_KEYS: Byte = 42
private const val EXISTS_KEYS: Byte = 43

/**
 * The command the log should hold for [command], which answered [reply], or null when nothing
 * changed: an error changed nothing, nor did a nil, which is a refused `SET` or an empty `POP`,
 * nor did a read.
 *
 * A logged write is the write that happened, not the one that was asked for. The coordinator
 * decided `SET`'s `NX`/`XX`, so a replay applies the plain write. `ZADD`'s condition is kept
 * instead: it answers a count that a refusal and a moved score share (`:0` without `CH`), and it
 * may refuse some of its members and take the rest, so it is replayed under its condition against
 * the same state the live command saw. `CH` is not logged either way; it changes only the reply.
 */
fun whatChanged(command: Command, reply: Reply): Command? {
    if (reply is Reply.Error || (reply is Reply.Bulk && reply.bytes == null)) return null
    return when (command) {
        is Command.Set ->
            if (command.condition == null) command
            else Command.Set(command.key, command.value, null, command.ttl)
        is Command.HMSet -> Command.HSet(command.key, command.entries)
        is Command.ZAdd ->
            if (!command.changed) command
            else Command.ZAdd(command.key, command.entries, command.condition, changed = false)
        is Command.Del, is Command.Expire, is Command.Persist, is Command.IncrBy, is Command.Append,
        is Command.HSet, is Command.HDel, is Command.Push, is Command.Pop, is Command.LSet,
        is Command.LRem, is Command.ZRem, is Command.ZIncrBy, Command.FlushDb,
        -> command
        else -> null
    }
}

/** One encoding of a command as bytes, total over every keyed and fanned command. */
object CommandCodec {

    /**
     * [command] as an op code and the body of its arguments. [now] is the log's: it turns a `SET`'s
     * TTL into the absolute instant the engine settled on. A forward passes none.
     */
    fun encode(command: Command, now: Instant? = null): Pair<Byte, ByteArray> = when (command) {
        // Writes, in the shapes an older log holds.
        is Command.Set -> SET to body {
            // The settled deadline and the asked duration are one field read two ways, and only one
            // is ever there: the log's entry carries the deadline, the wire's the duration.
            val deadline = now?.let { command.ttl?.let(it::plus)?.toEpochMilli() } ?: NO_TTL
            val asked = if (now == null) command.ttl?.toMillis() ?: NO_TTL else NO_TTL
            key(command.key); bytes(command.value); writeLong(deadline)
            if (command.condition != null || asked != NO_TTL) {
                condition(command.condition)
                writeLong(asked)
            }
        }
        is Command.Del -> DEL to body { key(command.key) }
        is Command.Expire -> EXPIRE to body { key(command.key); writeLong(command.deadline.toEpochMilli()) }
        is Command.Persist -> PERSIST to body { key(command.key) }
        is Command.IncrBy -> INCR_BY to body { key(command.key); writeLong(command.delta) }
        is Command.Append -> APPEND to body { key(command.key); bytes(command.value) }
        is Command.HSet -> HSET to body { key(command.key); pairs(command.entries) }
        is Command.HMSet -> HMSET to body { key(command.key); pairs(command.entries) }
        is Command.HDel -> HDEL to body { key(command.key); list(command.fields) }
        is Command.Push -> PUSH to body { key(command.key); end(command.end); list(command.values) }
        is Command.Pop -> POP to body { key(command.key); end(command.end) }
        is Command.LSet -> LSET to body { key(command.key); writeLong(command.index); bytes(command.value) }
        is Command.LRem -> LREM to body { key(command.key); writeLong(command.count); bytes(command.value) }
        is Command.ZAdd -> ZADD to body {
            key(command.key); pairs(command.entries); condition(command.condition)
            if (command.changed) writeByte(1)
        }
        is Command.ZRem -> ZREM to body { key(command.key); list(command.members) }
        is Command.ZIncrBy -> ZINCR_BY to body { key(command.key); bytes(command.delta); bytes(command.member) }
        Command.FlushDb -> FLUSH_DB to ByteArray(0)

        // Reads.
        is Command.Get -> GET to body { key(command.key) }
        is Command.Exists -> EXISTS to body { key(command.key) }
        is Command.Type -> TYPE to body { key(command.key) }
        is Command.Ttl -> TTL to body { key(command.key); precision(command.precision) }
        is Command.StrLen -> STRLEN to body { key(command.key) }
        is Command.HGet -> HGET to body { key(command.key); bytes(command.field) }
        is Command.HGetAll -> HGETALL to body { key(command.key) }
        is Command.HMGet -> HMGET to body { key(command.key); list(command.fields) }
        is Command.HExists -> HEXISTS to body { key(command.key); bytes(command.field) }
        is Command.HKeys -> HKEYS to body { key(command.key) }
        is Command.HVals -> HVALS to body { key(command.key) }
        is Command.HLen -> HLEN to body { key(command.key) }
        is Command.HScan -> HSCAN to body { key(command.key); scan(command.cursor, command.pattern, command.count) }
        is Command.LRange -> LRANGE to body { key(command.key); writeLong(command.start); writeLong(command.stop) }
        is Command.LLen -> LLEN to body { key(command.key) }
        is Command.LIndex -> LINDEX to body { key(command.key); writeLong(command.index) }
        is Command.ZScore -> ZSCORE to body { key(command.key); bytes(command.member) }
        is Command.ZCard -> ZCARD to body { key(command.key) }
        is Command.ZRange -> ZRANGE to body {
            key(command.key); writeLong(command.start); writeLong(command.stop)
            writeBoolean(command.withScores); writeBoolean(command.reverse)
        }
        is Command.ZRank -> ZRANK to body { key(command.key); bytes(command.member); writeBoolean(command.reverse) }
        is Command.ZRangeByScore -> ZRANGE_BY_SCORE to body {
            key(command.key); bytes(command.min); bytes(command.max)
            writeBoolean(command.withScores); writeLong(command.offset); writeLong(command.count)
        }
        is Command.ZScan -> ZSCAN to body { key(command.key); scan(command.cursor, command.pattern, command.count) }

        // Fanned: a forward carries one whole, and the node it reaches splits it as this one would.
        is Command.MGet -> MGET to body { keys(command.keys) }
        is Command.MSet -> MSET to body {
            writeInt(command.keys.size)
            for (index in command.keys.indices) {
                val part = command.single(index) as Command.Set
                key(part.key)
                bytes(part.value)
            }
        }
        is Command.DelKeys -> DEL_KEYS to body { keys(command.keys) }
        is Command.ExistsKeys -> EXISTS_KEYS to body { keys(command.keys) }

        // Nothing else crosses: the router runs it on this node (T19), and a CP command is answered
        // by the CP state machine over its own wire (C16).
        is Command.Cp, is Command.EveryPartition, Command.Ping, Command.CommandTable, is Command.Scan,
        -> throw IllegalArgumentException("no wire form for $command: only a keyed or fanned command crosses")
    }

    /** The commands one encoding redoes, in order: a `SET` with a deadline is a `SET` and then an `EXPIRE`. */
    fun decode(op: Byte, body: ByteArray): List<Command> {
        val input = DataInputStream(ByteArrayInputStream(body))
        return when (op) {
            SET -> {
                val key = input.key()
                val value = input.bytes()
                val deadline = input.readLong()
                val asked = input.available() > 0
                val condition = if (asked) input.condition() else null
                val ttl = if (asked) input.readLong() else NO_TTL
                val set = Command.Set(key, value, condition, ttl.takeIf { it != NO_TTL }?.let(Duration::ofMillis))
                if (deadline == NO_TTL) listOf(set) else listOf(set, Command.Expire(key, Instant.ofEpochMilli(deadline)))
            }
            DEL -> listOf(Command.Del(input.key()))
            EXPIRE -> listOf(Command.Expire(input.key(), Instant.ofEpochMilli(input.readLong())))
            PERSIST -> listOf(Command.Persist(input.key()))
            INCR_BY -> listOf(Command.IncrBy(input.key(), input.readLong()))
            APPEND -> listOf(Command.Append(input.key(), input.bytes()))
            HSET -> listOf(Command.HSet(input.key(), input.pairs()))
            HMSET -> listOf(Command.HMSet(input.key(), input.pairs()))
            HDEL -> listOf(Command.HDel(input.key(), input.list()))
            PUSH -> {
                val key = input.key()
                val end = input.end()
                listOf(Command.Push(key, input.list(), end))
            }
            POP -> listOf(Command.Pop(input.key(), input.end()))
            LSET -> listOf(Command.LSet(input.key(), input.readLong(), input.bytes()))
            LREM -> listOf(Command.LRem(input.key(), input.readLong(), input.bytes()))
            ZADD -> {
                val key = input.key()
                val entries = input.pairs()
                val condition = input.condition()
                val changed = input.available() > 0 && input.readByte() != 0.toByte()
                listOf(Command.ZAdd(key, entries, condition, changed))
            }
            ZREM -> listOf(Command.ZRem(input.key(), input.list()))
            ZINCR_BY -> listOf(Command.ZIncrBy(input.key(), input.bytes(), input.bytes()))
            FLUSH_DB -> listOf(Command.FlushDb)

            GET -> listOf(Command.Get(input.key()))
            EXISTS -> listOf(Command.Exists(input.key()))
            TYPE -> listOf(Command.Type(input.key()))
            TTL -> listOf(Command.Ttl(input.key(), input.precision()))
            STRLEN -> listOf(Command.StrLen(input.key()))
            HGET -> listOf(Command.HGet(input.key(), input.bytes()))
            HGETALL -> listOf(Command.HGetAll(input.key()))
            HMGET -> listOf(Command.HMGet(input.key(), input.list()))
            HEXISTS -> listOf(Command.HExists(input.key(), input.bytes()))
            HKEYS -> listOf(Command.HKeys(input.key()))
            HVALS -> listOf(Command.HVals(input.key()))
            HLEN -> listOf(Command.HLen(input.key()))
            HSCAN -> {
                val key = input.key()
                listOf(Command.HScan(key, input.readLong(), input.optional(), input.readInt()))
            }
            LRANGE -> listOf(Command.LRange(input.key(), input.readLong(), input.readLong()))
            LLEN -> listOf(Command.LLen(input.key()))
            LINDEX -> listOf(Command.LIndex(input.key(), input.readLong()))
            ZSCORE -> listOf(Command.ZScore(input.key(), input.bytes()))
            ZCARD -> listOf(Command.ZCard(input.key()))
            ZRANGE -> listOf(
                Command.ZRange(input.key(), input.readLong(), input.readLong(), input.readBoolean(), input.readBoolean()),
            )
            ZRANK -> listOf(Command.ZRank(input.key(), input.bytes(), input.readBoolean()))
            ZRANGE_BY_SCORE -> listOf(
                Command.ZRangeByScore(
                    input.key(),
                    input.bytes(),
                    input.bytes(),
                    input.readBoolean(),
                    input.readLong(),
                    input.readLong(),
                ),
            )
            ZSCAN -> {
                val key = input.key()
                listOf(Command.ZScan(key, input.readLong(), input.optional(), input.readInt()))
            }

            MGET -> listOf(Command.MGet(input.keys()))
            MSET -> listOf(Command.MSet(List(input.readInt()) { input.key() to input.bytes() }))
            DEL_KEYS -> listOf(Command.DelKeys(input.keys()))
            EXISTS_KEYS -> listOf(Command.ExistsKeys(input.keys()))
            else -> throw IllegalArgumentException("unknown command op $op")
        }
    }

    private inline fun body(fill: DataOutputStream.() -> Unit): ByteArray =
        ByteArrayOutputStream().also { DataOutputStream(it).use(fill) }.toByteArray()

    private fun DataOutputStream.bytes(bytes: ByteArray) {
        writeInt(bytes.size)
        write(bytes)
    }

    private fun DataOutputStream.key(key: Key) = bytes(key.bytes)

    /** Fixed by the format, not the enum's order: reordering [Command.End] must not change a file's meaning. */
    private fun DataOutputStream.end(end: Command.End) = writeByte(if (end == Command.End.HEAD) 0 else 1)

    /** Fixed by the format, as [end] is: the enum's order is free to change, these bytes are not. */
    private fun DataOutputStream.condition(condition: Command.Set.Condition?) = writeByte(
        when (condition) {
            null -> 0
            Command.Set.Condition.NX -> 1
            Command.Set.Condition.XX -> 2
        },
    )

    /** Fixed by the format too, and for the same reason. */
    private fun DataOutputStream.precision(precision: Command.Ttl.Precision) =
        writeByte(if (precision == Command.Ttl.Precision.SECONDS) 0 else 1)

    private fun DataOutputStream.list(items: List<ByteArray>) {
        writeInt(items.size)
        items.forEach { bytes(it) }
    }

    private fun DataOutputStream.keys(keys: List<Key>) = list(keys.map { it.bytes })

    private fun DataOutputStream.pairs(pairs: List<Pair<ByteArray, ByteArray>>) {
        writeInt(pairs.size)
        for ((first, second) in pairs) {
            bytes(first)
            bytes(second)
        }
    }

    /** A `MATCH` pattern that may not be there: the flag first, so its absence costs one byte. */
    private fun DataOutputStream.optional(bytes: ByteArray?) {
        writeBoolean(bytes != null)
        if (bytes != null) bytes(bytes)
    }

    /** The `cursor [MATCH pattern] COUNT n` tail `HSCAN` and `ZSCAN` share. */
    private fun DataOutputStream.scan(cursor: Long, pattern: ByteArray?, count: Int) {
        writeLong(cursor)
        optional(pattern)
        writeInt(count)
    }

    private fun DataInputStream.bytes(): ByteArray = readNBytes(readInt())
    private fun DataInputStream.key(): Key = Key(bytes())
    private fun DataInputStream.keys(): List<Key> = List(readInt()) { key() }
    private fun DataInputStream.end(): Command.End = if (readByte() == 0.toByte()) Command.End.HEAD else Command.End.TAIL
    private fun DataInputStream.condition(): Command.Set.Condition? = when (val byte = readByte().toInt()) {
        0 -> null
        1 -> Command.Set.Condition.NX
        2 -> Command.Set.Condition.XX
        else -> throw IllegalArgumentException("unknown condition $byte")
    }

    private fun DataInputStream.precision(): Command.Ttl.Precision =
        if (readByte() == 0.toByte()) Command.Ttl.Precision.SECONDS else Command.Ttl.Precision.MILLIS

    private fun DataInputStream.optional(): ByteArray? = if (readBoolean()) bytes() else null

    private fun DataInputStream.list(): List<ByteArray> = List(readInt()) { bytes() }
    private fun DataInputStream.pairs(): List<Pair<ByteArray, ByteArray>> = List(readInt()) { bytes() to bytes() }
}
