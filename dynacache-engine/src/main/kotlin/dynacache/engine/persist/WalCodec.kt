package dynacache.engine.persist

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.time.Instant

/*
 * A mutation as the log holds it. The entry's op byte is the command's kind and the payload its
 * arguments, big-endian and length-prefixed as the RDB's are. A TTL travels as the absolute
 * instant the engine settled on, never as the duration the client sent, so a replay lands the
 * same deadline however late it runs (spec 5.4). A conditional `SET` is logged only when it took,
 * and then as a plain one: what is logged is what changed, not what was asked.
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

/** The mutating commands' encoding, one entry per command that changed a partition's store. */
internal object WalCodec {

    /** The entry to log for [command] answered with [reply] at [now], or null when nothing changed. */
    fun encode(command: Command, reply: Reply, now: Instant): Pair<Byte, ByteArray>? {
        // An error changed nothing; nor did a nil, which is a refused SET or an empty POP.
        if (reply is Reply.Error || (reply is Reply.Bulk && reply.bytes == null)) return null
        return when (command) {
            is Command.Set -> SET to body {
                key(command.key); bytes(command.value)
                writeLong(command.ttl?.let(now::plus)?.toEpochMilli() ?: NO_TTL)
            }
            is Command.Del -> DEL to body { key(command.key) }
            is Command.Expire -> EXPIRE to body { key(command.key); writeLong(command.deadline.toEpochMilli()) }
            is Command.Persist -> PERSIST to body { key(command.key) }
            is Command.IncrBy -> INCR_BY to body { key(command.key); writeLong(command.delta) }
            is Command.Append -> APPEND to body { key(command.key); bytes(command.value) }
            is Command.HSet -> HSET to body { key(command.key); pairs(command.entries) }
            is Command.HMSet -> HSET to body { key(command.key); pairs(command.entries) }
            is Command.HDel -> HDEL to body { key(command.key); list(command.fields) }
            is Command.Push -> PUSH to body { key(command.key); end(command.end); list(command.values) }
            is Command.Pop -> POP to body { key(command.key); end(command.end) }
            is Command.LSet -> LSET to body { key(command.key); writeLong(command.index); bytes(command.value) }
            is Command.LRem -> LREM to body { key(command.key); writeLong(command.count); bytes(command.value) }
            is Command.ZAdd -> ZADD to body { key(command.key); pairs(command.entries) }
            is Command.ZRem -> ZREM to body { key(command.key); list(command.members) }
            is Command.ZIncrBy -> ZINCR_BY to body { key(command.key); bytes(command.delta); bytes(command.member) }
            is Command.FlushDb -> FLUSH_DB to ByteArray(0)
            else -> null
        }
    }

    /** The commands that redo one entry, in order: a `SET` with a TTL is a `SET` and then an `EXPIRE`. */
    fun decode(op: Byte, payload: ByteArray): List<Command> {
        val input = DataInputStream(ByteArrayInputStream(payload))
        return when (op) {
            SET -> {
                val key = input.key()
                val set = Command.Set(key, input.bytes())
                val ttl = input.readLong()
                if (ttl == NO_TTL) listOf(set) else listOf(set, Command.Expire(key, Instant.ofEpochMilli(ttl)))
            }
            DEL -> listOf(Command.Del(input.key()))
            EXPIRE -> listOf(Command.Expire(input.key(), Instant.ofEpochMilli(input.readLong())))
            PERSIST -> listOf(Command.Persist(input.key()))
            INCR_BY -> listOf(Command.IncrBy(input.key(), input.readLong()))
            APPEND -> listOf(Command.Append(input.key(), input.bytes()))
            HSET -> listOf(Command.HSet(input.key(), input.pairs()))
            HDEL -> listOf(Command.HDel(input.key(), input.list()))
            PUSH -> {
                val key = input.key()
                val end = input.end()
                listOf(Command.Push(key, input.list(), end))
            }
            POP -> listOf(Command.Pop(input.key(), input.end()))
            LSET -> listOf(Command.LSet(input.key(), input.readLong(), input.bytes()))
            LREM -> listOf(Command.LRem(input.key(), input.readLong(), input.bytes()))
            ZADD -> listOf(Command.ZAdd(input.key(), input.pairs()))
            ZREM -> listOf(Command.ZRem(input.key(), input.list()))
            ZINCR_BY -> listOf(Command.ZIncrBy(input.key(), input.bytes(), input.bytes()))
            FLUSH_DB -> listOf(Command.FlushDb)
            else -> throw IllegalArgumentException("unknown WAL op $op")
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

    private fun DataOutputStream.list(items: List<ByteArray>) {
        writeInt(items.size)
        items.forEach { bytes(it) }
    }

    private fun DataOutputStream.pairs(pairs: List<Pair<ByteArray, ByteArray>>) {
        writeInt(pairs.size)
        for ((first, second) in pairs) {
            bytes(first)
            bytes(second)
        }
    }

    private fun DataInputStream.bytes(): ByteArray = readNBytes(readInt())
    private fun DataInputStream.key(): Key = Key(bytes())
    private fun DataInputStream.end(): Command.End = if (readByte() == 0.toByte()) Command.End.HEAD else Command.End.TAIL
    private fun DataInputStream.list(): List<ByteArray> = List(readInt()) { bytes() }
    private fun DataInputStream.pairs(): List<Pair<ByteArray, ByteArray>> = List(readInt()) { bytes() to bytes() }
}
