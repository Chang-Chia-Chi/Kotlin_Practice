package dynacache.server

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * What a token list means: a [Command] the engine can run, or the [Reply.Error] to write back.
 * A parse failure is an answer, not an exception: the connection stays open and the client
 * reads the error, exactly as Redis behaves.
 */
sealed interface Parsed {
    data class Ok(val command: Command) : Parsed
    data class Failed(val error: Reply.Error) : Parsed
}

/**
 * Turns the tokens of one client frame into a [Command]. Names are matched case-insensitively;
 * everything else is bytes and stays bytes.
 *
 * The parser is where the wire's several spellings of one meaning collapse: `EXPIRE`, `PEXPIRE`
 * and `EXPIREAT` all reduce to the absolute instant the engine stores (spec 5.4), and `INCR`,
 * `DECR`, `INCRBY` and `DECRBY` to a signed delta. [clock] is what "now" means while it does so.
 */
class CommandParser(private val clock: Clock = Clock.systemUTC()) {

    /** [tokens] is one client frame, never empty: [RespDecoder] skips the frames that would be. */
    fun parse(tokens: List<ByteArray>): Parsed =
        try {
            Parsed.Ok(dispatch(tokens[0].text().lowercase(), tokens.drop(1)))
        } catch (rejected: Rejected) {
            Parsed.Failed(rejected.error)
        }

    private fun dispatch(name: String, args: List<ByteArray>): Command = when (name) {
        // Server and keyspace. A trailing argument this node has no use for -- INFO's section,
        // FLUSHDB's ASYNC -- is accepted and ignored, which is what a Redis client expects.
        "ping" -> exactly(name, args, 0).let { Command.Ping }
        "command" -> Command.CommandTable
        "info" -> within(name, args, 0, 1).let { Command.Info }
        "dbsize" -> exactly(name, args, 0).let { Command.DbSize }
        "flushdb" -> within(name, args, 0, 1).let { Command.FlushDb }
        "keys" -> Command.Keys(exactly(name, args, 1)[0])
        "randomkey" -> exactly(name, args, 0).let { Command.RandomKey }
        "type" -> Command.Type(key(name, args, 1))
        "del" -> keys(name, args).let { if (it.size == 1) Command.Del(it[0]) else Command.DelKeys(it) }
        "exists" -> keys(name, args).let { if (it.size == 1) Command.Exists(it[0]) else Command.ExistsKeys(it) }
        "scan" -> atLeast(name, args, 1).let { options(it.drop(1)).run { Command.Scan(cursor(it[0]), pattern, count) } }

        // String
        "get" -> Command.Get(key(name, args, 1))
        "set" -> set(args)
        "setnx" -> exactly(name, args, 2).let { Command.Set(Key(it[0]), it[1], Command.Set.Condition.NX) }
        "setex" -> exactly(name, args, 3).let { Command.Set(Key(it[0]), it[2], ttl = seconds(it[1])) }
        "psetex" -> exactly(name, args, 3).let { Command.Set(Key(it[0]), it[2], ttl = millis(it[1])) }
        "incr" -> Command.IncrBy(key(name, args, 1), 1)
        "decr" -> Command.IncrBy(key(name, args, 1), -1)
        "incrby" -> exactly(name, args, 2).let { Command.IncrBy(Key(it[0]), integer(it[1])) }
        "decrby" -> exactly(name, args, 2).let { Command.IncrBy(Key(it[0]), -integer(it[1])) }
        "append" -> exactly(name, args, 2).let { Command.Append(Key(it[0]), it[1]) }
        "strlen" -> Command.StrLen(key(name, args, 1))
        "mget" -> Command.MGet(keys(name, args))
        "mset" -> Command.MSet(pairs(name, args, from = 0).map { (k, v) -> Key(k) to v })

        // Hash
        "hget" -> exactly(name, args, 2).let { Command.HGet(Key(it[0]), it[1]) }
        "hset" -> Command.HSet(Key(atLeast(name, args, 3)[0]), pairs(name, args, from = 1))
        "hmset" -> Command.HMSet(Key(atLeast(name, args, 3)[0]), pairs(name, args, from = 1))
        "hdel" -> Command.HDel(Key(atLeast(name, args, 2)[0]), args.drop(1))
        "hmget" -> Command.HMGet(Key(atLeast(name, args, 2)[0]), args.drop(1))
        "hgetall" -> Command.HGetAll(key(name, args, 1))
        "hexists" -> exactly(name, args, 2).let { Command.HExists(Key(it[0]), it[1]) }
        "hkeys" -> Command.HKeys(key(name, args, 1))
        "hvals" -> Command.HVals(key(name, args, 1))
        "hlen" -> Command.HLen(key(name, args, 1))
        "hscan" -> atLeast(name, args, 2).let {
            options(it.drop(2)).run { Command.HScan(Key(it[0]), cursor(it[1]), pattern, count) }
        }

        // List
        "lpush" -> Command.Push(Key(atLeast(name, args, 2)[0]), args.drop(1), Command.End.HEAD)
        "rpush" -> Command.Push(Key(atLeast(name, args, 2)[0]), args.drop(1), Command.End.TAIL)
        "lpop" -> Command.Pop(key(name, args, 1), Command.End.HEAD)
        "rpop" -> Command.Pop(key(name, args, 1), Command.End.TAIL)
        "lrange" -> exactly(name, args, 3).let { Command.LRange(Key(it[0]), integer(it[1]), integer(it[2])) }
        "llen" -> Command.LLen(key(name, args, 1))
        "lindex" -> exactly(name, args, 2).let { Command.LIndex(Key(it[0]), integer(it[1])) }
        "lset" -> exactly(name, args, 3).let { Command.LSet(Key(it[0]), integer(it[1]), it[2]) }
        "lrem" -> exactly(name, args, 3).let { Command.LRem(Key(it[0]), integer(it[1]), it[2]) }

        // Key expiry: three spellings of one deadline (spec 5.4)
        "expire" -> exactly(name, args, 2).let { Command.Expire(Key(it[0]), now().plusSeconds(integer(it[1]))) }
        "pexpire" -> exactly(name, args, 2).let { Command.Expire(Key(it[0]), now().plusMillis(integer(it[1]))) }
        "expireat" -> exactly(name, args, 2).let { Command.Expire(Key(it[0]), Instant.ofEpochSecond(integer(it[1]))) }
        "ttl" -> Command.Ttl(key(name, args, 1), Command.Ttl.Precision.SECONDS)
        "pttl" -> Command.Ttl(key(name, args, 1), Command.Ttl.Precision.MILLIS)
        "persist" -> Command.Persist(key(name, args, 1))

        // The Sorted Set rows (ZADD, ZREM, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANK, ZREVRANK,
        // ZSCORE, ZCARD, ZINCRBY, ZSCAN) belong here, and arrive with their variants in T07.

        else -> unknown(name, args)
    }

    /** `SET key value [NX|XX] [EX s|PX ms|EXAT unix-s|PXAT unix-ms]`, in any order, each once. */
    private fun set(args: List<ByteArray>): Command.Set {
        if (args.size < 2) wrongArity("set")
        var condition: Command.Set.Condition? = null
        var ttl: Duration? = null
        var at = 2
        while (at < args.size) {
            val flag = args[at].text().lowercase()
            // A repeated or contradictory flag is a syntax error, exactly as in Redis.
            val takesArgument = flag in TTL_FLAGS
            if (takesArgument && at + 1 >= args.size) syntaxError()
            when {
                flag == "nx" || flag == "xx" -> {
                    if (condition != null) syntaxError()
                    condition = if (flag == "nx") Command.Set.Condition.NX else Command.Set.Condition.XX
                }
                takesArgument -> {
                    if (ttl != null) syntaxError()
                    ttl = when (flag) {
                        "ex" -> seconds(args[at + 1])
                        "px" -> millis(args[at + 1])
                        "exat" -> until(Instant.ofEpochSecond(integer(args[at + 1])))
                        else -> until(Instant.ofEpochMilli(integer(args[at + 1])))
                    }
                }
                else -> syntaxError()
            }
            at += if (takesArgument) 2 else 1
        }
        return Command.Set(Key(args[0]), args[1], condition, ttl)
    }

    /** `[MATCH pattern] [COUNT n]`, the tail both `SCAN` and `HSCAN` end with. */
    private class Options(val pattern: ByteArray?, val count: Int)

    private fun options(args: List<ByteArray>): Options {
        var pattern: ByteArray? = null
        var count = DEFAULT_SCAN_COUNT
        var at = 0
        while (at < args.size) {
            if (at + 1 >= args.size) syntaxError()
            when (args[at].text().lowercase()) {
                "match" -> pattern = args[at + 1]
                "count" -> count = integer(args[at + 1])
                    .let { if (it < 1 || it > Int.MAX_VALUE) syntaxError() else it.toInt() }
                else -> syntaxError()
            }
            at += 2
        }
        return Options(pattern, count)
    }

    private fun now(): Instant = clock.instant()

    private fun seconds(token: ByteArray) = Duration.ofSeconds(integer(token))

    private fun millis(token: ByteArray) = Duration.ofMillis(integer(token))

    /** The span from now to [deadline]; how `EXAT` and `PXAT` reach [Command.Set]'s duration. */
    private fun until(deadline: Instant): Duration = Duration.between(clock.instant(), deadline)

    // ---- errors, in Redis's own wording ------------------------------------------------------

    /** Carries the reply out of a nested parse; never leaves [parse]. */
    private class Rejected(val error: Reply.Error) : RuntimeException(null, null, false, false)

    private fun reject(message: String): Nothing = throw Rejected(Reply.Error("ERR", message))

    private fun unknown(name: String, args: List<ByteArray>): Nothing =
        reject(
            "unknown command '$name', with args beginning with: " +
                args.joinToString("") { "'${it.text()}', " },
        )

    private fun wrongArity(name: String): Nothing =
        reject("wrong number of arguments for '$name' command")

    private fun syntaxError(): Nothing = reject("syntax error")

    private fun integer(token: ByteArray): Long =
        token.text().toLongOrNull() ?: reject("value is not an integer or out of range")

    /** A cursor is unsigned on the wire; ours packs a partition index in its high bits (C15). */
    private fun cursor(token: ByteArray): Long =
        token.text().toULongOrNull()?.toLong() ?: reject("invalid cursor")

    // ---- argument shapes ---------------------------------------------------------------------

    /** [args] when there are exactly [arity] of them; the arity error otherwise. */
    private fun exactly(name: String, args: List<ByteArray>, arity: Int): List<ByteArray> =
        within(name, args, arity, arity)

    private fun atLeast(name: String, args: List<ByteArray>, min: Int): List<ByteArray> =
        within(name, args, min, Int.MAX_VALUE)

    private fun within(name: String, args: List<ByteArray>, min: Int, max: Int): List<ByteArray> {
        if (args.size < min || args.size > max) wrongArity(name)
        return args
    }

    /** The key of a command that takes exactly [arity] arguments, the key first. */
    private fun key(name: String, args: List<ByteArray>, arity: Int): Key = Key(exactly(name, args, arity)[0])

    /** Every argument as a key: the variadic `DEL`, `EXISTS` and `MGET` shape. */
    private fun keys(name: String, args: List<ByteArray>): List<Key> = atLeast(name, args, 1).map { Key(it) }

    /** The `field value ...` tail of `HSET`, `HMSET` and `MSET`; an odd tail is an arity error. */
    private fun pairs(name: String, args: List<ByteArray>, from: Int): List<Pair<ByteArray, ByteArray>> {
        val tail = args.size - from
        if (tail < 2 || tail % 2 != 0) wrongArity(name)
        return (from until args.size step 2).map { args[it] to args[it + 1] }
    }

    private companion object {
        val TTL_FLAGS = setOf("ex", "px", "exat", "pxat")

        /** Redis's own `COUNT` when a `SCAN` does not name one. */
        const val DEFAULT_SCAN_COUNT = 10
    }
}

/**
 * A token as text. ISO-8859-1 is a bijection over the 256 byte values, so a token that is not
 * text survives the trip and back, which is what an error message quoting it needs.
 */
private fun ByteArray.text(): String = toString(Charsets.ISO_8859_1)
