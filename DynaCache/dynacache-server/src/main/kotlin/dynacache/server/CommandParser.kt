package dynacache.server

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.time.Clock
import java.time.DateTimeException
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
        "setex" -> exactly(name, args, 3).let { Command.Set(Key(it[0]), it[2], ttl = span(name, seconds(it[1]))) }
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

        // Key expiry: three spellings of one deadline (spec 5.4). Redis takes any value it can
        // hold here -- a deadline already past deletes the key -- so only [deadline] guards them.
        "expire" -> exactly(name, args, 2).let {
            Command.Expire(Key(it[0]), deadline(name) { now().plusSeconds(integer(it[1])) })
        }
        "pexpire" -> exactly(name, args, 2).let {
            Command.Expire(Key(it[0]), deadline(name) { now().plusMillis(integer(it[1])) })
        }
        "expireat" -> exactly(name, args, 2).let {
            Command.Expire(Key(it[0]), deadline(name) { Instant.ofEpochSecond(integer(it[1])) })
        }
        "pexpireat" -> exactly(name, args, 2).let {
            Command.Expire(Key(it[0]), deadline(name) { Instant.ofEpochMilli(integer(it[1])) })
        }
        "ttl" -> Command.Ttl(key(name, args, 1), Command.Ttl.Precision.SECONDS)
        "pttl" -> Command.Ttl(key(name, args, 1), Command.Ttl.Precision.MILLIS)
        "persist" -> Command.Persist(key(name, args, 1))

        // Sorted Set. ZRANGE/ZREVRANGE and ZRANK/ZREVRANK are each one command read from the
        // other end, which is why eleven names make nine rows here.
        "zadd" -> zadd(args)
        "zrem" -> Command.ZRem(Key(atLeast(name, args, 2)[0]), args.drop(1))
        "zrange" -> zrange(name, args, reverse = false)
        "zrevrange" -> zrange(name, args, reverse = true)
        "zrangebyscore" -> zrangeByScore(args)
        "zrank" -> exactly(name, args, 2).let { Command.ZRank(Key(it[0]), it[1]) }
        "zrevrank" -> exactly(name, args, 2).let { Command.ZRank(Key(it[0]), it[1], reverse = true) }
        "zscore" -> exactly(name, args, 2).let { Command.ZScore(Key(it[0]), it[1]) }
        "zcard" -> Command.ZCard(key(name, args, 1))
        "zincrby" -> exactly(name, args, 3).let { Command.ZIncrBy(Key(it[0]), it[1], it[2]) }
        "zscan" -> atLeast(name, args, 2).let {
            options(it.drop(2)).run { Command.ZScan(Key(it[0]), cursor(it[1]), pattern, count) }
        }

        // The CP verbs of CP spec 6. A CP key stays whole -- `cp:counter:x`, not `x` -- so the
        // dispatcher reads the same key the CP engine keys its state by (C16). A lock or
        // semaphore verb takes no session on the wire: the connection owns one (CP spec 4) and
        // the handler puts it in on the way to the engine, so the parser leaves it [NO_SESSION].
        "cp.long.set" -> exactly(name, args, 2).let { Command.Cp.LongSet(Key(it[0]), integer(it[1])) }
        "cp.long.get" -> Command.Cp.LongGet(key(name, args, 1))
        "cp.long.incr" -> Command.Cp.LongIncr(key(name, args, 1))
        "cp.long.decr" -> Command.Cp.LongDecr(key(name, args, 1))
        "cp.long.add" -> exactly(name, args, 2).let { Command.Cp.LongIncrBy(Key(it[0]), integer(it[1])) }
        "cp.long.getadd" -> exactly(name, args, 2).let { Command.Cp.LongGetAdd(Key(it[0]), integer(it[1])) }
        "cp.long.cas" -> exactly(name, args, 3).let {
            Command.Cp.LongCas(Key(it[0]), integer(it[1]), integer(it[2]))
        }
        "cp.lock.try" -> exactly(name, args, 2).let { Command.Cp.LockTry(Key(it[0]), NO_SESSION, millis(it[1])) }
        "cp.lock.unlock" -> exactly(name, args, 2).let {
            Command.Cp.LockUnlock(Key(it[0]), NO_SESSION, integer(it[1]))
        }
        "cp.lock.renew" -> exactly(name, args, 3).let {
            Command.Cp.LockRenew(Key(it[0]), NO_SESSION, integer(it[1]), millis(it[2]))
        }
        "cp.lock.state" -> Command.Cp.LockState(key(name, args, 1))
        "cp.lock.force_unlock" -> Command.Cp.LockForceUnlock(key(name, args, 1))
        "cp.sem.init" -> exactly(name, args, 2).let { Command.Cp.SemInit(Key(it[0]), counted(it[1])) }
        "cp.sem.acquire" -> exactly(name, args, 2).let {
            Command.Cp.SemAcquire(Key(it[0]), NO_SESSION, counted(it[1]))
        }
        "cp.sem.release" -> exactly(name, args, 2).let {
            Command.Cp.SemRelease(Key(it[0]), NO_SESSION, counted(it[1]))
        }
        "cp.sem.available" -> Command.Cp.SemAvailable(key(name, args, 1))
        "cp.sem.drain" -> Command.Cp.SemDrain(key(name, args, 1), NO_SESSION)
        "cp.latch.set" -> exactly(name, args, 2).let { Command.Cp.LatchSet(Key(it[0]), counted(it[1])) }
        "cp.latch.down" -> Command.Cp.LatchDown(key(name, args, 1))
        "cp.latch.get" -> Command.Cp.LatchGet(key(name, args, 1))
        "cp.latch.reset" -> exactly(name, args, 2).let { Command.Cp.LatchReset(Key(it[0]), counted(it[1])) }
        "cp.ref.set" -> exactly(name, args, 2).let { Command.Cp.RefSet(Key(it[0]), it[1]) }
        "cp.ref.get" -> Command.Cp.RefGet(key(name, args, 1))
        "cp.ref.cas" -> exactly(name, args, 3).let { Command.Cp.RefCas(Key(it[0]), it[1], it[2]) }
        "cp.session.create" -> exactly(name, args, 0).let { Command.Cp.SessionCreate() }
        "cp.session.heartbeat" -> Command.Cp.SessionHeartbeat(integer(exactly(name, args, 1)[0]))
        "cp.session.close" -> Command.Cp.SessionClose(integer(exactly(name, args, 1)[0]))
        "cp.info" -> exactly(name, args, 0).let { Command.Cp.Info }
        "cp.members" -> exactly(name, args, 0).let { Command.Cp.Members }

        else -> unknown(name, args)
    }

    /** `SET key value [NX|XX] [EX s|PX ms]`, in any order, each once (spec 2.1). */
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
                    ttl = if (flag == "ex") span("set", seconds(args[at + 1])) else span("set", millis(args[at + 1]))
                }
                else -> syntaxError()
            }
            at += if (takesArgument) 2 else 1
        }
        return Command.Set(Key(args[0]), args[1], condition, ttl)
    }

    /**
     * `ZADD key [NX|XX] [CH] score member [score member ...]`. The flags stop at the first token
     * that is not one, which is the score: a member may be spelled `nx` and still be a member.
     */
    private fun zadd(args: List<ByteArray>): Command.ZAdd {
        if (args.isEmpty()) wrongArity("zadd")
        var condition: Command.Set.Condition? = null
        var changed = false
        var at = 1
        while (at < args.size) {
            val flag = args[at].text().lowercase()
            when (flag) {
                "nx", "xx" -> {
                    if (condition != null) syntaxError()
                    condition = if (flag == "nx") Command.Set.Condition.NX else Command.Set.Condition.XX
                }
                "ch" -> changed = true
                else -> break
            }
            at++
        }
        return Command.ZAdd(Key(args[0]), pairs("zadd", args, from = at), condition, changed)
    }

    /** `ZRANGE`/`ZREVRANGE key start stop [WITHSCORES]`: one window read from either end. */
    private fun zrange(name: String, args: List<ByteArray>, reverse: Boolean): Command.ZRange {
        within(name, args, 3, 4)
        if (args.size == 4 && args[3].text().lowercase() != "withscores") syntaxError()
        return Command.ZRange(Key(args[0]), integer(args[1]), integer(args[2]), args.size == 4, reverse)
    }

    /** `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`; the bounds stay bytes. */
    private fun zrangeByScore(args: List<ByteArray>): Command.ZRangeByScore {
        if (args.size < 3) wrongArity("zrangebyscore")
        var withScores = false
        var offset = 0L
        var count = -1L
        var at = 3
        while (at < args.size) {
            when (args[at].text().lowercase()) {
                "withscores" -> {
                    withScores = true
                    at += 1
                }
                "limit" -> {
                    if (at + 2 >= args.size) syntaxError()
                    offset = integer(args[at + 1])
                    count = integer(args[at + 2])
                    at += 3
                }
                else -> syntaxError()
            }
        }
        return Command.ZRangeByScore(Key(args[0]), args[1], args[2], withScores, offset, count)
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

    // ---- expiry, the one place in this parser that does arithmetic ---------------------------

    /**
     * The deadline [compute] names, when a clock can hold it; Redis's `invalid expire time`
     * otherwise. Every expiry-taking row goes through here, and that is what makes the parser
     * total: an argument big enough to overflow the instant arithmetic, or to name a moment past
     * what epoch milliseconds can count -- which is the bound Redis itself checks, and the one
     * the engine's WAL writes a deadline in -- becomes a reply the client reads, not a throwable
     * the pipeline treats as fatal and closes the connection over (C8).
     */
    private fun deadline(name: String, compute: () -> Instant): Instant =
        try {
            compute().also { it.toEpochMilli() }
        } catch (overflowed: ArithmeticException) {
            rejectExpireTime(name)
        } catch (unrepresentable: DateTimeException) {
            rejectExpireTime(name)
        }

    /**
     * A relative TTL the `SET` family will take: strictly positive, and near enough that the
     * engine's own `now + ttl` is a moment that exists. Redis refuses a zero or negative span
     * outright here, unlike `EXPIRE`, where the same number is a deadline already past.
     */
    private fun span(name: String, ttl: Duration): Duration {
        if (ttl.isZero || ttl.isNegative) rejectExpireTime(name)
        deadline(name) { now().plus(ttl) } // the engine's own sum, refused here while it is a reply
        return ttl
    }

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

    /** Redis names the command in its own lower-case spelling here, as it does for arity. */
    private fun rejectExpireTime(name: String): Nothing = reject("invalid expire time in '$name' command")

    /** A permit or latch count: an integer that fits in one and is not negative. */
    private fun counted(token: ByteArray): Int =
        integer(token).let { if (it < 0 || it > Int.MAX_VALUE) reject("value is out of range") else it.toInt() }

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
        val TTL_FLAGS = setOf("ex", "px")

        /** Redis's own `COUNT` when a `SCAN` does not name one. */
        const val DEFAULT_SCAN_COUNT = 10
    }
}

/**
 * The session a lock or semaphore verb carries before the connection's own is put in. Session ids
 * start at 1 (CP spec 4), so nothing the CP engine hands out is ever mistaken for this.
 */
internal const val NO_SESSION = 0L

/**
 * A token as text. ISO-8859-1 is a bijection over the 256 byte values, so a token that is not
 * text survives the trip and back, which is what an error message quoting it needs.
 */
private fun ByteArray.text(): String = toString(Charsets.ISO_8859_1)
