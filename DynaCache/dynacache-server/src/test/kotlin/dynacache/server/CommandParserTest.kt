package dynacache.server

import dynacache.engine.Command
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.random.Random
import kotlin.reflect.KClass

private val FIXED_CLOCK: Clock = Clock.fixed(Instant.ofEpochSecond(1_000_000), ZoneOffset.UTC)

/** The token list a client's frame becomes, written the way the command reads on the wire. */
private fun tokens(vararg words: String): List<ByteArray> = words.map { it.toByteArray(Charsets.ISO_8859_1) }

private fun parse(vararg words: String): Parsed = CommandParser(FIXED_CLOCK).parse(tokens(*words))

private fun command(vararg words: String): Command = (parse(*words) as Parsed.Ok).command

private fun error(vararg words: String): Reply.Error = (parse(*words) as Parsed.Failed).error

/** A token as text; ISO-8859-1 both ways, the way the parser reads one. */
private fun ByteArray.text(): String = toString(Charsets.ISO_8859_1)

/** One row of the command table: the wire text, the variant it must become, and what it must mean. */
private class Row(val wire: String, val variant: KClass<out Command>, val means: (Command) -> Unit = {})

private fun row(wire: String, variant: KClass<out Command>, means: (Command) -> Unit = {}) =
    Row(wire, variant, means)

private fun at(secondsFromNow: Long): Instant = FIXED_CLOCK.instant().plusSeconds(secondsFromNow)

class CommandParserTest {

    /**
     * One row per command name the engine has today (spec 2.1, tickets T02 to T09). The variant
     * proves the name is wired; the probe proves the meaning wherever two names share a variant.
     * Every command name the engine has is here, Sorted Set included (T16).
     */
    @Test
    fun parser_maps_every_command() {
        val table = listOf(
            // Server and keyspace
            row("PING", Command.Ping::class),
            row("COMMAND", Command.CommandTable::class),
            row("INFO", Command.Info::class),
            row("DBSIZE", Command.DbSize::class),
            row("FLUSHDB", Command.FlushDb::class),
            row("KEYS h?llo*", Command.Keys::class) { assertEquals("h?llo*", (it as Command.Keys).pattern.text()) },
            row("RANDOMKEY", Command.RandomKey::class),
            row("TYPE k", Command.Type::class),
            row("DEL k", Command.Del::class),
            row("DEL a b", Command.DelKeys::class) { assertEquals(2, (it as Command.DelKeys).keys.size) },
            row("EXISTS k", Command.Exists::class),
            row("EXISTS a b", Command.ExistsKeys::class) { assertEquals(2, (it as Command.ExistsKeys).keys.size) },
            row("SCAN 0", Command.Scan::class) {
                assertEquals(0L, (it as Command.Scan).cursor)
                assertNull(it.pattern)
                assertEquals(10, it.count)
            },
            row("SCAN 17 MATCH a* COUNT 50", Command.Scan::class) {
                assertEquals(17L, (it as Command.Scan).cursor)
                assertEquals("a*", it.pattern!!.text())
                assertEquals(50, it.count)
            },
            // String
            row("GET k", Command.Get::class),
            row("SET k v", Command.Set::class),
            row("SETNX k v", Command.Set::class) {
                assertEquals(Command.Set.Condition.NX, (it as Command.Set).condition)
            },
            row("SETEX k 30 v", Command.Set::class),
            row("INCR k", Command.IncrBy::class) { assertEquals(1L, (it as Command.IncrBy).delta) },
            row("DECR k", Command.IncrBy::class) { assertEquals(-1L, (it as Command.IncrBy).delta) },
            row("INCRBY k 5", Command.IncrBy::class) { assertEquals(5L, (it as Command.IncrBy).delta) },
            row("DECRBY k 5", Command.IncrBy::class) { assertEquals(-5L, (it as Command.IncrBy).delta) },
            row("APPEND k v", Command.Append::class) { assertEquals("v", (it as Command.Append).value.text()) },
            row("STRLEN k", Command.StrLen::class),
            row("MGET a b", Command.MGet::class) { assertEquals(2, (it as Command.MGet).keys.size) },
            row("MSET a 1 b 2", Command.MSet::class) { assertEquals(2, (it as Command.MSet).keys.size) },
            // Hash
            row("HGET k f", Command.HGet::class) { assertEquals("f", (it as Command.HGet).field.text()) },
            row("HSET k f v", Command.HSet::class) { assertEquals(1, (it as Command.HSet).entries.size) },
            row("HSET k f v g w", Command.HSet::class) { assertEquals(2, (it as Command.HSet).entries.size) },
            row("HDEL k f g", Command.HDel::class) { assertEquals(2, (it as Command.HDel).fields.size) },
            row("HGETALL k", Command.HGetAll::class),
            row("HMGET k f g", Command.HMGet::class) { assertEquals(2, (it as Command.HMGet).fields.size) },
            row("HMSET k f v", Command.HMSet::class) { assertEquals(1, (it as Command.HMSet).entries.size) },
            row("HEXISTS k f", Command.HExists::class),
            row("HKEYS k", Command.HKeys::class),
            row("HVALS k", Command.HVals::class),
            row("HLEN k", Command.HLen::class),
            row("HSCAN k 3 MATCH f* COUNT 7", Command.HScan::class) {
                assertEquals(3L, (it as Command.HScan).cursor)
                assertEquals("f*", it.pattern!!.text())
                assertEquals(7, it.count)
            },
            // List
            row("LPUSH k a b", Command.Push::class) {
                assertEquals(Command.End.HEAD, (it as Command.Push).end)
                assertEquals(2, it.values.size)
            },
            row("RPUSH k a", Command.Push::class) { assertEquals(Command.End.TAIL, (it as Command.Push).end) },
            row("LPOP k", Command.Pop::class) { assertEquals(Command.End.HEAD, (it as Command.Pop).end) },
            row("RPOP k", Command.Pop::class) { assertEquals(Command.End.TAIL, (it as Command.Pop).end) },
            row("LRANGE k 0 -1", Command.LRange::class) {
                assertEquals(0L, (it as Command.LRange).start)
                assertEquals(-1L, it.stop)
            },
            row("LLEN k", Command.LLen::class),
            row("LINDEX k -2", Command.LIndex::class) { assertEquals(-2L, (it as Command.LIndex).index) },
            row("LSET k 1 v", Command.LSet::class) { assertEquals(1L, (it as Command.LSet).index) },
            row("LREM k -3 v", Command.LRem::class) { assertEquals(-3L, (it as Command.LRem).count) },
            // Key expiry: three spellings of one deadline (spec 5.4)
            row("EXPIRE k 10", Command.Expire::class) { assertEquals(at(10), (it as Command.Expire).deadline) },
            row("PEXPIRE k 10000", Command.Expire::class) { assertEquals(at(10), (it as Command.Expire).deadline) },
            row("EXPIREAT k ${FIXED_CLOCK.instant().epochSecond + 10}", Command.Expire::class) {
                assertEquals(at(10), (it as Command.Expire).deadline)
            },
            row("TTL k", Command.Ttl::class) {
                assertEquals(Command.Ttl.Precision.SECONDS, (it as Command.Ttl).precision)
            },
            row("PTTL k", Command.Ttl::class) {
                assertEquals(Command.Ttl.Precision.MILLIS, (it as Command.Ttl).precision)
            },
            row("PEXPIREAT k ${FIXED_CLOCK.instant().toEpochMilli() + 10_000}", Command.Expire::class) {
                assertEquals(at(10), (it as Command.Expire).deadline)
            },
            row("PERSIST k", Command.Persist::class),
            // Sorted Set (T07's eleven names, in nine variants: ZRANGE/ZREVRANGE and
            // ZRANK/ZREVRANK are each one command read from the other end)
            row("ZADD k 1 a", Command.ZAdd::class) {
                assertEquals(1, (it as Command.ZAdd).entries.size)
                assertNull(it.condition)
                assertFalse(it.changed)
            },
            row("ZADD k NX CH 1 a 2 b", Command.ZAdd::class) {
                assertEquals(2, (it as Command.ZAdd).entries.size)
                assertEquals(Command.Set.Condition.NX, it.condition)
                assertTrue(it.changed)
            },
            row("ZADD k XX 1 a", Command.ZAdd::class) {
                assertEquals(Command.Set.Condition.XX, (it as Command.ZAdd).condition)
            },
            row("ZREM k a b", Command.ZRem::class) { assertEquals(2, (it as Command.ZRem).members.size) },
            row("ZRANGE k 0 -1", Command.ZRange::class) {
                assertEquals(0L, (it as Command.ZRange).start)
                assertEquals(-1L, it.stop)
                assertFalse(it.withScores)
                assertFalse(it.reverse)
            },
            row("ZRANGE k 0 -1 WITHSCORES", Command.ZRange::class) {
                assertTrue((it as Command.ZRange).withScores)
                assertFalse(it.reverse)
            },
            row("ZREVRANGE k 0 -1", Command.ZRange::class) { assertTrue((it as Command.ZRange).reverse) },
            row("ZRANGEBYSCORE k (1 +inf", Command.ZRangeByScore::class) {
                assertEquals("(1", (it as Command.ZRangeByScore).min.text())
                assertEquals("+inf", it.max.text())
                assertFalse(it.withScores)
                assertEquals(0L, it.offset)
                assertEquals(-1L, it.count)
            },
            row("ZRANGEBYSCORE k -inf 5 WITHSCORES LIMIT 2 3", Command.ZRangeByScore::class) {
                assertTrue((it as Command.ZRangeByScore).withScores)
                assertEquals(2L, it.offset)
                assertEquals(3L, it.count)
            },
            row("ZRANK k m", Command.ZRank::class) { assertFalse((it as Command.ZRank).reverse) },
            row("ZREVRANK k m", Command.ZRank::class) { assertTrue((it as Command.ZRank).reverse) },
            row("ZSCORE k m", Command.ZScore::class) { assertEquals("m", (it as Command.ZScore).member.text()) },
            row("ZCARD k", Command.ZCard::class),
            row("ZINCRBY k 5 m", Command.ZIncrBy::class) {
                assertEquals("5", (it as Command.ZIncrBy).delta.text())
                assertEquals("m", it.member.text())
            },
            row("ZSCAN k 3 MATCH m* COUNT 7", Command.ZScan::class) {
                assertEquals(3L, (it as Command.ZScan).cursor)
                assertEquals("m*", it.pattern!!.text())
                assertEquals(7, it.count)
            },
            // The CP verbs of CP spec 6 (T44). A CP key stays whole on the wire, and a lock or
            // semaphore verb carries no session: the connection owns that (CP spec 4), so the
            // parser leaves it unset and the handler fills it in.
            row("CP.LONG.SET cp:counter:k 5", Command.Cp.LongSet::class) {
                assertEquals(5L, (it as Command.Cp.LongSet).value)
                assertEquals("cp:counter:k", it.key.toString())
                assertNull(it.ttl)
            },
            row("CP.LONG.GET cp:counter:k", Command.Cp.LongGet::class),
            row("CP.LONG.INCR cp:counter:k", Command.Cp.LongIncr::class),
            row("CP.LONG.DECR cp:counter:k", Command.Cp.LongDecr::class),
            row("CP.LONG.ADD cp:counter:k -5", Command.Cp.LongIncrBy::class) {
                assertEquals(-5L, (it as Command.Cp.LongIncrBy).delta)
            },
            row("CP.LONG.GETADD cp:counter:k -5", Command.Cp.LongGetAdd::class) {
                assertEquals(-5L, (it as Command.Cp.LongGetAdd).delta)
            },
            row("CP.LONG.CAS cp:counter:k 1 2", Command.Cp.LongCas::class) {
                assertEquals(1L, (it as Command.Cp.LongCas).expected)
                assertEquals(2L, it.new)
            },
            row("CP.LOCK.TRY cp:lock:k 30000", Command.Cp.LockTry::class) {
                assertEquals(Duration.ofSeconds(30), (it as Command.Cp.LockTry).lease)
                assertEquals(NO_SESSION, it.session)
            },
            row("CP.LOCK.UNLOCK cp:lock:k 7", Command.Cp.LockUnlock::class) {
                assertEquals(7L, (it as Command.Cp.LockUnlock).token)
                assertEquals(NO_SESSION, it.session)
            },
            row("CP.LOCK.RENEW cp:lock:k 7 30000", Command.Cp.LockRenew::class) {
                assertEquals(7L, (it as Command.Cp.LockRenew).token)
                assertEquals(Duration.ofSeconds(30), it.lease)
            },
            row("CP.LOCK.STATE cp:lock:k", Command.Cp.LockState::class),
            row("CP.LOCK.FORCE_UNLOCK cp:lock:k", Command.Cp.LockForceUnlock::class),
            row("CP.SEM.INIT cp:sem:k 3", Command.Cp.SemInit::class) {
                assertEquals(3, (it as Command.Cp.SemInit).permits)
            },
            row("CP.SEM.ACQUIRE cp:sem:k 2", Command.Cp.SemAcquire::class) {
                assertEquals(2, (it as Command.Cp.SemAcquire).permits)
                assertEquals(NO_SESSION, it.session)
            },
            row("CP.SEM.RELEASE cp:sem:k 2", Command.Cp.SemRelease::class) {
                assertEquals(2, (it as Command.Cp.SemRelease).permits)
            },
            row("CP.SEM.AVAILABLE cp:sem:k", Command.Cp.SemAvailable::class),
            row("CP.SEM.DRAIN cp:sem:k", Command.Cp.SemDrain::class) {
                assertEquals(NO_SESSION, (it as Command.Cp.SemDrain).session)
            },
            row("CP.LATCH.SET cp:latch:k 4", Command.Cp.LatchSet::class) {
                assertEquals(4, (it as Command.Cp.LatchSet).count)
            },
            row("CP.LATCH.DOWN cp:latch:k", Command.Cp.LatchDown::class),
            row("CP.LATCH.GET cp:latch:k", Command.Cp.LatchGet::class),
            row("CP.LATCH.RESET cp:latch:k 4", Command.Cp.LatchReset::class) {
                assertEquals(4, (it as Command.Cp.LatchReset).count)
            },
            row("CP.REF.SET cp:ref:k v", Command.Cp.RefSet::class) {
                assertEquals("v", (it as Command.Cp.RefSet).value.text())
            },
            row("CP.REF.GET cp:ref:k", Command.Cp.RefGet::class),
            row("CP.REF.CAS cp:ref:k a b", Command.Cp.RefCas::class) {
                assertEquals("a", (it as Command.Cp.RefCas).expected.text())
                assertEquals("b", it.new.text())
            },
            row("CP.SESSION.CREATE", Command.Cp.SessionCreate::class),
            row("CP.SESSION.HEARTBEAT 5", Command.Cp.SessionHeartbeat::class) {
                assertEquals(5L, (it as Command.Cp.SessionHeartbeat).session)
            },
            row("CP.SESSION.CLOSE 5", Command.Cp.SessionClose::class) {
                assertEquals(5L, (it as Command.Cp.SessionClose).session)
            },
            row("CP.INFO", Command.Cp.Info::class),
            row("CP.MEMBERS", Command.Cp.Members::class),
        )

        for (case in table) {
            val words = case.wire.split(' ')
            val parsed = parse(*words.toTypedArray())
            assertTrue(parsed is Parsed.Ok, "${case.wire} did not parse: $parsed")
            val command = (parsed as Parsed.Ok).command
            assertEquals(case.variant, command::class, case.wire)
            case.means(command)
            // The lower-case spelling is the same command; a Redis name is case-insensitive.
            val lowerCase = listOf(words[0].lowercase()) + words.drop(1)
            assertEquals(command::class, command(*lowerCase.toTypedArray())::class, case.wire)
        }
    }

    @Test
    fun `a command name is case-insensitive`() {
        val get = command("GeT", "k") as Command.Get
        assertEquals("k", get.key.toString())
    }

    @Test
    fun `SET reduces its flags to a condition and one TTL`() {
        val plain = command("set", "k", "v") as Command.Set
        assertEquals("v", plain.value.toString(Charsets.ISO_8859_1))
        assertNull(plain.condition)
        assertNull(plain.ttl)

        assertEquals(Command.Set.Condition.NX, (command("set", "k", "v", "nx") as Command.Set).condition)
        assertEquals(Command.Set.Condition.XX, (command("SET", "k", "v", "XX") as Command.Set).condition)
        assertEquals(Duration.ofSeconds(30), (command("set", "k", "v", "EX", "30") as Command.Set).ttl)
        assertEquals(Duration.ofMillis(1500), (command("set", "k", "v", "px", "1500") as Command.Set).ttl)

        // Order does not matter, and both halves survive together.
        val both = command("set", "k", "v", "EX", "5", "NX") as Command.Set
        assertEquals(Command.Set.Condition.NX, both.condition)
        assertEquals(Duration.ofSeconds(5), both.ttl)
    }

    @Test
    fun `SET rejects contradictory and unknown flags`() {
        val syntaxError = Reply.Error("ERR", "syntax error")
        assertEquals(syntaxError, error("set", "k", "v", "NX", "XX"))
        assertEquals(syntaxError, error("set", "k", "v", "XX", "nx"))
        assertEquals(syntaxError, error("set", "k", "v", "EX", "5", "PX", "5"))
        assertEquals(syntaxError, error("set", "k", "v", "KEEPTTL"))
        assertEquals(syntaxError, error("set", "k", "v", "EX"))
        assertEquals(
            Reply.Error("ERR", "value is not an integer or out of range"),
            error("set", "k", "v", "EX", "banana"),
        )
    }

    @Test
    fun `SETNX and SETEX are spellings of SET`() {
        val setnx = command("setnx", "k", "v") as Command.Set
        assertEquals(Command.Set.Condition.NX, setnx.condition)
        assertEquals(Duration.ofSeconds(30), (command("setex", "k", "30", "v") as Command.Set).ttl)
        assertEquals("v", (command("setex", "k", "30", "v") as Command.Set).value.toString(Charsets.ISO_8859_1))
    }

    /**
     * No spec line asks for `EXAT`, `PXAT` or `PSETEX`. Spec 2.1 gives `SET` the flags `NX`, `XX`,
     * `EX` and `PX`, and the Redis-compat-for-CP set (CP spec 6.2) names `SETEX` without its
     * millisecond twin, so T62 deleted the three rows instead of leaving unrequested commands in
     * the parser. `PEXPIREAT` stays: it was the forward's spelling of every `Command.Expire`
     * from T19 until the codec took over (T64, T65).
     */
    @Test
    fun `EXAT PXAT and PSETEX are not commands here`() {
        val syntaxError = Reply.Error("ERR", "syntax error")
        assertEquals(syntaxError, error("set", "k", "v", "EXAT", "1000000060"))
        assertEquals(syntaxError, error("set", "k", "v", "PXAT", "1000000250"))
        assertEquals(
            Reply.Error("ERR", "unknown command 'psetex', with args beginning with: 'k', '30', 'v', "),
            error("psetex", "k", "30", "v"),
        )
        assertTrue(parse("pexpireat", "k", "1000000000000") is Parsed.Ok, "PEXPIREAT is the forward's carrier")
    }

    @Test
    fun `an unknown command names itself and the arguments it came with`() {
        assertEquals(
            Reply.Error("ERR", "unknown command 'foo', with args beginning with: 'a', 'b', "),
            error("foo", "a", "b"),
        )
        assertEquals(
            Reply.Error("ERR", "unknown command 'zrevrangebyscore', with args beginning with: 'k', '5', '1', "),
            error("ZREVRANGEBYSCORE", "k", "5", "1"),
        )
    }

    @Test
    fun `too few or too many arguments is Redis's arity error, under the lower-case name`() {
        for (wire in listOf(listOf("GET"), listOf("get", "a", "b"), listOf("PING", "hello"))) {
            val name = wire[0].lowercase()
            assertEquals(
                Reply.Error("ERR", "wrong number of arguments for '$name' command"),
                error(*wire.toTypedArray()),
                wire.joinToString(" "),
            )
        }
        // An odd field/value tail is an arity error too, not a syntax error.
        assertEquals(
            Reply.Error("ERR", "wrong number of arguments for 'mset' command"),
            error("mset", "a", "1", "b"),
        )
        assertEquals(
            Reply.Error("ERR", "wrong number of arguments for 'hset' command"),
            error("hset", "k", "f", "v", "g"),
        )
    }

    @Test
    fun `a number that is not one is refused where the command wanted a number`() {
        val notInteger = Reply.Error("ERR", "value is not an integer or out of range")
        assertEquals(notInteger, error("incrby", "k", "1.5"))
        assertEquals(notInteger, error("expire", "k", "soon"))
        assertEquals(notInteger, error("lrange", "k", "0", "end"))
        assertEquals(Reply.Error("ERR", "invalid cursor"), error("scan", "abc"))
        assertEquals(Reply.Error("ERR", "syntax error"), error("scan", "0", "COUNT", "0"))
        assertEquals(Reply.Error("ERR", "syntax error"), error("scan", "0", "NOSUCH", "x"))
    }

    /**
     * C8: an expiry no clock can hold is Redis's `invalid expire time` reply, under the
     * lower-case command name, and never an exception. Redis's own bound is the deadline as
     * epoch milliseconds in a signed 64-bit, which is also what the engine's WAL writes, so
     * `EXPIRE` a second past that is refused while `PEXPIREAT Long.MAX_VALUE` is exactly at it.
     */
    @Test
    fun `an unrepresentable expiry is Redis's error, not an exception`() {
        for (wire in listOf(
            listOf("expire", "k", "${Long.MAX_VALUE}"),
            listOf("expire", "k", "${Long.MIN_VALUE}"),
            listOf("pexpire", "k", "${Long.MAX_VALUE}"),
            listOf("expireat", "k", "99999999999999999"),
            listOf("setex", "k", "${Long.MAX_VALUE}", "v"),
        )) {
            assertEquals(invalidExpireTime(wire[0]), error(*wire.toTypedArray()), wire.joinToString(" "))
        }
        assertEquals(invalidExpireTime("set"), error("set", "k", "v", "EX", "${Long.MAX_VALUE}"))

        // The largest deadline epoch milliseconds can name is a deadline, not an error.
        assertTrue(parse("pexpireat", "k", "${Long.MAX_VALUE}") is Parsed.Ok)
    }

    /**
     * C8: Redis refuses a non-positive span on the `SET` family -- the value would be a TTL that
     * has already run out -- under the name the client actually typed.
     */
    @Test
    fun `a non-positive TTL on the SET family is Redis's error`() {
        for (argument in listOf("0", "-1", "${Long.MIN_VALUE}")) {
            assertEquals(invalidExpireTime("set"), error("set", "k", "v", "EX", argument), "EX $argument")
            assertEquals(invalidExpireTime("set"), error("set", "k", "v", "PX", argument), "PX $argument")
            assertEquals(invalidExpireTime("setex"), error("setex", "k", argument, "v"), "SETEX $argument")
        }
    }

    /**
     * Redis's `EXPIRE` family takes any value it can hold: a past deadline deletes the key, and
     * `expireGenericCommand` says so in as many words ("EXPIRE allows negative numbers"). Only
     * the `SET` family refuses a non-positive one, so zero and negative stay commands here.
     */
    @Test
    fun `the EXPIRE family accepts zero and negative, as Redis does`() {
        assertEquals(at(0), (command("expire", "k", "0") as Command.Expire).deadline)
        assertEquals(at(-1), (command("expire", "k", "-1") as Command.Expire).deadline)
        assertEquals(at(0), (command("pexpire", "k", "0") as Command.Expire).deadline)
        assertEquals(Instant.EPOCH, (command("expireat", "k", "0") as Command.Expire).deadline)
        assertEquals(Instant.EPOCH.minusSeconds(1), (command("expireat", "k", "-1") as Command.Expire).deadline)
    }

    /**
     * The property behind the two above: whatever number reaches an expiry-taking command, the
     * parser answers with a value. Nothing it does with a `Clock` may leave `CommandParser.parse`.
     */
    @Test
    fun `no expiry argument escapes the parser as an exception`() {
        val random = Random(20260906)
        val shapes = listOf(
            listOf("expire", "k", ARGUMENT),
            listOf("pexpire", "k", ARGUMENT),
            listOf("expireat", "k", ARGUMENT),
            listOf("pexpireat", "k", ARGUMENT),
            listOf("setex", "k", ARGUMENT, "v"),
            listOf("set", "k", "v", "EX", ARGUMENT),
            listOf("set", "k", "v", "PX", ARGUMENT),
        )
        repeat(2_000) {
            val argument = when (random.nextInt(4)) {
                0 -> random.nextLong().toString()
                1 -> random.nextLong(Long.MIN_VALUE / 2, 0).toString()
                2 -> listOf(Long.MAX_VALUE, Long.MIN_VALUE, 0L, -1L).random(random).toString()
                else -> random.nextLong(-1_000, 1_000).toString()
            }
            for (shape in shapes) {
                val words = shape.map { if (it === ARGUMENT) argument else it }.toTypedArray()
                assertDoesNotThrow(words.joinToString(" ")) { parse(*words) }
            }
        }
    }
}

/** The slot a random expiry argument goes in; matched by identity, so a literal `"k"` is never it. */
private val ARGUMENT = String(charArrayOf('%', 's'))

/** Redis's own wording, which the parser must answer byte for byte (C8). */
private fun invalidExpireTime(name: String) =
    Reply.Error("ERR", "invalid expire time in '${name.lowercase()}' command")
