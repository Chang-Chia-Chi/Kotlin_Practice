package dynacache.server

import dynacache.engine.Command
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
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
     * Sorted Set arrives with T07 and its rows (`ZADD`, `ZREM`, `ZRANGE`, `ZREVRANGE`,
     * `ZRANGEBYSCORE`, `ZRANK`, `ZREVRANK`, `ZSCORE`, `ZCARD`, `ZINCRBY`, `ZSCAN`) belong here.
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
            row("PSETEX k 30 v", Command.Set::class),
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
            row("PERSIST k", Command.Persist::class),
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

        // EXAT and PXAT name the deadline instead of the span; the clock turns it back into one.
        val exat = command("set", "k", "v", "EXAT", "${1_000_000 + 60}") as Command.Set
        assertEquals(Duration.ofSeconds(60), exat.ttl)
        val pxat = command("set", "k", "v", "PXAT", "${1_000_000_000L + 250}") as Command.Set
        assertEquals(Duration.ofMillis(250), pxat.ttl)

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
    fun `SETNX SETEX and PSETEX are spellings of SET`() {
        val setnx = command("setnx", "k", "v") as Command.Set
        assertEquals(Command.Set.Condition.NX, setnx.condition)
        assertEquals(Duration.ofSeconds(30), (command("setex", "k", "30", "v") as Command.Set).ttl)
        assertEquals(Duration.ofMillis(30), (command("psetex", "k", "30", "v") as Command.Set).ttl)
        assertEquals("v", (command("setex", "k", "30", "v") as Command.Set).value.toString(Charsets.ISO_8859_1))
    }

    @Test
    fun `an unknown command names itself and the arguments it came with`() {
        assertEquals(
            Reply.Error("ERR", "unknown command 'foo', with args beginning with: 'a', 'b', "),
            error("foo", "a", "b"),
        )
        assertEquals(
            Reply.Error("ERR", "unknown command 'zadd', with args beginning with: 'k', '1', 'm', "),
            error("ZADD", "k", "1", "m"),
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
}
