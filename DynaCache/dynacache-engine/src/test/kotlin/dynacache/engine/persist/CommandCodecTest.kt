package dynacache.engine.persist

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.time.Duration
import java.time.Instant

/**
 * The engine's one encoding of a command as bytes, and the log's "what is logged is what changed"
 * rule that decides which command the WAL hands it.
 */
class CommandCodecTest {

    private val now: Instant = Instant.parse("2024-01-01T00:00:00Z")

    private fun key(name: String) = Key(name.toByteArray())
    private fun bytes(text: String) = text.toByteArray()

    /**
     * One command of every variant. The `when` in [assertCodec] is exhaustive over the hierarchy,
     * so a variant added without a codec case stops the build; this list is what it runs on.
     */
    private fun samples(): List<Command> = listOf(
        // Reads.
        Command.Get(key("k")),
        Command.Exists(key("k")),
        Command.Type(key("k")),
        Command.Ttl(key("k"), Command.Ttl.Precision.MILLIS),
        Command.StrLen(key("k")),
        Command.HGet(key("h"), bytes("f")),
        Command.HGetAll(key("h")),
        Command.HMGet(key("h"), listOf(bytes("f"), bytes("g"))),
        Command.HExists(key("h"), bytes("f")),
        Command.HKeys(key("h")),
        Command.HVals(key("h")),
        Command.HLen(key("h")),
        Command.HScan(key("h"), 7, bytes("f*"), 20),
        Command.LRange(key("l"), -3, 9),
        Command.LLen(key("l")),
        Command.LIndex(key("l"), -1),
        Command.ZScore(key("z"), bytes("m")),
        Command.ZCard(key("z")),
        // The two flags differ, so a codec that read them back the other way round is caught.
        Command.ZRange(key("z"), 0, -1, withScores = true, reverse = false),
        Command.ZRank(key("z"), bytes("m"), reverse = true),
        Command.ZRangeByScore(key("z"), bytes("(1"), bytes("+inf"), withScores = true, offset = 2, count = 5),
        Command.ZScan(key("z"), 3, null, 10),

        // Writes.
        Command.Set(key("k"), bytes("v"), Command.Set.Condition.XX, Duration.ofMillis(250)),
        Command.Del(key("k")),
        Command.Expire(key("k"), now.plusSeconds(30)),
        Command.Persist(key("k")),
        Command.IncrBy(key("n"), -4),
        Command.Append(key("k"), bytes("tail")),
        Command.HSet(key("h"), listOf(bytes("f") to bytes("v"))),
        Command.HMSet(key("h"), listOf(bytes("f") to bytes("v"))),
        Command.HDel(key("h"), listOf(bytes("f"))),
        Command.Push(key("l"), listOf(bytes("a"), bytes("b")), Command.End.HEAD),
        Command.Pop(key("l"), Command.End.TAIL),
        Command.LSet(key("l"), 2, bytes("v")),
        Command.LRem(key("l"), -1, bytes("v")),
        Command.ZAdd(key("z"), listOf(bytes("1") to bytes("m")), Command.Set.Condition.NX, changed = true),
        Command.ZRem(key("z"), listOf(bytes("m"))),
        Command.ZIncrBy(key("z"), bytes("1.5"), bytes("m")),
        Command.FlushDb,

        // Fanned.
        Command.MGet(listOf(key("a"), key("b"))),
        Command.MSet(listOf(key("a") to bytes("1"), key("b") to bytes("2"))),
        Command.DelKeys(listOf(key("a"), key("b"))),
        Command.ExistsKeys(listOf(key("a"), key("b"))),

        // Neither keyed nor fanned: never crosses, never logged.
        Command.Ping,
        Command.CommandTable,
        Command.Scan(0, null, 10),
        Command.Info,
        Command.DbSize,
        Command.Keys(bytes("*")),
        Command.RandomKey,
        Command.Cp.LongIncr(key("cp:counter:c")),
    )

    @Test
    fun command_codec_round_trips_every_keyed_variant() {
        samples().forEach(::assertCodec)
    }

    /**
     * Every variant's two answers: does it cross the wire, and does the log hold it? The `when` is
     * exhaustive over [Command], so a variant added without a codec case stops the build here.
     */
    private fun assertCodec(command: Command) = when (command) {
        // A read crosses, and changes nothing.
        is Command.Get, is Command.Exists, is Command.Type, is Command.Ttl, is Command.StrLen,
        is Command.HGet, is Command.HGetAll, is Command.HMGet, is Command.HExists, is Command.HKeys,
        is Command.HVals, is Command.HLen, is Command.HScan, is Command.LRange, is Command.LLen,
        is Command.LIndex, is Command.ZScore, is Command.ZCard, is Command.ZRange, is Command.ZRank,
        is Command.ZRangeByScore, is Command.ZScan,
        // A fanned command crosses whole; the log holds the single-key parts it splits into.
        is Command.MGet, is Command.MSet, is Command.DelKeys, is Command.ExistsKeys,
        -> {
            assertRoundTrips(command)
            assertNull(whatChanged(command, Reply.Simple("OK")), "$command changed nothing")
        }

        // A write crosses, and the log holds what it changed.
        Command.FlushDb, is Command.Set, is Command.Del, is Command.Expire, is Command.Persist,
        is Command.IncrBy, is Command.Append, is Command.HSet, is Command.HMSet, is Command.HDel,
        is Command.Push, is Command.Pop, is Command.LSet, is Command.LRem, is Command.ZAdd,
        is Command.ZRem, is Command.ZIncrBy,
        -> {
            assertRoundTrips(command)
            assertRoundTrips(whatChanged(command, Reply.Simple("OK")) ?: fail(command))
        }

        // Nothing else crosses: the router runs it on this node, and a CP command is CpWire's.
        is Command.Cp, is Command.EveryPartition, Command.Ping, Command.CommandTable, is Command.Scan,
        -> {
            assertThrows(IllegalArgumentException::class.java) { CommandCodec.encode(command) }
            assertNull(whatChanged(command, Reply.Simple("OK")), "$command changed nothing")
        }
    }

    private fun fail(command: Command): Command = throw AssertionError("$command should have been logged")

    /** Byte-exact: what the bytes decode to encodes back to the same bytes, under the same op. */
    private fun assertRoundTrips(command: Command) {
        val (op, body) = CommandCodec.encode(command)
        val back = CommandCodec.decode(op, body).single()
        assertEquals(command::class, back::class, "$command decoded as another variant")
        val (againOp, againBody) = CommandCodec.encode(back)
        assertEquals(op, againOp, "$command changed op code")
        assertArrayEquals(body, againBody, "$command did not round-trip byte-exact")
    }

    @Test
    fun codec_round_trips_conditions_and_both_ttl_forms() {
        // The wire's form: the condition and the duration the client sent, undecided.
        val asked = Command.Set(key("k"), bytes("v"), Command.Set.Condition.NX, Duration.ofSeconds(10))
        val (op, body) = CommandCodec.encode(asked)
        val back = CommandCodec.decode(op, body).single() as Command.Set
        assertEquals(Command.Set.Condition.NX, back.condition)
        assertEquals(Duration.ofSeconds(10), back.ttl)
        assertArrayEquals(bytes("v"), back.value)

        // The log's form: the instant the engine settled on, so a late replay lands the same deadline.
        val (logOp, logBody) = CommandCodec.encode(Command.Set(key("k"), bytes("v"), null, Duration.ofSeconds(90)), now)
        val redo = CommandCodec.decode(logOp, logBody)
        assertEquals(2, redo.size, "a SET with a deadline redoes as a SET and then an EXPIRE")
        assertNull((redo[0] as Command.Set).ttl)
        assertEquals(now.plusSeconds(90), (redo[1] as Command.Expire).deadline)

        // An EXPIRE's instant is the same instant however it is spelled.
        val expire = Command.Expire(key("k"), now.plusSeconds(5))
        val (expireOp, expireBody) = CommandCodec.encode(expire)
        assertEquals(now.plusSeconds(5), (CommandCodec.decode(expireOp, expireBody).single() as Command.Expire).deadline)
    }

    @Test
    fun codec_reads_the_entries_written_before_the_reads_were_added() {
        // A SET and a ZADD as an older log holds them: no condition tail on the one, no CH byte on
        // the other. What that log meant is what they decode to now.
        val set = payload {
            writeInt(1); write(bytes("k"))
            writeInt(1); write(bytes("v"))
            writeLong(now.toEpochMilli())
        }
        val redo = CommandCodec.decode(1, set)
        assertNull((redo[0] as Command.Set).condition)
        assertEquals(now, (redo[1] as Command.Expire).deadline)

        val zadd = payload {
            writeInt(1); write(bytes("z"))
            writeInt(1); writeInt(1); write(bytes("1")); writeInt(1); write(bytes("m"))
            writeByte(1)
        }
        val decoded = CommandCodec.decode(13, zadd).single() as Command.ZAdd
        assertEquals(Command.Set.Condition.NX, decoded.condition)
        assertEquals(false, decoded.changed, "CH was never logged, so an older entry never carried it")
    }

    private fun payload(fill: DataOutputStream.() -> Unit): ByteArray =
        ByteArrayOutputStream().also { DataOutputStream(it).use(fill) }.toByteArray()

    @Test
    fun what_changed_logs_nothing_for_an_error_or_a_refused_write() {
        val set = Command.Set(key("k"), bytes("v"), Command.Set.Condition.NX)
        assertNull(whatChanged(set, Reply.Bulk(null)), "a refused conditional SET changed nothing")
        assertNull(whatChanged(set, Reply.Error("WRONGTYPE", "no")), "an error changed nothing")
        assertNull(whatChanged(Command.Pop(key("l"), Command.End.HEAD), Reply.Bulk(null)), "an empty POP changed nothing")
    }

    @Test
    fun what_changed_decides_the_condition_of_a_set_it_took() {
        val taken = whatChanged(Command.Set(key("k"), bytes("v"), Command.Set.Condition.NX, Duration.ofSeconds(5)), Reply.Simple("OK"))
        val decided = taken as Command.Set
        assertNull(decided.condition, "the coordinator decided NX; a replay applies the plain write")
        assertEquals(Duration.ofSeconds(5), decided.ttl, "the TTL is still the engine's to turn into a deadline")
    }

    @Test
    fun what_changed_keeps_the_condition_of_a_conditional_zadd() {
        // T48: a `:0` is a refusal and a moved score alike, so the condition is replayed, not read
        // off the reply. `CH` is not logged; it changes only the reply.
        val zadd = Command.ZAdd(key("z"), listOf(bytes("1") to bytes("m")), Command.Set.Condition.XX, changed = true)
        val logged = whatChanged(zadd, Reply.Integer(0)) as Command.ZAdd
        assertEquals(Command.Set.Condition.XX, logged.condition)
        assertEquals(false, logged.changed)
    }

    @Test
    fun what_changed_logs_an_hmset_as_the_hset_it_is() {
        val logged = whatChanged(Command.HMSet(key("h"), listOf(bytes("f") to bytes("v"))), Reply.Simple("OK"))
        assertEquals(Command.HSet::class, logged!!::class, "HMSET is HSET's write, answering +OK")
        assertEquals(CommandCodec.encode(Command.HSet(key("h"), listOf(bytes("f") to bytes("v")))).first, CommandCodec.encode(logged).first)
    }
}
