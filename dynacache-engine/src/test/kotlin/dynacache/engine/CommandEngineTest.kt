package dynacache.engine

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Collections
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class CommandEngineTest {

    /** Time moves only when a test says so; the partition thread reads it, hence volatile. */
    private class MutableClock(@Volatile var now: Instant) : Clock() {
        override fun instant(): Instant = now
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
    }

    private val clock = MutableClock(Instant.parse("2026-09-06T00:00:00Z"))
    private val engine = ApEngine(partitionCount = 4, clock = clock, random = Random(20260906))

    @AfterEach
    fun close() = engine.close()

    private fun run(command: Command): Reply = engine.submit(command).get()

    private fun info(): String = (run(Command.Info) as Reply.Bulk).bytes!!.toString(Charsets.ISO_8859_1)

    /** The keys `KEYS pattern` answers with, as text; a set, since the order is unspecified. */
    private fun keys(pattern: String): Set<String> {
        val items = (run(Command.Keys(pattern.toByteArray())) as Reply.Array).items
        val names = items.map { (it as Reply.Bulk).bytes!!.toString(Charsets.ISO_8859_1) }
        assertEquals(names.size, names.toSet().size, "KEYS reports each key once")
        return names.toSet()
    }

    private fun bulks(vararg values: String): Reply =
        Reply.Array(values.map { Reply.Bulk(it.toByteArray()) })

    private fun push(key: Key, end: Command.End, vararg values: String): Command =
        Command.Push(key, values.map { it.toByteArray() }, end)

    /** A key the engine puts on a different partition than [key]. */
    private fun otherPartitionThan(key: Key, of: ApEngine = engine): Key =
        (0..99).map { Key("z$it") }.first { of.partitionOf(it) != of.partitionOf(key) }

    @Test
    fun string_set_get_roundtrip() {
        assertEquals(Reply.Simple("OK"), run(Command.Set(Key("k"), "v".toByteArray())))
        assertEquals(Reply.Bulk("v".toByteArray()), run(Command.Get(Key("k"))))
    }

    @Test
    fun string_set_nx_rejects_existing() {
        run(Command.Set(Key("k"), "old".toByteArray()))
        assertEquals(Reply.Bulk(null), run(Command.Set(Key("k"), "new".toByteArray(), Command.Set.Condition.NX)))
        assertEquals(Reply.Bulk("old".toByteArray()), run(Command.Get(Key("k"))))
    }

    @Test
    fun string_set_xx_rejects_missing() {
        assertEquals(Reply.Bulk(null), run(Command.Set(Key("k"), "v".toByteArray(), Command.Set.Condition.XX)))
        assertEquals(Reply.Bulk(null), run(Command.Get(Key("k"))))
    }

    @Test
    fun string_set_ex_expires() {
        run(Command.Set(Key("k"), "v".toByteArray(), ttl = Duration.ofSeconds(1)))
        clock.now += Duration.ofMillis(999)
        tick()
        assertEquals(Reply.Bulk("v".toByteArray()), run(Command.Get(Key("k"))))
        clock.now += Duration.ofMillis(2)
        tick()
        assertEquals(Reply.Bulk(null), run(Command.Get(Key("k"))))
    }

    @Test
    fun string_incr_atomic() {
        run(Command.Set(Key("n"), "10".toByteArray()))
        assertEquals(Reply.Integer(11), run(Command.IncrBy(Key("n"), 1)))
        assertEquals(Reply.Bulk("11".toByteArray()), run(Command.Get(Key("n"))), "the stored value is the new number")
        assertEquals(Reply.Integer(1), run(Command.IncrBy(Key("fresh"), 1)), "a missing key counts as 0")
        assertEquals(Reply.Integer(-1), run(Command.IncrBy(Key("down"), -1)))
        run(Command.Set(Key("s"), "abc".toByteArray()))
        assertEquals(NOT_AN_INTEGER, run(Command.IncrBy(Key("s"), 1)))
        assertEquals(Reply.Bulk("abc".toByteArray()), run(Command.Get(Key("s"))), "the rejected INCR left the value alone")
    }

    @Test
    fun `APPEND extends the value and replies with the new length, STRLEN measures it`() {
        assertEquals(Reply.Integer(0), run(Command.StrLen(Key("k"))), "a missing key is empty")
        assertEquals(Reply.Integer(2), run(Command.Append(Key("k"), "he".toByteArray())))
        assertEquals(Reply.Integer(5), run(Command.Append(Key("k"), "llo".toByteArray())))
        assertEquals(Reply.Bulk("hello".toByteArray()), run(Command.Get(Key("k"))))
        assertEquals(Reply.Integer(5), run(Command.StrLen(Key("k"))))
    }

    @Test
    fun mget_spans_partitions() {
        val here = Key("a")
        val elsewhere = otherPartitionThan(here)
        assertNotEquals(engine.partitionOf(here), engine.partitionOf(elsewhere))
        run(Command.Set(here, "va".toByteArray()))
        run(Command.Set(elsewhere, "vb".toByteArray()))
        assertEquals(
            Reply.Array(listOf(Reply.Bulk("va".toByteArray()), Reply.Bulk(null), Reply.Bulk("vb".toByteArray()))),
            run(Command.MGet(listOf(here, Key("missing"), elsewhere))),
            "one array in argument order, nil for the missing key",
        )
    }

    @Test
    fun `MSET writes every key and DEL and EXISTS count across partitions`() {
        val here = Key("a")
        val elsewhere = otherPartitionThan(here)
        assertEquals(Reply.Simple("OK"), run(Command.MSet(listOf(here to "va".toByteArray(), elsewhere to "vb".toByteArray()))))
        assertEquals(Reply.Bulk("va".toByteArray()), run(Command.Get(here)))
        assertEquals(Reply.Bulk("vb".toByteArray()), run(Command.Get(elsewhere)))
        assertEquals(Reply.Integer(2), run(Command.ExistsKeys(listOf(here, elsewhere, Key("missing")))))
        assertEquals(Reply.Integer(2), run(Command.DelKeys(listOf(here, elsewhere, Key("missing")))))
        assertEquals(Reply.Integer(0), run(Command.ExistsKeys(listOf(here, elsewhere))))
    }

    /** Parks the first command that runs on one named partition thread, until [release]. */
    private class ParkingClock : Clock() {
        val entered = Semaphore(0)
        private val gate = CountDownLatch(1)

        @Volatile
        private var parked: String? = null

        fun park(threadName: String) {
            parked = threadName
        }

        fun release() = gate.countDown()

        override fun instant(): Instant {
            if (Thread.currentThread().name == parked) {
                parked = null
                entered.release()
                check(gate.await(5, TimeUnit.SECONDS)) { "never released" }
            }
            return Instant.EPOCH
        }

        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
    }

    /**
     * Pins ADR 0002. The gate parks the MGET's first part, and the fan-out has not reached the
     * other partition yet, so a whole MSET lands there in the gap. The reply then mixes the
     * value of one key from before that write with the other from after it: an outcome neither
     * an atomic MGET nor an atomic MSET could produce.
     */
    @Test
    fun mget_across_partitions_is_not_atomic() {
        val gate = ParkingClock()
        val engine = ApEngine(partitionCount = 2, clock = gate)
        try {
            val x = Key("x")
            val y = otherPartitionThan(x, engine)
            engine.submit(Command.MSet(listOf(x to "old".toByteArray(), y to "old".toByteArray()))).get()

            gate.park("partition-${engine.partitionOf(x).index}")
            val read = engine.submit(Command.MGet(listOf(x, y)))
            assertTrue(gate.entered.tryAcquire(5, TimeUnit.SECONDS), "the MGET parked on x's partition")

            // y first, so this write reaches y's partition while the MGET is still parked on x's.
            val write = engine.submit(Command.MSet(listOf(y to "new".toByteArray(), x to "new".toByteArray())))
            assertEquals(
                Reply.Bulk("new".toByteArray()),
                engine.submit(Command.Get(y)).get(5, TimeUnit.SECONDS),
                "the write reached y's partition",
            )
            assertFalse(read.isDone, "the MGET has not reached y's partition yet")

            gate.release()
            assertEquals(
                Reply.Array(listOf(Reply.Bulk("old".toByteArray()), Reply.Bulk("new".toByteArray()))),
                read.get(5, TimeUnit.SECONDS),
                "x from before the write, y from after it",
            )
            write.get(5, TimeUnit.SECONDS)
        } finally {
            engine.close()
        }
    }

    @Test
    fun hash_field_independence() {
        val h = Key("h")
        assertEquals(
            Reply.Integer(2),
            run(Command.HSet(h, listOf("a".toByteArray() to "1".toByteArray(), "b".toByteArray() to "2".toByteArray()))),
            "both fields are new",
        )
        assertEquals(
            Reply.Integer(0),
            run(Command.HSet(h, listOf("a".toByteArray() to "9".toByteArray()))),
            "an overwrite adds no new field",
        )
        assertEquals(Reply.Bulk("9".toByteArray()), run(Command.HGet(h, "a".toByteArray())))
        assertEquals(Reply.Bulk("2".toByteArray()), run(Command.HGet(h, "b".toByteArray())), "the other field is untouched")
        assertEquals(Reply.Bulk(null), run(Command.HGet(h, "c".toByteArray())), "a missing field is nil")
        assertEquals(Reply.Bulk(null), run(Command.HGet(Key("none"), "a".toByteArray())), "a missing key is nil")
    }

    @Test
    fun hash_getall_complete() {
        val h = Key("h")
        assertEquals(Reply.Array(emptyList()), run(Command.HGetAll(h)), "a missing hash is an empty array")
        run(Command.HSet(h, listOf("a".toByteArray() to "1".toByteArray(), "b".toByteArray() to "2".toByteArray())))
        assertEquals(
            Reply.Array(
                listOf(
                    Reply.Bulk("a".toByteArray()), Reply.Bulk("1".toByteArray()),
                    Reply.Bulk("b".toByteArray()), Reply.Bulk("2".toByteArray()),
                ),
            ),
            run(Command.HGetAll(h)),
            "every field and its value, flat and in one array",
        )
    }

    @Test
    fun `HDEL removes fields and the key goes with its last field`() {
        val h = Key("h")
        run(Command.HSet(h, listOf("a".toByteArray() to "1".toByteArray(), "b".toByteArray() to "2".toByteArray())))
        assertEquals(
            Reply.Integer(1),
            run(Command.HDel(h, listOf("a".toByteArray(), "gone".toByteArray()))),
            "only the field that was there counts",
        )
        assertEquals(Reply.Simple("hash"), run(Command.Type(h)), "the hash is still here with one field left")
        assertEquals(Reply.Integer(1), run(Command.HDel(h, listOf("b".toByteArray()))))
        assertEquals(Reply.Simple("none"), run(Command.Type(h)), "the empty hash is gone, as in Redis")
        assertEquals(Reply.Integer(0), run(Command.HDel(h, listOf("a".toByteArray()))), "a missing key deletes nothing")
    }

    @Test
    fun `HMSET, HMGET, HEXISTS, HKEYS, HVALS and HLEN reply in Redis shapes`() {
        val h = Key("h")
        val a = "a".toByteArray()
        val b = "b".toByteArray()
        val gone = "gone".toByteArray()
        assertEquals(Reply.Integer(0), run(Command.HLen(h)), "a missing key has no fields")
        assertEquals(Reply.Array(listOf(Reply.Bulk(null))), run(Command.HMGet(h, listOf(a))), "one nil per field asked for")
        assertEquals(
            Reply.Simple("OK"),
            run(Command.HMSet(h, listOf(a to "1".toByteArray(), b to "2".toByteArray()))),
        )
        assertEquals(Reply.Integer(2), run(Command.HLen(h)))
        assertEquals(Reply.Integer(1), run(Command.HExists(h, a)))
        assertEquals(Reply.Integer(0), run(Command.HExists(h, gone)))
        assertEquals(
            Reply.Array(listOf(Reply.Bulk("1".toByteArray()), Reply.Bulk(null), Reply.Bulk("2".toByteArray()))),
            run(Command.HMGet(h, listOf(a, gone, b))),
            "in the order the fields were asked for",
        )
        assertEquals(Reply.Array(listOf(Reply.Bulk(a), Reply.Bulk(b))), run(Command.HKeys(h)))
        assertEquals(
            Reply.Array(listOf(Reply.Bulk("1".toByteArray()), Reply.Bulk("2".toByteArray()))),
            run(Command.HVals(h)),
        )
    }

    /** The C13 mechanism; T04's `C13_wrongtype_leaves_value_intact` names it for List. */
    @Test
    fun `a command meant for another kind is refused without touching the key`() {
        val s = Key("s")
        val h = Key("h")
        run(Command.Set(s, "v".toByteArray()))
        assertEquals(WRONG_TYPE, run(Command.HGet(s, "f".toByteArray())))
        assertEquals(WRONG_TYPE, run(Command.HSet(s, listOf("f".toByteArray() to "1".toByteArray()))))
        assertEquals(Reply.Bulk("v".toByteArray()), run(Command.Get(s)), "the String is intact")

        run(Command.HSet(h, listOf("f".toByteArray() to "1".toByteArray())))
        assertEquals(WRONG_TYPE, run(Command.Get(h)))
        assertEquals(WRONG_TYPE, run(Command.IncrBy(h, 1)))
        assertEquals(WRONG_TYPE, run(Command.Append(h, "x".toByteArray())))
        assertEquals(WRONG_TYPE, run(Command.StrLen(h)))
        assertEquals(Reply.Simple("hash"), run(Command.Type(h)), "TYPE answers for any kind")
        assertEquals(Reply.Integer(1), run(Command.Exists(h)), "EXISTS answers for any kind")
        assertEquals(Reply.Bulk("1".toByteArray()), run(Command.HGet(h, "f".toByteArray())), "the Hash is intact")
        assertEquals(
            Reply.Array(listOf(Reply.Bulk(null))),
            run(Command.MGet(listOf(h))),
            "MGET answers nil for a key that is not a String, as Redis does, rather than an error",
        )
        assertEquals(Reply.Simple("OK"), run(Command.Set(h, "v".toByteArray())), "SET replaces a key of any kind")
    }

    @Test
    fun keys_with_same_hash_tag_share_a_partition() {
        assertEquals(engine.partitionOf(Key("{user1}.a")), engine.partitionOf(Key("{user1}.b")))
        assertEquals(engine.partitionOf(Key("{user1}")), engine.partitionOf(Key("x{user1}y")))
    }

    @Test
    fun `DEL replies 1 for a deleted key and 0 for a missing one`() {
        run(Command.Set(Key("k"), "v".toByteArray()))
        assertEquals(Reply.Integer(1), run(Command.Del(Key("k"))))
        assertEquals(Reply.Integer(0), run(Command.Del(Key("k"))))
        assertEquals(Reply.Bulk(null), run(Command.Get(Key("k"))))
    }

    @Test
    fun `EXISTS replies 1 for a live key and 0 for a missing or expired one`() {
        assertEquals(Reply.Integer(0), run(Command.Exists(Key("k"))))
        run(Command.Set(Key("k"), "v".toByteArray(), ttl = Duration.ofMillis(5)))
        assertEquals(Reply.Integer(1), run(Command.Exists(Key("k"))))
        clock.now += Duration.ofMillis(6)
        assertEquals(Reply.Integer(0), run(Command.Exists(Key("k"))))
    }

    @Test
    fun `TYPE replies string for a String key and none for a missing one`() {
        run(Command.Set(Key("k"), "v".toByteArray()))
        assertEquals(Reply.Simple("string"), run(Command.Type(Key("k"))))
        assertEquals(Reply.Simple("none"), run(Command.Type(Key("missing"))))
    }

    @Test
    fun list_push_pop_order() {
        assertEquals(Reply.Integer(3), run(push(Key("l"), Command.End.HEAD, "a", "b", "c")))
        assertEquals(Reply.Bulk("a".toByteArray()), run(Command.Pop(Key("l"), Command.End.TAIL)), "LPUSH a b c leaves a at the tail")
        assertEquals(Reply.Bulk("c".toByteArray()), run(Command.Pop(Key("l"), Command.End.HEAD)), "and c at the head")
        assertEquals(Reply.Simple("list"), run(Command.Type(Key("l"))))
        assertEquals(Reply.Bulk("b".toByteArray()), run(Command.Pop(Key("l"), Command.End.HEAD)))
        assertEquals(Reply.Bulk(null), run(Command.Pop(Key("l"), Command.End.HEAD)), "an empty list is a missing key")
        assertEquals(Reply.Simple("none"), run(Command.Type(Key("l"))), "the last pop took the key with it")
    }

    @Test
    fun list_lrange_bounds() {
        run(push(Key("l"), Command.End.TAIL, "a", "b", "c"))
        assertEquals(bulks("a", "b", "c"), run(Command.LRange(Key("l"), 0, -1)))
        assertEquals(bulks("a", "b", "c"), run(Command.LRange(Key("l"), -100, 100)), "both ends clamp, neither errors")
        assertEquals(bulks("b", "c"), run(Command.LRange(Key("l"), 1, 5)))
        assertEquals(bulks("c"), run(Command.LRange(Key("l"), -1, -1)), "negative indices count from the tail")
        assertEquals(EMPTY_ARRAY, run(Command.LRange(Key("l"), 2, 1)), "start past stop is empty")
        assertEquals(EMPTY_ARRAY, run(Command.LRange(Key("l"), 5, 9)), "start past the end is empty")
        assertEquals(EMPTY_ARRAY, run(Command.LRange(Key("missing"), 0, -1)), "a missing key is an empty list")
    }

    @Test
    fun wrongtype_rejected() {
        run(Command.Set(Key("s"), "v".toByteArray()))
        assertEquals(WRONG_TYPE, run(push(Key("s"), Command.End.HEAD, "x")))
        assertEquals(Reply.Simple("string"), run(Command.Type(Key("s"))), "the key is still a String")
        run(push(Key("l"), Command.End.HEAD, "a"))
        assertEquals(WRONG_TYPE, run(Command.Get(Key("l"))), "and the refusal runs both ways")
        assertEquals(WRONG_TYPE, run(Command.HGet(Key("l"), "f".toByteArray())))
        assertEquals(WRONG_TYPE, run(Command.LLen(Key("s"))))
        assertEquals(Reply.Integer(1), run(Command.Exists(Key("l"))), "EXISTS, TYPE and DEL work on any kind")
        assertEquals(Reply.Integer(1), run(Command.Del(Key("l"))))
    }

    @Test
    fun C13_wrongtype_leaves_value_intact() {
        run(Command.Set(Key("s"), "original".toByteArray()))
        assertEquals(WRONG_TYPE, run(push(Key("s"), Command.End.HEAD, "a", "b")))
        assertEquals(Reply.Bulk("original".toByteArray()), run(Command.Get(Key("s"))))
        assertEquals(WRONG_TYPE, run(Command.Pop(Key("s"), Command.End.TAIL)))
        assertEquals(WRONG_TYPE, run(Command.LSet(Key("s"), 0, "b".toByteArray())))
        assertEquals(WRONG_TYPE, run(Command.LRem(Key("s"), 0, "original".toByteArray())))
        assertEquals(WRONG_TYPE, run(Command.LRange(Key("s"), 0, -1)))
        assertEquals(WRONG_TYPE, run(Command.LIndex(Key("s"), 0)))
        assertEquals(Reply.Bulk("original".toByteArray()), run(Command.Get(Key("s"))), "no List branch ever reached the entry")
        assertEquals(Reply.Simple("string"), run(Command.Type(Key("s"))))
    }

    @Test
    fun `LLEN and LINDEX read the list without changing it`() {
        assertEquals(Reply.Integer(0), run(Command.LLen(Key("missing"))))
        assertEquals(Reply.Bulk(null), run(Command.LIndex(Key("missing"), 0)))
        run(push(Key("l"), Command.End.TAIL, "a", "b", "c"))
        assertEquals(Reply.Integer(3), run(Command.LLen(Key("l"))))
        assertEquals(Reply.Bulk("a".toByteArray()), run(Command.LIndex(Key("l"), 0)))
        assertEquals(Reply.Bulk("c".toByteArray()), run(Command.LIndex(Key("l"), -1)), "negative counts from the tail")
        assertEquals(Reply.Bulk(null), run(Command.LIndex(Key("l"), 3)), "past the end is nil, not an error")
        assertEquals(Reply.Bulk(null), run(Command.LIndex(Key("l"), -4)))
        assertEquals(Reply.Integer(3), run(Command.LLen(Key("l"))), "reads leave the list alone")
    }

    @Test
    fun `LSET replaces an element and errors outside the list`() {
        assertEquals(NO_SUCH_KEY, run(Command.LSet(Key("missing"), 0, "x".toByteArray())))
        run(push(Key("l"), Command.End.TAIL, "a", "b", "c"))
        assertEquals(Reply.Simple("OK"), run(Command.LSet(Key("l"), 1, "B".toByteArray())))
        assertEquals(Reply.Simple("OK"), run(Command.LSet(Key("l"), -1, "C".toByteArray())))
        assertEquals(bulks("a", "B", "C"), run(Command.LRange(Key("l"), 0, -1)))
        assertEquals(INDEX_OUT_OF_RANGE, run(Command.LSet(Key("l"), 3, "x".toByteArray())))
        assertEquals(INDEX_OUT_OF_RANGE, run(Command.LSet(Key("l"), -4, "x".toByteArray())))
        assertEquals(bulks("a", "B", "C"), run(Command.LRange(Key("l"), 0, -1)), "a rejected LSET changed nothing")
    }

    @Test
    fun lrem_count_semantics() {
        fun seed() {
            run(Command.Del(Key("l")))
            run(push(Key("l"), Command.End.TAIL, "a", "x", "b", "x", "c", "x"))
        }
        seed()
        assertEquals(Reply.Integer(2), run(Command.LRem(Key("l"), 2, "x".toByteArray())), "a positive count works from the head")
        assertEquals(bulks("a", "b", "c", "x"), run(Command.LRange(Key("l"), 0, -1)))
        seed()
        assertEquals(Reply.Integer(2), run(Command.LRem(Key("l"), -2, "x".toByteArray())), "a negative count works from the tail")
        assertEquals(bulks("a", "x", "b", "c"), run(Command.LRange(Key("l"), 0, -1)))
        seed()
        assertEquals(Reply.Integer(3), run(Command.LRem(Key("l"), 0, "x".toByteArray())), "zero removes every match")
        assertEquals(bulks("a", "b", "c"), run(Command.LRange(Key("l"), 0, -1)))
        seed()
        assertEquals(Reply.Integer(3), run(Command.LRem(Key("l"), 9, "x".toByteArray())), "a count past the matches removes them all")
        assertEquals(Reply.Integer(0), run(Command.LRem(Key("l"), 0, "gone".toByteArray())), "a value that is not there goes uncounted")
        assertEquals(Reply.Integer(0), run(Command.LRem(Key("missing"), 0, "x".toByteArray())))
        run(Command.Del(Key("l")))
        run(push(Key("l"), Command.End.TAIL, "x", "x"))
        assertEquals(Reply.Integer(2), run(Command.LRem(Key("l"), 0, "x".toByteArray())))
        assertEquals(Reply.Simple("none"), run(Command.Type(Key("l"))), "an emptied list takes its key with it")
    }

    @Test
    fun dbsize_and_flushdb_span_partitions() {
        val here = Key("k")
        val elsewhere = otherPartitionThan(here)
        assertNotEquals(engine.partitionOf(here), engine.partitionOf(elsewhere), "the two keys are on different partitions")
        assertEquals(Reply.Integer(0), run(Command.DbSize))
        run(Command.Set(here, "1".toByteArray()))
        run(Command.Set(elsewhere, "2".toByteArray()))
        assertEquals(Reply.Integer(2), run(Command.DbSize), "DBSIZE counts every partition")
        run(Command.Set(here, "1".toByteArray(), ttl = Duration.ofMillis(5)))
        clock.now += Duration.ofMillis(6)
        assertEquals(Reply.Integer(1), run(Command.DbSize), "an expired key is not counted")
        assertEquals(Reply.Simple("OK"), run(Command.FlushDb))
        assertEquals(Reply.Integer(0), run(Command.DbSize), "FLUSHDB emptied every partition")
        assertEquals(Reply.Bulk(null), run(Command.Get(elsewhere)))
    }

    @Test
    fun keys_glob_patterns() {
        val seeded = listOf("user:1", "user:2", "user:10", "admin", "a", "b", "c[x]")
        seeded.forEach { run(Command.Set(Key(it), "v".toByteArray())) }
        assertTrue(seeded.map(::Key).map(engine::partitionOf).toSet().size > 1, "the keys span partitions")
        assertEquals(seeded.toSet(), keys("*"))
        assertEquals(setOf("user:1", "user:2"), keys("user:?"), "? is exactly one byte")
        assertEquals(setOf("user:1", "user:2", "user:10"), keys("user:*"))
        assertEquals(setOf("a", "b"), keys("[ab]"))
        assertEquals(setOf("admin", "a", "b", "c[x]"), keys("[a-c]*"), "a range inside a class")
        assertEquals(setOf("a", "b"), keys("[^c]"), "^ negates the class")
        assertEquals(setOf("c[x]"), keys("c\\[x]"), "a backslash escapes the class")
        assertEquals(emptySet<String>(), keys("nothing*"))
        run(Command.Set(Key("fleeting"), "v".toByteArray(), ttl = Duration.ofMillis(5)))
        clock.now += Duration.ofMillis(6)
        assertEquals(seeded.toSet(), keys("*"), "an expired key is not in KEYS")
    }

    @Test
    fun randomkey_nil_when_empty() {
        assertEquals(Reply.Bulk(null), run(Command.RandomKey), "an empty keyspace has no random key")
        run(Command.Set(Key("only"), "v".toByteArray()))
        assertEquals(Reply.Bulk("only".toByteArray()), run(Command.RandomKey), "one key is the only answer")
        run(Command.Del(Key("only")))
        assertEquals(Reply.Bulk(null), run(Command.RandomKey), "and nil again once the last key goes")
    }

    @Test
    fun `RANDOMKEY draws from every partition and never from an expired key`() {
        val seeded = (0..19).map { Key("k$it") }
        assertTrue(seeded.map(engine::partitionOf).toSet().size > 1, "the keys span partitions")
        seeded.forEach { run(Command.Set(it, "v".toByteArray())) }
        run(Command.Set(Key("fleeting"), "v".toByteArray(), ttl = Duration.ofMillis(5)))
        clock.now += Duration.ofMillis(6)
        val drawn = (1..200).map { (run(Command.RandomKey) as Reply.Bulk).bytes!!.toString(Charsets.ISO_8859_1) }.toSet()
        assertTrue(seeded.map(Key::toString).containsAll(drawn), "every draw is a live key, never the expired one")
        assertTrue(drawn.map { engine.partitionOf(Key(it)) }.toSet().size > 1, "the draws come from more than one partition")
    }

    @Test
    fun `COMMAND and INFO answer in Redis shapes`() {
        assertEquals(EMPTY_ARRAY, run(Command.CommandTable), "COMMAND is minimal: an empty table")
        assertTrue(info().contains("dynacache_version:"), info())
        assertTrue(info().contains("db0:keys=0\r\n"), info())
        run(Command.Set(Key("a"), "v".toByteArray()))
        run(Command.Set(otherPartitionThan(Key("a")), "v".toByteArray()))
        assertTrue(info().contains("db0:keys=2\r\n"), "INFO counts every partition: " + info())
    }

    private fun ttl(key: Key, precision: Command.Ttl.Precision = Command.Ttl.Precision.SECONDS): Long =
        (run(Command.Ttl(key, precision)) as Reply.Integer).value

    @Test
    fun ttl_reports_remaining_and_minus_values() {
        assertEquals(-2L, ttl(Key("k")), "Redis answers -2 for a key that is not there")
        run(Command.Set(Key("k"), "v".toByteArray()))
        assertEquals(-1L, ttl(Key("k")), "and -1 for a key with no TTL")
        run(Command.Set(Key("k"), "v".toByteArray(), ttl = Duration.ofSeconds(10)))
        assertEquals(10L, ttl(Key("k")))
        assertEquals(10_000L, ttl(Key("k"), Command.Ttl.Precision.MILLIS), "PTTL reports milliseconds")
        // Redis rounds TTL half up: 9500 ms is 10 s, 9499 ms is 9 s.
        clock.now += Duration.ofMillis(500)
        assertEquals(10L, ttl(Key("k")))
        assertEquals(9_500L, ttl(Key("k"), Command.Ttl.Precision.MILLIS))
        clock.now += Duration.ofMillis(1)
        assertEquals(9L, ttl(Key("k")))
        clock.now += Duration.ofMillis(9_499)
        assertEquals(0L, ttl(Key("k")), "the key is readable through its deadline")
        assertEquals(0L, ttl(Key("k"), Command.Ttl.Precision.MILLIS))
        clock.now += Duration.ofMillis(1)
        assertEquals(-2L, ttl(Key("k")), "and gone after it")
    }

    /** What the server's scheduler will call once per tick: every partition advances its wheel. */
    private fun tick() {
        engine.tick().get(5, TimeUnit.SECONDS)
    }

    /**
     * C7 at the command level: never early, at most one tick late. The lazy check on access
     * covers the gap between the deadline and the tick that follows it, so both halves of spec
     * 5.4 answer the same at every instant a client can look.
     */
    @Test
    fun C7_key_readable_until_deadline_then_absent() {
        val key = Key("k")
        val deadline = clock.now + Duration.ofSeconds(10)
        run(Command.Set(key, "v".toByteArray(), ttl = Duration.ofSeconds(10)))
        clock.now = deadline - Duration.ofMillis(1)
        tick()
        assertEquals(Reply.Bulk("v".toByteArray()), run(Command.Get(key)), "never early")
        assertEquals(Reply.Integer(1), run(Command.DbSize), "and DBSIZE agrees the key is there")
        clock.now = deadline + Duration.ofMillis(engine.tickMillis)
        tick()
        assertEquals(Reply.Bulk(null), run(Command.Get(key)), "gone at most one tick after the deadline")
        assertEquals(Reply.Integer(0), run(Command.DbSize), "and DBSIZE agrees it is gone")
    }

    @Test
    fun expire_replaces_wheel_entry() {
        val key = Key("k")
        // SET EX puts a deadline on the wheel; the re-EXPIRE has to take it off again, or the
        // tick below fires the old one and deletes a key that should have 80 s left.
        run(Command.Set(key, "v".toByteArray(), ttl = Duration.ofSeconds(10)))
        run(Command.Expire(key, clock.now + Duration.ofSeconds(100)))
        clock.now += Duration.ofSeconds(20)
        tick()
        assertEquals(
            Reply.Bulk("v".toByteArray()),
            run(Command.Get(key)),
            "the replaced deadline was cancelled, so the wheel had nothing to fire at 10 s",
        )
        assertEquals(80L, ttl(key))
        run(Command.Expire(key, clock.now + Duration.ofSeconds(5)))
        clock.now += Duration.ofSeconds(6)
        tick()
        assertEquals(-2L, ttl(key), "a shortened TTL takes the key at its new deadline")
    }

    @Test
    fun persist_cancels_expiry() {
        val key = Key("k")
        assertEquals(Reply.Integer(0), run(Command.Persist(key)), "a missing key has no TTL to drop")
        run(Command.Set(key, "v".toByteArray(), ttl = Duration.ofSeconds(5)))
        assertEquals(Reply.Integer(1), run(Command.Persist(key)))
        assertEquals(Reply.Integer(0), run(Command.Persist(key)), "and 0 again: there is no TTL left")
        assertEquals(-1L, ttl(key))
        clock.now += Duration.ofSeconds(30)
        tick()
        assertEquals(Reply.Bulk("v".toByteArray()), run(Command.Get(key)), "PERSIST cancelled the wheel entry")
        assertEquals(Reply.Integer(1), run(Command.DbSize))
    }

    @Test
    fun del_cancels_wheel_entry_so_a_new_value_survives() {
        val key = Key("k")
        run(Command.Set(key, "old".toByteArray(), ttl = Duration.ofSeconds(5)))
        assertEquals(Reply.Integer(1), run(Command.Del(key)))
        run(Command.Set(key, "new".toByteArray()))
        clock.now += Duration.ofSeconds(30)
        tick()
        assertEquals(
            Reply.Bulk("new".toByteArray()),
            run(Command.Get(key)),
            "the deleted key's deadline cannot reach the value that replaced it",
        )
        assertEquals(Reply.Integer(1), run(Command.DbSize))
    }

    @Test
    fun expireat_absolute() {
        val key = Key("k")
        val deadline = clock.now + Duration.ofSeconds(30)
        assertEquals(Reply.Integer(0), run(Command.Expire(key, deadline)), "a missing key takes no TTL")
        run(Command.Set(key, "v".toByteArray()))
        assertEquals(Reply.Integer(1), run(Command.Expire(key, deadline)))
        assertEquals(30L, ttl(key))
        // The deadline is an instant, not a duration: it does not move when the clock does.
        clock.now += Duration.ofSeconds(10)
        assertEquals(20L, ttl(key))
        assertEquals(Reply.Bulk("v".toByteArray()), run(Command.Get(key)))
        run(Command.Expire(key, clock.now - Duration.ofSeconds(1)))
        assertEquals(Reply.Bulk(null), run(Command.Get(key)), "a deadline already past takes the key at once")
    }

    @Test
    fun `PING replies PONG`() {
        assertEquals(Reply.Simple("PONG"), run(Command.Ping))
    }

    /**
     * Proof of C1 without Lincheck. Every command reads the clock once on its partition thread,
     * so a clock that records the caller and blocks until released is a probe into the executor
     * without a test-only command: it says which thread ran, and holds a partition busy at will.
     */
    @Test
    fun C1_one_command_at_a_time_per_partition() {
        val ranOn = Collections.synchronizedList(mutableListOf<Thread>())
        val entered = Semaphore(0)
        val release = CountDownLatch(1)
        val gate = object : Clock() {
            override fun instant(): Instant {
                ranOn += Thread.currentThread()
                entered.release()
                check(release.await(5, TimeUnit.SECONDS)) { "never released" }
                return Instant.EPOCH
            }
            override fun getZone(): ZoneId = ZoneOffset.UTC
            override fun withZone(zone: ZoneId): Clock = this
        }
        val engine = ApEngine(partitionCount = 2, clock = gate)
        val a1 = Key("{p}.1")
        val a2 = Key("{p}.2")
        val other = (0..99).map { Key("q$it") }.first { engine.partitionOf(it) != engine.partitionOf(a1) }
        try {
            val first = engine.submit(Command.Get(a1))
            assertTrue(entered.tryAcquire(5, TimeUnit.SECONDS), "the first command runs")
            val queued = engine.submit(Command.Get(a2))
            val elsewhere = engine.submit(Command.Get(other))
            assertTrue(entered.tryAcquire(5, TimeUnit.SECONDS), "another partition runs while this one is busy")
            assertFalse(queued.isDone, "the same partition waits")
            release.countDown()
            listOf(first, queued, elsewhere).forEach { it.get(5, TimeUnit.SECONDS) }
        } finally {
            engine.close()
        }
        assertEquals(3, ranOn.size)
        assertSame(ranOn[0], ranOn[2], "the queued command ran on the busy partition's one thread, after it")
        assertNotSame(ranOn[0], ranOn[1], "the other partition ran on its own thread, concurrently")
    }

    @Test
    fun `atomically is a stub until the partition executors arrive`() {
        assertThrows(NotImplementedError::class.java) {
            engine.atomically(listOf(Key("{user1}.a"), Key("{user1}.b"))) { ctx ->
                ctx.execute(Command.Ping)
            }
        }
    }

    private companion object {
        val EMPTY_ARRAY = Reply.Array(emptyList<Reply>())
        val NO_SUCH_KEY = Reply.Error("ERR", "no such key")
        val INDEX_OUT_OF_RANGE = Reply.Error("ERR", "index out of range")
        val NOT_AN_INTEGER = Reply.Error("ERR", "value is not an integer or out of range")
        val WRONG_TYPE = Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
    }

    @Test
    fun `a partition is identified by its index`() {
        assertEquals(PartitionId(3), PartitionId(3))
        assertNotEquals(PartitionId(3), PartitionId(4))
    }
}
