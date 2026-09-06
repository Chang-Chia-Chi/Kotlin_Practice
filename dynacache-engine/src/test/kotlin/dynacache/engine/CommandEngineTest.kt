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
    private val engine = ApEngine(partitionCount = 4, clock = clock)

    @AfterEach
    fun close() = engine.close()

    private fun run(command: Command): Reply = engine.submit(command).get()

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
        assertEquals(Reply.Bulk("v".toByteArray()), run(Command.Get(Key("k"))))
        clock.now += Duration.ofMillis(2)
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
        val NOT_AN_INTEGER = Reply.Error("ERR", "value is not an integer or out of range")
        val WRONG_TYPE = Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
    }

    @Test
    fun `a partition is identified by its index`() {
        assertEquals(PartitionId(3), PartitionId(3))
        assertNotEquals(PartitionId(3), PartitionId(4))
    }
}
