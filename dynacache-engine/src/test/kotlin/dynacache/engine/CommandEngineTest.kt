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

    @Test
    fun `a partition is identified by its index`() {
        assertEquals(PartitionId(3), PartitionId(3))
        assertNotEquals(PartitionId(3), PartitionId(4))
    }
}
