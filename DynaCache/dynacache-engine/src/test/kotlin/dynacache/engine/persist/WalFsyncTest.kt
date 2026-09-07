package dynacache.engine.persist

import dynacache.engine.testkit.MutableClock
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class WalFsyncTest {

    @TempDir
    lateinit var dir: Path

    private val clock = MutableClock(Instant.parse("2026-09-06T00:00:00Z"))

    /** How long a group-commit batch stays open, read off the policy the writer runs. */
    private val deadline: Duration = checkNotNull(FsyncPolicy.GROUP_COMMIT.deadline)

    @Test
    fun wal_fsync_always_durable() {
        val sink = CountingSink(FileChannelSink(dir.resolve("always.wal")))

        writer(sink, FsyncPolicy.ALWAYS).use { writer ->
            repeat(5) { i ->
                val append = writer.append(OP_SET, byteArrayOf(i.toByte()))
                assertTrue(append.durable.isDone, "entry ${append.seq} is durable when append returns")
                assertEquals(i + 1, sink.fsyncs.get(), "one fsync per append")
            }
        }
    }

    @Test
    fun wal_fsync_every_second_batches() {
        val sink = CountingSink(FileChannelSink(dir.resolve("every-second.wal")))

        writer(sink, FsyncPolicy.EVERY_SECOND).use { writer ->
            val appends = (1..50).map { writer.append(OP_SET, byteArrayOf(it.toByte())) }
            assertEquals(0, sink.fsyncs.get(), "nothing is forced until a tick")
            assertTrue(appends.none { it.durable.isDone })

            clock.now = clock.now.plusMillis(999)
            writer.tick()
            assertEquals(0, sink.fsyncs.get(), "a tick inside the second forces nothing")
            assertTrue(appends.none { it.durable.isDone })

            clock.now = clock.now.plusMillis(1)
            writer.tick()
            assertEquals(1, sink.fsyncs.get(), "fifty appends, one fsync")
            assertTrue(appends.all { it.durable.isDone })

            writer.tick()
            assertEquals(1, sink.fsyncs.get(), "nothing new to force")

            val late = writer.append(OP_SET, byteArrayOf(51))
            clock.now = clock.now.plusSeconds(1)
            writer.tick()
            assertEquals(2, sink.fsyncs.get())
            assertTrue(late.durable.isDone)
        }
    }


    @Test
    fun wal_group_commit_amortizes() {
        val sink = CountingSink(FileChannelSink(dir.resolve("group.wal")))

        val appends = writer(sink, FsyncPolicy.ALWAYS).use { writer ->
            concurrentAppends(writer, sink, count = 100)
        }

        assertEquals(100, appends.size)
        assertTrue(appends.all { it.durable.isDone && !it.durable.isCompletedExceptionally })
        assertTrue(sink.fsyncs.get() <= 2, "100 appenders shared fsyncs, saw ${sink.fsyncs.get()}")
        assertTrue(sink.writes.get() <= 2, "100 appenders shared writes, saw ${sink.writes.get()}")
    }

    @Test
    fun wal_group_commit_preserves_seq_order() {
        val path = dir.resolve("order.wal")
        val sink = CountingSink(FileChannelSink(path))

        val appends = writer(sink, FsyncPolicy.ALWAYS).use { writer ->
            concurrentAppends(writer, sink, count = 100)
        }

        val onDisk = WalReader(path).readAll()
        assertEquals(WalStop.CLEAN_END, onDisk.stop)
        // File order is sequence order, with nothing missing and nothing repeated.
        assertEquals((1L..100L).toList(), onDisk.entries.map { it.seq })
        assertEquals((1L..100L).toList(), appends.map { it.seq }.sorted())
        // The seq an appender was handed is the seq stamped on its own bytes.
        val payloadBySeq = onDisk.entries.associate { it.seq to it.payload.single() }
        appends.forEachIndexed { i, append ->
            assertEquals((i + 1).toByte(), payloadBySeq[append.seq])
        }
    }


    @Test
    fun C14_group_commit_replies_only_after_fsync() {
        val sink = CountingSink(FileChannelSink(dir.resolve("c14-group.wal")))
        val appends = mutableListOf<WalAppend>()
        // The sink is the boundary a reply must not cross first: whatever is waiting when a force
        // begins is still waiting.
        sink.beforeFsync = { assertTrue(appends.none { it.durable.isDone }, "a waiter completed before its force") }

        writer(sink, FsyncPolicy.GROUP_COMMIT).use { writer ->
            repeat(3) { i -> appends += writer.append(OP_SET, byteArrayOf(i.toByte())) }
            assertEquals(0, sink.fsyncs.get(), "written is not forced")
            assertTrue(appends.none { it.durable.isDone }, "written is not durable")

            clock.advance(deadline)
            writer.tick()

            assertEquals(1, sink.fsyncs.get(), "one force covers the batch")
            assertTrue(appends.all { it.durable.isDone && !it.durable.isCompletedExceptionally })
        }
    }

    @Test
    fun group_commit_forces_at_the_deadline_when_the_batch_stays_open() {
        val sink = CountingSink(FileChannelSink(dir.resolve("deadline.wal")))

        writer(sink, FsyncPolicy.GROUP_COMMIT).use { writer ->
            val lonely = writer.append(OP_SET, byteArrayOf(1))

            clock.advance(deadline.minusNanos(1))
            writer.tick()
            assertEquals(0, sink.fsyncs.get(), "a batch inside its deadline is not forced")
            assertFalse(lonely.durable.isDone)

            clock.advance(Duration.ofNanos(1))
            writer.tick()
            assertEquals(1, sink.fsyncs.get(), "the deadline forces the open batch")
            assertTrue(lonely.durable.isDone)

            writer.tick()
            assertEquals(1, sink.fsyncs.get(), "an idle tick forces nothing")
        }
    }

    @Test
    fun group_commit_forces_once_per_batch_not_per_write() {
        val sink = CountingSink(FileChannelSink(dir.resolve("once.wal")))

        writer(sink, FsyncPolicy.GROUP_COMMIT).use { writer ->
            val appends = (1..50).map { writer.append(OP_SET, byteArrayOf(it.toByte())) }
            assertEquals(0, sink.fsyncs.get(), "fifty writes and no deadline passed, no force")

            clock.advance(deadline)
            writer.tick()

            assertEquals(1, sink.fsyncs.get(), "fifty writes, one force")
            assertTrue(appends.all { it.durable.isDone && !it.durable.isCompletedExceptionally })
        }
    }

    @Test
    fun reused_batch_buffer_writes_each_batch_whole() {
        val path = dir.resolve("reuse.wal")
        val sink = CountingSink(FileChannelSink(path))
        val big = ByteArray(96 * 1024) { it.toByte() }

        writer(sink, FsyncPolicy.NEVER).use { writer ->
            // The first batch grows the reused buffer; the second must carry its own bytes only.
            writer.append(OP_SET, big)
            writer.append(OP_SET, byteArrayOf(7))
        }

        val onDisk = WalReader(path).readAll()
        assertEquals(WalStop.CLEAN_END, onDisk.stop)
        assertEquals(2, onDisk.entries.size)
        assertArrayEquals(big, onDisk.entries[0].payload)
        assertArrayEquals(byteArrayOf(7), onDisk.entries[1].payload)
    }

    /**
     * [count] appenders on their own threads. The first to flush is held inside its fsync until
     * every other appender has enqueued and returned, so the batch that follows holds them all.
     */
    private fun concurrentAppends(writer: WalWriter, sink: CountingSink, count: Int): List<WalAppend> {
        val release = CountDownLatch(1)
        val othersEnqueued = CountDownLatch(count - 1)
        sink.holdFirstFsync = release
        val pool = Executors.newFixedThreadPool(count)
        try {
            val tasks = (1..count).map { i ->
                pool.submit(Callable {
                    writer.append(OP_SET, byteArrayOf(i.toByte())).also { othersEnqueued.countDown() }
                })
            }
            assertTrue(othersEnqueued.await(5, TimeUnit.SECONDS), "the others returned while the first fsync was held")
            release.countDown()
            val appends = tasks.map { it.get(5, TimeUnit.SECONDS) }
            appends.forEach { it.durable.get(5, TimeUnit.SECONDS) }
            return appends
        } finally {
            pool.shutdownNow()
        }
    }


    private fun writer(sink: WalSink, policy: FsyncPolicy) =
        WalWriter(sink, firstSeq = 1L, policy = policy, clock = clock)

    /** The filesystem is a true boundary: this adapter counts what reaches it and can hold an fsync. */
    private class CountingSink(private val delegate: WalSink) : WalSink {
        val writes = AtomicInteger()
        val fsyncs = AtomicInteger()

        /** When set, the first fsync waits here until the test lets it through. */
        @Volatile
        var holdFirstFsync: CountDownLatch? = null

        /** Run at the head of every fsync, before the force it counts. */
        @Volatile
        var beforeFsync: () -> Unit = {}

        override fun write(bytes: ByteBuffer) {
            writes.incrementAndGet()
            delegate.write(bytes)
        }

        override fun fsync() {
            beforeFsync()
            if (fsyncs.incrementAndGet() == 1) holdFirstFsync?.let { assertTrue(it.await(5, TimeUnit.SECONDS)) }
            delegate.fsync()
        }

        override fun close() = delegate.close()
    }

    private companion object {
        const val OP_SET: Byte = 1
    }
}
