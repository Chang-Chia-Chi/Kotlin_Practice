package dynacache.engine.persist

import dynacache.engine.testkit.MutableClock
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.file.Path
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

        override fun write(bytes: ByteBuffer) {
            writes.incrementAndGet()
            delegate.write(bytes)
        }

        override fun fsync() {
            if (fsyncs.incrementAndGet() == 1) holdFirstFsync?.let { assertTrue(it.await(5, TimeUnit.SECONDS)) }
            delegate.fsync()
        }

        override fun close() = delegate.close()
    }

    private companion object {
        const val OP_SET: Byte = 1
    }
}
