package dynacache.engine.persist

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32

/*
 * The write-ahead log: an append-only file of mutations, replayed on restart (spec 2.8).
 *
 * One entry on disk, big-endian, exactly the spec's layout:
 *
 *     [crc32:u32][length:u32][seq:u64][op:u8][payload:length bytes]
 *
 * `length` is the payload's byte count, so the header is a fixed 17 bytes. The checksum covers
 * everything after itself - length, seq, op and payload - so a corrupted length field is caught
 * by the same check as a corrupted payload.
 */

private const val HEADER_BYTES = 4 + 4 + 8 + 1

private const val CRC_BYTES = 4
private const val LENGTH_AT = 4
private const val SEQ_AT = 8
private const val OP_AT = 16

/** The flusher's buffer starts here and grows to whatever a batch needs. */
private const val INITIAL_BATCH_BYTES = 64 * 1024

/** One logged mutation: an opaque [payload] under an [op] code, stamped with its [seq]. */
class WalEntry(val seq: Long, val op: Byte, val payload: ByteArray) {

    override fun equals(other: Any?): Boolean =
        this === other ||
            (other is WalEntry && seq == other.seq && op == other.op && payload.contentEquals(other.payload))

    override fun hashCode(): Int = (31 * seq.hashCode() + op) * 31 + payload.contentHashCode()

    override fun toString(): String = "WalEntry(seq=$seq, op=$op, payload=${payload.size} bytes)"
}

/** Why a read of the log stopped where it did. */
enum class WalStop {
    /** The last entry ended exactly at the end of the file: nothing was lost. */
    CLEAN_END,

    /**
     * The file ends in the middle of an entry, the shape a crash mid-append leaves. A length
     * field too large for the bytes that remain reads the same way, and is reported the same way:
     * there is no complete entry at this offset.
     */
    TORN_TAIL,

    /** An entry's bytes do not match its checksum. Nothing at or past it is trusted. */
    CRC_MISMATCH,
}

/**
 * What one pass over a log file found: every complete entry up to the point where reading
 * stopped, why it stopped, and the byte [stoppedAt] offset it stopped at.
 */
class WalScan(val entries: List<WalEntry>, val stop: WalStop, val stoppedAt: Long)

/** When an appended entry is forced to disk (spec 2.8). */
enum class FsyncPolicy(
    /**
     * How long an entry may sit written but unforced, or null for a policy that leaves none
     * waiting. `EVERY_SECOND` measures it from the last fsync, so a burst after an idle spell is
     * forced at the next due tick; `GROUP_COMMIT` measures it from its batch's oldest waiter, so
     * it bounds one entry's wait rather than the log's.
     */
    val deadline: Duration? = null,
) {
    /** Every append is fsynced before its future completes. */
    ALWAYS,

    /** Fsynced by the first [WalWriter.tick] at least one second after the last fsync. */
    EVERY_SECOND(Duration.ofSeconds(1)),

    /** Never fsynced by the writer; the operating system decides. The future completes on write. */
    NEVER,

    /**
     * Reply-after-durable on a short deadline (C14): a batch is written at once and forced by the
     * first [WalWriter.tick] or flush that finds its oldest waiter aged past [deadline], so one
     * fsync covers every writer that arrived inside the window. A deadline this short is shorter
     * than the engine's tick, so the server gives it a cadence of its own.
     */
    GROUP_COMMIT(Duration.ofMillis(2)),
}

/** Where the log's bytes go: written, then forced to disk. The filesystem is a true boundary. */
interface WalSink : AutoCloseable {
    /** Writes every remaining byte of [bytes]. */
    fun write(bytes: ByteBuffer)

    fun fsync()
}

/** The real sink: one file opened for append. */
class FileChannelSink(path: Path) : WalSink {

    private val channel: FileChannel = FileChannel.open(
        path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.APPEND,
    )

    override fun write(bytes: ByteBuffer) {
        while (bytes.hasRemaining()) channel.write(bytes)
    }

    override fun fsync() = channel.force(true)

    override fun close() = channel.close()
}

/**
 * One append: the [seq] it was given at once, and [durable], which completes when the policy
 * says the entry is on disk.
 */
class WalAppend(val seq: Long, val durable: CompletableFuture<Unit>)

/**
 * Appends entries to one log, handing each a sequence number one higher than the last, and
 * makes them durable per [policy].
 *
 * Group commit: an append encodes its entry and enqueues it, then the first appender to find no
 * flusher running becomes the flusher. It drains everything enqueued so far into one reused
 * buffer, writes it and, under `ALWAYS`, forces it, completes those appends, and repeats while
 * the queue refills. So one write and one fsync carry every appender that arrived while the
 * flusher was busy, which is what amortizes the fsync across concurrent writers (spec 2.8).
 *
 * Under `GROUP_COMMIT` the force is deferred instead: the batch stays open, and is forced by the
 * first [tick] or flush to find its oldest waiter aged past the policy's deadline, so one fsync
 * covers a window of writers rather than one batch of them. No waiter completes before the force
 * that covers it under any policy (C14).
 *
 * No thread is owned here and the engine's rule of no timers holds: every force that is not the
 * flusher's own comes from the caller's [tick].
 */
class WalWriter(
    private var sink: WalSink,
    firstSeq: Long,
    private val policy: FsyncPolicy,
    private val clock: Clock,
) : AutoCloseable {

    constructor(
        path: Path,
        firstSeq: Long,
        policy: FsyncPolicy = FsyncPolicy.NEVER,
        clock: Clock = Clock.systemUTC(),
    ) : this(FileChannelSink(path), firstSeq, policy, clock)

    private class Pending(val record: ByteBuffer, val durable: CompletableFuture<Unit>)

    private var nextSeq: Long = firstSeq

    /** The seq handed out last; [firstSeq] minus one before the first append. */
    val lastSeq: Long get() = synchronized(this) { nextSeq - 1 }

    /** Held while the sink is forced or swapped, so a [tick] never forces a sink [rotate] just closed. */
    private val sinkLock = Any()

    /** Enqueued under the writer's monitor in seq order, so a FIFO drain is file order. */
    private val pending = ConcurrentLinkedQueue<Pending>()
    private val flushing = AtomicBoolean(false)

    /** Written but not yet forced; `EVERY_SECOND` and `GROUP_COMMIT` fill it. Guarded by itself. */
    private val awaitingFsync = ArrayList<CompletableFuture<Unit>>()

    /** When the oldest waiter now in [awaitingFsync] was parked, null while none is. Guarded by it. */
    private var awaitingSince: Instant? = null

    /**
     * The flusher's buffer, reused across batches and grown to fit. Only the flusher touches it,
     * and [flushing] is what publishes it from one flusher to the next: the outgoing one releases
     * the flag after its write, the incoming one takes it before its own.
     */
    private var batchBuffer: ByteBuffer = ByteBuffer.allocate(INITIAL_BATCH_BYTES)

    @Volatile
    private var lastFsync: Instant = clock.instant()

    /** Enqueues one entry, returns its seq at once, and flushes if nobody else is. */
    fun append(op: Byte, payload: ByteArray): WalAppend {
        val durable = CompletableFuture<Unit>()
        val seq = synchronized(this) {
            val seq = nextSeq++
            pending.add(Pending(encode(seq, op, payload), durable))
            seq
        }
        flushIfIdle()
        return WalAppend(seq, durable)
    }

    /**
     * The caller's clock tick, which forces what the policy has left waiting and completes those
     * appends. Under `EVERY_SECOND` that is everything written since the last fsync, once a second
     * has passed; under `GROUP_COMMIT`, the open batch once its oldest waiter has aged past the
     * deadline. A no-op under `ALWAYS` and `NEVER`, which leave nothing waiting.
     */
    fun tick() {
        when (policy) {
            FsyncPolicy.EVERY_SECOND -> if (clock.instant() >= lastFsync.plusSeconds(1)) forceAwaiting()
            FsyncPolicy.GROUP_COMMIT -> forcePastDeadline()
            FsyncPolicy.ALWAYS, FsyncPolicy.NEVER -> Unit
        }
    }

    /**
     * The checkpoint cut: everything appended so far is forced into the old sink, which is then
     * closed, and every later entry goes to [next]. The caller guarantees no append is in flight,
     * which the engine has by parking every partition at the cut; the check names that contract.
     */
    fun rotate(next: WalSink) {
        check(pending.isEmpty() && !flushing.get()) { "rotate needs a quiet log" }
        synchronized(sinkLock) {
            forceAwaiting()
            sink.close()
            sink = next
        }
    }

    private fun flushIfIdle() {
        // The rule this flag exists to keep: every thread that touches the sink holds it, or is
        // the caller's tick thread. [rotate] relies on exactly that, since a clear flag and an
        // empty queue is the whole of its quiet-log check and the tick is its own thread. So
        // nothing between taking the flag and releasing it may be moved out of the flag, however
        // idle the writer looks at that moment: a write or a force running with the flag clear is
        // one rotate cannot see, and it would land on a sink rotate had already closed and swapped.
        //
        // Re-check after releasing the flag: an entry enqueued between the drain and the release
        // would otherwise sit with nobody flushing it. An appender that finds a flusher running
        // leaves without forcing, which is safe for the same reason: that flusher re-checks the
        // queue after its own batch, so it writes and then forces this entry too.
        while (pending.peek() != null) {
            if (!flushing.compareAndSet(false, true)) return
            try {
                writeBatch()
                // A batch already past its deadline is forced here rather than left to the next
                // tick, so a busy log forces at the rate its writers arrive.
                if (policy == FsyncPolicy.GROUP_COMMIT) forcePastDeadline()
            } finally {
                flushing.set(false)
            }
        }
    }

    private fun writeBatch() {
        val batch = generateSequence { pending.poll() }.toList()
        if (batch.isEmpty()) return
        val waiters = batch.map { it.durable }
        try {
            sink.write(fill(batch))
        } catch (e: IOException) {
            waiters.forEach { it.completeExceptionally(e) }
            return
        }
        // Parked only now that the bytes are in the sink, which is what makes a later force sound:
        // any fsync that starts after a waiter was parked has covered that waiter's entry.
        when (policy) {
            FsyncPolicy.ALWAYS -> fsyncAndComplete(waiters)
            FsyncPolicy.NEVER -> waiters.forEach { it.complete(Unit) }
            FsyncPolicy.EVERY_SECOND, FsyncPolicy.GROUP_COMMIT -> park(waiters)
        }
    }

    /**
     * Every record of [batch] end to end in the reused buffer, which grows to fit and never
     * shrinks. Only the flusher calls this, and there is one of those at a time.
     */
    private fun fill(batch: List<Pending>): ByteBuffer {
        val need = batch.sumOf { it.record.remaining() }
        if (batchBuffer.capacity() < need) batchBuffer = ByteBuffer.allocate(maxOf(need, batchBuffer.capacity() * 2))
        batchBuffer.clear()
        batch.forEach { batchBuffer.put(it.record) }
        return batchBuffer.flip()
    }

    /** Written, waiting for the force that makes it durable, and aging from now if it is first. */
    private fun park(waiters: List<CompletableFuture<Unit>>) = synchronized(awaitingFsync) {
        if (awaitingFsync.isEmpty()) awaitingSince = clock.instant()
        awaitingFsync.addAll(waiters)
    }

    private fun takeAwaiting(): List<CompletableFuture<Unit>> = synchronized(awaitingFsync) { drainAwaiting() }

    /** The caller holds [awaitingFsync]'s monitor. */
    private fun drainAwaiting(): List<CompletableFuture<Unit>> {
        awaitingSince = null
        return ArrayList(awaitingFsync).also { awaitingFsync.clear() }
    }

    private fun forceAwaiting() {
        val left = takeAwaiting()
        if (left.isNotEmpty()) fsyncAndComplete(left)
    }

    /** Forces the open batch, if there is one and its oldest waiter has aged past the deadline. */
    private fun forcePastDeadline() {
        val deadline = policy.deadline ?: return
        val batch = synchronized(awaitingFsync) {
            val since = awaitingSince ?: return
            if (clock.instant() < since.plus(deadline)) return
            drainAwaiting()
        }
        fsyncAndComplete(batch)
    }

    private fun fsyncAndComplete(batch: List<CompletableFuture<Unit>>) {
        try {
            synchronized(sinkLock) { sink.fsync() }
            lastFsync = clock.instant()
            batch.forEach { it.complete(Unit) }
        } catch (e: IOException) {
            batch.forEach { it.completeExceptionally(e) }
        }
    }

    private fun encode(seq: Long, op: Byte, payload: ByteArray): ByteBuffer {
        val record = ByteBuffer.allocate(HEADER_BYTES + payload.size)
        record.position(CRC_BYTES)
        record.putInt(payload.size).putLong(seq).put(op).put(payload)
        val crc = CRC32()
        crc.update(record.array(), CRC_BYTES, record.position() - CRC_BYTES)
        record.putInt(0, crc.value.toInt())
        return record.flip()
    }

    /** Writes what is still queued, forces what is still waiting, then closes the sink. */
    override fun close() {
        flushIfIdle()
        forceAwaiting()
        sink.close()
    }
}

/** Reads one log file back, entry by entry, in the order they were appended. */
class WalReader(private val path: Path) {

    fun readAll(): WalScan {
        val entries = ArrayList<WalEntry>()
        FileChannel.open(path, StandardOpenOption.READ).use { channel ->
            val size = channel.size()
            var offset = 0L
            val header = ByteBuffer.allocate(HEADER_BYTES)
            while (offset < size) {
                if (size - offset < HEADER_BYTES) return WalScan(entries, WalStop.TORN_TAIL, offset)
                header.clear()
                readFully(channel, header, offset)
                val length = header.getInt(LENGTH_AT)
                if (length < 0 || size - offset - HEADER_BYTES < length) {
                    return WalScan(entries, WalStop.TORN_TAIL, offset)
                }
                val payload = ByteBuffer.allocate(length)
                readFully(channel, payload, offset + HEADER_BYTES)
                val crc = CRC32()
                crc.update(header.array(), CRC_BYTES, HEADER_BYTES - CRC_BYTES)
                crc.update(payload.array())
                if (crc.value.toInt() != header.getInt(0)) {
                    return WalScan(entries, WalStop.CRC_MISMATCH, offset)
                }
                entries.add(WalEntry(header.getLong(SEQ_AT), header.get(OP_AT), payload.array()))
                offset += HEADER_BYTES + length
            }
            return WalScan(entries, WalStop.CLEAN_END, offset)
        }
    }

    private fun readFully(channel: FileChannel, into: ByteBuffer, from: Long) {
        var at = from
        while (into.hasRemaining()) {
            val read = channel.read(into, at)
            if (read < 0) throw IOException("$path ended at $at")
            at += read
        }
    }
}
