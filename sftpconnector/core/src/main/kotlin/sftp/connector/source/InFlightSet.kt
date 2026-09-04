package sftp.connector.source

import kotlinx.coroutines.sync.Semaphore
import sftp.connector.transport.RemoteFile
import java.util.concurrent.atomic.AtomicReference

/**
 * The files handed to the consumer and not yet given back, and the only state the connector
 * keeps about processed files.
 *
 * Three promises live here and nowhere else. A file in the set is not handed over again, by this
 * poll or any other running alongside it, so a slow consumer never receives the same file twice.
 * The set holds at most [capacity] files, and a poll that wants to add one more waits until an
 * ack or a nack makes room - that wait is the whole of the connector's backpressure. And every
 * file comes back exactly once, whichever of ack, nack, gone or a withdrawn poll gives it back.
 *
 * Two things are decided here, and they are keyed differently. A file's *identity* is its path,
 * size and modification time together: the same name uploaded again with a new size is a
 * different file, an answer is accepted once per file, and a file nacked for good is remembered
 * by that key until the process ends. *Exclusivity* is the path alone: while any file at a path
 * is out with the consumer, nothing else at that path enters, whatever its size or time, because
 * a consumer working a file must never be racing itself on a second copy of the same name. The
 * second copy is not lost, only later: it enters once the first has been given back.
 *
 * Nothing slow happens under the lock. Deciding whether a file may enter and recording that it
 * left are the only things done while holding it; waiting for room happens before it is taken and
 * the consumer's work happens long after it was released.
 */
internal class InFlightSet(capacity: Int) {

    private val lock = Any()

    /**
     * Keyed by path, which is the exclusivity; the value is the file's place, so the set can answer
     * a path with the very slot that was handed over. Insertion-ordered, so [outstanding] can say
     * what is out in the order it was handed over.
     */
    private val inFlight = LinkedHashMap<String, InFlightSlot>()
    private val excluded = HashSet<RemoteFile>()
    private val room = Semaphore(capacity)

    val size: Int get() = synchronized(lock) { inFlight.size }

    /**
     * Whether [file] would be turned away right now: a file is out at its path - this one or
     * another - or this exact file was nacked for good. Never waits.
     */
    fun holds(file: RemoteFile): Boolean = synchronized(lock) { file.path in inFlight || file in excluded }

    /** The file out with the consumer at [path], or null when nothing is. Never waits. */
    fun outAt(path: String): RemoteFile? = slotAt(path)?.file

    /**
     * The place of the file out with the consumer at [path], or null when nothing is - one look
     * under the lock, so a caller with a path and nothing else gets back to the file without a
     * table of its own. Never waits.
     */
    fun slotAt(path: String): InFlightSlot? = synchronized(lock) { inFlight[path] }

    /**
     * Every file out with the consumer right now, oldest first: handed over and not yet given back,
     * whichever poll handed it over. One copy taken under the lock, so a caller never iterates the
     * set while a poll alongside is changing it. Files nacked for good are not out; they are kept out.
     */
    fun outstanding(): List<RemoteFile> = synchronized(lock) { inFlight.values.map { it.file } }

    /**
     * Puts [file] in the set and returns its slot, or null when the file is already out or was
     * nacked for good. Suspends while the set is full, until a slot comes back.
     *
     * A file that would be turned away is turned away before it waits, so a duplicate seldom
     * queues for room it will not use - it does when the duplicate arrived between the two looks.
     * The check is made again once room is taken, because a poll running alongside may have
     * admitted the same file in the meantime, and that second look is what keeps the promise.
     */
    suspend fun admit(file: RemoteFile): InFlightSlot? {
        if (holds(file)) return null
        room.acquire()
        return place(file) ?: run {
            room.release()
            null
        }
    }

    internal fun leave(file: RemoteFile, forGood: Boolean) {
        exit(file, forGood)
        room.release()
    }

    /**
     * The lock body of [admit] as the model checker sees it: whether the file entered.
     * Non-suspending on purpose, so the lock can be model-checked on its own.
     */
    internal fun enter(file: RemoteFile): Boolean = place(file) != null

    /** The second look, and the file's place when it passes. */
    private fun place(file: RemoteFile): InFlightSlot? = synchronized(lock) {
        if (file.path in inFlight || file in excluded) {
            null
        } else {
            InFlightSlot(file, this).also { inFlight[file.path] = it }
        }
    }

    /**
     * The lock body of [leave]. Whether the file was in the set - this exact file, so leaving
     * never takes out another file that holds the same path.
     */
    internal fun exit(file: RemoteFile, forGood: Boolean): Boolean = synchronized(lock) {
        if (forGood) excluded += file
        val out = inFlight[file.path]
        out != null && out.file == file && inFlight.remove(file.path) != null
    }
}

/**
 * One file's place in the set, and the guard that it is given back once.
 *
 * Settling and releasing are two steps on purpose. Settling is the decision - this file is acked,
 * or nacked, or gone - and it is taken first and atomically, so the second of two competing calls
 * learns it lost and does nothing. What the decision implies - a move, a delete - then runs with
 * the file still in the set, so an overlapping poll cannot hand it over while it is half moved.
 * Only when that work is over does the slot go back and the room with it.
 */
internal class InFlightSlot(val file: RemoteFile, private val set: InFlightSet) {

    private val settledAs = AtomicReference<Settlement?>(null)
    private val handedOverAs = AtomicReference<SftpEvent.FileSeen?>(null)

    /** How this file was given back, or null while the consumer still has it. */
    val settlement: Settlement? get() = settledAs.get()

    /**
     * The handle the consumer was given for this place, or null between the place being taken
     * and the tick handing it over. Answering a path with this, rather than with a fresh handle
     * over the same place, is what lets an answer through either be the same answer.
     */
    val handle: SftpEvent.FileSeen? get() = handedOverAs.get()

    /** Records the one handle this place was handed over on; a place is handed over once. */
    fun handedOverAs(handle: SftpEvent.FileSeen) {
        check(handedOverAs.compareAndSet(null, handle)) { "${file.path} was handed over twice on one place" }
    }

    /** Records [outcome] as this file's fate, and says whether this call was the one that got to. */
    fun settle(outcome: Settlement): Boolean = settledAs.compareAndSet(null, outcome)

    /** Gives the place back. [forGood] keeps the file out until the process restarts. */
    fun release(forGood: Boolean = false) = set.leave(file, forGood)
}

/** The ways a file leaves the in-flight set. [label] is the metric tag. */
internal enum class Settlement(val label: String) {
    ACK("ack"),
    NACK("nack"),

    /** The poll that handed the file over was cancelled before the consumer said anything. */
    CANCELLED("cancelled"),

    /**
     * The watch that handed the file over ended - its collector left, it was cancelled, the
     * connector closed - before the consumer said anything. Counted with [CANCELLED]: the file
     * goes back the same way and for the same reason, and the metric's labels are fixed.
     */
    WATCH_ENDED("cancelled"),

    /** The file was not there to download; there is nothing to act on. */
    GONE("gone"),
}
