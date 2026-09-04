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
 * A file is its path, size and modification time together: the same name uploaded again is a
 * different file, and a file nacked for good is remembered by that key until the process ends.
 *
 * Nothing slow happens under the lock. Deciding whether a file may enter and recording that it
 * left are the only things done while holding it; waiting for room happens before it is taken and
 * the consumer's work happens long after it was released.
 */
internal class InFlightSet(capacity: Int) {

    private val lock = Any()

    /** Insertion-ordered, so [outstanding] can say what is out in the order it was handed over. */
    private val inFlight = LinkedHashSet<RemoteFile>()
    private val excluded = HashSet<RemoteFile>()
    private val room = Semaphore(capacity)

    val size: Int get() = synchronized(lock) { inFlight.size }

    /** Whether [file] is out with the consumer right now, or was nacked for good. Never waits. */
    fun holds(file: RemoteFile): Boolean = synchronized(lock) { file in inFlight || file in excluded }

    /**
     * Every file out with the consumer right now, oldest first: handed over and not yet given back,
     * whichever poll handed it over. One copy taken under the lock, so a caller never iterates the
     * set while a poll alongside is changing it. Files nacked for good are not out; they are kept out.
     */
    fun outstanding(): List<RemoteFile> = synchronized(lock) { inFlight.toList() }

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
        if (!enter(file)) {
            room.release()
            return null
        }
        return InFlightSlot(file, this)
    }

    internal fun leave(file: RemoteFile, forGood: Boolean) {
        exit(file, forGood)
        room.release()
    }

    /**
     * The lock body of [admit]: the second look, and the file's entry when it passes. Whether the
     * file entered. Non-suspending on purpose, so the lock can be model-checked on its own.
     */
    internal fun enter(file: RemoteFile): Boolean = synchronized(lock) {
        if (file in inFlight || file in excluded) false else inFlight.add(file)
    }

    /** The lock body of [leave]. Whether the file was in the set. */
    internal fun exit(file: RemoteFile, forGood: Boolean): Boolean = synchronized(lock) {
        if (forGood) excluded += file
        inFlight.remove(file)
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

    /** How this file was given back, or null while the consumer still has it. */
    val settlement: Settlement? get() = settledAs.get()

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

    /** The file was not there to download; there is nothing to act on. */
    GONE("gone"),
}
