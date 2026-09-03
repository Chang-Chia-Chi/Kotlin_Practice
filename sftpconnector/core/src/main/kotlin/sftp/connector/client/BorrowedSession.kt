package sftp.connector.client

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpSession
import java.io.InputStream
import java.io.OutputStream

/**
 * A session lent to somebody who did not open it, and who therefore does not get to end it or to
 * keep it.
 *
 * The pool hands the same session to the next caller as soon as this one is finished with it, so a
 * reference that outlives the block it was handed to is a second caller on a session that is being
 * used by someone else - which is the one thing the pool exists to prevent, broken from outside it.
 * It cannot be prevented by asking, so the session is made to stop working instead.
 */
internal class BorrowedSession(private val session: SftpSession) : SftpSession {

    /**
     * One call at a time, and the end of the loan waits its turn like a call. A flag read at the
     * start of each call was not enough: a call that had passed the check and was still on the
     * wire when the block returned went on using the session after the pool had lent it to
     * somebody else. Holding the lock for the length of the call means the loan cannot end while
     * a call is in flight, and no call can start once it has. An SFTP channel does one thing at a
     * time in any case, so nothing is lost by taking calls in turn - and a call made from inside
     * another call's callback, such as a stat from a listing's entry callback, now waits for a
     * lock the listing holds rather than corrupting the channel's stream underneath it. Neither
     * was ever going to work; this one at least stops where the fault is.
     */
    private val oneAtATime = Mutex()

    /** Guarded by [oneAtATime]. */
    private var mine = true

    /**
     * Ends the loan, once whatever call is in flight has returned. Anything still holding this
     * afterwards finds it inert.
     */
    suspend fun handItBack() = oneAtATime.withLock { mine = false }

    private suspend fun <T> inHand(use: suspend SftpSession.() -> T): T = oneAtATime.withLock {
        check(mine) {
            "This session was handed back when the withSession block returned and now belongs to " +
                "whoever borrowed it next. A session cannot be kept past the block that was given it."
        }
        session.use()
    }

    override suspend fun realpath(path: String): String = inHand { realpath(path) }

    override suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing) = inHand { list(dir, onEntry) }

    override suspend fun stat(path: String): RemoteFile = inHand { stat(path) }

    override suspend fun readTo(path: String, sink: OutputStream) = inHand { readTo(path, sink) }

    override suspend fun writeFrom(path: String, source: InputStream) = inHand { writeFrom(path, source) }

    override suspend fun rename(from: String, to: String) = inHand { rename(from, to) }

    /** A fact about the server rather than a call on the session, so it is answered without the lock and past the loan. */
    override val renameReplaces: Boolean get() = session.renameReplaces

    override suspend fun delete(path: String) = inHand { delete(path) }

    override suspend fun mkdir(path: String) = inHand { mkdir(path) }
}
