package sftp.connector.client

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
     * Volatile because the block may well have run on a different thread from the one that reads
     * this next, and a stale reading here is a second caller getting through to somebody else's
     * session - which is the whole thing this exists to stop.
     */
    @Volatile
    private var mine = true

    /** Ends the loan. Anything still holding this afterwards finds it inert. */
    fun handItBack() {
        mine = false
    }

    private fun inHand(): SftpSession {
        check(mine) {
            "This session was handed back when the withSession block returned and now belongs to " +
                "whoever borrowed it next. A session cannot be kept past the block that was given it."
        }
        return session
    }

    override suspend fun realpath(path: String): String = inHand().realpath(path)

    override suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing) = inHand().list(dir, onEntry)

    override suspend fun stat(path: String): RemoteFile = inHand().stat(path)

    override suspend fun readTo(path: String, sink: OutputStream) = inHand().readTo(path, sink)

    override suspend fun writeFrom(path: String, source: InputStream) = inHand().writeFrom(path, source)

    override suspend fun rename(from: String, to: String) = inHand().rename(from, to)

    override val renameReplaces: Boolean get() = inHand().renameReplaces

    override suspend fun delete(path: String) = inHand().delete(path)

    override suspend fun mkdir(path: String) = inHand().mkdir(path)
}
