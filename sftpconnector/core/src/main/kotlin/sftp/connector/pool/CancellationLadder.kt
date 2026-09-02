package sftp.connector.pool

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import sftp.connector.error.LeaseFate
import sftp.connector.error.SftpException
import kotlin.time.Duration

/**
 * What the pool does about a caller that stopped waiting while its session was busy.
 *
 * Cancelling a coroutine cancels a coroutine. It does not reach the thread inside the SSH library,
 * and a blocking socket read reacts to neither a cancelled job nor an interrupted thread - so
 * without this a cancelled download would return to its caller and go on reading a file nobody
 * wanted, on a session the pool believes is free, until the file ran out. There are three ways
 * such a call can come to a stop, and they cost three different amounts:
 *
 *  1. **It notices and stops itself.** A transfer asks between chunks whether its caller is still
 *     there and a listing has nowhere left to put an entry. The remote handle closes cleanly and
 *     the session is as good as it was. This costs nothing and needs nothing from here: the
 *     cancellation that started all this is the same signal those two are watching.
 *  2. **The keepalive gives up on the server.** Probes go unanswered until the SSH library ends
 *     the read itself, which takes twice the keepalive interval and leaves a session nobody
 *     should reuse. It is a floor under every blocked read, cancelled or not, rather than
 *     something this climbs to - so what it costs is not the waiting but the session.
 *  3. **The session is cut apart.** Closing the socket is the one thing a blocked read cannot
 *     ignore. It costs a handshake, and it is what this waits out the grace period to avoid.
 *
 * Which of those happened is what decides whether the session goes back on the shelf, and it is
 * decided here rather than at each of the client's operations - a caller cannot know how its call
 * stopped, and every caller guessing separately would eventually guess differently.
 */
internal class CancellationLadder(private val grace: Duration) {

    /**
     * Runs [borrowing] against the session [entry] holds and hands back what it produced.
     *
     * A cancelled caller is not let go of until the call it left behind has really stopped, which
     * is the point: until then the session is still in use, and a pool that lent it to somebody
     * else would have two callers on one channel. So this returns when the work does, and the
     * grace period is what bounds how long that can be.
     *
     * Once a caller has been cancelled, the cancellation is the whole of what it hears. The work
     * left behind usually does end by failing - a session cut apart raises a lost connection, and
     * that is this method's own doing rather than news - and a scope that let the loser of that
     * race decide would report a network fault to a caller that had simply changed its mind. This
     * is why the work is watched rather than merely run: it is one child, and reading its outcome
     * is the whole job.
     */
    suspend fun <T> carry(entry: PoolEntry, borrowing: suspend () -> T): T = supervisorScope {
        val work = async { borrowing() }
        try {
            work.await()
        } catch (givenUpOn: CancellationException) {
            // Under NonCancellable because everything from here is work for a coroutine that has
            // already been cancelled, and none of it would get a turn otherwise.
            withContext(NonCancellable) { bringToAStop(work, entry) }
            throw givenUpOn
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun bringToAStop(work: Deferred<*>, entry: PoolEntry) {
        if (withTimeoutOrNull(grace) { work.join() } == null) {
            LOG.warn(
                "{} did not stop within {} of its caller giving up, so the session is being cut apart to " +
                    "get its thread back. It costs a handshake, and the alternative is a thread the " +
                    "process never sees again.",
                entry,
                grace,
            )
            entry.cutLoose()
            return
        }

        // It stopped on its own, which is usually the cheap rung - but not always. The keepalive
        // giving up on a silent server stops a call just as effectively and leaves a session
        // nobody should be handed next, and from out here the two look the same. So what ended it
        // is asked what became of the session, rather than the answer being guessed from the fact
        // that it ended in time.
        val ending = work.getCompletionExceptionOrNull()
        if ((ending as? SftpException)?.disposition?.lease == LeaseFate.EVICTED) {
            entry.unfitAfterCancelling = true
        }
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(CancellationLadder::class.java)
    }
}
