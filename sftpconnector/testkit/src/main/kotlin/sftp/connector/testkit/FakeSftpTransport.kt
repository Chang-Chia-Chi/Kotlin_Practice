package sftp.connector.testkit

import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * A transport that answers from a script instead of from a socket.
 *
 * Everything a test wants to arrange - a connect that succeeds, one that fails, one that takes its
 * time, a session that dies while parked, a check on what the caller was holding at the moment of
 * the call - is the same arrangement: something to run when a call arrives. So there is one hook,
 * [answer], and it does all of it. Suspending in it is a slow server; throwing from it is a
 * failing one; asserting in it is a test of what the caller may do while it waits.
 *
 * Every call is recorded in [calls], and sessions are numbered from one so a test can say which
 * session a call was made on rather than merely that some call happened.
 */
class FakeSftpTransport(
    /**
     * Runs at the start of every call, before the fake does anything about it. The default lets
     * everything through.
     */
    private val answer: suspend (Call) -> Unit = {},
) : SftpTransport {

    /**
     * One call the fake was asked to make. [session] is 0 for a connect, which has no session yet.
     *
     * The operation is named rather than spelled, because a test that filters the record for a
     * misspelled operation finds nothing and reports that nothing happened, which is exactly what
     * a passing test looks like.
     */
    data class Call(val operation: Operation, val session: Int)

    enum class Operation { Connect, Realpath, Close }

    val calls: MutableList<Call> = CopyOnWriteArrayList()

    private val sessionsOpened = AtomicInteger()
    private val sessionsClosed = AtomicInteger()

    /** Sessions opened and not yet closed. A pool that leaks one leaves this above what it should be. */
    val openSessions: Int get() = sessionsOpened.get() - sessionsClosed.get()

    override suspend fun connect(): SftpConnection {
        record(Call(Operation.Connect, session = 0))
        return FakeSession(sessionsOpened.incrementAndGet())
    }

    private suspend fun record(call: Call) {
        calls += call
        answer(call)
    }

    private inner class FakeSession(val id: Int) : SftpConnection {

        private var closed = false

        override suspend fun realpath(path: String): String {
            record(Call(Operation.Realpath, id))
            check(!closed) { "session $id was used after it was closed" }
            return if (path == ".") "/home/etl" else path
        }

        override suspend fun close() {
            record(Call(Operation.Close, id))
            if (!closed) {
                closed = true
                sessionsClosed.incrementAndGet()
            }
        }

        override fun toString(): String = "fake session $id"
    }
}
