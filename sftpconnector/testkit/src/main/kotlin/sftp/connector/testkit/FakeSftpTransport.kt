package sftp.connector.testkit

import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import java.io.OutputStream
import java.time.Instant
import java.util.Collections
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
 * What the fake server holds is a second, separate thing, because a listing and a download have to
 * agree about it: [file] and [directory] put paths there and [remove] takes one away, which is how
 * a test stages a file that vanishes between being listed and being fetched.
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
    data class Call(val operation: Operation, val session: Int, val path: String? = null)

    enum class Operation { Connect, Realpath, List, Stat, Read, Close }

    val calls: MutableList<Call> = CopyOnWriteArrayList()

    private val sessionsOpened = AtomicInteger()
    private val sessionsClosed = AtomicInteger()

    /** Sessions opened and not yet closed. A pool that leaks one leaves this above what it should be. */
    val openSessions: Int get() = sessionsOpened.get() - sessionsClosed.get()

    /**
     * What the server holds, in the order it was put there, which is the order a listing reports.
     * A null value is a directory: it has no bytes, and it is the one thing a listing reports that
     * cannot be downloaded.
     */
    private val contents: MutableMap<String, ByteArray?> = Collections.synchronizedMap(LinkedHashMap())

    /** Puts a file on the server. [bytes] is what a download of it delivers. */
    fun file(path: String, bytes: ByteArray): FakeSftpTransport = apply { contents[path] = bytes }

    fun file(path: String, text: String): FakeSftpTransport = file(path, text.toByteArray())

    fun directory(path: String): FakeSftpTransport = apply { contents[path] = null }

    /** Takes a path away, the way another consumer moving a file out from under this one does. */
    fun remove(path: String) {
        contents.remove(path)
    }

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
            record(Call(Operation.Realpath, id, path))
            check(!closed) { "session $id was used after it was closed" }
            return if (path == ".") "/home/etl" else path
        }

        override suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing) {
            record(Call(Operation.List, id, dir))
            check(!closed) { "session $id was used after it was closed" }
            val prefix = if (dir.endsWith("/")) dir else "$dir/"
            // Copied under the map's own lock before anything is reported, because the callback is
            // free to change the server underneath a listing and a real server would not notice.
            val entries = synchronized(contents) { contents.entries.map { it.key to it.value?.size } }
            for ((path, size) in entries) {
                if (!path.startsWith(prefix) || path.substringAfter(prefix).contains('/')) continue
                if (onEntry(describe(path, size)) == Listing.STOP) return
            }
        }

        override suspend fun stat(path: String): RemoteFile {
            record(Call(Operation.Stat, id, path))
            check(!closed) { "session $id was used after it was closed" }
            if (!contents.containsKey(path)) throw missing(path, "stat")
            return describe(path, contents[path]?.size)
        }

        override suspend fun readTo(path: String, sink: OutputStream) {
            record(Call(Operation.Read, id, path))
            check(!closed) { "session $id was used after it was closed" }
            val bytes = contents[path] ?: throw missing(path, "read")
            sink.write(bytes)
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

    private fun missing(path: String, operation: String) =
        NoSuchFile(Attempt(ENDPOINT, operation, path), "the server has no such path: $path")

    private companion object {
        private const val ENDPOINT = "fake:22"

        /** Fixed, so a test comparing entries does not have to say anything about time. */
        private val MODIFIED_AT: Instant = Instant.parse("2024-01-01T00:00:00Z")

        private fun describe(path: String, size: Int?) = RemoteFile(
            path = path,
            size = (size ?: 0).toLong(),
            modifiedAt = MODIFIED_AT,
            isDirectory = size == null,
        )
    }
}
