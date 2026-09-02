package sftp.connector.testkit

import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.error.ServerFailure
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import java.io.InputStream
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

    enum class Operation { Connect, Realpath, List, Stat, Read, Write, Rename, Delete, Mkdir, Close }

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

        /**
         * Everything that happens before an operation does its own work: the call goes on the
         * record, the test's hook gets its chance to make the server slow or make it fail, and a
         * session somebody kept past its hang-up is caught here rather than quietly answering.
         */
        private suspend fun asked(operation: Operation, path: String? = null) {
            record(Call(operation, id, path))
            check(!closed) { "session $id was used after it was closed" }
        }

        override suspend fun realpath(path: String): String {
            asked(Operation.Realpath, path)
            return if (path == ".") "/home/etl" else path
        }

        override suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing) {
            asked(Operation.List, dir)
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
            asked(Operation.Stat, path)
            if (!contents.containsKey(path)) throw missing(path, "stat")
            return describe(path, contents[path]?.size)
        }

        override suspend fun readTo(path: String, sink: OutputStream) {
            asked(Operation.Read, path)
            val bytes = contents[path] ?: throw missing(path, "read")
            sink.write(bytes)
        }

        override suspend fun writeFrom(path: String, source: InputStream) {
            asked(Operation.Write, path)
            contents[path] = source.readBytes()
        }

        override suspend fun rename(from: String, to: String) {
            asked(Operation.Rename, from)
            synchronized(contents) {
                if (!contents.containsKey(from)) throw missing(from, "rename")
                if (contents.containsKey(to)) throw occupied(to, "rename")
                contents[to] = contents.remove(from)
            }
        }

        override suspend fun delete(path: String) {
            asked(Operation.Delete, path)
            synchronized(contents) {
                if (!contents.containsKey(path)) throw missing(path, "delete")
                contents.remove(path)
            }
        }

        override suspend fun mkdir(path: String) {
            asked(Operation.Mkdir, path)
            synchronized(contents) {
                if (contents.containsKey(path)) throw occupied(path, "mkdir")
                contents[path] = null
            }
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

    /**
     * What a version 3 server without the POSIX rename extension answers when the path it was
     * asked to take is already occupied, which is the same generic status it answers with for
     * everything else it will not do. This fake is always that server: the atomic replacement an
     * extension would give is the easy case, and the sequence a caller has to fall back on is the
     * one worth being able to stage.
     */
    private fun occupied(path: String, operation: String) = ServerFailure(
        Attempt(ENDPOINT, operation, path),
        statusCode = 4,
        detail = "there is already something at $path",
    )

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
