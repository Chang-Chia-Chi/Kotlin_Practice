package sftp.connector

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.pool.SftpPool
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpConnection
import sftp.connector.transport.SftpTransport
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import java.time.Instant
import java.util.Collections
import kotlin.coroutines.coroutineContext

/**
 * The marker the start-up probe writes has to be gone whichever way the probe ends. A wire failure
 * leaves a dead session that cannot delete it, and the lister's prefix skip is the defence there; a
 * *cancellation* leaves a session that is perfectly alive, so the tidy-up has to run under
 * `NonCancellable` or the marker is left on a healthy server for the next start's lister to find.
 *
 * The fake here observes cancellation the way the real transport does - every call checks the
 * coroutine first, as `withContext(io)` does - so that a tidy-up not wrapped in `NonCancellable`
 * would throw before it deleted anything, which is exactly the bug.
 */
class StartupProbeMarkerTest {

    @TempDir
    lateinit var stage: Path

    @Test
    fun `a probe cancelled after the marker is written leaves no marker`() = runBlocking {
        val markerWritten = CompletableDeferred<Unit>()
        val transport = CancellationAwareServer(onMarkerWritten = { markerWritten.complete(Unit) })
        val config = configWithProbe()
        val client = SftpClient(SftpPool(transport, config), config)
        val probe = StartupProbe(client, config)

        val running = launch { probe.run() }
        // Cancel the moment the marker is on the server, while the write call is parked - the very
        // window in which the probe still holds a live session and could still take the marker away.
        markerWritten.await()
        running.cancel()
        running.join()

        assertTrue(
            transport.paths.none { it.substringAfterLast('/').startsWith(PROBE_MARKER_PREFIX) },
            "a marker left behind by a cancelled probe: ${transport.paths}",
        )
    }

    private fun configWithProbe(): SftpConnectorConfig = sftpConnector("marker-test") {
        endpoint { host = "fake.example"; port = 22 }
        auth { password("etl", "s3cret") }
        hostKey = HostKeyPolicy.AcceptAll
        polling {
            staging { dir = stage }
            directories("/drop")
            onAck = move("/drop/done")
        }
    }

    /**
     * A server that keeps a set of paths and, like the real transport, refuses every call once the
     * coroutine driving it has been cancelled. [onMarkerWritten] fires - and the write then parks -
     * so a test can land a cancellation with the marker on the server and the session still alive.
     */
    private class CancellationAwareServer(private val onMarkerWritten: () -> Unit) : SftpTransport {

        val paths: MutableSet<String> = Collections.synchronizedSet(mutableSetOf("/drop", "/drop/done"))

        override suspend fun connect(): SftpConnection = Connection()

        private inner class Connection : SftpConnection {

            override suspend fun realpath(path: String): String {
                coroutineContext.ensureActive()
                return path
            }

            override suspend fun stat(path: String): RemoteFile {
                coroutineContext.ensureActive()
                if (path !in paths) throw NoSuchFile(Attempt(ENDPOINT, "stat", path), "no such path")
                // Only directories are ever statted by the probe; a marker file is written and
                // deleted but never looked at.
                return RemoteFile(path, size = 0, modifiedAt = Instant.EPOCH, isDirectory = true)
            }

            override suspend fun writeFrom(path: String, source: InputStream) {
                coroutineContext.ensureActive()
                paths += path
                onMarkerWritten()
                // Park after announcing the write, so the cancellation lands here rather than after
                // the probe has moved on.
                CompletableDeferred<Unit>().await()
            }

            override suspend fun rename(from: String, to: String) {
                coroutineContext.ensureActive()
                if (!paths.remove(from)) throw NoSuchFile(Attempt(ENDPOINT, "rename", from), "no such path")
                paths += to
            }

            override suspend fun delete(path: String) {
                coroutineContext.ensureActive()
                paths.remove(path)
            }

            override suspend fun mkdir(path: String) {
                coroutineContext.ensureActive()
                paths += path
            }

            override val renameReplaces: Boolean = false

            override suspend fun list(dir: String, onEntry: (RemoteFile) -> Listing) = Unit

            override suspend fun readTo(path: String, sink: OutputStream) = Unit

            override suspend fun close() = Unit

            override fun abort() = Unit
        }
    }

    private companion object {
        private const val ENDPOINT = "fake.example:22"
    }
}
