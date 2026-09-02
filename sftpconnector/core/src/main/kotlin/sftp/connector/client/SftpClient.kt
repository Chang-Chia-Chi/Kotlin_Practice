package sftp.connector.client

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.pool.SftpPool
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import java.nio.file.Path

/**
 * The file operations, as suspend functions, over a pool that hands out the sessions.
 *
 * Every operation here borrows a session for exactly as long as it needs one and gives it back
 * however it ends. Sessions are fungible and none of these calls cares which one it gets: a
 * listing and a download run on different sessions on purpose, because an SFTP channel does one
 * thing at a time and pinning the lister for the length of a batch of downloads would stop the
 * next listing from ever starting.
 */
class SftpClient(
    private val pool: SftpPool,
    private val config: SftpConnectorConfig,
    /** Whatever the host supplies; a private one when the connector is used on its own. */
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {

    private val endpoint = config.endpoint.address

    private val meters = ClientMeters(meterRegistry, endpoint)

    private val staging = StagingArea(config.polling.staging.digest)

    /**
     * The entries of [dir], as a cold flow: collecting it starts the listing, and abandoning the
     * collection stops it where it is.
     *
     * Nothing here holds the directory. The server sends entries in batches, the flow hands them
     * on one at a time, and a consumer that is busy stops the server sending more - so a directory
     * of a hundred thousand files costs the same memory as one of ten, and a consumer that wanted
     * the first thousand pays for the first thousand. That is the whole reason this is a flow and
     * not a list.
     *
     * Directories are not reported. A listing of a directory is about the files in it, and a
     * caller wanting to walk into subdirectories is doing something this operation does not
     * pretend to do.
     *
     * @param maxEntries stop after this many have been handed on, however much of the directory is
     *   left. It bounds the work of one listing, not the size of the directory.
     * @param filter runs on the session's own thread as each entry arrives, before the entry is
     *   handed on, so a filter that rejects most of a directory saves the consumer from ever
     *   seeing it. Keep it cheap: it holds the read up.
     */
    fun list(
        dir: String,
        maxEntries: Int = Int.MAX_VALUE,
        filter: (RemoteFile) -> Boolean = { true },
    ): Flow<RemoteFile> = channelFlow {
        meters.timing("list") {
            pool.withLease { lease ->
                var handedOn = 0
                lease.connection.list(dir) { entry ->
                    when {
                        entry.isDirectory || !filter(entry) -> Listing.CONTINUE
                        !handOn(entry) -> Listing.STOP
                        ++handedOn >= maxEntries -> Listing.STOP
                        else -> Listing.CONTINUE
                    }
                }
            }
        }
    }

    /**
     * What the server currently says about [path], or null if there is nothing there.
     *
     * A path that is not there is an answer to this question rather than a failure of it, which is
     * why it comes back as null. That is only true of asking: an operation that needed the file to
     * be there still fails when it is not.
     */
    suspend fun stat(path: String): RemoteFile? = statOrNull("stat", path)

    /** Whether there is anything at [path] at all. */
    suspend fun exists(path: String): Boolean = statOrNull("exists", path) != null

    private suspend fun statOrNull(operation: String, path: String): RemoteFile? =
        meters.timing(operation) {
            pool.withLease { lease ->
                try {
                    lease.connection.stat(path)
                } catch (absent: NoSuchFile) {
                    null
                }
            }
        }

    /**
     * Fetches [remote] onto local disk and reports what landed there.
     *
     * The bytes go to a partial file first, are counted and digested on the way, are checked
     * against the size the server said the file had, and only then does the file take the name a
     * caller may act on - so nothing ever sees a name that holds half a file, and however this
     * ends the partial file is not left behind.
     *
     * A file that has gone from the server since it was listed fails with
     * [sftp.connector.error.NoSuchFile], which is the connector saying "not this file" rather than
     * "not right now". Anything downloading files it listed a moment ago has to expect it: on a
     * directory another system is writing into and moving files out of, it is ordinary.
     *
     * @param localTarget where the finished file goes. The default puts it in the configured
     *   staging directory under the name it has on the server, which collides when two watched
     *   directories hold a file of the same name - a caller in that position names its own target.
     */
    suspend fun download(
        remote: RemoteFile,
        localTarget: Path = config.polling.staging.dir.resolve(remote.name),
    ): LocalFile = meters.timing("download") {
        pool.withLease { lease ->
            staging.receive(
                target = localTarget,
                expectedSize = remote.size,
                attempt = Attempt(endpoint, "download", remote.path),
            ) { sink -> lease.connection.readTo(remote.path, sink) }
        }
    }
}

/**
 * Hands one entry to the consumer, answering whether there is any point in the next.
 *
 * The wait is blocking because the listing callback belongs to the SSH library and cannot suspend,
 * and blocking is what that thread would be doing anyway - it is the thread reading the socket, and
 * holding it here is precisely how the server is stopped from sending a batch that has nowhere to
 * go. That thread is one of the connector's bounded few, and the reason waiting on it cannot starve
 * the rest of the connector is an accounting one: the pool has exactly as many places as there are
 * threads, and everything that reaches for a thread is already holding a place - a listing, a
 * download, a session being opened, a session being hung up on. So the number of threads wanted can
 * never exceed the number there are, and nothing that got as far as wanting one waits for it.
 * A later operation that runs on that dispatcher without first holding a place would take this
 * away, and would be a deadlock rather than a slow path.
 *
 * A consumer that has stopped collecting - because it had seen enough, or because the collection
 * was cancelled - leaves nowhere to put an entry, and that is an answer rather than a fault: the
 * listing stops where it is, the server closes the handle, and the session goes back to the pool
 * healthy. That is what a cancelled listing is supposed to do.
 */
private fun SendChannel<RemoteFile>.handOn(entry: RemoteFile): Boolean = trySendBlocking(entry).isSuccess
