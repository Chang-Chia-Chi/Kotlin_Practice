package sftp.connector.client

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.error.Retry
import sftp.connector.error.ServerFailure
import sftp.connector.error.SftpException
import sftp.connector.error.UnsafeFileName
import sftp.connector.pool.SftpPool
import sftp.connector.resilience.Resilience
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpSession
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.InvalidPathException
import java.nio.file.Path
import java.time.Clock

/**
 * The file operations, as suspend functions, over a pool that hands out the sessions.
 *
 * Every operation here borrows a session for exactly as long as it needs one and gives it back
 * however it ends. Sessions are fungible and none of these calls cares which one it gets: a
 * listing and a download run on different sessions on purpose, because an SFTP channel does one
 * thing at a time and pinning the lister for the length of a batch of downloads would stop the
 * next listing from ever starting.
 *
 * A session that dies under an operation does not reach the caller as a failure. The operation
 * is tried again on a fresh session, within a budget, and each operation knows what a lost reply
 * means for it - a rename whose first try may already have moved the file, a delete whose file
 * is already gone - so a retry never reports as failed something that in fact succeeded.
 */
class SftpClient(
    private val pool: SftpPool,
    private val config: SftpConnectorConfig,
    /** Whatever the host supplies; a private one when the connector is used on its own. */
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    /** What the breaker's wait in open is measured on. Injected so a test can move it. */
    clock: Clock = Clock.systemUTC(),
) {

    private val endpoint = config.endpoint.address

    private val meters = ClientMeters(meterRegistry, endpoint)

    private val staging = StagingArea(config.polling.staging.digest)

    private val resilience = Resilience(config.resilience, pool, endpoint, meterRegistry, clock)

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
     * Directories are not reported unless asked for. A listing of a directory is about the files
     * in it; a caller that wants to walk into subdirectories asks to see them and does the
     * walking itself, because this operation holds a session for as long as it runs and a walk
     * from inside it would hold one per level.
     *
     * @param maxEntries stop after this many have been handed on, however much of the directory is
     *   left. It bounds the work of one listing, not the size of the directory.
     * @param withDirectories report subdirectories as entries too, [RemoteFile.isDirectory] set.
     * @param filter runs on the session's own thread as each entry arrives, before the entry is
     *   handed on, so a filter that rejects most of a directory saves the consumer from ever
     *   seeing it. Keep it cheap: it holds the read up.
     */
    fun list(
        dir: String,
        maxEntries: Int = Int.MAX_VALUE,
        withDirectories: Boolean = false,
        filter: (RemoteFile) -> Boolean = { true },
    ): Flow<RemoteFile> = channelFlow {
        meters.timing("list") {
            var handedOn = 0
            // A listing that dies before its first entry starts over on a fresh session. One
            // that dies after is not: starting over would hand the same entries on twice, and
            // remembering which ones would cost the memory this flow exists not to spend. The
            // consumer sees the failure and lists again when it is ready to.
            resilience.attempting("list", dir, unhurried = true, stillWorthRetrying = { handedOn == 0 }) { session, _ ->
                session.list(dir) { entry ->
                    when {
                        (entry.isDirectory && !withDirectories) || !filter(entry) -> Listing.CONTINUE
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
            resilience.attempting(operation, path) { session, _ -> session.entryAt(path) }
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
     * directory another system is writing into and moving files out of, it is ordinary. It is
     * the one failure a download is never retried for; every other one restarts the transfer
     * from the first byte into a fresh partial file.
     *
     * @param localTarget where the finished file goes. Null puts it in the configured staging
     *   directory under the name it has on the server, once that name has been checked to be one
     *   (see [stagingTargetFor]); that collides when two watched directories hold a file of the
     *   same name - a caller in that position names its own target, and a caller that names its
     *   own target has taken over deciding what is safe to write.
     * @throws sftp.connector.error.UnsafeFileName when no target was given and the listed name is
     *   not one that can be written under the staging directory, and - whether or not a target was
     *   given - when something this connector did not write is already sitting at the partial
     *   file's name, which is a symbolic link somebody else planted or a directory in the way.
     */
    suspend fun download(remote: RemoteFile, localTarget: Path? = null): LocalFile = meters.timing("download") {
        val target = localTarget ?: stagingTargetFor(remote)
        resilience.attempting("download", remote.path, transfer = true) { session, attempt ->
            staging.receive(target = target, expectedSize = remote.size, attempt = attempt) { sink ->
                session.readTo(remote.path, sink)
            }
        }
    }

    /**
     * The staging directory joined to the name the server listed [remote] under - the one place a
     * name the server chose reaches the local filesystem, so the one place it is checked.
     *
     * The check reads the join backwards. A name of `..` resolves to the directory above; a name
     * with a backslash in it is two directories up on Windows and one odd file on everything else,
     * and a name that means different things on different machines is never one to write; a
     * drive-relative name is rewritten by the filesystem into some other name or onto some other
     * drive; and a name the filesystem cannot spell at all has nowhere to go. Every one of those
     * shows up the same way: after resolving and normalising, the result either left the staging
     * directory or no longer ends in exactly the listed name. Refusing on that, rather than on a
     * list of bad characters, means a shape nobody thought of is still caught.
     */
    private fun stagingTargetFor(remote: RemoteFile): Path {
        val dir = config.polling.staging.dir.normalize()
        val name = remote.name
        val target = try {
            dir.resolve(name).normalize()
        } catch (unspellable: InvalidPathException) {
            null
        }
        if ('\\' !in name && target != null && target.startsWith(dir) && target.fileName?.toString() == name) return target
        throw UnsafeFileName(
            Attempt(endpoint, "download", remote.path),
            detail = "the listed name '$name' cannot be a file name under the staging directory $dir, so nothing was written",
        )
    }

    /**
     * Sends [local] to [remote].
     *
     * The bytes go straight to the path they are meant for, which means a reader of that path sees
     * a partial file for the length of the transfer. That is the arrangement spec-level retries
     * are built on - an upload that broke restarts from zero over the top of what it left - and it
     * is why an uploader whose files are being watched by something else should write under a name
     * nobody is watching and rename it into place afterwards. This client can do both halves of
     * that; it does not do them on its own, because where the temporary name should live is the
     * caller's knowledge and not this method's.
     *
     * @param overwrite [Overwrite.REFUSE] asks first and refuses if anything is at [remote], so a
     *   writer arriving between the question and the write still wins. Nothing on this protocol
     *   closes that race for an upload: there is no way to ask a version 3 server to create a file
     *   only if it is not already there. It asks once: a retry after a lost reply writes over
     *   whatever the earlier try left, which is either nothing, part of this file, or all of it.
     */
    suspend fun upload(local: Path, remote: String, overwrite: Overwrite = Overwrite.REFUSE): Unit =
        meters.timing("upload") {
            // Decided on the first try that gets to ask. Asking again on a retry would find the
            // earlier try's own file there and refuse the upload for it.
            var targetFoundFree = overwrite == Overwrite.REPLACE
            resilience.attempting("upload", remote, transfer = true) { session, attempt ->
                if (!targetFoundFree) {
                    if (session.entryAt(remote) != null) refuse(attempt, "upload", remote)
                    targetFoundFree = true
                }
                Files.newInputStream(local).use { session.writeFrom(remote, it) }
            }
        }

    /**
     * Moves [from] to [to] on the server.
     *
     * @param overwrite what to do about a file already at [to]. See [Overwrite]: replacing one is
     *   a single atomic request against a server offering the POSIX rename extension, and a delete
     *   followed by a second rename against a server without it - during which [to] holds nothing,
     *   and after which a failure has left [to] empty rather than holding either file.
     * @param listed the file at [from] as the caller listed it. Its size and modification time
     *   are what let a retry after a lost reply tell its own landed file at [to] from somebody
     *   else's (D46); a caller without it costs one extra round trip to find out.
     * @throws sftp.connector.error.NoSuchFile naming, on its attempt's path, the path that is not
     *   there: [from], or [to] when the source is still there and it is the target's location the
     *   server could not find. A missing source after a lost reply is looked into before it is
     *   reported - see [RenameTries] - so it means the file is at neither place.
     * @throws sftp.connector.error.Recoverable from the wire when the last permitted try lost its
     *   reply and one look afterwards found the file not at [to]; when that look itself fails,
     *   the lost reply is reported with the look's failure suppressed under it, and whether the
     *   file moved is then unknown.
     */
    suspend fun rename(from: String, to: String, overwrite: Overwrite = Overwrite.REFUSE, listed: RemoteFile? = null): Unit =
        meters.timing("rename") {
            require(listed == null || listed.path == from) { "the listed file is ${listed?.path}, not the source $from" }
            val tries = RenameTries(from, to, overwrite, listed)
            try {
                resilience.attempting("rename", from) { session, attempt -> tries.attempt(session, attempt) }
            } catch (replyLost: SftpException) {
                // A reply lost on the last permitted try: the retry that would have looked for
                // the landed file is not coming, so the look is made once, here, on a fresh
                // session behind the breaker. Anything the look cannot settle is reported as
                // the lost reply it was.
                if (replyLost.disposition.retry != Retry.IMMEDIATELY) throw replyLost
                val landed = try {
                    resilience.once("rename") { session -> tries.landedAfterAll(session) }
                } catch (lookFailed: SftpException) {
                    replyLost.addSuppressed(lookFailed)
                    throw replyLost
                }
                if (!landed) throw replyLost
            }
        }

    /**
     * Removes [remote]. A path that is not there is reported, because only the caller knows
     * whether that is a failure - unless an earlier try of this same delete had reached the
     * server and its reply was lost, in which case a path that is not there is the delete having
     * worked.
     */
    suspend fun delete(remote: String): Unit = meters.timing("delete") {
        var reachedTheServer = false
        resilience.attempting("delete", remote) { session, _ ->
            val anEarlierTryMayHaveLanded = reachedTheServer
            reachedTheServer = true
            try {
                session.delete(remote)
            } catch (gone: NoSuchFile) {
                if (!anEarlierTryMayHaveLanded) throw gone
                LOG.info("{} is gone, so the delete whose reply was lost had landed; reported as success.", remote)
            }
        }
    }

    /**
     * Makes sure there is a directory at [remote].
     *
     * About the directory existing afterwards rather than about who created it: a directory that
     * was already there is the outcome asked for, not a collision, which is what lets a startup
     * that creates the folders it needs run twice without special-casing the second time.
     *
     * @param parents create the missing directories above [remote] as well. Off by default,
     *   because filling in a path nobody asked for turns a typo into a directory tree.
     */
    suspend fun mkdir(remote: String, parents: Boolean = false): Unit = meters.timing("mkdir") {
        resilience.attempting("mkdir", remote) { session, _ -> session.makeDirectory(remote, parents) }
    }

    /**
     * Runs [block] with one session held for the whole of it, and gives that session back however
     * [block] ends.
     *
     * For work where the operations belong together: a sequence that would be wrong if another
     * caller's request landed in the middle of it, or anything that depends on state the channel
     * carries, such as its working directory. Everything else should use the operations above,
     * which take a session for one call and hand it straight back - a session held across several
     * round trips is one the rest of the connector cannot use, and there are only ever a handful.
     *
     * The block is handed the session's operations and not the session's life. It cannot close it,
     * because the pool lends the same session out again afterwards and a caller that hung up on it
     * would break the next caller's work rather than its own; and the session it was handed stops
     * working the moment [block] returns, so a reference kept past the block fails loudly here
     * instead of quietly using a session that now belongs to somebody else. A call the block
     * launched and did not wait for is let finish first: the session is not given back while
     * anything is still using it, whatever the block did.
     *
     * Nothing here retries. The connector cannot know which part of a caller's sequence is safe to
     * send twice, so a caller that wants a second go says so itself. The breaker still stands in
     * front of it: an open breaker means nothing is sent, whoever is asking.
     */
    suspend fun <T> withSession(block: suspend SftpSession.() -> T): T = meters.timing("session") {
        resilience.once("session") { session ->
            val borrowed = BorrowedSession(session)
            try {
                borrowed.block()
            } finally {
                // Uncancellable because ending the loan may have to wait for a call still in
                // flight, and a cancelled block is the likeliest to have left one behind.
                withContext(NonCancellable) { borrowed.handItBack() }
            }
        }
    }
}

/**
 * Makes sure there is a directory at [path], creating the ones above it too when [parents] says so.
 *
 * It is out here rather than inside [SftpClient.mkdir] because the same work has to be doable on a
 * session somebody else is holding: a sequence that creates the folders it needs and then uses them
 * would otherwise have to give the session back in the middle of itself.
 */
internal suspend fun SftpSession.makeDirectory(path: String, parents: Boolean) {
    if (parents) ancestorsOf(path).forEach { ensureDirectory(it) }
    ensureDirectory(path)
}

/**
 * Creates [path] unless a directory is there already.
 *
 * The server has one status for "there is something there" and for everything else it refuses, so
 * telling those apart means looking. A directory found where one was wanted is the outcome; a file
 * found there, or nothing found there at all, means the refusal was about something else and is
 * passed on.
 */
private suspend fun SftpSession.ensureDirectory(path: String) {
    try {
        mkdir(path)
    } catch (refused: ServerFailure) {
        if (entryAt(path)?.isDirectory != true) throw refused
    }
}

/**
 * The directories [path] sits under, shallowest first. The root is not one of them: it is there on
 * every server there has ever been, and asking to create it would fail on all of them.
 */
private fun ancestorsOf(path: String): List<String> =
    path.trimEnd('/')
        .split('/')
        .runningReduce { above, name -> "$above/$name" }
        .dropLast(1)
        .filter { it.isNotEmpty() }

private val LOG = LoggerFactory.getLogger(SftpClient::class.java)

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
