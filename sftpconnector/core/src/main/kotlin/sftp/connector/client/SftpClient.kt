package sftp.connector.client

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import org.slf4j.LoggerFactory
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.error.OverwriteRefused
import sftp.connector.error.ServerFailure
import sftp.connector.error.UnsafeFileName
import sftp.connector.pool.SftpPool
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpSession
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.InvalidPathException
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
            pool.withLease { lease ->
                var handedOn = 0
                lease.connection.list(dir) { entry ->
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
     * @param localTarget where the finished file goes. Null puts it in the configured staging
     *   directory under the name it has on the server, once that name has been checked to be one
     *   (see [stagingTargetFor]); that collides when two watched directories hold a file of the
     *   same name - a caller in that position names its own target, and a caller that names its
     *   own target has taken over deciding what is safe to write.
     * @throws sftp.connector.error.UnsafeFileName when no target was given and the listed name is
     *   not one that can be written under the staging directory.
     */
    suspend fun download(remote: RemoteFile, localTarget: Path? = null): LocalFile = meters.timing("download") {
        val target = localTarget ?: stagingTargetFor(remote)
        pool.withLease { lease ->
            staging.receive(
                target = target,
                expectedSize = remote.size,
                attempt = Attempt(endpoint, "download", remote.path),
            ) { sink -> lease.connection.readTo(remote.path, sink) }
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
     *   only if it is not already there.
     */
    suspend fun upload(local: Path, remote: String, overwrite: Overwrite = Overwrite.REFUSE): Unit =
        meters.timing("upload") {
            pool.withLease { lease ->
                val session = lease.connection
                if (overwrite == Overwrite.REFUSE && session.entryAt(remote) != null) refuse("upload", remote)
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
     * @throws sftp.connector.error.NoSuchFile when [from] is not there. It is the answer worth
     *   telling apart from the rest: a retry that gets it after a reply went missing is being told
     *   its own earlier attempt may already have landed, and can settle the question by looking at
     *   [to]. No other path's absence is ever reported this way - a target that was already gone
     *   is what a replacement wanted in the first place, and is not passed on as a failure.
     */
    suspend fun rename(from: String, to: String, overwrite: Overwrite = Overwrite.REFUSE): Unit =
        meters.timing("rename") {
            pool.withLease { lease -> lease.connection.moveOnto(from, to, overwrite) }
        }

    /** Removes [remote]. A path that is not there is reported, because only the caller knows whether that is a failure. */
    suspend fun delete(remote: String): Unit = meters.timing("delete") {
        pool.withLease { lease -> lease.connection.delete(remote) }
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
        pool.withLease { lease -> lease.connection.makeDirectory(remote, parents) }
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
     * instead of quietly using a session that now belongs to somebody else.
     *
     * Nothing here retries. The connector cannot know which part of a caller's sequence is safe to
     * send twice, so a caller that wants a second go says so itself.
     */
    suspend fun <T> withSession(block: suspend SftpSession.() -> T): T = meters.timing("session") {
        pool.withLease { lease ->
            val borrowed = BorrowedSession(lease.connection)
            try {
                borrowed.block()
            } finally {
                borrowed.handItBack()
            }
        }
    }

    /**
     * Carries out a rename under an overwrite policy, which on this protocol is a sequence rather
     * than a request, and a different sequence depending on the policy.
     *
     * **Refusing is the connector's own doing, not the server's.** A server offering the POSIX
     * rename extension replaces the target without being asked to and reports success, so a rename
     * sent to such a server is a replacement whatever the caller wanted - which was measured
     * against this connector's own SSH library and embedded server, not assumed. So a refusal has
     * to be decided before the request goes out. That is a look and then a rename, and a writer
     * arriving between the two still wins; on a server without the extension the request itself is
     * refused as well, which closes the race there but nowhere else.
     *
     * **Replacing is a sequence with a gap.** Against a server with the extension the first rename
     * is the whole story. Without it the server refuses - with the one generic status it uses for
     * everything it will not do, so the refusal alone does not say the target is what is in the
     * way. Looking before clearing keeps a replacement from deleting and retrying its way through a
     * refusal that was never about the target, and gets the caller the refusal the server actually
     * gave rather than the one a pointless second attempt would have produced. What no care closes
     * is the gap in the middle: between the delete and the second rename the target path holds
     * nothing, so anything watching it can find it empty, and a failure in the gap leaves it empty
     * with the source still where it started. A caller that cannot afford that needs a server with
     * the extension, which the startup probe is the place to find out about.
     */
    private suspend fun SftpSession.moveOnto(from: String, to: String, overwrite: Overwrite) {
        if (overwrite == Overwrite.REFUSE) {
            if (entryAt(to) != null) refuse("rename", to)
            rename(from, to)
            return
        }
        try {
            rename(from, to)
        } catch (refused: ServerFailure) {
            if (entryAt(to) == null) throw refused
            clearTheWay(to)
            rename(from, to)
        }
    }

    /**
     * Says no on the caller's own instruction, before anything is sent.
     *
     * It has a failure class of its own rather than borrowing the server's generic refusal,
     * because the two want opposite treatment. A server's refusal is worth another go and worth
     * counting against the server; this one is neither - the file in the way will still be in the
     * way on the next attempt, and the server did nothing wrong.
     */
    private fun refuse(operation: String, path: String): Nothing = throw OverwriteRefused(
        Attempt(endpoint, operation, path),
        detail = "there is already something at $path and this $operation was told not to replace it",
    )
}

/**
 * Takes whatever is at [path] away so that a rename can land there.
 *
 * A path that has gone in the meantime is the state this was trying to reach, so it is not passed
 * on. That also keeps the one failure a rename retry reads as a signal unambiguous: a missing path
 * reported by a rename is always the source, never the target.
 */
private suspend fun SftpSession.clearTheWay(path: String) {
    try {
        delete(path)
    } catch (alreadyGone: NoSuchFile) {
        LOG.debug("{} was already gone when it was cleared for a rename: {}", path, alreadyGone.message)
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

/** What the server says is at [path], or null if there is nothing there. */
private suspend fun SftpSession.entryAt(path: String): RemoteFile? =
    try {
        stat(path)
    } catch (absent: NoSuchFile) {
        null
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
