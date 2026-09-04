package sftp.connector

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import sftp.connector.client.SftpClient
import sftp.connector.client.makeDirectory
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.ConfigurationError
import sftp.connector.error.Disposition
import sftp.connector.error.NoSuchFile
import sftp.connector.error.SftpException
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpSession
import java.io.ByteArrayInputStream
import java.util.UUID

/**
 * The prefix every start-up marker carries. It is what the lister skips ([sftp.connector.source]),
 * so a marker any instance of this connector left behind - a start-up killed mid-check, or one
 * whose tidy-up met a dead session - is inert to the poll rather than handed over as a file. It is
 * a class of names, not one name: the random suffix per marker is what keeps two instances from
 * moving each other's, and the prefix is what makes all of them recognisable.
 */
internal const val PROBE_MARKER_PREFIX = ".sftpconnector-probe-"

/**
 * Asks the server, before the connector runs, whether the configuration it was given describes
 * anything the server can actually do.
 *
 * All of its value is in the difference between two moments. A watched directory that is not
 * there, a folder the account may not create, an action target on another filesystem: every one
 * of those is discovered anyway - at the first file, which on an hourly pipeline is an hour after
 * anyone was looking, in a log line that says a file failed rather than that the deployment is
 * wrong. Running the same operations now turns each of them into a start-up that refuses with the
 * remedy in the message.
 *
 * So the message is the deliverable. A probe that reports "start-up failed" has done the work and
 * thrown away the reason for doing it, and every check here names the path it was looking at, what
 * it was trying, and what an operator should change. The one that earns the whole class is the
 * move: a rename that has to cross a filesystem boundary is refused with the same featureless
 * status the server uses for everything else it will not do, so nothing but trying it tells the
 * difference between a target that works and one that will fail on every single ack.
 *
 * The whole of it runs on one borrowed session. There is nothing else for the connector to be
 * doing yet, and a sequence that gave the session back between its steps would be asking the pool
 * for another one eight times over to no purpose.
 */
internal class StartupProbe(
    private val client: SftpClient,
    private val config: SftpConnectorConfig,
) {

    /**
     * Named so that one found lying about is recognisable as this connector's and as a leftover:
     * it is deleted on the way out of every path, and the lister skips its whole prefix so that one
     * left by a dead session is never handed over. The random part is because two instances of the
     * same connector may start at once and must not each move the other's marker.
     */
    private val marker = "$PROBE_MARKER_PREFIX${config.name}-${UUID.randomUUID()}"

    suspend fun run() {
        // A connector that watches nothing has nothing to check here, and should not open a
        // session to find that out.
        if (config.polling.directories.isEmpty()) return
        client.withSession { checkEverything() }
    }

    private suspend fun SftpSession.checkEverything() {
        val polling = config.polling
        polling.directories.forEach { directory ->
            val resolved = resolve(directory)
            polling.actionTargetsUnder(resolved).forEach { target ->
                prepare(directory, resolved, target)
                if (polling.startupProbe) proveTheMoveInto(resolved, target)
            }
        }
    }

    /**
     * Turns a configured directory into the path the server knows it by, and insists there is a
     * directory there.
     *
     * The second half is not redundant, which was measured against this connector's own embedded
     * server: resolving a path is arithmetic on names, and the server canonicalises one that leads
     * nowhere just as happily as one that leads somewhere. So a typo in a watched directory
     * survives being resolved - and a typo in a watched directory is the most ordinary
     * configuration fault there is.
     */
    private suspend fun SftpSession.resolve(directory: String): String {
        val resolved = checking(
            trying = "resolving the watched directory $directory",
            remedy = "The path is sent to the server as it is written here, so a leading slash makes it " +
                "absolute and its absence makes it relative to the account's login directory.",
        ) { realpath(directory) }

        insistOnADirectory(
            path = resolved,
            what = "watched directory",
            whenMissing = "There is nothing at $resolved. Create it, or correct the path.",
        )
        return resolved
    }

    /**
     * Makes sure there is a folder for the actions to move files into, and that it is not the
     * directory those files came out of.
     *
     * The self-move check is here rather than only in the configuration because only here is the
     * watched directory's real path known. A relative target and an absolute one naming the same
     * folder look nothing alike until the server has resolved the first, and an action that files
     * a message back where it came from would hand the same file to every poll for as long as the
     * connector ran - while succeeding at every step, which is why nothing later would catch it.
     *
     * Creating the folder and checking it are one step on purpose: creating a directory that is
     * already there is the outcome asked for rather than a collision, so the check that follows is
     * the same check whether this connector made the folder or somebody made it upstream a year
     * ago.
     */
    private suspend fun SftpSession.prepare(directory: String, resolved: String, target: String) {
        if (target == resolved) {
            throw refuse(
                trying = "checking where files taken from $directory are moved to",
                remedy = "They are moved to $target, which is that same directory, so acting on a file " +
                    "would move it onto itself and every later poll would find it again.",
            )
        }
        if (config.polling.createActionTargets) {
            checking(
                trying = "creating the folder $target that files are moved into",
                remedy = "Set createActionTargets = false and have it created upstream if this account " +
                    "may not create directories.",
            ) { makeDirectory(target, parents = true) }
        }
        insistOnADirectory(
            path = target,
            what = "folder files are moved into",
            whenMissing = "There is nothing at $target and createActionTargets is off, so nothing will create it.",
        )
    }

    /**
     * Moves an empty file from the watched directory into the action target and back out again,
     * which is the only way to find out whether the move an ack performs can be performed at all.
     *
     * A rename cannot cross a filesystem boundary. Neither the configuration nor a listing nor a
     * stat shows where a boundary is, and the server reports the refusal with the one status it
     * uses for everything it will not do, so there is nothing to read and nothing to infer - only
     * a rename that was tried and failed. Trying it here costs two round trips once per process.
     * Not trying it costs every ack, from the first file until somebody notices.
     */
    private suspend fun SftpSession.proveTheMoveInto(directory: String, target: String) {
        val home = "$directory/$marker"
        val parked = "$target/$marker"
        try {
            checking(
                trying = "writing an empty file into the watched directory $directory to move it with",
                remedy = "The account needs to be able to create a file there. Set startupProbe = false " +
                    "if writing to that directory is unwelcome, and accept finding out at the first ack.",
            ) { writeFrom(home, ByteArrayInputStream(ByteArray(0))) }

            checking(
                trying = "moving that file from $directory into $target",
                remedy = "A rename cannot cross a filesystem boundary, and a server refuses one it cannot " +
                    "make with the same featureless status it refuses everything else with - so an action " +
                    "target on another disk, mount or share looks exactly like this and would fail on every " +
                    "single file. Put $target on the same filesystem as $directory.",
            ) { rename(home, parked) }

            checking(
                trying = "moving that file back out of $target",
                remedy = "The account needs to be able to move a file out of the folder as well as into " +
                    "it, or the probe cannot leave the server as it found it.",
            ) { rename(parked, home) }
        } finally {
            tidyAway(parked)
            tidyAway(home)
        }
    }

    /**
     * Insists there is a directory at [path], and says which of the two ways there is not one.
     *
     * They are two different remedies. Nothing there at all is a path to correct or a folder to
     * create; a file where a directory was wanted is a name collision, and nobody would guess that
     * from being told only that the path was no good.
     */
    private suspend fun SftpSession.insistOnADirectory(path: String, what: String, whenMissing: String) {
        val trying = "looking at the $what $path"
        val entry = checking(trying, "The account has to be able to see it.") { entryAt(path) }
        when {
            entry == null -> throw refuse(trying, whenMissing)
            !entry.isDirectory -> throw refuse(trying, "$path is a file, and a $what has to be a directory.")
        }
    }

    private suspend fun SftpSession.entryAt(path: String): RemoteFile? =
        try {
            stat(path)
        } catch (absent: NoSuchFile) {
            null
        }

    /**
     * Takes the marker away wherever it ended up. Nothing here is allowed to throw: on the failing
     * path a failure raised while clearing up would replace the failure worth reporting with one
     * about a file nobody asked about.
     *
     * Under `NonCancellable`, because the likeliest reason the probe is unwinding is that it was
     * cancelled, and a cancelled `delete` never reaches the server - `withContext(io)` refuses it -
     * so the marker would be left on a session that is still perfectly alive. A wire failure leaves
     * it anyway (the session is dead), which is what the lister's prefix skip is for; a cancellation
     * is the case this closes.
     */
    private suspend fun SftpSession.tidyAway(path: String) {
        withContext(NonCancellable) {
            try {
                delete(path)
            } catch (failure: SftpException) {
                LOG.debug("The probe file {} did not need clearing away: {}", path, failure.message)
            }
        }
    }

    /**
     * Runs one check, and turns whatever the server *says* about it into a refusal to start that
     * names the check, the path and what to do about it.
     *
     * Only an answer is evidence about the configuration. A path that is not there, an account
     * that may not, a rename the server refuses: those arrived, were understood, and were
     * answered no, and the remedy this check carries is what to change about them. A connection
     * that broke, a request that ran out of time, a pool with nothing to lend - none of those
     * reached the server or came back from it, and they say nothing about what was asked. Dressed
     * up as a configuration fault they would send an operator to respell a path that is spelled
     * correctly, on a start-up that would have worked a minute later; so they go up as themselves,
     * and the start-up still refuses, carrying the truth about why.
     *
     * That line is exactly the one spec 10.2 already draws, and it is read off the failure rather
     * than off a list of classes here: "the server answered" is what [Disposition.RETRY_ON_THE_NEXT_TICK]
     * means.
     */
    private suspend fun <T> checking(trying: String, remedy: String, check: suspend () -> T): T =
        try {
            check()
        } catch (failure: SftpException) {
            if (failure.disposition != Disposition.RETRY_ON_THE_NEXT_TICK) throw failure
            throw refuse(trying, remedy, failure)
        }

    private fun refuse(trying: String, remedy: String, cause: SftpException? = null) = ConfigurationError(
        buildString {
            append("connector \"").append(config.name).append("\" cannot start: ").append(trying)
            append(" failed. ").append(remedy)
            if (cause != null) append(" The server's answer was: ").append(cause.message)
        },
        cause,
    )

    private companion object {
        private val LOG = LoggerFactory.getLogger(StartupProbe::class.java)
    }
}
