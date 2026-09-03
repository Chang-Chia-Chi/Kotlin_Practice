package sftp.connector.client

import org.slf4j.LoggerFactory
import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.error.OverwriteRefused
import sftp.connector.error.ServerFailure
import sftp.connector.transport.RemoteFile
import sftp.connector.transport.SftpSession

/**
 * A rename across its tries: what a later try knows that the first one did not.
 *
 * A reply that went missing carries no information. The rename may have landed or not, and the
 * next try is sent to a fresh session that has never heard of the first. Three things let it
 * tell the two apart, and they live here rather than in the retry loop because they are facts
 * about this rename and not about retrying:
 *
 *  - **whether a try before this one reached the server.** Only then is the target worth
 *    looking at before anything is sent, and only then can a source that is gone mean "my
 *    earlier try moved it"; before any request has gone out it means what it says.
 *  - **the size the file had before the first request.** A file at the target with that size is
 *    the one that was moved there; a file with another size is somebody else's, and a missing
 *    source is then the truth (I11). The size comes from the caller when it listed the file,
 *    and is measured once here when the caller has none. A later try looks for that file
 *    *before* it sends its rename: a rename sent regardless would, on a server that replaces
 *    without being asked, move whatever the uploader had since put at the source over the file
 *    that had already landed.
 *  - **that the target was looked at and found free.** Under a refusing policy the look runs
 *    once, before the first request. Looking again on a later try would find the first try's own
 *    landed file there and refuse the rename for it - a phantom failure with the disposition
 *    that says never retry. So the policy is decided once, and every later try sends the rename
 *    as a plain request against a target already known to be the connector's to take.
 */
internal class RenameTries(
    private val from: String,
    private val to: String,
    private val overwrite: Overwrite,
    private var expectedSize: Long?,
) {
    private var targetFoundFree = false
    private var reachedTheServer = false

    suspend fun attempt(session: SftpSession, attempt: Attempt) {
        val size = expectedSize ?: session.stat(from).size.also { expectedSize = it }
        if (overwrite == Overwrite.REFUSE && !targetFoundFree) {
            if (session.entryAt(to) != null) refuse(attempt, "rename", to)
            targetFoundFree = true
        }
        val anEarlierTryMayHaveLanded = reachedTheServer
        if (anEarlierTryMayHaveLanded && session.holdsTheMovedFile(size)) return
        reachedTheServer = true
        try {
            if (overwrite == Overwrite.REPLACE) session.moveOnto(from, to) else session.renameNamingWhatIsMissing(from, to)
        } catch (missing: NoSuchFile) {
            // The source vanished between the look and the request: only the earlier try's own
            // rename landing in between could make that a success, so the target is looked at again.
            if (!anEarlierTryMayHaveLanded || missing.attempt.path != from || !session.holdsTheMovedFile(size)) throw missing
        }
    }

    /** Whether [to] holds a file of the size the source had, which is the earlier try's rename having landed. */
    private suspend fun SftpSession.holdsTheMovedFile(size: Long): Boolean {
        val atTarget = entryAt(to) ?: return false
        if (atTarget.size != size) return false
        LOG.info("{} is at {} with the expected {} bytes, so the rename whose reply was lost had landed; reported as success.", from, to, size)
        return true
    }
}

/**
 * Carries out a replacing rename, which on this protocol is a sequence rather than a request.
 *
 * Against a server with the POSIX rename extension the first rename is the whole story: it
 * replaces without being asked, so a refusal from it was never about the target being in the
 * way, and is passed on as given. Without the extension the server refuses an occupied target -
 * with the one generic status it uses for everything it will not do, so the refusal alone does
 * not say the target is what is in the way. Looking before clearing keeps a replacement from
 * deleting and retrying its way through a refusal that was never about the target, and gets the
 * caller the refusal the server actually gave rather than the one a pointless second attempt
 * would have produced. Only a file is ever cleared: a directory at the target is not what
 * replacing a file means. What no care closes is the gap in the middle: between the delete and
 * the second rename the target path holds nothing, so anything watching it can find it empty,
 * and a failure in the gap leaves it empty with the source still where it started. A caller that
 * cannot afford that needs a server with the extension, which the startup probe is the place to
 * find out about.
 */
internal suspend fun SftpSession.moveOnto(from: String, to: String) {
    try {
        renameNamingWhatIsMissing(from, to)
    } catch (refused: ServerFailure) {
        val inTheWay = entryAt(to)
        if (renameReplaces || inTheWay == null || inTheWay.isDirectory) throw refused
        clearTheWay(to)
        try {
            renameNamingWhatIsMissing(from, to)
        } catch (refusedAgain: ServerFailure) {
            // The refusal was never about the target, and the target is gone. The caller
            // reads a refusal as "the source is still where it was", which is true, and must
            // not read it as "nothing changed", which is not.
            throw ServerFailure(
                refusedAgain.attempt,
                refusedAgain.statusCode,
                "$to was cleared to make room for this rename and the rename was refused anyway, so " +
                    "$to now holds nothing and the source is still at $from: ${refusedAgain.message}",
                refusedAgain,
            )
        }
    }
}

/**
 * A rename whose "no such file" names the path that is not there.
 *
 * The server gives one answer for a source that is gone and for a target whose directory does
 * not exist, and the transport reports it against the source. A retry reads a missing source
 * as "my earlier attempt may have landed" and goes to look at the target, so a missing target
 * directory reported as a missing source would have it find nothing there and report the
 * source gone while the source sat where it always was. So the source is looked at: still
 * there, and the answer was about the target, which is what the failure then names.
 */
internal suspend fun SftpSession.renameNamingWhatIsMissing(from: String, to: String) {
    try {
        rename(from, to)
    } catch (missing: NoSuchFile) {
        if (entryAt(from) == null) throw missing
        throw NoSuchFile(
            missing.attempt.copy(path = to),
            "the source is still at $from, so what the server could not find is where $to would go: ${missing.message}",
            missing,
        )
    }
}

/**
 * Takes whatever is at [path] away so that a rename can land there.
 *
 * A path that has gone in the meantime is the state this was trying to reach, so it is not passed
 * on. That also keeps the one failure a rename retry reads as a signal unambiguous: a rename never
 * reports its target as missing on account of the clearing, only on account of the server having
 * nowhere to put it.
 */
private suspend fun SftpSession.clearTheWay(path: String) {
    try {
        delete(path)
    } catch (alreadyGone: NoSuchFile) {
        LOG.debug("{} was already gone when it was cleared for a rename: {}", path, alreadyGone.message)
    }
}

/** What the server says is at [path], or null if there is nothing there. */
internal suspend fun SftpSession.entryAt(path: String): RemoteFile? =
    try {
        stat(path)
    } catch (absent: NoSuchFile) {
        null
    }

/**
 * Says no on the caller's own instruction, before anything is sent.
 *
 * It has a failure class of its own rather than borrowing the server's generic refusal,
 * because the two say different things. A server's refusal is the server's decision about a
 * request it received and is counted against it; this one never reached the server - the file
 * in the way will still be in the way on the next attempt, and the server did nothing wrong.
 */
internal fun refuse(attempt: Attempt, operation: String, path: String): Nothing = throw OverwriteRefused(
    attempt.copy(operation = operation, path = path),
    detail = "there is already something at $path and this $operation was told not to replace it",
)

private val LOG = LoggerFactory.getLogger("sftp.connector.client.Compensation")
