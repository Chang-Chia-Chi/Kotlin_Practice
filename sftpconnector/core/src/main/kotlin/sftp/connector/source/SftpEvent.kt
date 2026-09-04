package sftp.connector.source

import sftp.connector.client.LocalFile
import sftp.connector.error.SftpException
import sftp.connector.transport.RemoteFile
import java.nio.file.Path

/**
 * What a poll tells its collector. Metadata only, never bytes: the consumer decides whether, when
 * and how many at a time to download, and may ack a file it already knows without downloading it.
 */
sealed interface SftpEvent {

    /** The tick is a number for reading a log, counting up per source; never a metric tag. */
    data class PollStarted(val tick: Long, val directory: String) : SftpEvent

    /**
     * A file that passed its readiness checks and is now the consumer's until it says otherwise.
     *
     * The consumer says so with [ack] or [nack], exactly once - whichever comes second is logged
     * and ignored. Until then the file is in flight: no poll hands it over again, and it holds one
     * of the places that bound how far a poll may run ahead of the consumer. A collector that is
     * cancelled with files still in flight gives them all back as if nacked, so a consumer that
     * wants to ack after the poll has ended lets the poll end rather than cancelling it.
     */
    class FileSeen internal constructor(
        val file: RemoteFile,
        private val slot: InFlightSlot,
        private val handling: SftpSource.FileHandling,
    ) : SftpEvent {

        /**
         * Fetches the file, or returns null if it has gone from the server since it was listed.
         *
         * Gone is an answer, not a failure: on a directory another system moves files out of, it is
         * ordinary. The file leaves the in-flight set on the spot - there is nothing to ack - and
         * the null is the one signal that always arrives. A collector that downloads inside its
         * own collect block, which is the usual shape, also receives a [FileGone] right after this
         * event; a download made after the poll has ended has no poll left to say so.
         *
         * @param localTarget where the file lands; by default the staging directory under the
         *   file's own name, **checked to be a name** before it is joined to that directory, as
         *   [sftp.connector.client.SftpClient.download] decides it. Naming a target turns that
         *   check off, and the obvious thing to name - `myDir.resolve(event.file.name)` - is the
         *   one that needs it most: `file.name` is the *server's* word, and a server that lists
         *   `..\..\evil.csv` has just chosen a directory two above `myDir` for you. A caller that
         *   names its own target has taken over deciding what is safe to write, so either leave
         *   this null or check the name the way [sftp.connector.client.SftpClient] does.
         */
        suspend fun download(localTarget: Path? = null): LocalFile? = handling.download(slot, localTarget)

        /**
         * The consumer is done with the file: the ack action runs and the place comes free. The
         * action's own failure reaches the caller, and the file is then still where it was.
         */
        suspend fun ack() = handling.ack(slot)

        /**
         * The consumer could not process the file: the nack action runs and the place comes free.
         * With [redeliver] the file is handed over again on a later poll; without it, not until
         * the process restarts.
         */
        suspend fun nack(reason: Throwable, redeliver: Boolean = true) = handling.nack(slot, reason, redeliver)

        override fun toString(): String = "FileSeen(${file.path})"
    }

    /** Listed, and then not there when the consumer went to download it. */
    data class FileGone(val file: RemoteFile) : SftpEvent

    /** A tick of a watch that deliberately sent nothing. Not a fault; the next tick runs as usual. */
    data class PollSkipped(val tick: Long, val cause: SkipCause) : SftpEvent

    /**
     * A tick of a watch that failed in a way the next tick may not - a session lost, a full
     * pool, a listing the server refused. The watch goes on; a failure that no later tick could
     * survive ends the watch with the error instead of arriving as an event.
     */
    data class PollFailed(val tick: Long, val error: SftpException) : SftpEvent

    /**
     * The listing is over. [seen] entries passed through the checks, [emitted] of them were handed
     * over, [notReady] are waiting to pass and will be looked at again next poll. The rest were
     * already in flight or skipped.
     *
     * [inFlight] is every file out with the consumer at the instant the tick ended - handed over
     * and not yet given back, by this tick or an earlier one, in the order they were handed over.
     * It is here so a ledger downstream reconciles against the connector's own set instead of
     * keeping a copy of it. A file that was acked or nacked is given back only once its action
     * has run, so one whose move is still under way is still in this list.
     *
     * [truncated] says the listing stopped at `maxFilesPerPoll`, so the directory may hold more
     * than this tick saw. A tick that was not truncated is the only ground for "everything in the
     * directory has now been listed". The listing stops at the cap without looking past it, so a
     * directory holding exactly the cap reads as truncated.
     */
    data class PollCompleted(
        val tick: Long,
        val seen: Int,
        val emitted: Int,
        val notReady: Int,
        val inFlight: List<RemoteFile> = emptyList(),
        val truncated: Boolean = false,
    ) : SftpEvent
}

/** Why a tick sent nothing. */
enum class SkipCause {
    /** The tick before it was still running, and the overlap policy says one at a time. */
    OVERLAP,

    /** The circuit breaker is open, so the connector is deliberately leaving the server alone. */
    BREAKER_OPEN,
}
