package sftp.connector.source

import sftp.connector.client.LocalFile
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
         *   file's own name, as [sftp.connector.client.SftpClient.download] decides it.
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

    /**
     * The listing is over. [seen] entries passed through the checks, [emitted] of them were handed
     * over, [notReady] are waiting to pass and will be looked at again next poll. The rest were
     * already in flight or skipped.
     */
    data class PollCompleted(val tick: Long, val seen: Int, val emitted: Int, val notReady: Int) : SftpEvent
}
