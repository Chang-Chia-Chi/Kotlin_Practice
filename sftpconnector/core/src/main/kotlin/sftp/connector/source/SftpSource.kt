package sftp.connector.source

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import org.slf4j.LoggerFactory
import sftp.connector.client.LocalFile
import sftp.connector.client.SftpClient
import sftp.connector.config.PostAction
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.NoSuchFile
import sftp.connector.transport.RemoteFile
import java.nio.file.Path
import java.time.Clock
import java.util.concurrent.atomic.AtomicLong

/**
 * A watched directory as a cold flow of events, with the consumer saying when each file is done.
 *
 * Cold, so collecting is what starts the listing and cancelling is what stops it, and a consumer
 * that is busy slows the listing down rather than being sent files it has no room for. The room
 * is [sftp.connector.config.PollingConfig.maxInFlight]: files handed over and not yet acked or
 * nacked, across every directory this source polls.
 */
class SftpSource(
    private val client: SftpClient,
    private val config: SftpConnectorConfig,
    /** Whatever the host supplies; a private one when the connector is used on its own. */
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    /** What the readiness checks read. Injected so a test can age a file without waiting. */
    clock: Clock = Clock.systemUTC(),
) {

    private val polling = config.polling

    private val inFlight = InFlightSet(polling.maxInFlight)

    private val meters = SourceMeters(meterRegistry, config.endpoint.address) { inFlight.size }

    private val readinessContext = ReadinessContext(client, clock)

    private val ticks = AtomicLong()

    /**
     * One listing of [directory], as events: [SftpEvent.PollStarted], a [SftpEvent.FileSeen] for
     * every file that is ready and not already out with the consumer, and [SftpEvent.PollCompleted]
     * once the listing is exhausted.
     *
     * A poll is three phases: the listing, whose session is back in the pool before the next
     * begins; the readiness checks, over everything the listing found as one batch, so a check
     * that waits holds nothing while it waits and waits once rather than once per file; and the
     * handing over. A file the consumer downloads while still collecting is followed by
     * [SftpEvent.FileGone] if it turned out not to be there.
     *
     * A collection that ends any way but normally - cancelled, or failed by the listing or by the
     * consumer's own block - gives every file this poll handed over and not yet answered back to
     * the set, as if the consumer had nacked each with redelivery. The poll ends by itself after
     * one listing, so a consumer that acks later has only to let it.
     *
     * @throws IllegalArgumentException when [directory] is not one the configuration names, because
     *   only those were checked at start-up and only those have their action folders in place.
     */
    fun poll(directory: String): Flow<SftpEvent> {
        require(directory in polling.directories) {
            "$directory is not a directory this connector was configured to watch: ${polling.directories}"
        }
        val handling = FileHandling(directory)
        return flow {
            val tick = ticks.incrementAndGet()
            val handedOver = mutableListOf<InFlightSlot>()
            var seen = 0
            var emitted = 0
            var notReady = 0
            try {
                meters.timingPoll {
                    emit(SftpEvent.PollStarted(tick, directory))
                    val candidates = mutableListOf<RemoteFile>()
                    filesUnder(directory).collect { file ->
                        seen++
                        if (!inFlight.holds(file)) candidates += file
                    }
                    val verdicts = polling.readiness.check(candidates, readinessContext)
                    for (file in candidates) {
                        when (val readiness = verdicts.getValue(file)) {
                            Readiness.Skip -> Unit
                            is Readiness.NotReady -> {
                                notReady++
                                LOG.debug("{} is not ready yet: {}", file.path, readiness.reason)
                            }
                            Readiness.Ready -> {
                                val slot = inFlight.admit(file) ?: continue
                                handedOver += slot
                                emitted++
                                emit(SftpEvent.FileSeen(file, slot, handling))
                                if (slot.settlement == Settlement.GONE) emit(SftpEvent.FileGone(file))
                            }
                        }
                    }
                    meters.listed(seen, emitted, notReady)
                    emit(SftpEvent.PollCompleted(tick, seen, emitted, notReady))
                }
            } catch (ended: Throwable) {
                // Whatever ended the collection - a cancel, a failed listing, a consumer's block
                // throwing - the consumer will not be answering for these, and a place nobody
                // gives back is a place lost until restart.
                handedOver.forEach { handling.withdraw(it) }
                throw ended
            }
        }
    }

    /**
     * The files of [directory], and of everything under it when the configuration says to
     * descend - except the folders this source's own actions move files into, so a file that has
     * been dealt with is never listed again by the poll that dealt with it.
     */
    private fun filesUnder(directory: String): Flow<RemoteFile> {
        val actionTargets = polling.actionTargetsUnder(directory).toSet()
        return flow { walk(directory, actionTargets) }.take(polling.maxFilesPerPoll)
    }

    /**
     * Subdirectories are walked after the directory they were found in, not as they are found: a
     * listing holds a session for as long as it runs, and walking a subdirectory from inside its
     * parent's listing would hold one session per level of the tree.
     */
    private suspend fun FlowCollector<RemoteFile>.walk(directory: String, actionTargets: Set<String>) {
        val below = mutableListOf<String>()
        client.list(directory, maxEntries = polling.maxFilesPerPoll, withDirectories = polling.recursive).collect {
            if (it.isDirectory) below += it.path else emit(it)
        }
        below.filterNot { it in actionTargets }.forEach { walk(it, actionTargets) }
    }

    /**
     * What happens to a file once the consumer has answered, for files taken from one directory.
     *
     * The directory is the one thing the answer needs that the event does not carry: a relative
     * action target names a different folder under each watched directory, and it is resolved in
     * the configuration's one place for doing so, which is also where the start-up check and the
     * lister resolved it.
     */
    internal inner class FileHandling(private val directory: String) {

        /** [localTarget] null means the client's own default, so the local name is decided in one place. */
        suspend fun download(slot: InFlightSlot, localTarget: Path?): LocalFile? =
            try {
                client.download(slot.file, localTarget)
            } catch (absent: NoSuchFile) {
                // Not a settlement the consumer made, so a slot already settled is left as it is:
                // a file acked and then downloaded is gone because the ack moved it.
                if (slot.settle(Settlement.GONE)) {
                    LOG.info("{} was listed and is gone from the server; nothing to act on.", slot.file.path)
                    meters.settled(Settlement.GONE)
                    slot.release()
                }
                null
            }

        suspend fun ack(slot: InFlightSlot) {
            if (!slot.settle(Settlement.ACK)) return alreadySettled("ack", slot)
            try {
                perform(polling.onAck, slot.file)
                meters.settled(Settlement.ACK)
            } finally {
                slot.release()
            }
        }

        suspend fun nack(slot: InFlightSlot, reason: Throwable, redeliver: Boolean) {
            if (!slot.settle(Settlement.NACK)) return alreadySettled("nack", slot)
            LOG.warn(
                "The consumer could not process {} and it will {}: {}",
                slot.file.path,
                if (redeliver) "be handed over again on a later poll" else "not be handed over again until restart",
                reason.toString(),
            )
            try {
                perform(polling.onNack, slot.file)
                meters.settled(Settlement.NACK)
            } finally {
                slot.release(forGood = !redeliver)
            }
        }

        /** The poll that handed the file over ended without an answer. No action runs: nobody said the file failed. */
        fun withdraw(slot: InFlightSlot) {
            if (!slot.settle(Settlement.CANCELLED)) return
            meters.settled(Settlement.CANCELLED)
            slot.release()
        }

        /**
         * The action's own failure - a refused overwrite, a lost session - reaches the consumer,
         * and is not counted as an answer: the file is still where it was and the next poll hands
         * it over again.
         */
        private suspend fun perform(action: PostAction, file: RemoteFile) {
            when (action) {
                is PostAction.Move -> client.rename(file.path, "${action.targetUnder(directory)}/${file.name}", action.overwrite)
                PostAction.Delete -> client.delete(file.path)
                PostAction.Noop -> Unit
            }
        }

        private fun alreadySettled(call: String, slot: InFlightSlot) {
            LOG.warn(
                "A {} of {} was ignored: the file had already been given back as {}. Each file is answered once.",
                call,
                slot.file.path,
                slot.settlement?.label,
            )
        }
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(SftpSource::class.java)
    }
}
