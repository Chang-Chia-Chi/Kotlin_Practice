package sftp.connector.source

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.consume
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import sftp.connector.PROBE_MARKER_PREFIX
import sftp.connector.client.LocalFile
import sftp.connector.client.SftpClient
import sftp.connector.config.OverlapPolicy
import sftp.connector.config.PostAction
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.NoSuchFile
import sftp.connector.error.SftpException
import sftp.connector.error.WatchReaction
import sftp.connector.transport.RemoteFile
import java.nio.file.Path
import java.time.Clock
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

/**
 * A watched directory as a cold flow of events, with the consumer saying when each file is done.
 *
 * Cold, so collecting is what starts the listing and cancelling is what stops it, and a consumer
 * that is busy slows the listing down rather than being sent files it has no room for. The room
 * is [sftp.connector.config.PollingConfig.maxInFlight]: files handed over and not yet acked or
 * nacked, across every directory this source polls.
 *
 * [poll] is one listing. [watch] is the same listing on a ticker, for as long as the collector
 * keeps collecting: it reports a failed tick as an event and goes on, because a lost session or a
 * full pool is the next tick's to survive, and ends only on a failure no later tick could survive
 * - a rejected password, a rejected host key. [consume] is the ordinary pipeline over a watch:
 * the consumer's block per file, acked when it returns and nacked when it throws.
 */
class SftpSource(
    private val client: SftpClient,
    private val config: SftpConnectorConfig,
    /** Whatever the host supplies; a private one when the connector is used on its own. */
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    /** What the readiness checks read. Injected so a test can age a file without waiting. */
    clock: Clock = Clock.systemUTC(),
    /**
     * Where a watch's ticker runs. The connector hands over its own scope, so that stopping the
     * connector stops every watch; a source built on its own gets a scope nothing stops, and its
     * watches end when their collectors do. A supervisor, because one watch ending on a fatal
     * failure says nothing about the watch of another directory.
     */
    private val background: CoroutineScope = CoroutineScope(SupervisorJob()),
) {

    private val polling = config.polling

    private val inFlight = InFlightSet(polling.maxInFlight)

    private val meters = SourceMeters(meterRegistry, config.endpoint.address) { inFlight.size }

    private val readinessContext = ReadinessContext(client::stat, clock)

    private val ticks = AtomicLong()

    /**
     * A watched path named so that two connectors watching `inbound/` on two servers are two
     * different lines. Every line here is about a path, and a path on its own is the one thing
     * that cannot be grepped for when a host runs more than one connector.
     */
    private fun at(path: String) = "$path on ${config.endpoint.address}"

    /**
     * The directories a watch is running on right now. One consumer per directory is what the
     * in-flight set's promise rests on, so the second is refused; adding to a synchronised set
     * is the whole decision, made under its lock.
     */
    private val watching: MutableSet<String> = Collections.synchronizedSet(HashSet())

    /**
     * One listing of [directory], as events: [SftpEvent.PollStarted], a [SftpEvent.FileSeen] for
     * every file that is ready and not already out with the consumer, and [SftpEvent.PollCompleted]
     * once the listing is exhausted.
     *
     * A poll is three phases: the listing, whose session is back in the pool before the next
     * begins; the readiness checks, over everything the listing found as one batch, so a check
     * that waits holds nothing while it waits and waits once rather than once per file; and the
     * handing over. A file that has gone by the time the consumer downloads it answers null from
     * the download and is given back on the spot; no event says so.
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
        requireConfigured(directory)
        return flow { emitAll(tickOf(directory, ticks.incrementAndGet())) }
    }

    /**
     * [poll], repeated: once as soon as the flow is collected, and again every [every] after
     * that, for as long as the collector keeps collecting. The ticks continue the numbering of
     * this source's polls.
     *
     * A tick that fails is reported as [SftpEvent.PollFailed] and the next tick runs as usual,
     * whatever the failure was - a lost session, a full pool, a listing the server refused - and
     * a tick the breaker will not let through is [SftpEvent.PollSkipped]. The one thing that ends
     * the flow early is a failure that no later tick could survive: the flow ends with that error.
     * A failure the connector has no name for is a bug, and ends the flow too, since no tick
     * survives a bug by waiting.
     *
     * When the interval comes round while the previous tick is still running, the configured
     * overlap policy decides: a skipped tick is reported as [SftpEvent.PollSkipped], or the new
     * tick runs alongside, with the in-flight set keeping a file from being handed over twice.
     *
     * The ticker runs in the connector's own scope, so that stopping the connector stops every
     * watch; the flow then ends normally in its collector. Cancelling the collector stops the
     * ticker and gives the directory back. However the watch ends - its collector left, it was
     * cancelled, the connector closed - every file it handed over and not yet had an answer for
     * goes back to the set, to be handed over again by the next watch or the next process: the
     * running tick's, as for [poll], and those of every tick that finished and left its files
     * with the consumer. An answer that arrives after that is ignored as a second answer.
     *
     * @throws IllegalArgumentException when [directory] is not one the configuration names.
     * @throws IllegalStateException on collection, when another collector is already watching
     *   [directory] on this connector - one consumer per directory is what keeps a file from being
     *   handed over twice - or when the connector has stopped, since a ticker started in a scope
     *   that has been cancelled would end at once and a consumer looping on `watch` would spin.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    fun watch(directory: String, every: Duration): Flow<SftpEvent> {
        requireConfigured(directory)
        require(every.isPositive()) { "a watch of $directory needs a positive interval, not $every" }
        return flow {
            check(background.isActive) { "the connector has been closed, so $directory cannot be watched on it" }
            check(watching.add(directory)) {
                "$directory is already being watched on this connector; one consumer per directory " +
                    "is what keeps a file from being handed over twice"
            }
            // "Is it polling at all?" had no answer in the log before this: the only per-tick
            // lines are for a tick that failed or was skipped, so a healthy watch and a watch
            // that never started read identically.
            LOG.info("Watching {}, every {}.", at(directory), every)
            val handling = FileHandling(directory)
            val handovers = Handovers { handling.withdraw(it, Settlement.WATCH_ENDED) }
            try {
                val events = background.produce(failedAfterItsCollectorLeft(directory)) { tickEvery(directory, every, handovers) }
                events.consume {
                    while (true) {
                        // Only the receive is asked what happened to the producer. What `emit`
                        // throws is the collector's own - its block's exception, a timeout it
                        // let escape, an operator's abort - and passes through untouched, a
                        // cancellation included: it is never wrapped and never swallowed.
                        val next = receiveCatching()
                        if (next.isClosed) {
                            // The ticker never ends on its own. A closed channel with a failure
                            // is a tick no later tick could survive; a cancellation is the
                            // connector stopping its watchers, which ends the flow normally.
                            val why = next.exceptionOrNull()
                            if (why != null && why !is CancellationException) throw why
                            currentCoroutineContext().ensureActive()
                            LOG.info("The watch of {} ended because the connector stopped it.", at(directory))
                            break
                        }
                        emit(next.getOrThrow())
                    }
                }
            } finally {
                // Whichever way the watch ended, every file it handed over and never had an
                // answer for goes back now, before the directory is given up, so the next watch
                // lists them rather than finding them still out. Under `NonCancellable` because
                // the usual way a watch ends is a cancellation, and a give-back that the same
                // cancellation could cut short would be the leak it exists to close. The
                // cancellation itself is neither caught nor wrapped here; it goes on its way
                // once this has run.
                withContext(NonCancellable) {
                    val givenBack = handovers.end()
                    if (givenBack > 0) {
                        LOG.info(
                            "The watch of {} ended with {} file(s) still with the consumer; they are given back and will be listed again.",
                            at(directory),
                            givenBack,
                        )
                    }
                }
                watching.remove(directory)
            }
        }
    }

    /**
     * The ordinary pipeline: [watch], with [block] run for every file handed over, acked when it
     * returns and nacked when it throws. One file failing never ends the pipeline - the nack says
     * whether it comes round again - and neither does an ack or nack action that could not be
     * carried out: the file is then still where it was, and the next tick hands it over again.
     *
     * Only the consumer's own exception is its verdict. One of the connector's own failures out of
     * the block - a download refused by a full pool, a session lost under it - says nothing about
     * the file, so it is neither acked nor nacked: it is given back with no action run, the next
     * tick hands it over again, and the failure is treated as an action's failure is.
     *
     * What ends the pipeline is what ends a watch, or a bug: an exception that is not one of the
     * connector's own failures out of an ack or a nack.
     */
    suspend fun consume(directory: String, every: Duration, block: suspend (SftpEvent.FileSeen) -> Unit) {
        watch(directory, every).collect { event ->
            if (event !is SftpEvent.FileSeen) return@collect
            try {
                block(event)
            } catch (failed: Exception) {
                // This collector's own cancellation is not the block failing. Anything else is,
                // including a timeout the block set for itself.
                currentCoroutineContext().ensureActive()
                if (failed is SftpException) {
                    event.giveBack()
                    unlessFatal(failed, "The connector failed while the consumer had ${at(event.file.path)}; that is not the consumer's verdict, so it is given back and will be handed over again")
                } else {
                    answering(event) { event.nack(failed) }
                }
                return@collect
            }
            answering(event) { event.ack() }
        }
    }

    /**
     * The handle the file at [path] was handed over on, while it is in flight - handed over and
     * not yet given back - or null: nothing at that path was listed, or its file has been answered
     * and its action run, or the watch that handed it over has ended and given it back.
     *
     * It is the very [SftpEvent.FileSeen] the poll or watch emitted, so a download through it is
     * the download of the file that was listed and an ack through it is the same ack: one action,
     * and whichever call comes second is the ignored second answer. It exists for a consumer that
     * resumes from its own durable record, which has the path and nothing else, and would otherwise
     * keep a path-to-handle table of its own - a second copy of the in-flight set. A lookup, not a
     * claim: asking neither admits nor holds anything, so a handle looked up before the watch
     * ended and used after it is a late answer like any other. Never waits.
     */
    fun inFlightAt(path: String): SftpEvent.FileSeen? = inFlight.slotAt(path)?.handle

    private suspend fun answering(event: SftpEvent.FileSeen, answer: suspend () -> Unit) {
        try {
            answer()
        } catch (failed: SftpException) {
            unlessFatal(failed, "The answer for ${at(event.file.path)} could not be carried out, so it is still where it was and will be handed over again")
        }
    }

    /** A connector failure the pipeline goes on from is logged with its [consequence]; one no later tick could survive ends the pipeline. */
    private fun unlessFatal(failed: SftpException, consequence: String) {
        if (failed.disposition.watch == WatchReaction.STOP) throw failed
        LOG.warn("{}: {}", consequence, failed.toString())
    }

    /**
     * A tick's failure normally reaches the collector through the channel. It cannot when the
     * collector has already gone - cancelled at the very moment the tick was failing - and a
     * failure with nobody to tell is logged here rather than left to the thread's default handler.
     */
    private fun failedAfterItsCollectorLeft(directory: String) = CoroutineExceptionHandler { _, failure ->
        LOG.warn("A tick of {} failed just as its collector was leaving, so nobody was told.", at(directory), failure)
    }

    /**
     * The ticker. Each tick is numbered when it comes round, whether it runs or is skipped, and a
     * tick that runs is a coroutine of its own, so the ticker is free to come round again while
     * it works. A skipped tick is handed over like any other event, so under the skipping policy
     * the ticker waits for a collector that is busy, and the interval is counted from then. The
     * overlap decision needs no lock: the ticker is the only coroutine that makes it.
     */
    private suspend fun ProducerScope<SftpEvent>.tickEvery(directory: String, every: Duration, handovers: Handovers) {
        var latest: Job? = null
        while (true) {
            val tick = ticks.incrementAndGet()
            if (latest?.isActive == true && polling.overlap == OverlapPolicy.SKIP) {
                LOG.warn("Tick {} of {} is skipped: the tick before it is still running after {}.", tick, at(directory), every)
                send(SftpEvent.PollSkipped(tick, SkipCause.OVERLAP))
            } else {
                latest = launch { tickOf(directory, tick, handovers).reportingFailures(tick, directory).collect { send(it) } }
            }
            delay(every)
        }
    }

    /**
     * What a watch does about a tick that failed: what the failure itself says a watch should do.
     * Only the tick's own failures come through here - a failure of the consumer downstream, or
     * the tick being cancelled, passes untouched. A failure the connector has no name for is a
     * bug, and ends the watch, since no tick survives a bug by waiting; so does a cancellation
     * that is nobody's, which is what a check that lets its own timeout escape produces, because
     * letting it out of the tick would end the tick without a word to anyone.
     */
    private fun Flow<SftpEvent>.reportingFailures(tick: Long, directory: String): Flow<SftpEvent> = catch { failed ->
        if (failed is CancellationException) {
            currentCoroutineContext().ensureActive()
            throw IllegalStateException(
                "tick $tick of $directory was cancelled by something inside it while the tick itself was not being cancelled; " +
                    "a check or an action that times itself out has to catch its own timeout",
                failed,
            )
        }
        if (failed !is SftpException) {
            LOG.error("The watch of {} is ending on tick {} with a failure the connector has no name for.", at(directory), tick, failed)
            throw failed
        }
        when (failed.disposition.watch) {
            WatchReaction.REPORT_THE_FAILURE -> {
                LOG.warn("Tick {} of {} failed; the next tick will try again: {}", tick, at(directory), failed.toString())
                emit(SftpEvent.PollFailed(tick, failed))
            }
            WatchReaction.REPORT_A_SKIP -> {
                LOG.info("Tick {} of {} is skipped: {}", tick, at(directory), failed.message)
                emit(SftpEvent.PollSkipped(tick, SkipCause.BREAKER_OPEN))
            }
            WatchReaction.STOP -> {
                LOG.error("The watch of {} is ending on tick {}, because no later tick could survive this.", at(directory), tick, failed)
                throw failed
            }
        }
    }

    private fun requireConfigured(directory: String) {
        require(directory in polling.directories) {
            "$directory is not a directory this connector was configured to watch: ${polling.directories}"
        }
    }

    /**
     * [handovers] is where a watch records what its ticks hand over, so it can give it all back
     * when it ends. Null for a poll, whose files stay with the consumer when it ends by design:
     * a consumer acks after the poll is over, and nobody speaks for the poll then.
     */
    private fun tickOf(directory: String, tick: Long, handovers: Handovers? = null): Flow<SftpEvent> {
        val handling = FileHandling(directory)
        return flow {
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
                        if (!inFlight.holds(file)) {
                            candidates += file
                        } else {
                            // Neither emitted nor not-ready: a newer copy of a name the consumer
                            // is still working waits its turn, and is handed over once the copy
                            // being worked has been answered.
                            val beingWorked = inFlight.outAt(file.path)
                            if (beingWorked != null && beingWorked != file) {
                                LOG.debug(
                                    "{} has been uploaded again ({} bytes, modified {}) while the copy listed earlier ({} bytes, modified {}) is still with the consumer; it waits for that one to be answered.",
                                    at(file.path),
                                    file.size,
                                    file.modifiedAt,
                                    beingWorked.size,
                                    beingWorked.modifiedAt,
                                )
                            }
                        }
                    }
                    val verdicts = polling.readiness.check(candidates, readinessContext)
                    for (file in candidates) {
                        when (val readiness = verdicts.getValue(file)) {
                            Readiness.Skip -> Unit
                            is Readiness.NotReady -> {
                                notReady++
                                LOG.debug("{} is not ready yet: {}", at(file.path), readiness.reason)
                            }
                            Readiness.Ready -> {
                                val slot = inFlight.admit(file) ?: continue
                                handedOver += slot
                                handovers?.record(slot)
                                emitted++
                                val seen = SftpEvent.FileSeen(file, slot, handling)
                                slot.handedOverAs(seen)
                                emit(seen)
                            }
                        }
                    }
                    meters.listed(seen, emitted, notReady)
                    // The listing stops at the cap without looking past it, so reaching the cap
                    // is all "there may be more" can mean here.
                    val truncated = seen >= polling.maxFilesPerPoll
                    val outstanding = inFlight.outstanding()
                    LOG.debug(
                        "Tick {} of {} finished: {} seen{}, {} handed over, {} not ready yet, {} out with the consumer.",
                        tick,
                        at(directory),
                        seen,
                        if (truncated) " (stopped at the cap of $seen; the directory may hold more)" else "",
                        emitted,
                        notReady,
                        outstanding.size,
                    )
                    emit(SftpEvent.PollCompleted(tick, seen, emitted, notReady, outstanding, truncated))
                }
            } catch (ended: Throwable) {
                // Whatever ended the collection - a cancel, a failed listing, a consumer's block
                // throwing - the consumer will not be answering for these, and a place nobody
                // gives back is a place lost until restart.
                handedOver.forEach { handling.withdraw(it, Settlement.CANCELLED) }
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
        // A shared budget carries the `maxFilesPerPoll` cap through the walk, rather than a `.take`
        // around it. `.take` stops the flow by throwing a cancellation into the producer, and a
        // listing capped that way records `sftp_op_seconds{op=list,result=cancelled}` - a poll that
        // simply reached its limit, counted the same as one the time limiter killed. The budget caps
        // each listing through its own `maxEntries`, which ends it cleanly and records `ok`.
        return flow { walk(directory, actionTargets, FileBudget(polling.maxFilesPerPoll)) }
    }

    /**
     * Subdirectories are walked after the directory they were found in, not as they are found: a
     * listing holds a session for as long as it runs, and walking a subdirectory from inside its
     * parent's listing would hold one session per level of the tree.
     */
    private suspend fun FlowCollector<RemoteFile>.walk(directory: String, actionTargets: Set<String>, budget: FileBudget) {
        val below = mutableListOf<String>()
        client.list(directory, maxEntries = budget.remaining, withDirectories = polling.recursive).collect {
            when {
                it.isDirectory -> below += it.path
                // A start-up marker a dead session left behind is never handed over: whoever wrote
                // it, it is this connector's own bookkeeping and not a file the consumer asked for.
                it.name.startsWith(PROBE_MARKER_PREFIX) -> Unit
                budget.take() -> emit(it)
            }
        }
        for (sub in below.filterNot { it in actionTargets }) {
            if (budget.isSpent) break
            walk(sub, actionTargets, budget)
        }
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
                    LOG.info("{} was listed and is gone from the server; nothing to act on.", at(slot.file.path))
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
            // The throwable itself, not its `toString()`: `consume` nacks and goes on, so this
            // line is the only record the consumer's failure ever leaves, and without the stack
            // an operator is left with a class name and nothing to open.
            LOG.warn(
                "The consumer could not process {} and it will {}.",
                at(slot.file.path),
                if (redeliver) "be handed over again on a later poll" else "not be handed over again until restart",
                reason,
            )
            try {
                perform(polling.onNack, slot.file)
                meters.settled(Settlement.NACK)
            } finally {
                slot.release(forGood = !redeliver)
            }
        }

        /**
         * The poll or the watch that handed the file over ended without an answer - [how] says
         * which. No action runs: nobody said the file failed. A file already answered is left as
         * it is, so the running tick and the watch that ends around it may both call this.
         */
        fun withdraw(slot: InFlightSlot, how: Settlement) {
            if (!slot.settle(how)) return
            meters.settled(how)
            slot.release()
        }

        /**
         * The action's own failure - a refused overwrite, a lost session - reaches the consumer,
         * and is not counted as an answer: the file is still where it was and the next poll hands
         * it over again.
         */
        private suspend fun perform(action: PostAction, file: RemoteFile) {
            when (action) {
                is PostAction.Move ->
                    client.rename(file.path, "${action.targetUnder(directory)}/${file.name}", action.overwrite, listed = file)
                PostAction.Delete -> client.delete(file.path)
                PostAction.Noop -> Unit
            }
        }

        private fun alreadySettled(call: String, slot: InFlightSlot) {
            val why = when (slot.settlement) {
                // A file that was gone at download time was never anybody's to answer, and the
                // null download already said so; an answer after it is the ordinary pipeline
                // finishing its block, not a second answer worth a warning.
                Settlement.GONE -> return
                Settlement.WATCH_ENDED ->
                    "the watch that handed it over had already ended and given it back, so a later poll hands it over again"
                else -> "the file had already been given back as ${slot.settlement?.label}"
            }
            LOG.warn("The {} of {} was ignored: {}. Each file is answered once.", call, at(slot.file.path), why)
        }
    }

    private companion object {
        private val LOG = LoggerFactory.getLogger(SftpSource::class.java)
    }
}

/**
 * Every file a watch has handed over and not yet had an answer for, so that the watch can give
 * all of them back when it ends. The running tick withdraws its own when it is cancelled; this
 * is for the ticks that finished and left their files with the consumer, which used to stay in
 * flight for the life of the process once their watch was gone.
 *
 * Ticks run in the connector's scope and may run alongside each other, so recording is under a
 * lock. A slot recorded after the watch has ended is given back on the spot: its tick was still
 * handing over as the collector left, and its send is about to fail on a cancelled channel.
 * Answered slots are dropped as new ones are recorded, so a watch that runs for months holds
 * only what is actually out.
 */
private class Handovers(private val giveBack: (InFlightSlot) -> Unit) {

    private val lock = Any()
    private val slots = mutableListOf<InFlightSlot>()
    private var ended = false

    fun record(slot: InFlightSlot) {
        val tooLate = synchronized(lock) {
            if (!ended) {
                slots.removeAll { it.settlement != null }
                slots += slot
            }
            ended
        }
        if (tooLate) giveBack(slot)
    }

    /** Gives back every file still unanswered, outside the lock, and returns how many there were. */
    fun end(): Int {
        val unanswered = synchronized(lock) {
            ended = true
            slots.filter { it.settlement == null }.also { slots.clear() }
        }
        unanswered.forEach(giveBack)
        return unanswered.size
    }
}

/**
 * How many more files one poll may hand on, across the whole of a recursive walk.
 *
 * It is what carries `maxFilesPerPoll` through the walk without a `.take` around the flow: each
 * listing is capped by its own `maxEntries` from what is [remaining], so it ends cleanly rather
 * than by a cancellation, and the walk stops descending once it [isSpent]. One coroutine walks, so
 * it needs no synchronisation.
 */
private class FileBudget(private var left: Int) {
    val remaining: Int get() = left
    val isSpent: Boolean get() = left <= 0

    /** Claims one file's worth of budget, or answers false when there is none left. */
    fun take(): Boolean {
        if (left <= 0) return false
        left--
        return true
    }
}
