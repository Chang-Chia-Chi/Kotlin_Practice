package infra.shuttle.core

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.jboss.logging.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.toKotlinDuration

/**
 * Spec 4.1 stages 0 to 4 for one source object. The state store decides the entry point (4.3); the
 * `Fetcher` brings the bytes to a staging directory owned by the run and deleted in `finally` on every
 * path (I9, I18); the chain runs and its mappings are checked before any store; every object of the
 * final payload is stored, one row or N child rows through `children` (4.5); then the ack action and
 * the ACKED ledger write in the order the source needs (D6). Every error is caught at the object
 * boundary (spec 11): an attempt is counted on the row being driven, the trigger is told with the
 * right `redeliver` flag, and nothing but cancellation propagates. One instance per route, safe to
 * run concurrently: all per-object state lives in [Run].
 */
class TransferPipeline(
    private val route: Route,
    private val algorithm: DigestAlgorithm,
    private val store: StateStore,
    private val target: ObjectStoreTarget,
    private val chain: ProcessingChain,
    bodies: Map<ChannelName, MappingTable>,
    private val providerExists: (String) -> Boolean,
    private val wake: () -> Unit,
    private val hook: Hook,
    private val clock: Clock,
    private val registry: MeterRegistry,
    private val staging: Staging,
    private val usableSpace: (Path) -> Long = { Files.getFileStore(it).usableSpace },
    channels: Map<ChannelName, DeliveryChannel> = emptyMap(),
    private val renderer: MappingRenderer = MappingRenderer(),
    /** `expand` fetching from a store other than the one `run` was handed; the route's own fetch store needs no entry. */
    private val fetchers: Map<String, Fetcher> = emptyMap(),
) {
    private val name = RouteName(route.name)
    private val polled = route.source is Source.Poll
    private val kind = if (polled) TransferKind.OBJECT else TransferKind.MESSAGE
    private val targetKey = route.target?.key ?: "{name}"
    /** Spec 5.3: a `callback` ack names a channel the pipeline calls itself; rule 12 guarantees it is declared, the host must provide it. */
    private val callbackChannel = (route.source?.onAck as? AckAction.Callback)
        ?.let { checkNotNull(channels[ChannelName(it.channel)]) { "route ${route.name}: callback channel ${it.channel} was not provided" } }
    private val callbackBody = callbackChannel?.let { bodies[it.name] } ?: MappingTable(emptyList())
    private val tables = (route.notify.map { ChannelName(it.channel) } + listOfNotNull(callbackChannel?.name)).distinct().mapNotNull { bodies[it] }
    private val stagingStore = route.fetch?.store ?: (route.source as? Source.Poll)?.store ?: route.name
    private val freeBytes = registry.gauge(ShuttleMetrics.STAGING_FREE_BYTES, Tags.of("store", stagingStore), AtomicLong())!!
    /** Spec 4.5: uploads of this route, single objects and children alike, never exceed `parallelism` (rule 9's arithmetic). */
    private val uploads = Semaphore(route.parallelism)

    suspend fun run(event: RouteEvent.Seen, fetch: Fetcher) = Run(event, fetch).execute()

    /** One source object's run; [row] is the row an error is charged to, null until one is being driven. */
    private inner class Run(private val event: RouteEvent.Seen, private val fetch: Fetcher) {
        private var row: Transfer? = null

        suspend fun execute() {
            try {
                decide()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                failed(e)
            }
        }

        /** Stage 0, the table of spec 4.3. */
        private suspend fun decide() {
            val existing = store.find(event.identity)
            when (existing?.state) {
                TransferState.REJECTED, TransferState.FAILED -> return event.nack(false)
                TransferState.ACKED, TransferState.DONE -> return finished(existing)
                TransferState.STORED -> { row = existing; if (verified(existing)) return ack(existing) }
                else -> Unit
            }
            fullRun(existing ?: store.seen(event.identity, kind))
        }

        /** Stages 1 to 4 on [transfer]. */
        private suspend fun fullRun(transfer: Transfer) {
            row = transfer
            withFetched(transfer) { staged, dir -> resume(transfer, staged, dir) }
        }

        /**
         * Spec 4.3 at ACKED or DONE: a message is verified and acked again without a fetch. A polled file
         * is fetched and digested at most once per `recheckFinished` from `updated_at` (D40): the row's own
         * source digest means the same file came back (verify, re-ack, `reacked`), another digest is new
         * content under an old identity and gets the next revision through `supersede` (I24, S12).
         */
        private suspend fun finished(transfer: Transfer) {
            if (!polled) return if (verified(transfer)) reack(transfer) else fullRun(transfer)
            val since = java.time.Duration.between(transfer.updatedAt, clock.instant()).toKotlinDuration()
            if (route.recheckFinished.isPositive() && since < route.recheckFinished) return
            withFetched(transfer) { staged, dir ->
                when {
                    staged.digest != transfer.sourceDigest -> {
                        val next = store.supersede(transfer.id, kind)
                        registry.counter(ShuttleMetrics.SUPERSEDES, "route", route.name).increment()
                        log.infov("route {0}: {1} came back with new content; revision {2} supersedes transfer {3}", route.name, event.identity.sourceName, next.identity.revision, transfer.id.value)
                        row = next
                        resume(next, staged, dir)
                    }
                    verified(transfer) -> reack(transfer)
                    else -> { row = transfer; resume(transfer, staged, dir) }
                }
            }
        }

        /**
         * D41 then stage 1: below `staging.minFree` the object is deferred with no attempt counted; otherwise
         * the bytes land in `<staging.dir>/<transfer id>`, deleted on every exit with every file the chain created.
         */
        private suspend fun withFetched(transfer: Transfer, block: suspend (StagedObject, Path) -> Unit) {
            val free = usableSpace(staging.dir)
            freeBytes.set(free)
            if (free < staging.minFree) {
                registry.counter(ShuttleMetrics.STAGING_DEFERRED, "route", route.name).increment()
                log.warnv("route {0}: staging has {1} bytes free, below {2}; deferring {3}", route.name, free, staging.minFree, event.identity.sourceName)
                return event.nack(true)
            }
            val dir = Files.createDirectories(staging.dir.resolve(transfer.id.value.toString()))
            try {
                val staged = stage("fetch") { fetch(sourcePath(), dir.resolve(event.identity.sourceName.substringAfterLast('/')), algorithm) }
                block(staged, dir)
            } finally {
                dir.toFile().deleteRecursively()
            }
        }

        /** Spec 5.1: a polled file's path is the listing's; a subscribed transfer's is read from the message at `fetch.path`. */
        private fun sourcePath(): String = route.fetch?.let { f ->
            val body = event.source.body ?: throw IllegalStateException("route ${route.name}: the message has no body to read fetch.path ${f.path} from")
            JSON.readTree(body).at(f.path).takeIf { it.isTextual }?.asText()
                ?: throw IllegalStateException("route ${route.name}: fetch.path ${f.path} is absent from the message")
        } ?: event.source.path

        /** Stages 1 (ledger) to 4 over a staged object. */
        private suspend fun resume(transfer: Transfer, staged: StagedObject, dir: Path) {
            val id = transfer.id
            ledger(DeliveryMoment.FETCHED) { store.fetched(id, staged.summary, it) }
            hook.at(HookPoint.afterFetch, id)
            val ctx = Context(TransferView(id, name, transfer.identity, event.source.path, transfer.firstSeenAt, transfer.parentId), event.source, dir, fetch)
            val done = when (val result = stage("process") { chain.run(Payload(listOf(staged)), ctx) }) {
                is ChainResult.Rejected -> return reject(transfer, result.reason)
                is ChainResult.Done -> result
            }
            ProcessingChain.checkMappings(done.attributes, tables, providerExists)
            store.processed(id, done.attributes)
            hook.at(HookPoint.afterProcess, id)
            val objects = done.payload.objects
            val keys = objects.map { expandPattern(targetKey, it.name, transfer.identity.sourceName, done.attributes, clock) }
            keys.withIndex().groupBy({ it.value }, { objects[it.index].name }).entries.firstOrNull { it.value.size > 1 }?.let { (key, names) ->
                return reject(transfer, "cardinality: ${names.joinToString(" and ")} both resolve to key $key")
            }
            if (objects.size == 1) {
                val ref = uploads.withPermit { stage("store") { storeOne(id, objects.single(), keys.single(), done.attributes) } }
                ledger(DeliveryMoment.STORED) { store.stored(id, ref, it) }
                hook.at(HookPoint.afterLedgerStored, id)
            } else {
                val children = childRows(id, objects)
                // every child runs to its end whatever a sibling did; the first failure is the run's, after all have finished
                val errors = supervisorScope { children.indices.map { i -> async { storeChild(children[i], objects[i], keys[i], done.attributes) } } }
                    .mapNotNull { child -> try { child.await(); null } catch (e: CancellationException) { throw e } catch (e: Exception) { e } }
                errors.firstOrNull { it !is ChildFailed }?.let { throw it }
                if (errors.isNotEmpty()) throw ChildFailed(errors.any { (it as ChildFailed).terminal }, errors.first())
            }
            ack(transfer)
        }

        /**
         * Spec 4.3 for a parent that already has child rows: when the chain yielded the same children again (same
         * names and digests, none FAILED) the rows stay, so a STORED one can skip its store; anything else, a re-drive
         * after a failed child included, replaces them (4.5).
         */
        private suspend fun childRows(id: TransferId, objects: List<StagedObject>): List<Transfer> {
            val existing = store.childrenOf(id).associateBy { it.identity.sourceName }
            val same = existing.size == objects.size && objects.all { o -> existing[o.name]?.let { it.state != TransferState.FAILED && it.digest == o.digest } == true }
            if (same) return objects.map { existing.getValue(it.name) }
            return store.children(id, objects.map { it.summary })
                .also { registry.counter(ShuttleMetrics.CHILDREN, "route", route.name).increment(it.size.toDouble()) }
        }

        /**
         * One child's stage 3 (spec 4.5). A STORED child whose copy verifies skips the store (S28); the ledger write flips
         * the parent with the last sibling (D42). A failure is the child's attempt, and the store fails the parent when
         * the child reaches `maxAttempts` (I16); the parent's own count is untouched.
         */
        private suspend fun storeChild(child: Transfer, o: StagedObject, key: String, attributes: Map<String, String>) {
            if (child.state == TransferState.STORED && child.target?.let { target.verify(it) } == true) return
            try {
                uploads.withPermit {
                    val ref = stage("store") { storeOne(child.id, o, key, attributes) }
                    ledger(DeliveryMoment.STORED) { store.stored(child.id, ref, it) }
                    hook.at(HookPoint.afterLedgerStored, child.id)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warnv("route {0}: child {1} of transfer {2} failed: {3}", route.name, child.id.value, child.parentId?.value, e.toString())
                val after = store.failedAttempt(child.id, e.message ?: e.toString(), route.maxAttempts)
                throw ChildFailed(after.state == TransferState.FAILED, e)
            }
        }

        private suspend fun storeOne(id: TransferId, o: StagedObject, key: String, attributes: Map<String, String>): TargetRef {
            val metadata = buildMap {
                put(TargetMetadata.DIGEST, o.digest.hex)
                put(TargetMetadata.DIGEST_ALGORITHM, o.digest.algorithm.name.lowercase())
                put(TargetMetadata.SOURCE_MTIME, o.mtime.toString())
                put(TargetMetadata.SOURCE_NAME, o.name)
                put(TargetMetadata.TRANSFER_ID, id.value.toString())
                attributes.forEach { (k, v) -> put(TargetMetadata.ATTRIBUTE_PREFIX + k, v) }
            }
            val ref = target.store(key, o.path, metadata)
            hook.at(HookPoint.afterStore, id)
            return ref
        }

        /**
         * Stage 4. A `callback` ack (spec 5.3) is called first, whatever the source's order, so no ledger write and no
         * source-side ack precede upstream's answer; then the source's order (D6): a polled file is moved first, a
         * subscribed message is written ACKED first.
         */
        private suspend fun ack(transfer: Transfer) {
            val id = transfer.id
            if (callbackChannel != null) stage("ack") { callback(callbackChannel, id) }
            if (polled) {
                stage("ack") { event.ack() }; hook.at(HookPoint.afterAck, id)
                ledger(DeliveryMoment.ACKED) { store.acked(id, it) }; hook.at(HookPoint.afterLedgerAcked, id)
            } else {
                ledger(DeliveryMoment.ACKED) { store.acked(id, it) }; hook.at(HookPoint.afterLedgerAcked, id)
                stage("ack") { event.ack() }; hook.at(HookPoint.afterAck, id)
            }
            count("done")
        }

        /**
         * The `callback` ack: the channel's body rendered from the row as the notifier would for `acked`, one synchronous
         * call, no outbox row. Anything but `Delivered` is a stage error, retried with the stage (spec 11); a
         * `CancellationException` passes through untouched.
         */
        private suspend fun callback(channel: DeliveryChannel, id: TransferId) {
            val current = checkNotNull(store.byId(id)) { "transfer ${id.value} vanished before its callback" }
            val attempt = current.attempts + 1
            val body = renderer.render(callbackBody, current, DeliveryMoment.ACKED, attempt)
            val outcome = channel.deliver(DeliveryEvent(id, DeliveryMoment.ACKED, channel.name, attempt, body))
            log.infov("callback transfer={0} channel={1} attempt={2} outcome={3}", id.value, channel.name.value, attempt, outcome)
            when (outcome) {
                is DeliveryOutcome.Delivered -> Unit
                is DeliveryOutcome.Retry -> throw IllegalStateException("callback ${channel.name.value} answered Retry ${outcome.status}: ${outcome.reason}")
                is DeliveryOutcome.Reject -> throw IllegalStateException("callback ${channel.name.value} answered Reject ${outcome.status}: ${outcome.reason}")
            }
        }

        /** The ack action again for a finished row that came back; no ledger write, no new deliveries, and no callback: ACKED proves it succeeded. */
        private suspend fun reack(transfer: Transfer) {
            log.warnv("route {0}: transfer {1} ({2}) is {3} and came back unchanged; acking again", route.name, transfer.id.value, event.identity.sourceName, transfer.state)
            stage("ack") { event.ack() }; hook.at(HookPoint.afterAck, transfer.id)
            count("reacked")
        }

        private suspend fun reject(transfer: Transfer, reason: String) {
            store.rejected(transfer.id, reason)
            count("rejected")
            event.nack(false)
        }

        /**
         * Spec 11: the attempt is charged to [row]; FAILED at `maxAttempts`, or at once for a freeze failure; the trigger
         * is told. A child's failure was charged to the child already, and the parent is FAILED only if the store said so.
         */
        private suspend fun failed(e: Exception) {
            log.warnv("route {0}: transfer {1} failed: {2}", route.name, row?.id?.value ?: event.identity.sourceName, e.toString())
            log.debug("stage error", e)
            val after = if (e is ChildFailed) null else row?.let {
                try {
                    store.failedAttempt(it.id, e.message ?: e.toString(), if (e is FreezeFailure) 1 else route.maxAttempts)
                } catch (t: CancellationException) {
                    throw t
                } catch (t: Exception) {
                    log.error("state store unavailable while recording the failure", t); null
                }
            }
            val terminal = after?.state == TransferState.FAILED || (e is ChildFailed && e.terminal)
            if (terminal) count("failed")
            event.nack(!terminal)
        }

        /** A transition that may create outbox rows: the rows ride the transaction (I11), then the notifier is woken. */
        private suspend fun ledger(moment: DeliveryMoment, transition: suspend (List<DeliveryRequest>) -> Unit) {
            val events = route.notify.filter { it.on == moment }.map { DeliveryRequest(moment, ChannelName(it.channel)) }
            transition(events)
            if (events.isNotEmpty()) wake()
        }

        private suspend fun <T> stage(name: String, block: suspend () -> T): T {
            val started = clock.instant()
            var result = "error"
            try {
                return block().also { result = "ok" }
            } finally {
                registry.timer(ShuttleMetrics.STAGE_SECONDS, "route", route.name, "stage", name, "result", result).record(java.time.Duration.between(started, clock.instant()))
            }
        }
    }

    /** Spec 4.3 at STORED: `verify` of the row's reference, once, or of every child's for a parent, which has no reference of its own. */
    private suspend fun verified(row: Transfer): Boolean = row.target?.let { target.verify(it) }
        ?: store.childrenOf(row.id).let { children -> children.isNotEmpty() && children.all { c -> c.target?.let { target.verify(it) } ?: false } }

    private fun count(outcome: String) = registry.counter(ShuttleMetrics.TRANSFERS, "route", route.name, "outcome", outcome).increment()

    /** Spec 6.2 over the run's staging directory; `fetch` pulls `expand`'s children into it, so they die with the run (I9). */
    private inner class Context(override val transfer: TransferView, override val source: SourceView, private val dir: Path, private val own: Fetcher) : ProcessContext {
        override val attributes = LinkedHashMap<String, String>()
        override val clock: Clock get() = this@TransferPipeline.clock
        private var created = 0
        override fun setAttribute(name: String, value: String) { attributes[name] = value }
        override fun newStagedFile(name: String): Path = dir.resolve("${created++}-${name.substringAfterLast('/')}")
        override suspend fun fetch(store: String, path: String): StagedObject {
            val fetcher = if (store == stagingStore) own else fetchers[store] ?: throw IllegalStateException("route ${route.name}: no fetcher for store $store")
            return fetcher(path, newStagedFile(path), algorithm)
        }
    }

    private companion object {
        val log: Logger = Logger.getLogger(TransferPipeline::class.java)
    }
}

/** A child's store failed and was charged to the child; [terminal] when that child, and so the parent, is now FAILED. */
private class ChildFailed(val terminal: Boolean, cause: Throwable) : RuntimeException(cause.message, cause)
