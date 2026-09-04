package infra.shuttle.core

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import org.jboss.logging.Logger
import java.time.Clock
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * Spec 9.4 and 9.5: one loop per process turning PENDING outbox rows into channel calls. Each pass
 * selects at most `batch` due rows that are not in flight, adds their ids to the in-flight set, and
 * hands them to at most `workers` concurrent deliveries; the next select waits until every row of the
 * batch has a worker, so the set never holds more than `batch + workers` ids (I5). An id leaves the set
 * on every exit path (I4): when its worker's job completes, or with the sweep when no worker took it.
 * A [wake] follows every transaction that creates rows; the sweep every `sweepEvery` is the guarantee.
 * Cancelling [run] leaves rows PENDING and the set empty. A state store failure, in the select or in a
 * transition, is logged and the row waits for a later sweep (spec 11); only cancellation ends [run].
 * The body is rendered at send time from the transfer row (D19); `CancellationException` is never an outcome.
 */
class Notifier(
    private val store: StateStore,
    channels: Collection<DeliveryChannel>,
    private val bodies: Map<ChannelName, MappingTable>,
    private val renderer: MappingRenderer,
    private val config: NotifierConfig,
    private val registry: MeterRegistry,
    private val clock: Clock,
    private val random: Random = Random.Default,
    private val hook: Hook = Hook.None,
) {
    private val channels = channels.associateBy { it.name }
    private val inFlight: MutableSet<DeliveryId> = ConcurrentHashMap.newKeySet()
    private val wake = Channel<Unit>(Channel.CONFLATED)
    private val pendingGauge = HashMap<ChannelName, AtomicLong>()
    private val oldestGauge = HashMap<ChannelName, AtomicLong>()

    val inFlightCount: Int get() = inFlight.size

    init {
        registry.gauge(ShuttleMetrics.NOTIFIER_INFLIGHT, inFlight) { it.size.toDouble() }
        for (name in this.channels.keys) {
            pendingGauge[name] = registry.gauge(ShuttleMetrics.OUTBOX_PENDING, Tags.of("channel", name.value), AtomicLong())!!
            oldestGauge[name] = registry.gauge(ShuttleMetrics.OUTBOX_OLDEST_SECONDS, Tags.of("channel", name.value), AtomicLong())!!
        }
    }

    /** The signal a transaction that created rows sends; conflated, carries nothing (D7). */
    fun wake() {
        wake.trySend(Unit)
    }

    /** Runs until cancelled; a sweep the state store fails (spec 11) is logged and the next one runs after `sweepEvery`. */
    suspend fun run(): Unit = coroutineScope {
        val permits = Semaphore(config.workers)
        while (true) {
            val full = try {
                sweep(permits)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warnv(e, "notifier sweep failed; next in {0}", config.sweepEvery)
                false
            }
            if (!full) withTimeoutOrNull(config.sweepEvery) { wake.receive() }
        }
    }

    /** One pass of spec 9.4; true when the batch was full, so the next select should not wait. */
    private suspend fun CoroutineScope.sweep(permits: Semaphore): Boolean {
        val due = store.due(clock.instant(), inFlight.toSet(), config.batch)
        due.forEach { inFlight += it.id }
        var handed = 0
        try {
            for (delivery in due) {
                permits.acquire()
                launch { record(delivery) }.invokeOnCompletion { inFlight -= delivery.id; permits.release() }
                handed++
            }
        } finally {
            due.drop(handed).forEach { inFlight -= it.id }
        }
        refreshGauges()
        return due.size == config.batch
    }

    /** A transition the state store fails leaves the row PENDING for a later sweep (spec 11); the worker ends, the loop does not. */
    private suspend fun record(row: Delivery) {
        try {
            deliver(row)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.warnv(e, "recording delivery {0} of transfer {1} failed; the row stays PENDING", row.id.value, row.transferId.value)
        }
    }

    private suspend fun deliver(row: Delivery) {
        val attempt = row.attempts + 1
        val started = clock.instant()
        val channel = channels[row.channel]
        val outcome: DeliveryOutcome = if (channel == null) {
            DeliveryOutcome.Reject(null, "no channel named ${row.channel.value}")
        } else {
            try {
                val transfer = store.byId(row.transferId) ?: throw MappingFailure("", "transfer ${row.transferId.value} not found")
                val body = renderer.render(bodies[row.channel] ?: MappingTable(emptyList()), transfer, row.moment, attempt)
                channel.deliver(DeliveryEvent(row.transferId, row.moment, row.channel, attempt, body))
            } catch (e: CancellationException) {
                throw e
            } catch (e: MappingFailure) {
                DeliveryOutcome.Reject(null, e.message ?: "mapping failure")
            } catch (e: Exception) {
                DeliveryOutcome.Retry(null, e.toString())
            }
        }
        hook.at(HookPoint.afterDeliverySent, row.transferId)
        val policy = channel?.policy ?: DeliveryPolicy()
        val now = clock.instant()
        val tag = when (outcome) {
            is DeliveryOutcome.Delivered -> { store.delivered(row.id, outcome.reference); "delivered" }
            is DeliveryOutcome.Reject -> { store.deliveryFailed(row.id, outcome.status, outcome.reason); "rejected" }
            is DeliveryOutcome.Retry ->
                if (attempt >= policy.maxAttempts || java.time.Duration.between(row.createdAt, now).toKotlinDuration() >= policy.giveUpAfter) {
                    store.deliveryFailed(row.id, outcome.status, "gave up after $attempt attempts: ${outcome.reason}"); "gave_up"
                } else {
                    store.retryLater(row.id, now.plus(backoff(policy, attempt).toJavaDuration()), outcome.status, outcome.reason); "retry"
                }
        }
        log.infov(
            "delivery transfer={0} event={1} channel={2} attempt={3} outcome={4} status={5} reference={6}",
            row.transferId.value, row.moment, row.channel.value, attempt, tag,
            (outcome as? DeliveryOutcome.Retry)?.status ?: (outcome as? DeliveryOutcome.Reject)?.status,
            (outcome as? DeliveryOutcome.Delivered)?.reference,
        )
        registry.counter(ShuttleMetrics.DELIVERIES, "channel", row.channel.value, "event", row.moment.name.lowercase(), "outcome", tag).increment()
        registry.timer(ShuttleMetrics.DELIVERY_SECONDS, "channel", row.channel.value).record(java.time.Duration.between(started, now))
    }

    /** Spec 9.3: exponential from `initial`, capped at `max`; full jitter draws uniformly from zero to that. */
    private fun backoff(policy: DeliveryPolicy, attempt: Int): Duration {
        val b = policy.backoff
        val ceiling = min(b.max.inWholeMilliseconds.toDouble(), b.initial.inWholeMilliseconds * b.factor.pow(attempt - 1))
        val millis = if (policy.fullJitter) random.nextDouble(0.0, ceiling + 1) else ceiling
        return millis.toLong().milliseconds
    }

    // ponytail: a full PENDING scan per pass; an aggregate query if the outbox ever grows past what a sweep should read.
    private suspend fun refreshGauges() {
        val now = clock.instant()
        val pending = store.outboxPending().groupBy { it.channel }
        for ((name, gauge) in pendingGauge) {
            val rows = pending[name].orEmpty()
            gauge.set(rows.size.toLong())
            oldestGauge.getValue(name).set(rows.minOfOrNull { java.time.Duration.between(it.createdAt, now).seconds } ?: 0L)
        }
    }

    private companion object {
        val log: Logger = Logger.getLogger(Notifier::class.java)
    }
}
