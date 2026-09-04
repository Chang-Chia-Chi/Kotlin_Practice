package infra.shuttle.nats

import com.fasterxml.jackson.databind.ObjectMapper
import infra.shuttle.core.AckAction
import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryChannel
import infra.shuttle.core.DeliveryEvent
import infra.shuttle.core.DeliveryOutcome
import infra.shuttle.core.DeliveryPolicy
import infra.shuttle.core.RouteEvent
import infra.shuttle.core.RouteName
import infra.shuttle.core.Source
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.SourceKind
import infra.shuttle.core.SourceView
import infra.shuttle.core.NatsChannel as NatsChannelConfig
import io.nats.client.Connection
import io.nats.client.Message
import io.nats.client.PullSubscribeOptions
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import org.jboss.logging.Logger
import java.time.Duration

/**
 * Spec 5.1 and 9.2 over jnats. One class, both roles of a NATS channel:
 *
 * - [events] is the `subscribe` trigger. A durable JetStream pull consumer named after the route -
 *   created on first use, bound to the operator's when it already exists, so its ack wait stays the
 *   operator's - is fetched one message at a time and each message becomes one [RouteEvent.Seen].
 *   The `Seen`'s ack applies the route's `onAck` (`ack` or `term`, spec 5.3) and its nack is
 *   `nak` when it asks for redelivery and `term` when it does not. From the moment a `Seen` is
 *   handed out until one of those runs, the message is told `inProgress` every `inProgressEvery`
 *   (D38), so a transfer longer than the consumer's ack wait is not redelivered under our feet.
 *   Anything the client cannot recover from ends the flow with [RouteEvent.RouteDown]; the route's
 *   supervisor restarts it with backoff (spec 10).
 * - [deliver] is a JetStream publish on the channel's `subject`, the stream sequence the server
 *   answers with as the delivery's reference. A broker that does not answer is `Retry`.
 *
 * `CancellationException` is never caught or converted; the blocking jnats calls run on [io]
 * through `runInterruptible`, so cancelling the collector interrupts the call in flight.
 */
class NatsChannel(
    private val config: NatsChannelConfig,
    private val connection: Connection,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) : DeliveryChannel {

    override val name = ChannelName(config.name)
    override val policy = DeliveryPolicy()
    private val jetStream = connection.jetStream()

    override suspend fun deliver(event: DeliveryEvent): DeliveryOutcome {
        val subject = requireNotNull(config.subject) { "channel ${config.name} has no subject" }
        val outcome = try {
            val ack = runInterruptible(io) { jetStream.publish(subject, mapper.writeValueAsBytes(event.body)) }
            DeliveryOutcome.Delivered(ack.seqno.toString())
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // No ack from the stream: the message may or may not be stored, so the outbox tries again (spec 11).
            DeliveryOutcome.Retry(null, e.toString())
        }
        log.infof(
            "delivery transfer=%d event=%s channel=%s attempt=%d subject=%s outcome=%s",
            event.transferId.value, event.moment.name.lowercase(), config.name, event.attempt, subject,
            when (outcome) { is DeliveryOutcome.Delivered -> "delivered seq=${outcome.reference}"; is DeliveryOutcome.Retry -> "retry ${outcome.reason}"; is DeliveryOutcome.Reject -> "rejected" },
        )
        return outcome
    }

    /** The `subscribe` trigger of spec 5.1 as the cold flow the route runtime collects. */
    fun events(route: RouteName, source: Source.Subscribe): Flow<RouteEvent> = channelFlow {
        val subscription = try {
            runInterruptible(io) { jetStream.subscribe(source.subject, PullSubscribeOptions.builder().durable(durable(route)).build()) }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            send(RouteEvent.RouteDown(e))
            return@channelFlow
        }
        try {
            while (true) {
                if (connection.status == Connection.Status.CLOSED) {
                    send(RouteEvent.RouteDown(IllegalStateException("connection to ${config.name} is closed")))
                    return@channelFlow
                }
                val fetched = try {
                    runInterruptible(io) { subscription.fetch(1, FETCH_WAIT) }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    send(RouteEvent.RouteDown(e))
                    return@channelFlow
                }
                fetched.forEach { send(seen(route, source, it)) }
            }
        } finally {
            runCatching { subscription.unsubscribe() }
        }
    }

    private fun ProducerScope<RouteEvent>.seen(route: RouteName, source: Source.Subscribe, message: Message): RouteEvent.Seen {
        val signals = launch {
            while (true) {
                delay(source.inProgressEvery)
                try {
                    runInterruptible(io) { message.inProgress() }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // A signal the broker misses costs at worst one redelivery; it never takes the route down.
                    log.debugf(e, "in-progress signal for %s failed", message.subject)
                }
            }
        }
        suspend fun settle(action: () -> Unit) {
            signals.cancel()
            runInterruptible(io, block = action)
        }
        return RouteEvent.Seen(
            identity = identity(route, message),
            source = SourceView(message.subject, message.data, headers(message)),
            ack = { settle { if (source.onAck == AckAction.Term) message.term() else message.ack() } },
            nack = { redeliver -> settle { if (redeliver) message.nak() else message.term() } },
        )
    }

    /**
     * Spec 5.2: channel, subject and the message id. The id is the publisher's `Nats-Msg-Id` when it set one
     * and the stream sequence otherwise; both are the same on a redelivery, which the delivery count is not.
     */
    private fun identity(route: RouteName, message: Message) = SourceIdentity(
        route = route,
        sourceKind = SourceKind.NATS,
        sourceRef = "${config.name}:${message.subject}",
        sourceName = message.headers?.getFirst(MSG_ID) ?: message.metaData().streamSequence().toString(),
        sourceSize = null,
        sourceMtime = null,
    )

    /** What `extract` reads with `from: message`, beside the body (spec 6.3). */
    private fun headers(message: Message): Map<String, String> =
        message.headers?.let { headers -> headers.keySet().associateWith { headers.getFirst(it).orEmpty() } }.orEmpty()

    /**
     * One durable consumer per route: created on first use with the server's defaults, bound to the
     * operator's when it already exists, so the ack wait `inProgressEvery` is kept below stays theirs.
     */
    private fun durable(route: RouteName) = NOT_IN_A_CONSUMER_NAME.replace(route.value, "_")

    private companion object {
        val log: Logger = Logger.getLogger(NatsChannel::class.java)
        val mapper = ObjectMapper()

        /** How long one fetch waits for a message before the loop looks at the connection again. */
        val FETCH_WAIT: Duration = Duration.ofSeconds(1)

        /** Spec 5.2: the header a publisher sets when the stream sequence is not the stable id. */
        const val MSG_ID = "Nats-Msg-Id"

        /** A consumer name may not carry `.`, `*`, `>` or whitespace, which a route name may. */
        val NOT_IN_A_CONSUMER_NAME = Regex("[^A-Za-z0-9_-]")
    }
}
