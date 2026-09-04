package infra.shuttle.nats

import infra.shuttle.core.AckAction
import infra.shuttle.core.RouteEvent
import infra.shuttle.core.RouteName
import infra.shuttle.core.Source
import infra.shuttle.core.NatsChannel as NatsChannelConfig
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.JetStreamSubscription
import io.nats.client.Message
import io.nats.client.PullSubscribeOptions
import io.nats.client.impl.Headers
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The in-progress loop of spec 5.1 over a faked connection, so it runs in the default tier: the loop
 * holds a long transfer's message off redelivery (D38) and must end the moment the message is settled.
 * `CancellationException` is never caught, so the cancel `settle` sends is not swallowed by the loop's
 * own error handling; a loop that swallowed it would go on telling the broker a finished message is
 * still in progress.
 */
class NatsChannelSignalsTest {

    private val route = RouteName("image-sets")
    private val subject = "images.ready"
    private val interval = 10.milliseconds
    private val signals = AtomicInteger()

    /** What the second and later fetches block on, as the real one blocks for its fetch wait. */
    private val quiet = CountDownLatch(1)

    private val message = mock(Message::class.java)
    private val subscription = mock(JetStreamSubscription::class.java)
    private val jetStream = mock(JetStream::class.java)
    private val connection = mock(Connection::class.java)

    private fun channel(): NatsChannel {
        doAnswer { Connection.Status.CONNECTED }.`when`(connection).status
        doAnswer { jetStream }.`when`(connection).jetStream()
        doAnswer { subscription }.`when`(jetStream).subscribe(anyString(), any(PullSubscribeOptions::class.java))
        var first = true
        doAnswer { if (first) listOf(message).also { first = false } else { quiet.await(); emptyList<Message>() } }
            .`when`(subscription).fetch(anyInt(), any(java.time.Duration::class.java))
        doAnswer { subject }.`when`(message).subject
        doAnswer { ByteArray(0) }.`when`(message).data
        doAnswer { Headers().add("Nats-Msg-Id", "7") }.`when`(message).headers
        doAnswer { signals.incrementAndGet(); null }.`when`(message).inProgress()
        return NatsChannel(NatsChannelConfig("events", "nats://fake", subject = subject), connection)
    }

    @Test
    fun a_settled_message_ends_its_in_progress_loop_instead_of_swallowing_the_cancellation() = runTest {
        // Real time: the loop's own delay is what the settle has to interrupt.
        withContext(Dispatchers.Default) {
            val source = Source.Subscribe("events", subject, AckAction.Ack, AckAction.Nak, interval)
            val out = Channel<RouteEvent>(Channel.UNLIMITED)
            val channel = channel()
            val collector = launch { channel.events(route, source).collect { out.send(it) } }
            try {
                val seen = out.receive() as RouteEvent.Seen
                withTimeout(10.seconds) { while (signals.get() < 2) delay(interval) }

                seen.ack()
                val atSettle = signals.get()
                // Twenty more intervals: a loop that carried on would have signalled about as many times again.
                delay(interval * 20)
                assertTrue(
                    signals.get() <= atSettle + 1,
                    "the in-progress loop went on signalling a settled message: $atSettle at the settle, ${signals.get()} after",
                )
                verify(message).ack()
            } finally {
                quiet.countDown()
                collector.cancel()
            }
        }
    }
}
