package infra.shuttle.nats

import com.fasterxml.jackson.databind.ObjectMapper
import infra.shuttle.core.AckAction
import infra.shuttle.core.DeliveryEvent
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryOutcome
import infra.shuttle.core.RouteEvent
import infra.shuttle.core.RouteName
import infra.shuttle.core.Source
import infra.shuttle.core.SourceKind
import infra.shuttle.core.TransferId
import infra.shuttle.core.NatsChannel as NatsChannelConfig
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.StorageType
import io.nats.client.api.StreamConfiguration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** One NATS with JetStream for every `nats`-tagged class in the JVM; Ryuk removes it when the JVM ends. */
object NatsBroker {
    val container: GenericContainer<*> by lazy {
        GenericContainer(DockerImageName.parse("nats:2.10-alpine"))
            .withCommand("-js")
            .withExposedPorts(4222)
            .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1))
            .also { it.start() }
    }
    val url: String get() = "nats://${container.host}:${container.getMappedPort(4222)}"
}

/**
 * G15 against a real broker. Every test owns a stream and a subject, and the route's durable consumer
 * is created up front with a two second ack wait so redelivery, and the in-progress signals that hold
 * it off, happen inside a test's patience rather than the server's default thirty seconds.
 */
@Tag("nats")
class NatsChannelTest {

    private val route = RouteName("image-sets")
    private val ackWait = Duration.ofSeconds(2)
    private lateinit var connection: Connection
    private lateinit var channel: NatsChannel
    private lateinit var subject: String
    private val mapper = ObjectMapper()

    @BeforeEach fun broker() {
        connection = Nats.connect(NatsBroker.url)
        val stream = "s${++counter}"
        subject = "images.ready.$counter"
        val management = connection.jetStreamManagement()
        management.addStream(StreamConfiguration.builder().name(stream).storageType(StorageType.Memory).subjects(subject).build())
        management.addOrUpdateConsumer(
            stream,
            ConsumerConfiguration.builder().durable(route.value).filterSubject(subject).ackWait(ackWait).build(),
        )
        channel = NatsChannel(NatsChannelConfig("events", NatsBroker.url, subject = subject), connection)
    }

    @AfterEach fun close() {
        connection.close()
    }

    @Test
    fun `a publish lands on the subject, answers the stream sequence, and becomes one Seen`() = subscribed { events ->
        assertEquals(DeliveryOutcome.Delivered("1"), channel.deliver(event(1)))
        assertEquals(DeliveryOutcome.Delivered("2"), channel.deliver(event(2)))

        val first = events.receive() as RouteEvent.Seen
        assertEquals(SourceKind.NATS, first.identity.sourceKind)
        assertEquals("events:$subject", first.identity.sourceRef)
        assertEquals("1", first.identity.sourceName)
        assertEquals(subject, first.source.path)
        assertEquals("""{"n":1}""", String(first.source.body!!))
        first.ack()

        assertEquals("2", (events.receive() as RouteEvent.Seen).identity.sourceName)
    }

    @Test
    fun `an acked message is not redelivered`() = subscribed { events ->
        channel.deliver(event(1))
        (events.receive() as RouteEvent.Seen).ack()
        assertNull(withTimeoutOrNull(quiet) { events.receive() })
    }

    @Test
    fun `a nak redelivers the message under the same identity`() = subscribed { events ->
        channel.deliver(event(1))
        val first = events.receive() as RouteEvent.Seen
        first.nack(true)

        val again = events.receive() as RouteEvent.Seen
        assertEquals(first.identity, again.identity)
        again.ack()
    }

    @Test
    fun `a nack that asks for no redelivery terms the message`() = subscribed { events ->
        channel.deliver(event(1))
        (events.receive() as RouteEvent.Seen).nack(false)
        assertNull(withTimeoutOrNull(quiet) { events.receive() })
    }

    @Test
    fun `onAck term stops redelivery too`() = subscribed(onAck = AckAction.Term) { events ->
        channel.deliver(event(1))
        (events.receive() as RouteEvent.Seen).ack()
        assertNull(withTimeoutOrNull(quiet) { events.receive() })
    }

    /** D38: the run below is three times the consumer's ack wait and is not redelivered while signals flow. */
    @Test
    fun `in progress signals hold off redelivery for a run longer than the ack wait`() = subscribed(inProgressEvery = 500.milliseconds) { events ->
        channel.deliver(event(1))
        val seen = events.receive() as RouteEvent.Seen
        assertNull(withTimeoutOrNull(6.seconds) { events.receive() })
        seen.ack()
    }

    @Test
    fun `a closed connection ends the flow with RouteDown`() = subscribed { events ->
        channel.deliver(event(1))
        events.receive()
        connection.close()
        assertInstanceOf(RouteEvent.RouteDown::class.java, events.receive())
    }

    /** Two and a half ack waits: long enough for a message the broker still owns to come back. */
    private val quiet = 5.seconds

    private fun event(n: Int) = DeliveryEvent(TransferId(n.toLong()), DeliveryMoment.ACKED, channel.name, 1, mapper.readTree("""{"n":$n}"""))

    /**
     * Collects the trigger into an unbounded channel for the body to read, and cancels the collector
     * afterwards: `withTimeout` is a scope and would otherwise wait for the endless flow.
     */
    private fun subscribed(
        inProgressEvery: kotlin.time.Duration = 10.seconds,
        onAck: AckAction = AckAction.Ack,
        body: suspend CoroutineScope.(ReceiveChannel<RouteEvent>) -> Unit,
    ) = runBlocking {
        val source = Source.Subscribe("events", subject, onAck, AckAction.Nak, inProgressEvery)
        withTimeout(60.seconds) {
            val out = Channel<RouteEvent>(Channel.UNLIMITED)
            val collector = launch { channel.events(route, source).collect { out.send(it) } }
            try {
                body(out)
            } finally {
                collector.cancel()
            }
        }
    }

    private companion object {
        var counter = 0
    }
}
