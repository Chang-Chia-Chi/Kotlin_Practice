package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.LogCapture
import infra.shuttle.testkit.RecordingChannel
import kotlinx.coroutines.test.runTest
import org.jboss.logging.Logger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Instant

/** Spec 9.2, 9.6 and 6.4 in one place: the render-and-deliver both the notifier and the callback ack go through. */
class DelivererTest {
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val downstream = ChannelName("downstream")
    private val body = MappingTable(listOf(MappingRow("fileId", field = Field.TRANSFER_ID), MappingRow("event", field = Field.EVENT)))

    private suspend fun transfer() =
        store.seen(SourceIdentity(RouteName("drop"), SourceKind.SFTP, "sftp:/in", "a.csv", 10, Instant.EPOCH), TransferKind.OBJECT)

    private fun deliverer(vararg channels: DeliveryChannel, table: MappingTable = body, renderer: MappingRenderer = MappingRenderer()) =
        Deliverer(store, channels.toList(), channels.associate { it.name to table }, renderer)

    @Test
    fun a_moment_is_rendered_from_the_row_and_delivered_on_the_named_channel() = runTest {
        val channel = RecordingChannel("downstream", outcomes = arrayOf(DeliveryOutcome.Delivered("ref-1")))
        val t = transfer()

        val outcome = deliverer(channel).deliver(t.id, downstream, DeliveryMoment.FETCHED, 3) { it }

        assertEquals(DeliveryOutcome.Delivered("ref-1"), outcome)
        val sent = channel.events.single()
        assertEquals(DeliveryEvent(t.id, DeliveryMoment.FETCHED, downstream, 3, sent.body), sent)
        assertEquals(t.id.value.toString(), sent.body.get("fileId").asText())
        assertEquals("fetched", sent.body.get("event").asText())
    }

    @Test
    fun Retry_and_Reject_come_back_as_the_channel_answered_them() = runTest {
        val channel = RecordingChannel("downstream", outcomes = arrayOf(DeliveryOutcome.Retry("503", "busy"), DeliveryOutcome.Reject("400", "bad request")))
        val t = transfer()
        val deliverer = deliverer(channel)

        assertEquals(DeliveryOutcome.Retry("503", "busy"), deliverer.deliver(t.id, downstream, DeliveryMoment.ACKED, 1) { it })
        assertEquals(DeliveryOutcome.Reject("400", "bad request"), deliverer.deliver(t.id, downstream, DeliveryMoment.ACKED, 2) { it })
    }

    @Test
    fun a_channel_exception_is_a_Retry_and_a_missing_row_an_unknown_channel_or_a_vanished_transfer_a_Reject() = runTest {
        val throwing = object : DeliveryChannel {
            override val name = downstream
            override val policy = DeliveryPolicy()
            override suspend fun deliver(event: DeliveryEvent): DeliveryOutcome = throw IOException("connection reset")
        }
        val t = transfer()
        assertEquals(DeliveryOutcome.Retry(null, "java.io.IOException: connection reset"), deliverer(throwing).deliver(t.id, downstream, DeliveryMoment.ACKED, 1) { it })

        val required = MappingTable(listOf(MappingRow("orderNumber", attribute = "orderNumber")))
        val channel = RecordingChannel("downstream")
        assertEquals(
            DeliveryOutcome.Reject(null, "mapping row orderNumber: no value for orderNumber"),
            deliverer(channel, table = required).deliver(t.id, downstream, DeliveryMoment.ACKED, 1) { it },
        )
        assertEquals(DeliveryOutcome.Reject(null, "no channel named other"), deliverer(channel).deliver(t.id, ChannelName("other"), DeliveryMoment.ACKED, 1) { it })
        assertEquals(DeliveryOutcome.Reject(null, "mapping row : transfer 99 not found"), deliverer(channel).deliver(TransferId(99), downstream, DeliveryMoment.ACKED, 1) { it })
        assertEquals(0, channel.events.size, "nothing reaches a channel without a body")
    }

    /** Spec 3.2: what the caller does with the outcome runs inside the same MDC as the channel call did. */
    @Test
    fun the_channel_call_and_the_callers_block_run_with_the_transfer_its_route_and_the_channel_in_the_MDC() = runTest {
        val log = Logger.getLogger("infra.shuttle.core.DelivererTest")
        val channel = object : DeliveryChannel {
            override val name = downstream
            override val policy = DeliveryPolicy()
            override suspend fun deliver(event: DeliveryEvent): DeliveryOutcome = DeliveryOutcome.Delivered(null).also { log.warn("in the channel") }
        }
        val t = transfer()
        val logs = LogCapture()
        logs.use { deliverer(channel).deliver(t.id, downstream, DeliveryMoment.ACKED, 1) { log.warn("in the block") } }

        val lines = logs.warnings().filter { it.message.startsWith("in the") }
        assertEquals(listOf("in the channel", "in the block"), lines.map { it.message })
        for (line in lines) {
            assertEquals(mapOf(Mdc.TRANSFER_ID to t.id.value.toString(), Mdc.ROUTE to "drop", Mdc.CHANNEL to "downstream"), line.mdc, line.message)
        }
    }

    /** Spec 6.4: the freeze check reads the named channels' tables and no other; a provider is judged by the renderer's own resolution. */
    @Test
    fun checkAttributes_judges_the_named_channels_tables_against_the_frozen_attributes() {
        val required = MappingTable(listOf(MappingRow("orderNumber", attribute = "orderNumber")))
        val provided = MappingTable(listOf(MappingRow("order", provider = "orderDetails")))
        val other = RecordingChannel("other")
        val deliverer = Deliverer(store, listOf(RecordingChannel("downstream"), other), mapOf(downstream to required, other.name to provided))

        deliverer.checkAttributes(mapOf("orderNumber" to "123"), listOf(downstream))
        deliverer.checkAttributes(emptyMap(), emptyList())
        val missing = assertThrows(FreezeFailure::class.java) { deliverer.checkAttributes(emptyMap(), listOf(downstream)) }
        assertEquals("mapping row orderNumber: attribute orderNumber is required and not set", missing.message)
        val noBean = assertThrows(FreezeFailure::class.java) { deliverer.checkAttributes(emptyMap(), listOf(other.name)) }
        assertEquals("row order: no bean named orderDetails", noBean.message)

        val resolving = Deliverer(store, listOf(other), mapOf(other.name to provided), MappingRenderer { Provider { com.fasterxml.jackson.databind.ObjectMapper().readTree("{}") } })
        resolving.checkAttributes(emptyMap(), listOf(other.name))
    }
}
