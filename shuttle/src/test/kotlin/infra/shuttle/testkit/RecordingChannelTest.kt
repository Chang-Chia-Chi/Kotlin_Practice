package infra.shuttle.testkit

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryEvent
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryOutcome
import infra.shuttle.core.DeliveryPolicy
import infra.shuttle.core.TransferId
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RecordingChannelTest {
    private fun event(attempt: Int) =
        DeliveryEvent(TransferId(1), DeliveryMoment.ACKED, ChannelName("hook"), attempt, JsonNodeFactory.instance.objectNode())

    @Test
    fun S7_returns_the_scripted_outcomes_in_order_repeating_the_last_and_records_every_event() = runTest {
        val channel = RecordingChannel(
            "hook", DeliveryPolicy(maxAttempts = 3),
            DeliveryOutcome.Retry("503", "busy"), DeliveryOutcome.Retry("503", "busy"), DeliveryOutcome.Delivered("ref-1"),
        )
        assertEquals(ChannelName("hook"), channel.name)
        assertEquals(3, channel.policy.maxAttempts)
        val outcomes = (1..4).map { channel.deliver(event(it)) }
        assertEquals(
            listOf(DeliveryOutcome.Retry("503", "busy"), DeliveryOutcome.Retry("503", "busy"), DeliveryOutcome.Delivered("ref-1"), DeliveryOutcome.Delivered("ref-1")),
            outcomes,
        )
        assertEquals(listOf(1, 2, 3, 4), channel.events.map { it.attempt })
    }

    @Test
    fun the_default_script_is_Delivered_with_no_reference() = runTest {
        assertEquals(DeliveryOutcome.Delivered(null), RecordingChannel("hook").deliver(event(1)))
    }
}
