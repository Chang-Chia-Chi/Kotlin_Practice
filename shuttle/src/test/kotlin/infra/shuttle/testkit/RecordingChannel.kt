package infra.shuttle.testkit

import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryChannel
import infra.shuttle.core.DeliveryEvent
import infra.shuttle.core.DeliveryOutcome
import infra.shuttle.core.DeliveryPolicy
import java.util.Collections

/** Spec 9.2 for tests: scripted outcomes in order, the last one repeating; every event delivered is in [events]. */
class RecordingChannel(name: String, override val policy: DeliveryPolicy = DeliveryPolicy(), vararg outcomes: DeliveryOutcome) : DeliveryChannel {
    override val name = ChannelName(name)
    private val script = ArrayDeque(outcomes.toList().ifEmpty { listOf(DeliveryOutcome.Delivered(null)) })
    val events: MutableList<DeliveryEvent> = Collections.synchronizedList(mutableListOf())

    override suspend fun deliver(event: DeliveryEvent): DeliveryOutcome {
        events += event
        return synchronized(script) { if (script.size > 1) script.removeFirst() else script.first() }
    }
}
