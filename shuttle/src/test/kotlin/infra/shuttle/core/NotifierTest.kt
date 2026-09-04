package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.LogCapture
import infra.shuttle.testkit.RecordingChannel
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class NotifierTest {
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val registry = SimpleMeterRegistry()
    private val downstream = ChannelName("downstream")
    private val body = MappingTable(listOf(MappingRow("fileId", field = Field.TRANSFER_ID), MappingRow("event", field = Field.EVENT)))

    private fun notifier(vararg channels: DeliveryChannel, config: NotifierConfig = NotifierConfig(workers = 2, batch = 10, sweepEvery = 30.seconds), store: StateStore = this.store) =
        Notifier(store, channels.toList(), channels.associate { it.name to body }, MappingRenderer(), config, registry, clock, kotlin.random.Random(1))

    /** Spec 11's "state store unavailable": the first call of [failing] throws, every other call reaches [inner]. */
    private class FailsOnce(private val inner: StateStore, private val failing: String) : StateStore by inner {
        private var failed = false
        private fun unavailable(method: String) {
            if (method == failing && !failed) { failed = true; throw IOException("state store unavailable") }
        }
        override suspend fun due(now: Instant, excluding: Set<DeliveryId>, limit: Int) = unavailable("due").let { inner.due(now, excluding, limit) }
        override suspend fun delivered(id: DeliveryId, reference: String?) = unavailable("delivered").let { inner.delivered(id, reference) }
    }

    /** An ACKED transfer with one PENDING `acked` delivery per channel named. */
    private suspend fun ackedTransfer(name: String, channels: List<ChannelName>): Transfer {
        val t = store.seen(SourceIdentity(RouteName("drop"), SourceKind.SFTP, "sftp:/in", name, 10, Instant.EPOCH), TransferKind.OBJECT)
        store.fetched(t.id, StagedSummary(name, 10, Instant.EPOCH, Digest(DigestAlgorithm.MD5, "d"), null), emptyList())
        store.processed(t.id, emptyMap())
        store.stored(t.id, TargetRef("memory", "bucket", name, "v1", 10), emptyList())
        store.acked(t.id, channels.map { DeliveryRequest(DeliveryMoment.ACKED, it) })
        return store.transfer(t.id)
    }

    /** Moves the wall clock and the virtual clock together. */
    private fun TestScope.tick(by: Duration) {
        clock.advance(by)
        advanceTimeBy(by)
        runCurrent()
    }

    @Test
    fun I3_a_delivery_is_DELIVERED_only_after_the_channel_returned_Delivered() = runTest {
        val channel = RecordingChannel("downstream", outcomes = arrayOf(DeliveryOutcome.Delivered("req-1")))
        val t = ackedTransfer("a.csv", listOf(downstream))
        val before = store.outbox.single()
        assertEquals(DeliveryState.PENDING, before.state)

        backgroundScope.launch { notifier(channel).run() }
        runCurrent()

        val after = store.outbox.single()
        assertEquals(DeliveryState.DELIVERED, after.state)
        assertEquals("req-1", after.reference)
        assertEquals(1, after.attempts)
        assertEquals(TransferState.DONE, store.transfer(t.id).state)
        val event = channel.events.single()
        assertEquals(t.id, event.transferId)
        assertEquals(1, event.attempt)
        assertEquals(t.id.value.toString(), event.body.get("fileId").asText())
        assertEquals("acked", event.body.get("event").asText())
    }

    private val noJitter = DeliveryPolicy(fullJitter = false)
    private val retry503 = DeliveryOutcome.Retry("503", "unavailable")
    private val ok = DeliveryOutcome.Delivered("ref")

    @Test
    fun S7_downstream_503_twice_then_200() = runTest {
        val channel = RecordingChannel("downstream", noJitter, retry503, retry503, ok)
        val t = ackedTransfer("a.csv", listOf(downstream))
        val start = clock.instant()
        backgroundScope.launch { notifier(channel).run() }
        runCurrent()
        assertEquals(start.plusSeconds(5), store.outbox.single().nextAttemptAt, "attempt 1 retries after initial 5 s")
        tick(30.seconds)
        assertEquals(start.plusSeconds(30 + 10), store.outbox.single().nextAttemptAt, "attempt 2 retries after 10 s")
        tick(30.seconds)
        val row = store.outbox.single()
        assertEquals(DeliveryState.DELIVERED, row.state)
        assertEquals(3, row.attempts)
        assertEquals(listOf(1, 2, 3), channel.events.map { it.attempt })
        assertEquals(TransferState.DONE, store.transfer(t.id).state)
        assertEquals(2.0, registry.counter(ShuttleMetrics.DELIVERIES, "channel", "downstream", "event", "acked", "outcome", "retry").count())
    }

    @Test
    fun S8_downstream_400() = runTest {
        val channel = RecordingChannel("downstream", noJitter, DeliveryOutcome.Reject("400", "bad request"))
        val t = ackedTransfer("a.csv", listOf(downstream))
        backgroundScope.launch { notifier(channel).run() }
        runCurrent()
        val row = store.outbox.single()
        assertEquals(DeliveryState.FAILED, row.state)
        assertEquals("400", row.lastStatus)
        assertEquals(TransferState.ACKED, store.transfer(t.id).state)
        assertEquals(1.0, registry.counter(ShuttleMetrics.DELIVERIES, "channel", "downstream", "event", "acked", "outcome", "rejected").count())
    }

    @Test
    fun S9_downstream_down_past_giveUpAfter() = runTest {
        val policy = DeliveryPolicy(giveUpAfter = 10.seconds, fullJitter = false)
        val channel = RecordingChannel("downstream", policy, retry503, retry503, retry503, ok)
        val t = ackedTransfer("a.csv", listOf(downstream))
        backgroundScope.launch { notifier(channel, config = NotifierConfig(workers = 2, batch = 10, sweepEvery = 5.seconds)).run() }
        runCurrent()
        tick(5.seconds)
        tick(5.seconds)
        tick(5.seconds)
        val failed = store.outbox.single()
        assertEquals(DeliveryState.FAILED, failed.state, "attempt at t=15 s is past giveUpAfter 10 s")
        assertEquals(3, failed.attempts)
        assertTrue(failed.lastError!!.startsWith("gave up"))
        assertEquals(TransferState.ACKED, store.transfer(t.id).state)
        assertEquals(1.0, registry.counter(ShuttleMetrics.DELIVERIES, "channel", "downstream", "event", "acked", "outcome", "gave_up").count())

        store.redriveDelivery(failed.id)
        tick(5.seconds)
        assertEquals(DeliveryState.DELIVERED, store.outbox.single().state)
        assertEquals(TransferState.DONE, store.transfer(t.id).state)
    }

    @Test
    fun maxAttempts_flips_a_delivery_to_FAILED_with_gave_up() = runTest {
        val channel = RecordingChannel("downstream", DeliveryPolicy(maxAttempts = 2, fullJitter = false), retry503)
        ackedTransfer("a.csv", listOf(downstream))
        backgroundScope.launch { notifier(channel).run() }
        runCurrent()
        assertEquals(DeliveryState.PENDING, store.outbox.single().state)
        tick(30.seconds)
        assertEquals(DeliveryState.FAILED, store.outbox.single().state)
        assertEquals(2, store.outbox.single().attempts)
        assertEquals(1.0, registry.counter(ShuttleMetrics.DELIVERIES, "channel", "downstream", "event", "acked", "outcome", "gave_up").count())
    }

    @Test
    fun backoff_follows_spec_9_3_with_full_jitter_below_the_ceiling_and_the_cap_at_max() = runTest {
        val channel = RecordingChannel("downstream", DeliveryPolicy(maxAttempts = 100, giveUpAfter = 1000.hours), retry503)
        ackedTransfer("a.csv", listOf(downstream))
        backgroundScope.launch { notifier(channel).run() }
        runCurrent()
        val ceilings = listOf(5, 10, 20, 40, 80, 160, 320, 640, 900, 900, 900)
        for ((i, ceiling) in ceilings.withIndex()) {
            val row = store.outbox.single()
            assertEquals(i + 1, row.attempts)
            val delay = java.time.Duration.between(clock.instant(), row.nextAttemptAt).seconds
            assertTrue(delay in 0..ceiling, "attempt ${i + 1}: delay $delay s within [0, $ceiling]")
            tick(15.minutes)
        }
    }

    /** A channel that parks every delivery until [release] completes; the boundary a cancellation or an in-flight test needs. */
    private class ParkingChannel(name: String) : DeliveryChannel {
        override val name = ChannelName(name)
        override val policy = DeliveryPolicy(fullJitter = false)
        val release = CompletableDeferred<Unit>()
        val arrived = AtomicInteger()
        override suspend fun deliver(event: DeliveryEvent): DeliveryOutcome {
            arrived.incrementAndGet()
            release.await()
            return DeliveryOutcome.Delivered("ref")
        }
    }

    @Test
    fun I4_a_delivery_id_is_never_inside_two_workers_at_once() = runTest {
        val channel = ParkingChannel("downstream")
        ackedTransfer("a.csv", listOf(downstream))
        val id = store.outbox.single().id
        val notifier = notifier(channel)
        backgroundScope.launch { notifier.run() }
        runCurrent()
        assertEquals(1, channel.arrived.get())
        notifier.wake()
        tick(30.seconds)
        tick(30.seconds)
        assertEquals(1, channel.arrived.get(), "a parked delivery is never selected again")
        val laterSelects = store.calls.filter { it.method == "due" }.drop(1)
        assertTrue(laterSelects.isNotEmpty())
        assertTrue(laterSelects.all { id in (it.args[1] as Set<*>) }, "every later select excludes the in-flight id")
        channel.release.complete(Unit)
        runCurrent()
        assertEquals(DeliveryState.DELIVERED, store.outbox.single().state)
        assertEquals(0, notifier.inFlightCount)
    }

    @Test
    fun I5_the_in_flight_set_never_exceeds_batch_plus_workers_and_is_empty_when_idle() = runTest {
        val channel = ParkingChannel("downstream")
        repeat(7) { ackedTransfer("f$it.csv", listOf(downstream)) }
        val notifier = notifier(channel, config = NotifierConfig(workers = 1, batch = 2, sweepEvery = 30.seconds))
        backgroundScope.launch { notifier.run() }
        runCurrent()
        tick(30.seconds)
        assertTrue(notifier.inFlightCount in 1..3, "in flight ${notifier.inFlightCount} <= batch 2 + workers 1")
        assertEquals(notifier.inFlightCount.toDouble(), registry.get(ShuttleMetrics.NOTIFIER_INFLIGHT).gauge().value())
        assertEquals(1, channel.arrived.get(), "one worker, so one delivery parked")
        channel.release.complete(Unit)
        repeat(8) { tick(30.seconds) }
        assertEquals(7, store.outbox.count { it.state == DeliveryState.DELIVERED })
        assertEquals(0, notifier.inFlightCount)
        assertEquals(0.0, registry.get(ShuttleMetrics.NOTIFIER_INFLIGHT).gauge().value())
    }

    @Test
    fun I13_two_channels_on_one_event_are_delivered_independently() = runTest {
        val a = RecordingChannel("a", noJitter, ok)
        val b = RecordingChannel("b", noJitter, ok)
        val t = ackedTransfer("a.csv", listOf(ChannelName("a"), ChannelName("b")))
        backgroundScope.launch { notifier(a, b).run() }
        runCurrent()
        assertEquals(listOf(DeliveryState.DELIVERED, DeliveryState.DELIVERED), store.outbox.map { it.state })
        assertEquals(1, a.events.size)
        assertEquals(1, b.events.size)
        assertEquals(TransferState.DONE, store.transfer(t.id).state)
    }

    @Test
    fun S17_two_channels_on_acked_one_always_503() = runTest {
        val a = RecordingChannel("a", noJitter, retry503)
        val b = RecordingChannel("b", noJitter, ok)
        val t = ackedTransfer("a.csv", listOf(ChannelName("a"), ChannelName("b")))
        backgroundScope.launch { notifier(a, b).run() }
        runCurrent()
        tick(30.seconds)
        assertEquals(DeliveryState.PENDING, store.outbox.single { it.channel == ChannelName("a") }.state)
        assertEquals(DeliveryState.DELIVERED, store.outbox.single { it.channel == ChannelName("b") }.state)
        assertEquals(TransferState.ACKED, store.transfer(t.id).state)
        assertEquals(1.0, registry.get(ShuttleMetrics.OUTBOX_PENDING).tag("channel", "a").gauge().value())
        assertEquals(0.0, registry.get(ShuttleMetrics.OUTBOX_PENDING).tag("channel", "b").gauge().value())
        assertEquals(30.0, registry.get(ShuttleMetrics.OUTBOX_OLDEST_SECONDS).tag("channel", "a").gauge().value())
    }

    @Test
    fun S22_one_provider_selected_by_three_rows_is_invoked_once_at_send_time() = runTest {
        val invocations = AtomicInteger()
        val provider = Provider { invocations.incrementAndGet(); com.fasterxml.jackson.databind.ObjectMapper().readTree("""{"id": 7, "name": "order", "total": 9.5}""") }
        val table = MappingTable(listOf(
            MappingRow("order.id", provider = "orderDetails", select = "/id"),
            MappingRow("order.name", provider = "orderDetails", select = "/name"),
            MappingRow("order.total", provider = "orderDetails", select = "/total"),
        ))
        val channel = RecordingChannel("downstream", noJitter, ok)
        ackedTransfer("a.csv", listOf(downstream))
        val notifier = Notifier(store, listOf(channel), mapOf(downstream to table), MappingRenderer { provider }, NotifierConfig(), registry, clock)
        assertEquals(0, invocations.get(), "nothing is rendered before send time")
        backgroundScope.launch { notifier.run() }
        runCurrent()
        assertEquals(1, invocations.get())
        val body = channel.events.single().body
        assertEquals("7", body.at("/order/id").asText())
        assertEquals("order", body.at("/order/name").asText())
        assertEquals("9.5", body.at("/order/total").asText())
    }

    @Test
    fun a_wake_causes_a_select_before_the_sweep_interval_elapses() = runTest {
        val channel = RecordingChannel("downstream", noJitter, ok)
        val notifier = notifier(channel)
        backgroundScope.launch { notifier.run() }
        runCurrent()
        ackedTransfer("a.csv", listOf(downstream))
        tick(1.seconds)
        assertEquals(DeliveryState.PENDING, store.outbox.single().state, "no wake: the row waits for the sweep")
        notifier.wake()
        tick(1.seconds)
        assertEquals(DeliveryState.DELIVERED, store.outbox.single().state, "woken: delivered 2 s in, well before the 30 s sweep")
    }

    @Test
    fun cancellation_mid_delivery_leaves_the_row_PENDING_and_the_set_empty() = runTest {
        val channel = ParkingChannel("downstream")
        ackedTransfer("a.csv", listOf(downstream))
        val notifier = notifier(channel)
        val job = launch { notifier.run() }
        runCurrent()
        assertEquals(1, channel.arrived.get())
        assertEquals(1, notifier.inFlightCount)
        job.cancel()
        runCurrent()
        assertEquals(DeliveryState.PENDING, store.outbox.single().state)
        assertEquals(0, store.outbox.single().attempts)
        assertEquals(0, notifier.inFlightCount)
        assertEquals(0.0, registry.get(ShuttleMetrics.NOTIFIER_INFLIGHT).gauge().value())
    }

    @Test
    fun B9_cancellation_while_a_batch_waits_for_a_permit_leaves_the_set_empty() = runTest {
        val channel = ParkingChannel("downstream")
        repeat(3) { ackedTransfer("f$it.csv", listOf(downstream)) }
        val notifier = notifier(channel, config = NotifierConfig(workers = 1, batch = 3, sweepEvery = 30.seconds))
        val job = launch { notifier.run() }
        runCurrent()
        assertEquals(1, channel.arrived.get(), "one worker parked, two rows of the batch wait for a permit")
        assertEquals(3, notifier.inFlightCount)
        job.cancel()
        runCurrent()
        assertEquals(0, notifier.inFlightCount, "the ids no worker owns leave the set with the cancelled sweep")
        assertEquals(listOf(DeliveryState.PENDING, DeliveryState.PENDING, DeliveryState.PENDING), store.outbox.map { it.state })
    }

    @Test
    fun B2_a_store_failure_during_the_select_is_logged_and_the_next_sweep_delivers() = runTest {
        val channel = RecordingChannel("downstream", noJitter, ok)
        ackedTransfer("a.csv", listOf(downstream))
        val job = backgroundScope.launch { notifier(channel, store = FailsOnce(store, "due")).run() }
        runCurrent()
        assertEquals(DeliveryState.PENDING, store.outbox.single().state, "the select threw: nothing delivered")
        assertTrue(job.isActive, "run survives the failure")
        tick(30.seconds)
        assertEquals(DeliveryState.DELIVERED, store.outbox.single().state, "the sweep after sweepEvery delivers")
        assertEquals(1, channel.events.size)
        assertTrue(job.isActive)
    }

    @Test
    fun SPEC4_a_delivery_that_could_not_be_recorded_is_logged_with_the_transfer_id_route_and_channel_in_the_MDC() = runTest {
        val channel = RecordingChannel("downstream", noJitter, ok)
        val t = ackedTransfer("a.csv", listOf(downstream))
        val logs = LogCapture()
        logs.use {
            backgroundScope.launch { notifier(channel, store = FailsOnce(store, "delivered")).run() }
            runCurrent()
        }

        val warn = logs.warnings().single { it.message.contains("the row stays PENDING") }
        assertEquals(t.id.value.toString(), warn.mdc[Mdc.TRANSFER_ID], warn.message)
        assertEquals("drop", warn.mdc[Mdc.ROUTE])
        assertEquals("downstream", warn.mdc[Mdc.CHANNEL])
    }

    @Test
    fun B2_a_store_failure_recording_a_delivery_leaves_the_row_PENDING_and_a_later_sweep_records_it_once() = runTest {
        val channel = RecordingChannel("downstream", noJitter, ok, ok)
        ackedTransfer("a.csv", listOf(downstream))
        val notifier = notifier(channel, store = FailsOnce(store, "delivered"))
        val job = backgroundScope.launch { notifier.run() }
        runCurrent()
        assertEquals(1, channel.events.size, "the channel accepted it")
        assertEquals(DeliveryState.PENDING, store.outbox.single().state, "the transition threw: the row is untouched")
        assertEquals(0, store.outbox.single().attempts)
        assertEquals(0, notifier.inFlightCount)
        assertTrue(job.isActive, "run survives the failure")
        tick(30.seconds)
        val row = store.outbox.single()
        assertEquals(DeliveryState.DELIVERED, row.state, "the next sweep delivers it again (at least once, spec 9.7)")
        assertEquals(1, row.attempts)
        assertEquals(2, channel.events.size)
        assertEquals(1, store.calls.count { it.method == "delivered" }, "recorded once")
        tick(30.seconds)
        assertEquals(2, channel.events.size, "never a third call")
        assertTrue(job.isActive)
    }
}
