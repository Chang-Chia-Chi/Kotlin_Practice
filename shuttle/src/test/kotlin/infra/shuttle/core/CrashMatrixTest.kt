package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.HookDriver
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import infra.shuttle.testkit.RecordingChannel
import infra.shuttle.testkit.ScriptedFetcher
import infra.shuttle.testkit.ScriptedSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Spec 4.4 replayed on the fakes (I8): the process dies at one hook point, the next trigger runs from the same
 * state store and target, and the end state is the table's row: at most one extra store, at most one extra
 * delivery per channel per event, never a lost object. One `I8_` test per row; S2 to S6 by id.
 */
class CrashMatrixTest {
    @TempDir lateinit var staging: Path
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val target = InMemoryTarget("landing")
    private val source = ScriptedSource(clock)
    private val fetcher = ScriptedFetcher(clock).file("a.csv", "a,b\n".toByteArray())
    private val registry = SimpleMeterRegistry()
    private val hook = HookDriver()
    private val downstream = RecordingChannel("downstream")
    private val notifier by lazy {
        Notifier(store, listOf(downstream), emptyMap(), MappingRenderer(), NotifierConfig(workers = 1, batch = 10, sweepEvery = 30.seconds), registry, clock, hook = hook)
    }

    private val file = ScriptedSource.identity("a.csv")
    private val polled = Route(
        name = "drop", source = Source.Poll("sftp", "/in", 1.minutes, onAck = AckAction.Move("done")),
        target = Target("minio", bucket = "landing"), notify = listOf(Notify(DeliveryMoment.ACKED, "downstream")),
    )

    // the chain hops to its bounded IO view (spec 3.3); unconfined here keeps the hop inside runTest's scheduler
    private val noChain = ProcessingChain(emptyList(), DigestAlgorithm.MD5, Dispatchers.Unconfined)

    private fun runner(route: Route = polled, chain: ProcessingChain = noChain): RouteRunner {
        val ledger = Ledger(store, route.notify) { notifier.wake() }
        val pipeline = TransferPipeline(
            route, DigestAlgorithm.MD5, ledger, target, chain, emptyMap(), { true },
            hook, clock, registry, Staging(staging), usableSpace = { 10.gib },
        )
        return RouteRunner(route, pipeline, fetcher, ledger, clock, registry)
    }

    /** One poll listing the file: the trigger every polled row starts from and, replayed, the next poll while the file is still there. */
    private val poll: Flow<RouteEvent> by lazy { source.seen(file).pollCompleted(setOf(file)).events() }

    private val subscribed = polled.copy(source = Source.Subscribe("nats", "images", onAck = AckAction.Ack), fetch = Fetch("minio", "/path"))
    private val message = SourceIdentity(RouteName("drop"), SourceKind.NATS, "nats:images", "msg-1", null, null)
    /** One message: the subscribed row's trigger and, replayed, the broker's redelivery of the unacked message. */
    private val redelivery: Flow<RouteEvent> by lazy { source.seen(message, SourceView("images", """{"path":"a.csv"}""".toByteArray())).events() }

    /** Spec 13.1's image-sets route: the message names a metadata file listing two images, each a child on the target; uploads one at a time. */
    private val imageSets = polled.copy(source = Source.Subscribe("nats", "images", onAck = AckAction.Ack), fetch = Fetch("minio", "/metadata"), parallelism = 1)
    private val imageChain = ProcessingChain(listOf(processorFor(ProcessorSpec.Expand(ExpandFormat.Json, "/images", "minio")) { null }), DigestAlgorithm.MD5, Dispatchers.Unconfined)
    /** One message: the parent's trigger and, replayed, the broker's redelivery of it. */
    private val set: Flow<RouteEvent> by lazy {
        fetcher.file("set.json", """{"images":["img/1.png","img/2.png"]}""".toByteArray()).file("img/1.png", "one".toByteArray()).file("img/2.png", "two".toByteArray())
        source.seen(message, SourceView("images", """{"metadata":"set.json"}""".toByteArray())).events()
    }

    /** The process dies at [point]: the pipeline parked there sees a `CancellationException`; the trigger's flow finishes. */
    private suspend fun TestScope.crashAt(point: HookPoint, events: Flow<RouteEvent>, route: Route = polled, chain: ProcessingChain = noChain): TransferId {
        hook.pauseAt(point)
        val run = launch { runner(route, chain).run(events) }
        val id = hook.awaitArrival(point)
        hook.crash(point)
        advanceUntilIdle()
        assertTrue(run.isCompleted, "the runner returns once the crashed pipeline is gone")
        assertEquals(0L, Files.list(staging).count(), "staging holds nothing after the crash (I9)")
        return id
    }

    /**
     * The process dies while one child is parked at [point]: the runner's job goes, and with it every sibling upload
     * under it. `hook.crash` would kill the parked child alone and its sibling would carry on, which is a retry, not a death.
     */
    private suspend fun TestScope.dieAt(point: HookPoint, events: Flow<RouteEvent>): TransferId {
        hook.pauseAt(point)
        val run = launch { runner(imageSets, imageChain).run(events) }
        val child = hook.awaitArrival(point)
        run.cancel()
        hook.resume(point)
        advanceUntilIdle()
        assertEquals(0L, Files.list(staging).count(), "staging holds nothing after the death (I9)")
        return child
    }

    /** The invariant of spec 4.4 for a parent of two: both children and the parent DONE, [stores] stores in all, one delivery, every child's copy current. */
    private suspend fun assertConvergedSet(stores: Int) {
        val parent = store.transfers.single { it.kind == TransferKind.MESSAGE }
        val children = store.childrenOf(parent.id)
        assertEquals(TransferState.DONE, parent.state)
        assertEquals(listOf(TransferState.DONE, TransferState.DONE), children.map { it.state })
        assertEquals(stores, stores(), "stores across both runs")
        assertEquals(listOf(DeliveryState.DELIVERED), ackedRows().map { it.state }, "one acked delivery row, DELIVERED once")
        assertEquals(1, downstream.events.size, "channel calls")
        assertEquals(listOf("one", "two"), listOf("1.png", "2.png").map { String(target.bytes(it)) })
        assertTrue(children.all { target.verify(it.target!!) }, "every child's reference is the current object")
        assertEquals(listOf(message), source.acks, "the message is acked once")
    }

    @Test
    fun I8_S28_after_the_first_childs_store_before_its_ledger_the_redelivery_stores_it_again_and_the_rest_once() = runTest {
        val child = dieAt(HookPoint.afterStore, set)
        val parent = store.transfer(child).parentId!!
        assertEquals(TransferState.PROCESSED, store.transfer(parent).state)
        assertEquals(listOf(TransferState.FETCHED, TransferState.FETCHED), store.childrenOf(parent).map { it.state }, "the ledger never saw the copy")
        assertEquals(setOf("1.png"), target.keys, "one copy on the target")

        runner(imageSets, imageChain).run(set)
        deliver()

        assertEquals(listOf("store", "store", "store"), target.calls.map { it.method }, "the first child again, then the second")
        assertConvergedSet(stores = 3)
    }

    @Test
    fun I8_S28_after_the_first_childs_ledger_the_redelivery_verifies_it_and_stores_only_the_rest() = runTest {
        val child = dieAt(HookPoint.afterLedgerStored, set)
        val parent = store.transfer(child).parentId!!
        assertEquals(TransferState.PROCESSED, store.transfer(parent).state)
        assertEquals(listOf(TransferState.STORED, TransferState.FETCHED), store.childrenOf(parent).map { it.state }, "half the children stored")

        runner(imageSets, imageChain).run(set)
        deliver()

        assertEquals(listOf(InMemoryTarget.Call("store", "1.png"), InMemoryTarget.Call("verify", "1.png"), InMemoryTarget.Call("store", "2.png")), target.calls)
        assertEquals(listOf(child), store.childrenOf(parent).map { it.id }.take(1), "the stored child kept its row")
        assertConvergedSet(stores = 2)
    }

    @Test
    fun I23_S32_a_parent_redelivered_after_ledger_ACKED_is_reacked_with_every_child_verified_and_no_new_outbox_rows() = runTest {
        val id = crashAt(HookPoint.afterLedgerAcked, set, imageSets, imageChain)
        assertEquals(TransferState.ACKED, store.transfer(id).state)
        assertTrue(store.childrenOf(id).all { it.state == TransferState.ACKED })
        val outbox = store.outbox
        assertEquals(listOf(DeliveryState.PENDING), ackedRows().map { it.state })
        assertTrue(source.acks.isEmpty(), "the broker was never acked")
        val fetches = fetcher.calls.size

        runner(imageSets, imageChain).run(set)
        assertEquals(1.0, registry.counter(ShuttleMetrics.TRANSFERS, "route", "drop", "outcome", "reacked").count())
        assertEquals(fetches, fetcher.calls.size, "no fetch for a redelivered message")
        assertEquals(listOf(InMemoryTarget.Call("verify", "1.png"), InMemoryTarget.Call("verify", "2.png")), target.calls.drop(2), "every child verified, nothing stored")
        assertEquals(outbox, store.outbox, "exactly the outbox rows the ledger wrote before the crash")
        deliver()

        assertConvergedSet(stores = 2)
    }

    /** The notifier of the restarted process delivers what is PENDING. */
    private fun TestScope.deliver() {
        backgroundScope.launch { notifier.run() }
        runCurrent()
    }

    private fun stores() = target.calls.count { it.method == "store" }
    private fun ackedRows() = store.outbox.filter { it.moment == DeliveryMoment.ACKED && it.channel == downstream.name }

    /** The invariant of spec 4.4: DONE, [stores] stores in all, one delivery row delivered once, [sent] channel calls, the object at the key. */
    private suspend fun assertConverged(stores: Int, sent: Int = 1) {
        val row = store.transfers.single()
        assertEquals(TransferState.DONE, row.state)
        assertEquals(stores, stores(), "stores across both runs")
        assertEquals(listOf(DeliveryState.DELIVERED), ackedRows().map { it.state }, "one acked delivery row per channel, DELIVERED once")
        assertEquals(sent, downstream.events.size, "channel calls")
        assertEquals("a,b\n", String(target.bytes("a.csv")), "the object reached the target")
        assertTrue(target.verify(row.target!!), "the row's reference is the current object")
    }

    @Test
    fun I8_after_fetch_the_next_poll_runs_fully_with_no_extra_store_and_no_extra_delivery() = runTest {
        val id = crashAt(HookPoint.afterFetch, poll)
        assertEquals(TransferState.FETCHED, store.transfer(id).state)
        assertEquals(0, stores())

        runner().run(poll)
        deliver()

        assertEquals(2, fetcher.calls.size, "full run: fetched again")
        assertEquals(listOf(file), source.acks)
        assertConverged(stores = 1)
    }

    @Test
    fun I8_after_process_the_next_poll_runs_fully_with_no_extra_store_and_no_extra_delivery() = runTest {
        val id = crashAt(HookPoint.afterProcess, poll)
        assertEquals(TransferState.PROCESSED, store.transfer(id).state)
        assertEquals(0, stores())

        runner().run(poll)
        deliver()

        assertEquals(2, fetcher.calls.size, "full run: fetched again")
        assertEquals(listOf(file), source.acks)
        assertConverged(stores = 1)
    }

    @Test
    fun I8_S2_after_store_before_ledger_the_next_poll_stores_again_one_extra_store_and_no_extra_delivery() = runTest {
        val id = crashAt(HookPoint.afterStore, poll)
        assertEquals(TransferState.PROCESSED, store.transfer(id).state, "the ledger never saw the copy")
        assertEquals(1, stores())
        assertEquals(setOf("a.csv"), target.keys, "one copy on the target")

        runner().run(poll)
        deliver()

        assertEquals(listOf(file), source.acks)
        assertConverged(stores = 2)
        assertEquals("v2", store.transfers.single().target!!.ref, "the row points at the second copy, the first is the non-current version")
    }

    @Test
    fun I8_S3_after_ledger_STORED_the_next_poll_verifies_and_acks_with_no_second_store_and_no_extra_delivery() = runTest {
        val id = crashAt(HookPoint.afterLedgerStored, poll)
        assertEquals(TransferState.STORED, store.transfer(id).state)
        assertTrue(source.acks.isEmpty(), "the file was never moved")

        runner().run(poll)
        deliver()

        assertEquals(1, fetcher.calls.size, "no second fetch")
        assertEquals(listOf(InMemoryTarget.Call("store", "a.csv"), InMemoryTarget.Call("verify", "a.csv")), target.calls)
        assertEquals(listOf(file), source.acks)
        assertConverged(stores = 1)
    }

    /** The move is visible to the next listing, so the file is gone from it; the repair is reconciliation's, not the pipeline's. */
    @Test
    fun I8_S4_poll_move_before_ledger_is_repaired_by_reconciliation_on_the_next_poll_with_a_delayed_delivery() = runTest {
        val id = crashAt(HookPoint.afterAck, poll)
        assertEquals(TransferState.STORED, store.transfer(id).state)
        assertEquals(listOf(file), source.acks, "moved")
        assertTrue(store.outbox.isEmpty(), "no delivery yet: it is delayed until the repair")

        clock.advance(1.minutes)
        runner().run(ScriptedSource(clock).pollCompleted(emptySet()).events())
        assertEquals(TransferState.ACKED, store.transfer(id).state, "reconciliation wrote ACKED")
        assertEquals(1, fetcher.calls.size, "the pipeline fetched nothing on the second poll")
        assertEquals(1, target.calls.size, "the pipeline stored and verified nothing on the second poll")
        assertEquals(listOf(file), source.acks, "the pipeline acked nothing on the second poll")
        assertEquals(1.0, registry.counter(ShuttleMetrics.RECONCILED, "route", "drop").count())
        deliver()

        assertConverged(stores = 1)
    }

    @Test
    fun I8_subscribe_ledger_ACKED_before_broker_ack_is_repaired_by_the_redelivery_reacked_with_no_new_deliveries() = runTest {
        val id = crashAt(HookPoint.afterLedgerAcked, redelivery, subscribed)
        assertEquals(TransferState.ACKED, store.transfer(id).state)
        assertEquals(listOf(DeliveryState.PENDING), ackedRows().map { it.state })
        assertTrue(source.acks.isEmpty(), "the broker was never acked")

        runner(subscribed).run(redelivery)
        assertEquals(listOf(message), source.acks, "the broker is acked again")
        assertEquals(1.0, registry.counter(ShuttleMetrics.TRANSFERS, "route", "drop", "outcome", "reacked").count())
        assertEquals(1, fetcher.calls.size, "no fetch for a redelivered message")
        assertEquals(listOf(InMemoryTarget.Call("store", "a.csv"), InMemoryTarget.Call("verify", "a.csv")), target.calls)
        deliver()

        assertConverged(stores = 1)
    }

    @Test
    fun I8_after_ledger_ACKED_the_notifier_delivers_and_the_next_poll_does_nothing() = runTest {
        val id = crashAt(HookPoint.afterLedgerAcked, poll)
        assertEquals(TransferState.ACKED, store.transfer(id).state)
        assertEquals(listOf(DeliveryState.PENDING), ackedRows().map { it.state })
        assertEquals(listOf(file), source.acks, "moved before the crash")

        clock.advance(1.minutes)
        runner().run(ScriptedSource(clock).pollCompleted(emptySet()).events())
        assertEquals(0.0, registry.counter(ShuttleMetrics.RECONCILED, "route", "drop").count(), "an ACKED row is not reconciliation's")
        deliver()

        assertEquals(1, fetcher.calls.size)
        assertEquals(listOf(InMemoryTarget.Call("store", "a.csv")), target.calls)
        assertEquals(listOf(file), source.acks)
        assertConverged(stores = 1)
    }

    /** The channel answered, the process died before `delivered` was written: the restarted notifier sends once more (deduplicated downstream). */
    @Test
    fun I8_S5_delivery_sent_before_ledger_is_delivered_again_two_calls_one_transfer_id_and_the_row_DELIVERED_once() = runTest {
        runner().run(poll)
        val id = store.transfers.single().id
        assertEquals(TransferState.ACKED, store.transfer(id).state)

        hook.pauseAt(HookPoint.afterDeliverySent)
        val process = launch { notifier.run() }
        assertEquals(id, hook.awaitArrival(HookPoint.afterDeliverySent))
        process.cancel()
        hook.resume(HookPoint.afterDeliverySent)
        advanceUntilIdle()
        assertEquals(listOf(DeliveryState.PENDING), ackedRows().map { it.state }, "sent but never recorded")
        assertEquals(1, downstream.events.size)
        assertEquals(0, notifier.inFlightCount, "the in-flight set died with the process")

        val restarted = Notifier(store, listOf(downstream), emptyMap(), MappingRenderer(), NotifierConfig(workers = 1, batch = 10, sweepEvery = 30.seconds), registry, clock)
        backgroundScope.launch { restarted.run() }
        runCurrent()

        assertEquals(listOf(id to 1, id to 1), downstream.events.map { it.transferId to it.attempt }, "two calls with one transfer id, both the first recorded attempt")
        assertEquals(1, ackedRows().single().attempts)
        assertConverged(stores = 1, sent = 2)
    }

    /** Not a row of the matrix but its neighbour: the crash left STORED and the copy went missing (here: overwritten) before the next poll. */
    @Test
    fun S6_copy_missing_at_STORED_runs_fully_on_the_same_row_and_reaches_DONE() = runTest {
        val id = crashAt(HookPoint.afterLedgerStored, poll)
        target.store("a.csv", Files.writeString(staging.resolve("other"), "someone else\n"), emptyMap())
        Files.delete(staging.resolve("other"))

        runner().run(poll)
        deliver()

        assertEquals(id, store.transfers.single().id, "the same row")
        assertEquals(listOf("store", "store", "verify", "store"), target.calls.map { it.method })
        assertEquals(2, fetcher.calls.size, "full run: fetched again")
        assertEquals(listOf(file), source.acks)
        assertConverged(stores = 3)
    }
}
