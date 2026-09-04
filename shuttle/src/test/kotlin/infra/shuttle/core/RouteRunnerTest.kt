package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.HookDriver
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import infra.shuttle.testkit.ScriptedFetcher
import infra.shuttle.testkit.ScriptedSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 4.1 (bounded pipelines), 4.6 (reconciliation) and 11 (poll failures) on the fakes: one route's event flow end to end. */
class RouteRunnerTest {
    @TempDir lateinit var staging: Path
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val target = InMemoryTarget("landing")
    private val source = ScriptedSource(clock)
    private val fetcher = ScriptedFetcher(clock).file("a.csv", "a\n".toByteArray()).file("b.csv", "b\n".toByteArray()).file("c.csv", "c\n".toByteArray())
    private val registry = SimpleMeterRegistry()

    private fun route(parallelism: Int = 1, notify: List<Notify> = emptyList()) = Route(
        name = "drop", source = Source.Poll("sftp", "/in", 1.minutes, onAck = AckAction.Move("done")),
        target = Target("minio", bucket = "landing"), notify = notify, parallelism = parallelism,
    )

    private fun runner(route: Route = route(), hook: Hook = Hook.None): RouteRunner {
        // the chain hops to its bounded IO view (spec 3.3); unconfined here keeps the hop inside runTest's scheduler
        val ledger = Ledger(store, route.notify) {}
        val pipeline = TransferPipeline(route, DigestAlgorithm.MD5, ledger, target, ProcessingChain(emptyList(), DigestAlgorithm.MD5, Dispatchers.Unconfined), emptyMap(), { true }, hook, clock, registry, Staging(staging), usableSpace = { 10.gib })
        return RouteRunner(route, pipeline, fetcher, ledger, clock, registry)
    }

    private fun id(name: String) = ScriptedSource.identity(name)

    @Test
    fun one_Seen_on_a_mirror_route_runs_one_pipeline_to_DONE() = runTest {
        runner().run(source.seen(id("a.csv")).events())

        assertEquals(TransferState.DONE, store.transfers.single().state)
        assertEquals(listOf(id("a.csv")), source.acks)
    }

    private fun inflight() = registry.find(ShuttleMetrics.INFLIGHT).tag("route", "drop").gauge()!!.value()
    private fun polls(result: String) = registry.counter(ShuttleMetrics.POLLS, "route", "drop", "result", result).count()

    /** The store-wide cap of I19 is the connector pool's and rule 9's; the runner's share is the bound per route. */
    @Test
    fun I19_with_parallelism_plus_one_objects_at_most_parallelism_pipelines_run_at_once() = runTest {
        val hook = HookDriver().apply { pauseAt(HookPoint.afterFetch) }
        val events = source.seen(id("a.csv")).seen(id("b.csv")).seen(id("c.csv")).events()
        val run = launch { runner(route(parallelism = 2), hook).run(events) }
        advanceUntilIdle()

        assertEquals(2.0, inflight(), "two pipelines parked at afterFetch, the third waits for a permit")
        assertEquals(2, fetcher.calls.size)
        assertTrue(run.isActive)

        hook.resume(HookPoint.afterFetch)
        advanceUntilIdle()
        assertTrue(run.isCompleted, "run returns once the flow is done and every pipeline has finished")
        assertEquals(0.0, inflight())
        assertEquals(3, store.transfers.count { it.state == TransferState.DONE })
    }

    @Test
    fun a_poll_failure_or_skip_is_counted_and_never_cancels_a_running_pipeline() = runTest {
        val hook = HookDriver().apply { pauseAt(HookPoint.afterFetch) }
        val events = source.seen(id("a.csv")).pollFailed(IllegalStateException("listing failed")).pollSkipped().events()
        launch { runner(hook = hook).run(events) }
        advanceUntilIdle()
        assertEquals(1.0, polls("failed"))
        assertEquals(1.0, polls("skipped"))
        assertEquals(1.0, inflight(), "the pipeline is still parked")

        hook.resume(HookPoint.afterFetch)
        advanceUntilIdle()
        assertEquals(TransferState.DONE, store.transfers.single().state)
    }

    /** A row parked at STORED, as a crash after the move but before the ACKED ledger write leaves it (S4). */
    private suspend fun storedRow(name: String): Transfer {
        val t = store.seen(id(name), TransferKind.OBJECT)
        store.fetched(t.id, StagedSummary(name, 1, clock.instant(), Digest(DigestAlgorithm.MD5, "d"), null), emptyList())
        store.processed(t.id, emptyMap())
        store.stored(t.id, TargetRef("memory", "landing", name, "v1", 1), emptyList())
        return store.transfer(t.id)
    }

    private fun reconciled() = registry.counter(ShuttleMetrics.RECONCILED, "route", "drop").count()
    private fun reconcileSkipped() = registry.counter(ShuttleMetrics.RECONCILE_SKIPPED, "route", "drop").count()

    @Test
    fun S4_a_complete_poll_acks_exactly_the_STORED_rows_older_than_its_start_and_absent_from_the_listing() = runTest {
        val unlisted = storedRow("moved.csv")
        val listed = storedRow("still-there.csv")
        clock.advance(1.minutes)
        val events = source.pollCompleted(setOf(id("still-there.csv"), id("young.csv"))).events()
        clock.advance(1.minutes)
        val young = storedRow("young.csv")

        runner(route(notify = listOf(Notify(DeliveryMoment.ACKED, "downstream")))).run(events)

        assertEquals(TransferState.ACKED, store.transfer(unlisted.id).state)
        assertEquals(listed, store.transfer(listed.id), "listed rows are untouched")
        assertEquals(young, store.transfer(young.id), "rows updated after the poll started are untouched")
        val delivery = store.outbox.single()
        assertEquals(unlisted.id to DeliveryMoment.ACKED, delivery.transferId to delivery.moment, "the acked delivery rides the same transition as stage 4")
        assertEquals(1.0, reconciled())
        assertEquals(1.0, polls("completed"))
    }

    @Test
    fun S14_a_truncated_listing_skips_reconciliation_and_counts_it() = runTest {
        val unlisted = storedRow("moved.csv")
        clock.advance(1.minutes)

        runner().run(source.pollCompleted(emptySet(), truncated = true).events())

        assertEquals(unlisted, store.transfer(unlisted.id))
        assertEquals(1.0, reconcileSkipped())
        assertEquals(0.0, reconciled())
        assertEquals(1.0, polls("completed"))
    }

    @Test
    fun the_stuck_gauge_is_refreshed_at_every_poll_completion() = runTest {
        val parked = store.seen(id("stuck.csv"), TransferKind.OBJECT)
        clock.advance(5.minutes)
        val runner = runner(route().copy(stuckAfter = 3.minutes))

        runner.run(source.pollCompleted(setOf(id("stuck.csv"))).events())
        assertEquals(1.0, stuck())

        store.rejected(parked.id, "gone")
        runner.run(ScriptedSource(clock).pollCompleted(emptySet()).events())
        assertEquals(0.0, stuck())
    }

    private fun stuck() = registry.find(ShuttleMetrics.STUCK_TRANSFERS).tag("route", "drop").gauge()!!.value()

    private fun subscribed(stuckAfter: kotlin.time.Duration? = null) = Route(
        name = "drop", source = Source.Subscribe("events", "images.ready", onAck = AckAction.Ack),
        fetch = Fetch("minio", "/path"), target = Target("minio", bucket = "landing"), stuckAfter = stuckAfter,
    )

    /** Spec 11: a subscribed route has no poll to hang the refresh on, so it beats on its own `inProgressEvery` (D51). */
    @Test
    fun SPEC5_a_subscribed_route_refreshes_the_stuck_gauge_on_its_own_interval_without_any_poll() = runTest {
        store.seen(id("stuck.csv"), TransferKind.OBJECT)
        clock.advance(5.minutes)
        val run = launch { runner(subscribed(stuckAfter = 3.minutes)).run(MutableSharedFlow()) }

        advanceTimeBy(11.seconds) // one inProgressEvery, which defaults to 10s
        assertEquals(1.0, stuck())

        run.cancel()
        store.seen(id("later.csv"), TransferKind.OBJECT)
        clock.advance(5.minutes)
        advanceTimeBy(1.minutes)
        assertEquals(1.0, stuck(), "the refresh stops with the route")
    }

    /** Spec 11: `stuckAfter` omitted is three trigger intervals, so the gauge exists on every route (D51). */
    @Test
    fun SPEC5_a_route_without_stuckAfter_counts_a_transfer_older_than_three_trigger_intervals() = runTest {
        store.seen(id("stuck.csv"), TransferKind.OBJECT)
        clock.advance(2.minutes) // the route polls every minute
        val runner = runner()

        runner.run(ScriptedSource(clock).pollCompleted(emptySet()).events())
        assertEquals(0.0, stuck(), "younger than three poll intervals")

        clock.advance(2.minutes)
        runner.run(ScriptedSource(clock).pollCompleted(emptySet()).events())
        assertEquals(1.0, stuck())
    }

    @Test
    fun RouteDown_ends_the_run_with_its_cause_once_the_in_flight_pipelines_have_finished() = runTest {
        val hook = HookDriver().apply { pauseAt(HookPoint.afterFetch) }
        val cause = IllegalStateException("connector fatal")
        val run = async { runCatching { runner(hook = hook).run(source.seen(id("a.csv")).routeDown(cause).events()) } }
        advanceUntilIdle()
        assertTrue(run.isActive, "the runner waits for the parked pipeline")
        assertEquals(1.0, inflight())

        hook.resume(HookPoint.afterFetch)
        assertSame(cause, run.await().exceptionOrNull())
        assertEquals(TransferState.DONE, store.transfers.single().state)
        assertEquals(0.0, inflight())
    }

    @Test
    fun cancelling_the_run_cancels_the_pipelines_and_releases_every_permit_and_the_gauge() = runTest {
        val hook = HookDriver().apply { pauseAt(HookPoint.afterFetch) }
        val runner = runner(hook = hook)
        val run = launch { runner.run(source.seen(id("a.csv")).events()) }
        advanceUntilIdle()
        assertEquals(1.0, inflight())

        run.cancel()
        advanceUntilIdle()
        assertEquals(0.0, inflight())
        assertEquals(0L, Files.list(staging).count(), "staging holds no file outside a running pipeline (I9)")
        assertEquals(TransferState.FETCHED, store.transfers.single().state)

        hook.resume(HookPoint.afterFetch) // disarm the gate; the cancelled pipeline is already gone
        runner.run(source.events()) // the same runner, and its one permit, serve the next run
        assertEquals(TransferState.DONE, store.transfers.single().state)
    }

    /** The state store of S16: while [down], every read the runner or the pipeline opens with throws. */
    private class Unavailable(private val inner: StateStore) : StateStore by inner {
        @Volatile var down = false
        private fun check() { if (down) throw IOException("injected: state store unavailable") }
        override suspend fun find(identity: SourceIdentity) = check().let { inner.find(identity) }
        override suspend fun unlisted(route: RouteName, olderThan: Instant, listed: Set<SourceIdentity>) = check().let { inner.unlisted(route, olderThan, listed) }
        override suspend fun stuck(route: RouteName, olderThan: Instant) = check().let { inner.stuck(route, olderThan) }
    }

    @Test
    fun S16_a_poll_with_the_state_store_unavailable_completes_nothing_and_the_next_poll_completes_all() = runTest {
        val flaky = Unavailable(store).apply { down = true }
        val route = route(parallelism = 2).copy(stuckAfter = 3.minutes)
        val ledger = Ledger(flaky, route.notify) {}
        val pipeline = TransferPipeline(route, DigestAlgorithm.MD5, ledger, target, ProcessingChain(emptyList(), DigestAlgorithm.MD5, Dispatchers.Unconfined), emptyMap(), { true }, Hook.None, clock, registry, Staging(staging), usableSpace = { 10.gib })
        val runner = RouteRunner(route, pipeline, fetcher, ledger, clock, registry)
        val poll = source.seen(id("a.csv")).seen(id("b.csv")).pollCompleted(setOf(id("a.csv"), id("b.csv"))).events()

        runner.run(poll)
        assertEquals(listOf(ScriptedSource.Nack(id("a.csv"), true), ScriptedSource.Nack(id("b.csv"), true)), source.nacks)
        assertTrue(store.transfers.isEmpty() && target.calls.isEmpty(), "nothing stored")
        assertEquals(1.0, polls("completed"), "the runner survived the failed reconciliation")

        flaky.down = false
        runner.run(poll)
        assertEquals(listOf(id("a.csv"), id("b.csv")), source.acks)
        assertTrue(store.transfers.all { it.state == TransferState.DONE })
        assertEquals(2, source.nacks.size)
    }
}
