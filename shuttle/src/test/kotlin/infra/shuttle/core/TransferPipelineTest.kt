package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.HookDriver
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import infra.shuttle.testkit.RecordingChannel
import infra.shuttle.testkit.ScriptedFetcher
import infra.shuttle.testkit.ScriptedSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 4.1 to 4.5 on the fakes: one source object through stages 0 to 4. */
class TransferPipelineTest {
    @TempDir lateinit var staging: Path
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val target = InMemoryTarget("landing")
    private val source = ScriptedSource(clock)
    private val fetcher = ScriptedFetcher(clock).file("a.csv", "a,b\n".toByteArray())
    private val registry = SimpleMeterRegistry()
    private var wakes = 0
    private var freeBytes = 10.gib

    private fun route(notify: List<Notify> = emptyList()) = Route(
        name = "drop", source = Source.Poll("sftp", "/in", 1.minutes, onAck = AckAction.Move("done")),
        target = Target("minio", bucket = "landing", key = "{name}"), notify = notify,
    )

    private fun pipeline(
        route: Route = route(), processors: List<Processor> = emptyList(), hook: Hook = Hook.None,
        channels: Map<ChannelName, DeliveryChannel> = emptyMap(), bodies: Map<ChannelName, MappingTable> = emptyMap(),
    ) = TransferPipeline(
        route, DigestAlgorithm.MD5, store, target, ProcessingChain(processors, DigestAlgorithm.MD5), bodies, { true },
        { wakes++ }, hook, clock, registry, Staging(staging), { freeBytes }, channels,
    )

    private suspend fun seen(name: String = "a.csv"): RouteEvent.Seen =
        source.seen(ScriptedSource.identity(name)).events().toList().last() as RouteEvent.Seen

    private fun stagingIsEmpty() = assertEquals(0L, Files.list(staging).count(), "staging holds no file outside a running pipeline (I9)")

    /** Spec 13.1's image-sets route on the fakes: a message names a metadata file, expand fans it out, children go to the target. */
    private val message = SourceIdentity(RouteName("drop"), SourceKind.NATS, "nats:images", "msg-1", null, null)
    private fun imageSets(notify: List<Notify> = emptyList(), parallelism: Int = 2) = route(notify).copy(
        source = Source.Subscribe("nats", "images", onAck = AckAction.Ack), fetch = Fetch("minio", "/metadata/path"), parallelism = parallelism,
    )
    private val imageChain = listOf(
        processorFor(ProcessorSpec.Extract(ExtractFrom.Message, json = mapOf("batchId" to "/batchId"))) { null },
        processorFor(ProcessorSpec.Expand("json", "/images[*].path", "minio")) { null },
    )
    private suspend fun imageMessage(): RouteEvent.Seen {
        fetcher.file("sets/set.json", """{"images":[{"path":"img/1.png"},{"path":"img/2.png"}]}""".toByteArray())
            .file("img/1.png", "one".toByteArray()).file("img/2.png", "two".toByteArray())
        val body = """{"batchId":"b7","metadata":{"path":"sets/set.json"}}""".toByteArray()
        return source.seen(message, SourceView("images", body)).events().toList().last() as RouteEvent.Seen
    }

    @Test
    fun S27_image_sets_happy_path_a_message_expands_into_children_stored_in_parallel_acked_once_with_fetched_and_acked_delivered_once_each() = runTest {
        val event = imageMessage()
        pipeline(imageSets(listOf(Notify(DeliveryMoment.FETCHED, "upstream"), Notify(DeliveryMoment.ACKED, "downstream"))), imageChain).run(event, fetcher)

        val parent = store.transfers.single { it.kind == TransferKind.MESSAGE }
        val children = store.transfers.filter { it.parentId == parent.id }
        assertEquals(TransferState.ACKED, parent.state)
        assertEquals(mapOf("batchId" to "b7"), parent.attributes)
        assertEquals(listOf("1.png", "2.png"), children.map { it.target!!.key })
        assertTrue(children.all { it.state == TransferState.ACKED }, "children follow the parent's ack")
        assertEquals("one", String(target.bytes("1.png")))
        assertEquals(listOf("sets/set.json", "img/1.png", "img/2.png"), fetcher.calls.map { it.path }, "the metadata through fetch.path, each image through ctx.fetch")
        assertEquals(listOf(message), source.acks, "the message is acked once")
        assertEquals(listOf(DeliveryMoment.FETCHED to parent.id, DeliveryMoment.ACKED to parent.id), store.outbox.map { it.moment to it.transferId }, "fetched and acked once each, on the parent")
        stagingIsEmpty()
    }

    @Test
    fun S28_half_the_children_stored_the_redelivery_verifies_them_stores_the_rest_and_acks_the_message_once() = runTest {
        val event = imageMessage()
        val pipeline = pipeline(imageSets(parallelism = 1), imageChain)
        target.failNextStore = true
        pipeline.run(event, fetcher) // the first child's store fails, the second is stored anyway

        val parent = store.transfers.single { it.kind == TransferKind.MESSAGE }
        val (one, two) = store.transfers.filter { it.parentId == parent.id }
        assertEquals(TransferState.PROCESSED, parent.state)
        assertEquals(listOf(TransferState.FETCHED, TransferState.STORED), listOf(one.state, two.state))
        assertEquals(1, one.attempts, "the failure is the child's attempt")
        assertEquals(0, parent.attempts)
        assertEquals(listOf(ScriptedSource.Nack(message, true)), source.nacks)
        stagingIsEmpty()
        target.calls.clear()

        pipeline.run(event, fetcher) // the redelivery
        assertEquals(listOf(InMemoryTarget.Call("store", "1.png"), InMemoryTarget.Call("verify", "2.png")), target.calls, "the stored child is verified and skipped, the other stored")
        assertEquals(listOf(one.id, two.id), store.transfers.filter { it.parentId == parent.id }.map { it.id }, "the same child rows")
        assertEquals(TransferState.DONE, store.transfer(parent.id).state)
        assertEquals(listOf(message), source.acks, "acked once")
        stagingIsEmpty()
    }

    @Test
    fun S29_one_child_failing_five_times_fails_the_parent_the_message_is_not_acked_and_a_redrive_replaces_the_children_and_reruns_the_chain() = runTest {
        val event = imageMessage()
        val pipeline = pipeline(imageSets(parallelism = 1), imageChain)
        repeat(5) { target.failNextStore = true; pipeline.run(event, fetcher) }

        val parent = store.transfers.single { it.kind == TransferKind.MESSAGE }
        val before = store.transfers.filter { it.parentId == parent.id }
        assertEquals(TransferState.FAILED, parent.state)
        assertEquals(listOf(TransferState.FAILED, TransferState.STORED), before.map { it.state })
        assertEquals(5, before[0].attempts)
        assertEquals("child ${before[0].id.value} failed: injected: store failed", parent.lastError)
        assertTrue(source.acks.isEmpty(), "the message is never acked")
        assertEquals(List(4) { ScriptedSource.Nack(message, true) } + ScriptedSource.Nack(message, false), source.nacks)
        assertEquals(1.0, counter("failed"))
        assertEquals(5, fetcher.calls.count { it.path == "sets/set.json" }, "the chain re-ran on every redelivery")
        stagingIsEmpty()

        pipeline.run(event, fetcher) // I7: FAILED does no work
        assertEquals(5, fetcher.calls.count { it.path == "sets/set.json" })

        store.redrive(parent.id)
        pipeline.run(event, fetcher)
        val after = store.transfers.filter { it.parentId == parent.id }
        assertEquals(TransferState.DONE, store.transfer(parent.id).state)
        assertEquals(2, after.size)
        assertTrue(after.none { a -> before.any { it.id == a.id } }, "the re-drive replaced the children")
        assertEquals(listOf(message), source.acks)
        stagingIsEmpty()
    }

    @Test
    fun I17_S19_a_mirror_route_with_no_notifications_goes_none_to_DONE_and_creates_no_outbox_row() = runTest {
        val event = seen()
        pipeline().run(event, fetcher)

        val row = store.transfers.single()
        assertEquals(TransferState.DONE, row.state)
        assertEquals("a,b\n", String(target.bytes("a.csv")))
        assertEquals(listOf(event.identity), source.acks)
        assertTrue(store.outbox.isEmpty(), "no outbox row (I17)")
        assertEquals(0, wakes)
        stagingIsEmpty()
    }

    @Test
    fun S1_vendor_drop_happy_path_one_file_one_channel() = runTest {
        fetcher.file("123-order.csv", "a,b\n".toByteArray())
        val extract = processorFor(ProcessorSpec.Extract(ExtractFrom.FileName, regex = "(?<orderNumber>\\d+)-.*")) { null }
        val event = seen("123-order.csv")
        pipeline(route(notify = listOf(Notify(DeliveryMoment.ACKED, "downstream"))), listOf(extract)).run(event, fetcher)

        val row = store.transfers.single()
        assertEquals(TransferState.ACKED, row.state)
        assertEquals(mapOf("orderNumber" to "123"), row.attributes)
        val metadata = target.metadata("123-order.csv")
        assertEquals(row.digest!!.hex, metadata[TargetMetadata.DIGEST])
        assertEquals("md5", metadata[TargetMetadata.DIGEST_ALGORITHM])
        assertEquals("123", metadata[TargetMetadata.ATTRIBUTE_PREFIX + "orderNumber"])
        assertEquals(row.id.value.toString(), metadata[TargetMetadata.TRANSFER_ID])
        val delivery = store.outbox.single()
        assertEquals(DeliveryMoment.ACKED, delivery.moment)
        assertEquals(ChannelName("downstream"), delivery.channel)
        assertEquals(1, wakes)
        assertEquals(listOf(event.identity), source.acks)
        assertEquals(1.0, registry.counter(ShuttleMetrics.TRANSFERS, "route", "drop", "outcome", "done").count())
        stagingIsEmpty()
    }

    private val rejecting = object : Processor {
        override val produces = emptySet<String>()
        override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = Outcome.Reject("quality: bad header")
    }

    @Test
    fun S10_processor_Reject_is_REJECTED_nothing_stored_and_the_object_stays_until_redrive() = runTest {
        val event = seen()
        val pipeline = pipeline(processors = listOf(rejecting))
        pipeline.run(event, fetcher)

        val row = store.transfers.single()
        assertEquals(TransferState.REJECTED, row.state)
        assertEquals("quality: bad header", row.lastError)
        assertTrue(target.calls.isEmpty(), "nothing stored")
        assertEquals(listOf(ScriptedSource.Nack(event.identity, redeliver = false)), source.nacks)
        assertEquals(1.0, registry.counter(ShuttleMetrics.TRANSFERS, "route", "drop", "outcome", "rejected").count())
        stagingIsEmpty()

        pipeline.run(event, fetcher) // I7: the next poll does no work
        assertEquals(1, fetcher.calls.size)
        assertEquals(2, source.nacks.size)

        store.redrive(row.id)
        pipeline(processors = emptyList()).run(event, fetcher) // re-drive re-runs from fetch
        assertEquals(2, fetcher.calls.size)
        assertEquals(TransferState.DONE, store.transfer(row.id).state)
    }

    @Test
    fun S11_fetch_fails_five_polls_in_a_row_is_FAILED_with_nack_no_redelivery() = runTest {
        val event = seen()
        val pipeline = pipeline()
        repeat(5) { fetcher.failNext = true; pipeline.run(event, fetcher) }

        val row = store.transfers.single()
        assertEquals(TransferState.FAILED, row.state)
        assertEquals(5, row.attempts)
        assertEquals(List(4) { ScriptedSource.Nack(event.identity, true) } + ScriptedSource.Nack(event.identity, false), source.nacks)
        assertEquals(1.0, registry.counter(ShuttleMetrics.TRANSFERS, "route", "drop", "outcome", "failed").count())
        stagingIsEmpty()

        pipeline.run(event, fetcher) // I7: a FAILED row is neither fetched nor stored
        assertEquals(5, fetcher.calls.size)
        assertEquals(ScriptedSource.Nack(event.identity, false), source.nacks.last())
        assertTrue(target.calls.isEmpty())
    }

    /** A row parked at STORED, as a crash after `afterLedgerStored` leaves it; [ref] is what the row points at. */
    private suspend fun storedRow(event: RouteEvent.Seen, ref: TargetRef): Transfer {
        val t = store.seen(event.identity, TransferKind.OBJECT)
        store.fetched(t.id, StagedSummary("a.csv", 4, clock.instant(), Digest(DigestAlgorithm.MD5, "d"), null), emptyList())
        store.processed(t.id, emptyMap())
        store.stored(t.id, ref, emptyList())
        return store.transfer(t.id)
    }

    @Test
    fun S3_a_STORED_row_whose_verify_is_true_skips_to_the_ack_with_no_second_store() = runTest {
        val event = seen()
        val ref = target.store("a.csv", Files.write(staging.resolve("seed"), "a,b\n".toByteArray()), emptyMap())
        Files.delete(staging.resolve("seed"))
        val row = storedRow(event, ref)
        target.calls.clear()

        pipeline().run(event, fetcher)

        assertEquals(listOf(InMemoryTarget.Call("verify", "a.csv")), target.calls)
        assertTrue(fetcher.calls.isEmpty(), "no fetch")
        assertEquals(TransferState.DONE, store.transfer(row.id).state)
        assertEquals(listOf(event.identity), source.acks)
    }

    @Test
    fun I1_S6_a_STORED_row_whose_copy_is_missing_is_stored_again_on_the_same_row_before_it_is_acked() = runTest {
        val event = seen()
        val row = storedRow(event, TargetRef("memory", "landing", "a.csv", "gone", 4))

        pipeline().run(event, fetcher)

        assertEquals(listOf(InMemoryTarget.Call("verify", "a.csv"), InMemoryTarget.Call("store", "a.csv")), target.calls)
        assertEquals(1, fetcher.calls.size)
        assertEquals(row.id, store.transfers.single().id)
        assertEquals(TransferState.DONE, store.transfer(row.id).state)
        assertEquals(listOf(event.identity), source.acks)
        stagingIsEmpty()
    }

    private fun counter(outcome: String) = registry.counter(ShuttleMetrics.TRANSFERS, "route", "drop", "outcome", outcome).count()

    @Test
    fun S12_same_identity_re_dropped_after_DONE_with_the_same_digest_is_verified_and_acked_again_as_reacked() = runTest {
        val event = seen()
        val pipeline = pipeline(route().copy(recheckFinished = kotlin.time.Duration.ZERO))
        pipeline.run(event, fetcher)
        val done = store.transfers.single()
        target.calls.clear()

        pipeline.run(event, fetcher)

        assertEquals(2, fetcher.calls.size, "fetched and digested")
        assertEquals(listOf(InMemoryTarget.Call("verify", "a.csv")), target.calls, "no second store")
        assertEquals(listOf(event.identity, event.identity), source.acks)
        assertEquals(done, store.transfers.single(), "no state write")
        assertTrue(store.outbox.isEmpty(), "no delivery")
        assertEquals(1.0, counter("reacked"))
        stagingIsEmpty()
    }

    @Test
    fun I24_a_finished_identity_returning_with_a_different_digest_becomes_a_new_revision_and_the_old_row_is_untouched() = runTest {
        val event = seen()
        val pipeline = pipeline(route(notify = listOf(Notify(DeliveryMoment.ACKED, "downstream"))).copy(recheckFinished = kotlin.time.Duration.ZERO))
        pipeline.run(event, fetcher)
        val first = store.transfers.single()
        val firstDelivery = store.outbox.single()
        fetcher.file("a.csv", "corrected\n".toByteArray())
        clock.advance(1.minutes)

        pipeline.run(event, fetcher)

        val second = store.transfers.single { it.id != first.id }
        assertEquals(first, store.transfer(first.id), "revision 1 untouched")
        assertEquals(2, second.identity.revision)
        assertEquals(first.id, second.supersedesId)
        assertEquals(TransferState.ACKED, second.state)
        assertEquals("corrected\n", String(target.bytes("a.csv")))
        assertEquals(listOf(firstDelivery.id, store.outbox.last().id), store.outbox.map { it.id })
        assertEquals(second.id, store.outbox.last().transferId)
        assertEquals(listOf(event.identity, event.identity), source.acks)
        assertEquals(1.0, registry.counter(ShuttleMetrics.SUPERSEDES, "route", "drop").count())
        stagingIsEmpty()
    }

    @Test
    fun D40_a_DONE_identity_listed_again_inside_recheckFinished_is_skipped_without_a_fetch_or_a_write_and_rechecked_outside_it() = runTest {
        val event = seen()
        val pipeline = pipeline(route().copy(recheckFinished = 24.hours))
        pipeline.run(event, fetcher)
        val done = store.transfers.single()
        store.calls.clear()
        clock.advance(23.hours)

        pipeline.run(event, fetcher)
        assertEquals(1, fetcher.calls.size, "no fetch inside the window")
        assertEquals(listOf("find"), store.calls.map { it.method }, "no state write")
        assertEquals(1, source.acks.size)
        assertEquals(listOf(ScriptedSource.Nack(event.identity, true)), source.nacks, "given back for the next poll, so the trigger holds no place for it (ticket 21)")

        clock.advance(2.hours)
        pipeline.run(event, fetcher)
        assertEquals(2, fetcher.calls.size, "fetched and digested outside the window")
        assertEquals(2, source.acks.size)
        assertEquals(done, store.transfers.single())
    }

    @Test
    fun D41_below_staging_minFree_the_object_is_deferred_with_redelivery_before_any_fetch_and_no_attempt_counted() = runTest {
        val event = seen()
        val pipeline = pipeline()
        freeBytes = 1.gib - 1

        pipeline.run(event, fetcher)

        assertTrue(fetcher.calls.isEmpty(), "no fetch")
        assertEquals(listOf(ScriptedSource.Nack(event.identity, redeliver = true)), source.nacks)
        assertEquals(0, store.transfers.single().attempts)
        assertEquals(1.0, registry.counter(ShuttleMetrics.STAGING_DEFERRED, "route", "drop").count())
        assertEquals((1.gib - 1).toDouble(), registry.find(ShuttleMetrics.STAGING_FREE_BYTES).tag("store", "sftp").gauge()!!.value())
        stagingIsEmpty()

        freeBytes = 1.gib
        pipeline.run(event, fetcher)
        assertEquals(TransferState.DONE, store.transfers.single().state)
        assertEquals(1.gib.toDouble(), registry.find(ShuttleMetrics.STAGING_FREE_BYTES).tag("store", "sftp").gauge()!!.value())
    }

    /** A processor that turns the input into one new file per name, through the context (children when more than one). */
    private fun splitInto(vararg names: String) = object : Processor {
        override val produces = emptySet<String>()
        override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
            val input = payload.objects.single()
            return Outcome.Continue(Payload(names.map { n ->
                val file = Files.write(ctx.newStagedFile(n), "$n\n".toByteArray())
                input.copy(name = n, path = file, size = Files.size(file))
            }))
        }
    }

    @Test
    fun I16_a_parent_is_acked_only_when_every_child_is_STORED_and_a_failed_child_fails_the_parent() = runTest {
        val event = seen()
        val pipeline = pipeline(processors = listOf(splitInto("a/x.csv", "b/y.csv")))
        pipeline.run(event, fetcher)

        val parent = store.transfers.single { it.kind != TransferKind.CHILD }
        val children = store.transfers.filter { it.parentId == parent.id }
        assertEquals(2, children.size)
        assertEquals(listOf("a/x.csv", "b/y.csv"), children.map { it.target!!.key })
        assertTrue(children.all { it.state == TransferState.DONE }, "children follow the parent's ack")
        assertEquals(TransferState.DONE, parent.state)
        assertEquals(listOf(event.identity), source.acks, "the parent's ack is the only ack")
        assertEquals(2, target.calls.count { it.method == "store" }, "store exactly once per object")
        assertEquals(2.0, registry.counter(ShuttleMetrics.CHILDREN, "route", "drop").count())
        stagingIsEmpty()

        // the second half: one child's store fails on every run until maxAttempts
        val other = seen("b.csv")
        fetcher.file("b.csv", "b\n".toByteArray())
        repeat(5) { target.failNextStore = true; pipeline.run(other, fetcher) }
        val failedParent = store.transfers.single { it.identity.sourceName == "b.csv" && it.kind != TransferKind.CHILD }
        assertEquals(TransferState.FAILED, failedParent.state)
        val failedChild = store.transfers.single { it.parentId == failedParent.id && it.state == TransferState.FAILED }
        assertEquals(5, failedChild.attempts, "the attempts are the child's, kept across the re-runs")
        assertEquals(0, failedParent.attempts)
        assertEquals("child ${failedChild.id.value} failed: injected: store failed", failedParent.lastError)
        assertEquals(1, store.transfers.count { it.parentId == failedParent.id && it.state == TransferState.STORED }, "the sibling stored once and kept its row")
        assertEquals(listOf(event.identity), source.acks, "the failed parent was never acked")
        assertEquals(ScriptedSource.Nack(other.identity, false), source.nacks.last())
        stagingIsEmpty()
    }

    @Test
    fun S33_two_children_of_one_parent_on_one_key_reject_the_transfer_with_both_paths_in_the_reason() = runTest {
        val event = seen()
        pipeline(route().copy(target = Target("minio", key = "{sourceName}.out")), listOf(splitInto("a/x.csv", "b/x.csv"))).run(event, fetcher)

        val parent = store.transfers.single()
        assertEquals(TransferState.REJECTED, parent.state)
        assertEquals("cardinality: a/x.csv and b/x.csv both resolve to key a.csv.out", parent.lastError)
        assertTrue(target.calls.isEmpty(), "nothing stored")
        assertEquals(listOf(ScriptedSource.Nack(event.identity, false)), source.nacks)
        stagingIsEmpty()
    }

    private val throwingAfterCreatingAFile = object : Processor {
        override val produces = emptySet<String>()
        override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
            Files.write(ctx.newStagedFile("scratch.tmp"), "x".toByteArray())
            throw IllegalStateException("boom")
        }
    }

    @Test
    fun I9_staging_holds_no_file_after_a_processor_throws_after_a_store_fails_and_after_a_freeze_failure() = runTest {
        val event = seen()
        pipeline(processors = listOf(throwingAfterCreatingAFile)).run(event, fetcher)
        stagingIsEmpty()
        assertEquals(1, store.transfers.single().attempts)
        assertEquals(listOf(ScriptedSource.Nack(event.identity, true)), source.nacks)

        target.failNextStore = true
        pipeline().run(event, fetcher)
        stagingIsEmpty()
        assertEquals(TransferState.PROCESSED, store.transfers.single().state)
        assertEquals(2, store.transfers.single().attempts)

        val required = MappingTable(listOf(MappingRow("orderNumber", attribute = "orderNumber")))
        val route = route(notify = listOf(Notify(DeliveryMoment.ACKED, "downstream")))
        pipeline(route, bodies = mapOf(ChannelName("downstream") to required))
            .run(event, fetcher)
        stagingIsEmpty()
        val row = store.transfers.single()
        assertEquals(TransferState.FAILED, row.state, "a missing required mapping input is FAILED with no retry (S26)")
        assertEquals("mapping row orderNumber: attribute orderNumber is required and not set", row.lastError)
        assertEquals(1, target.calls.count { it.method == "store" }, "nothing stored after the freeze failure")
        assertEquals(ScriptedSource.Nack(event.identity, false), source.nacks.last())
    }

    @Test
    fun I10_the_ack_action_runs_only_once_the_transfer_is_STORED() = runTest {
        val event = seen()
        target.failNextStore = true
        pipeline().run(event, fetcher)
        assertTrue(source.acks.isEmpty(), "a failed store is never acked")
        assertEquals(listOf(ScriptedSource.Nack(event.identity, true)), source.nacks)

        pipeline().run(event, fetcher)
        assertEquals(listOf(event.identity), source.acks)
        assertEquals(TransferState.DONE, store.transfers.single().state)
    }

    @Test
    fun I11_a_failing_ACKED_transaction_leaves_the_row_STORED_with_no_outbox_row_and_the_attempt_counted() = runTest {
        val event = seen()
        val armAtStored = object : Hook { // the switch is one-shot and every transition inserts, so arm it just before the ACKED transaction
            override suspend fun at(point: HookPoint, transfer: TransferId) { if (point == HookPoint.afterLedgerStored) store.failNextDeliveryInsert = true }
        }
        pipeline(route(notify = listOf(Notify(DeliveryMoment.ACKED, "downstream"))), hook = armAtStored).run(event, fetcher)

        val row = store.transfers.single()
        assertEquals(TransferState.STORED, row.state)
        assertEquals(1, row.attempts)
        assertTrue(store.outbox.isEmpty())
        assertEquals(0, wakes)
        assertEquals(listOf(event.identity), source.acks, "the polled file was moved before the ledger write (spec 4.4); reconciliation repairs it")
        assertEquals(listOf(ScriptedSource.Nack(event.identity, true)), source.nacks)
    }

    @Test
    fun I2_the_only_source_writes_are_the_ack_and_nack_actions_of_the_trigger() = runTest {
        val event = seen()
        fetcher.failNext = true
        val pipeline = pipeline()
        pipeline.run(event, fetcher)
        assertTrue(source.acks.isEmpty())
        pipeline.run(event, fetcher)
        assertEquals(listOf(event.identity), source.acks)
        assertEquals(listOf(ScriptedSource.Nack(event.identity, true)), source.nacks)
    }

    @Test
    fun a_row_parked_at_SEEN_FETCHED_or_PROCESSED_runs_fully_from_stage_1() = runTest {
        val staged = StagedSummary("x", 1, clock.instant(), Digest(DigestAlgorithm.MD5, "d"), null)
        val parked = listOf("seen.csv", "fetched.csv", "processed.csv").map { name ->
            val event = seen(name)
            fetcher.file(name, "$name\n".toByteArray())
            val t = store.seen(event.identity, TransferKind.OBJECT)
            if (name != "seen.csv") store.fetched(t.id, staged, emptyList())
            if (name == "processed.csv") store.processed(t.id, emptyMap())
            event to t.id
        }
        assertEquals(listOf(TransferState.SEEN, TransferState.FETCHED, TransferState.PROCESSED), parked.map { store.transfer(it.second).state })

        parked.forEach { (event, _) -> pipeline().run(event, fetcher) }

        assertEquals(3, fetcher.calls.size)
        assertEquals(3, target.calls.count { it.method == "store" })
        assertTrue(parked.all { store.transfer(it.second).state == TransferState.DONE })
        assertEquals(parked.map { it.first.identity }, source.acks)
    }

    @Test
    fun a_subscribed_message_is_written_ACKED_before_the_broker_ack_and_a_redelivery_is_reacked_without_a_fetch() = runTest {
        val route = route().copy(source = Source.Subscribe("nats", "images", onAck = AckAction.Ack), fetch = Fetch("minio", "/path"))
        val identity = SourceIdentity(RouteName("drop"), SourceKind.NATS, "nats:images", "msg-1", null, null)
        val event = source.seen(identity, SourceView("images", """{"path":"a.csv"}""".toByteArray())).events().toList().last() as RouteEvent.Seen
        val order = mutableListOf<String>()
        val hook = object : Hook {
            override suspend fun at(point: HookPoint, transfer: TransferId) { order += point.name }
        }
        val pipeline = pipeline(route, hook = hook)

        pipeline.run(event, fetcher)
        val row = store.transfers.single()
        assertEquals(TransferKind.MESSAGE, row.kind)
        assertEquals(TransferState.DONE, row.state)
        assertEquals(listOf("afterFetch", "afterProcess", "afterStore", "afterLedgerStored", "afterLedgerAcked", "afterAck"), order)
        assertEquals(listOf(identity), source.acks)

        target.calls.clear()
        pipeline.run(event, fetcher)
        assertEquals(1, fetcher.calls.size, "no fetch for a redelivered message")
        assertEquals(listOf(InMemoryTarget.Call("verify", "a.csv")), target.calls)
        assertEquals(listOf(identity, identity), source.acks)
        assertEquals(1.0, counter("reacked"))
    }

    /** Arms the store's one-shot delivery-insert failure at [point], or before the run when null, so exactly the next transition fails. */
    private fun armInsertFailure(point: HookPoint?): Hook {
        if (point == null) store.failNextDeliveryInsert = true
        return object : Hook {
            override suspend fun at(p: HookPoint, transfer: TransferId) { if (p == point) store.failNextDeliveryInsert = true }
        }
    }

    /** I20 for one moment: the failed transition leaves no row and the previous state; the committed one leaves exactly one PENDING row. */
    private suspend fun I20(moment: DeliveryMoment, armAt: HookPoint?, previous: TransferState) {
        val event = seen()
        val route = route(notify = listOf(Notify(moment, "downstream")))
        pipeline(route, hook = armInsertFailure(armAt)).run(event, fetcher)
        val row = store.transfers.single()
        assertEquals(previous, row.state, "$moment: the failed transaction left the previous state")
        assertTrue(store.outbox.isEmpty(), "$moment: no row without the transition")
        assertEquals(0, wakes)
        assertEquals(1, row.attempts)
        assertEquals(listOf(ScriptedSource.Nack(event.identity, true)), source.nacks)

        pipeline(route).run(event, fetcher)
        val delivery = store.outbox.single()
        assertEquals(moment, delivery.moment)
        assertEquals(DeliveryState.PENDING, delivery.state)
        assertEquals(ChannelName("downstream"), delivery.channel)
        assertEquals(TransferState.ACKED, store.transfers.single().state, "$moment: ACKED, not DONE, while the row is PENDING")
        assertEquals(1, wakes)
        stagingIsEmpty()
    }

    @Test
    fun I20_a_fetched_delivery_row_exists_iff_the_FETCHED_transition_committed() = runTest {
        I20(DeliveryMoment.FETCHED, armAt = null, previous = TransferState.SEEN) // `seen` inserts nothing, so `fetched` is the next transition
    }

    private val body = MappingTable(listOf(MappingRow("fileId", field = "TRANSFER_ID"), MappingRow("event", field = "EVENT")))

    @Test
    fun a_fetched_delivery_created_before_a_crash_right_after_fetch_is_delivered_by_the_notifier() = runTest {
        val event = seen()
        val hook = HookDriver().apply { pauseAt(HookPoint.afterFetch) }
        val job = launch { pipeline(route(notify = listOf(Notify(DeliveryMoment.FETCHED, "downstream"))), hook = hook).run(event, fetcher) }
        hook.awaitArrival(HookPoint.afterFetch)
        hook.crash(HookPoint.afterFetch)
        job.join()
        assertTrue(job.isCancelled, "the process died at afterFetch")
        val row = store.transfers.single()
        assertEquals(TransferState.FETCHED, row.state)
        assertEquals(DeliveryState.PENDING, store.outbox.single().state)
        stagingIsEmpty()

        val channel = RecordingChannel("downstream")
        val config = NotifierConfig(workers = 1, batch = 10, sweepEvery = 30.seconds)
        backgroundScope.launch { Notifier(store, listOf(channel), mapOf(channel.name to body), MappingRenderer(), config, registry, clock).run() }
        runCurrent()

        val delivery = store.outbox.single()
        assertEquals(DeliveryMoment.FETCHED, delivery.moment)
        assertEquals(DeliveryState.DELIVERED, delivery.state)
        assertEquals(TransferState.FETCHED, store.transfer(row.id).state, "a delivered fetched row moves the transfer nowhere")
        val sent = channel.events.single()
        assertEquals(row.id.value.toString(), sent.body.get("fileId").asText())
        assertEquals("fetched", sent.body.get("event").asText())
    }

    @Test
    fun S30_a_callback_ack_answering_500_then_200_keeps_the_transfer_STORED_through_the_failure_and_ACKED_after_with_one_acked_delivery() = runTest {
        val event = seen()
        val upstream = RecordingChannel("upstream", outcomes = arrayOf(DeliveryOutcome.Retry("500", "server error"), DeliveryOutcome.Delivered("cb-1")))
        val route = route(notify = listOf(Notify(DeliveryMoment.ACKED, "downstream")))
            .copy(source = Source.Poll("sftp", "/in", 1.minutes, onAck = AckAction.Callback("upstream")))
        val pipeline = pipeline(route, channels = mapOf(upstream.name to upstream), bodies = mapOf(upstream.name to body))

        pipeline.run(event, fetcher)
        val row = store.transfers.single()
        assertEquals(TransferState.STORED, row.state, "not ACKED until the callback succeeds (spec 5.3)")
        assertEquals(1, row.attempts)
        assertTrue(store.outbox.isEmpty(), "no acked delivery before the callback succeeds")
        assertTrue(source.acks.isEmpty(), "the connector's own ack waits for the callback too")
        assertEquals(listOf(ScriptedSource.Nack(event.identity, true)), source.nacks)
        stagingIsEmpty()
        target.calls.clear()

        pipeline.run(event, fetcher)
        assertEquals(TransferState.ACKED, store.transfer(row.id).state)
        assertEquals(listOf(InMemoryTarget.Call("verify", "a.csv")), target.calls, "verified, not stored again")
        val delivery = store.outbox.single()
        assertEquals(DeliveryMoment.ACKED, delivery.moment)
        assertEquals(ChannelName("downstream"), delivery.channel)
        assertEquals(listOf(event.identity), source.acks)
        assertEquals(listOf(1, 2), upstream.events.map { it.attempt }, "the callback is retried with the stage")
        upstream.events.forEach {
            assertEquals(DeliveryMoment.ACKED, it.moment)
            assertEquals(row.id, it.transferId)
            assertEquals("acked", it.body.get("event").asText())
        }
    }

    @Test
    fun a_callback_answering_Reject_or_throwing_is_a_stage_error_and_FAILED_at_maxAttempts() = runTest {
        val event = seen()
        val upstream = object : DeliveryChannel {
            override val name = ChannelName("upstream")
            override val policy = DeliveryPolicy()
            var calls = 0
            override suspend fun deliver(event: DeliveryEvent): DeliveryOutcome =
                if (calls++ % 2 == 0) DeliveryOutcome.Reject("400", "bad request") else throw java.io.IOException("connection reset")
        }
        val route = route().copy(source = Source.Poll("sftp", "/in", 1.minutes, onAck = AckAction.Callback("upstream")))
        val pipeline = pipeline(route, channels = mapOf(upstream.name to upstream))

        repeat(5) { pipeline.run(event, fetcher) }

        val row = store.transfers.single()
        assertEquals(TransferState.FAILED, row.state)
        assertEquals(5, row.attempts)
        assertEquals(5, upstream.calls)
        assertEquals("callback upstream answered Reject 400: bad request", row.lastError, "the fifth call was a Reject; the reason is on the row")
        assertTrue(source.acks.isEmpty(), "never acked at the source")
        assertEquals(List(4) { ScriptedSource.Nack(event.identity, true) } + ScriptedSource.Nack(event.identity, false), source.nacks)
        assertEquals(1, target.calls.count { it.method == "store" }, "stored once, verified on every retry")
    }

    @Test
    fun a_subscribed_callback_precedes_the_ACKED_ledger_and_the_broker_ack_and_a_redelivery_does_not_call_it_again() = runTest {
        val upstream = RecordingChannel("upstream")
        val route = route().copy(source = Source.Subscribe("nats", "images", onAck = AckAction.Callback("upstream")), fetch = Fetch("minio", "/path"))
        val identity = SourceIdentity(RouteName("drop"), SourceKind.NATS, "nats:images", "msg-1", null, null)
        val event = source.seen(identity, SourceView("images", """{"path":"a.csv"}""".toByteArray())).events().toList().last() as RouteEvent.Seen
        val order = mutableListOf<String>()
        val hook = object : Hook {
            override suspend fun at(point: HookPoint, transfer: TransferId) { order += point.name }
        }
        val observed = object : DeliveryChannel by upstream {
            override suspend fun deliver(event: DeliveryEvent) = upstream.deliver(event).also { order += "callback:${store.transfer(event.transferId).state}" }
        }
        val pipeline = pipeline(route, hook = hook, channels = mapOf(upstream.name to observed))

        pipeline.run(event, fetcher)
        assertEquals(listOf("afterFetch", "afterProcess", "afterStore", "afterLedgerStored", "callback:STORED", "afterLedgerAcked", "afterAck"), order)
        assertEquals(TransferState.DONE, store.transfers.single().state)

        pipeline.run(event, fetcher) // redelivered after the ledger write (S32): the ACKED row proves upstream answered
        assertEquals(listOf(identity, identity), source.acks)
        assertEquals(1, upstream.events.size, "no second callback on a re-ack")
        assertEquals(1.0, counter("reacked"))
    }

    @Test
    fun I20_a_stored_delivery_row_exists_iff_the_STORED_transition_committed() = runTest {
        I20(DeliveryMoment.STORED, armAt = HookPoint.afterStore, previous = TransferState.PROCESSED)
    }

    @Test
    fun I20_an_acked_delivery_row_exists_iff_the_ACKED_transition_committed() = runTest {
        I20(DeliveryMoment.ACKED, armAt = HookPoint.afterLedgerStored, previous = TransferState.STORED)
    }

    @Test
    fun the_hook_points_of_spec_4_4_are_reached_in_order_on_a_polled_route() = runTest {
        val order = mutableListOf<String>()
        val hook = object : Hook {
            override suspend fun at(point: HookPoint, transfer: TransferId) { order += point.name }
        }
        pipeline(hook = hook).run(seen(), fetcher)
        assertEquals(listOf("afterFetch", "afterProcess", "afterStore", "afterLedgerStored", "afterAck", "afterLedgerAcked"), order)
    }
}
