package infra.shuttle.testkit

import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryRequest
import infra.shuttle.core.DeliveryState
import infra.shuttle.core.Digest
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.RouteName
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.SourceKind
import infra.shuttle.core.StagedSummary
import infra.shuttle.core.StateStore
import infra.shuttle.core.TargetRef
import infra.shuttle.core.Transfer
import infra.shuttle.core.TransferKind
import infra.shuttle.core.TransferState
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Instant
import kotlin.time.Duration.Companion.minutes

class InMemoryStateStoreTest {
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private val route = RouteName("drop")
    private val hook = ChannelName("hook")
    private val onStored = listOf(DeliveryRequest(DeliveryMoment.STORED, hook))
    private val onAcked = listOf(DeliveryRequest(DeliveryMoment.ACKED, hook))

    private fun identity(name: String, route: RouteName = this.route) =
        SourceIdentity(route, SourceKind.SFTP, "sftp:/in", name, 10, Instant.EPOCH)

    private fun staged(name: String) = StagedSummary(name, 10, Instant.EPOCH, Digest(DigestAlgorithm.MD5, "d-$name"), null)
    private fun ref(key: String) = TargetRef("memory", "bucket", key, "v1", 10)

    private suspend fun StateStore.storedTransfer(name: String, events: List<DeliveryRequest> = emptyList(), route: RouteName = this@InMemoryStateStoreTest.route): Transfer {
        val t = seen(identity(name, route), TransferKind.OBJECT)
        fetched(t.id, staged(name), emptyList())
        processed(t.id, mapOf("k" to "v"))
        stored(t.id, ref(name), events)
        return store.transfer(t.id)
    }

    @Test
    fun seen_creates_a_SEEN_row_and_returns_the_existing_row_for_a_known_identity() = runTest {
        assertNull(store.find(identity("a")))
        val first = store.seen(identity("a"), TransferKind.OBJECT)
        assertEquals(TransferState.SEEN, first.state)
        assertEquals(first, store.seen(identity("a"), TransferKind.OBJECT))
        assertEquals(first, store.find(identity("a")))
        assertEquals(listOf("find", "seen", "seen", "find"), store.calls.map { it.method })
    }

    @Test
    fun the_happy_path_walks_the_states_and_stamps_the_clock() = runTest {
        val t = store.storedTransfer("a", onStored)
        assertEquals(TransferState.STORED, t.state)
        assertEquals(ref("a"), t.target)
        assertEquals(mapOf("k" to "v"), t.attributes)
        assertEquals(Digest(DigestAlgorithm.MD5, "d-a"), t.sourceDigest)
        assertEquals(clock.instant(), t.updatedAt)

        clock.advance(1.minutes)
        store.acked(t.id, onAcked)
        val acked = store.transfer(t.id)
        assertEquals(TransferState.ACKED, acked.state)
        assertEquals(clock.instant(), acked.ackedAt)
        assertEquals(listOf(DeliveryMoment.STORED, DeliveryMoment.ACKED), store.outbox.map { it.moment })
        assertTrue(store.outbox.all { it.state == DeliveryState.PENDING && it.transferId == t.id })
    }

    @Test
    fun I11_a_failing_delivery_insert_leaves_the_transfer_state_unchanged() = runTest {
        val t = store.seen(identity("a"), TransferKind.OBJECT)
        store.fetched(t.id, staged("a"), emptyList())
        store.failNextDeliveryInsert = true
        assertTrue(runCatching { store.stored(t.id, ref("a"), onStored) }.exceptionOrNull() is IOException)
        assertEquals(TransferState.FETCHED, store.transfer(t.id).state)
        assertNull(store.transfer(t.id).target)
        assertTrue(store.outbox.isEmpty())
        // the switch is one-shot: the retry commits both
        store.stored(t.id, ref("a"), onStored)
        assertEquals(TransferState.STORED, store.transfer(t.id).state)
        assertEquals(1, store.outbox.size)
    }

    @Test
    fun I17_acked_with_no_deliveries_pending_goes_straight_to_DONE() = runTest {
        val t = store.storedTransfer("a")
        store.acked(t.id, emptyList())
        assertEquals(TransferState.DONE, store.transfer(t.id).state)
        assertNotNull(store.transfer(t.id).completedAt)
        assertTrue(store.outbox.isEmpty())
    }

    @Test
    fun delivered_flips_DONE_when_the_last_delivery_lands_and_deliveryFailed_never_does() = runTest {
        val t = store.storedTransfer("a", onStored)
        store.acked(t.id, onAcked)
        val (d1, d2) = store.outbox
        store.deliveryFailed(d1.id, "400", "bad")
        assertEquals(TransferState.ACKED, store.transfer(t.id).state)
        store.delivered(d2.id, "ref-2")
        assertEquals(TransferState.ACKED, store.transfer(t.id).state) // one FAILED, so never DONE (D9)
        store.redriveDelivery(d1.id)
        assertEquals(DeliveryState.PENDING, store.outbox.first { it.id == d1.id }.state)
        store.delivered(d1.id, "ref-1")
        assertEquals(TransferState.DONE, store.transfer(t.id).state)
        assertEquals(listOf("ref-1", "ref-2"), store.outbox.map { it.reference })
    }

    @Test
    fun due_orders_by_next_attempt_excludes_ids_and_honours_the_limit() = runTest {
        val t = store.storedTransfer("a")
        store.acked(t.id, listOf("c1", "c2", "c3", "c4").map { DeliveryRequest(DeliveryMoment.ACKED, ChannelName(it)) })
        val (d1, d2, d3, d4) = store.outbox
        val later = clock.instant().plusSeconds(60)
        store.retryLater(d1.id, later, "503", "busy")
        store.retryLater(d3.id, clock.instant().plusSeconds(30), "503", "busy")
        assertEquals(listOf(d2.id, d4.id, d3.id), store.due(later, emptySet(), 3).map { it.id })
        assertEquals(listOf(d4.id, d3.id), store.due(later, setOf(d2.id), 2).map { it.id })
        assertEquals(listOf(d2.id, d4.id), store.due(clock.instant(), emptySet(), 10).map { it.id })
        assertEquals(1, store.outbox.first { it.id == d1.id }.attempts)
        assertEquals("503", store.outbox.first { it.id == d1.id }.lastStatus)
    }

    @Test
    fun unlisted_is_exactly_the_STORED_rows_older_than_the_instant_and_not_listed() = runTest {
        val old = store.storedTransfer("old")
        val old2 = store.storedTransfer("old2")
        val listed = store.storedTransfer("listed")
        val otherRoute = store.storedTransfer("x", route = RouteName("other"))
        val notStored = store.seen(identity("seen"), TransferKind.OBJECT)
        clock.advance(1.minutes)
        val pollStart = clock.instant()
        val young = store.storedTransfer("young")
        val ids = store.unlisted(route, pollStart, setOf(identity("listed"), identity("seen")))
        assertEquals(setOf(old.id, old2.id), ids.toSet())
        assertTrue(listOf(listed, otherRoute, notStored, young).none { it.id in ids })
    }

    @Test
    fun failedAttempt_counts_up_and_becomes_FAILED_at_max_then_redrive_returns_to_SEEN() = runTest {
        val t = store.seen(identity("a"), TransferKind.OBJECT)
        assertEquals(TransferState.SEEN, store.failedAttempt(t.id, "boom", 2).state)
        val failed = store.failedAttempt(t.id, "boom again", 2)
        assertEquals(TransferState.FAILED, failed.state)
        assertEquals(2, failed.attempts)
        assertEquals("boom again", failed.lastError)
        store.redrive(t.id)
        assertEquals(TransferState.SEEN, store.transfer(t.id).state)
        assertEquals(0, store.transfer(t.id).attempts)
    }

    @Test
    fun rejected_is_terminal_until_redrive() = runTest {
        val t = store.seen(identity("a"), TransferKind.OBJECT)
        store.rejected(t.id, "unreadable")
        assertEquals(TransferState.REJECTED, store.transfer(t.id).state)
        assertEquals("unreadable", store.transfer(t.id).lastError)
    }

    @Test
    fun I24_supersede_creates_the_next_revision_and_leaves_the_finished_row_untouched() = runTest {
        val t = store.storedTransfer("a")
        store.acked(t.id, emptyList())
        val done = store.transfer(t.id)
        val next = store.supersede(t.id, TransferKind.OBJECT)
        assertEquals(2, next.identity.revision)
        assertEquals(t.id, next.supersedesId)
        assertEquals(TransferState.SEEN, next.state)
        assertEquals(done, store.transfer(t.id))
        assertEquals(next, store.find(identity("a"))) // the listing's identity resolves to the latest revision
    }

    @Test
    fun children_replace_earlier_children_and_the_parent_is_STORED_when_the_last_child_is() = runTest {
        val parent = store.seen(identity("set.json"), TransferKind.MESSAGE)
        store.fetched(parent.id, staged("set.json"), emptyList())
        store.children(parent.id, listOf(staged("stale")))
        val children = store.children(parent.id, listOf(staged("c1"), staged("c2")))
        assertEquals(2, store.transfers.count { it.parentId == parent.id })
        assertTrue(children.all { it.kind == TransferKind.CHILD && it.state == TransferState.FETCHED })

        store.stored(children[0].id, ref("c1"), onStored)
        assertEquals(TransferState.FETCHED, store.transfer(parent.id).state)
        assertTrue(store.outbox.isEmpty())
        store.stored(children[1].id, ref("c2"), onStored)
        assertEquals(TransferState.STORED, store.transfer(parent.id).state)
        assertEquals(listOf(parent.id), store.outbox.map { it.transferId })

        store.acked(parent.id, emptyList())
        assertTrue(store.transfers.filter { it.parentId == parent.id }.all { it.state == TransferState.ACKED })
        assertEquals(TransferState.ACKED, store.transfer(parent.id).state) // the stored delivery is still PENDING
        store.delivered(store.outbox.single().id, "r")
        assertTrue(store.transfers.filter { it.parentId == parent.id }.all { it.state == TransferState.DONE })
        assertEquals(TransferState.DONE, store.transfer(parent.id).state)
    }

    @Test
    fun I16_a_child_at_maxAttempts_fails_the_parent() = runTest {
        val parent = store.seen(identity("set.json"), TransferKind.MESSAGE)
        val child = store.children(parent.id, listOf(staged("c1"))).single()
        store.failedAttempt(child.id, "boom", 1)
        assertEquals(TransferState.FAILED, store.transfer(parent.id).state)
    }

    @Test
    fun stuck_counts_the_rows_of_a_route_before_ACKED_older_than_the_instant() = runTest {
        store.seen(identity("a"), TransferKind.OBJECT)
        store.storedTransfer("b")
        store.acked(store.storedTransfer("c").id, emptyList())
        store.seen(identity("d", RouteName("other")), TransferKind.OBJECT)
        clock.advance(1.minutes)
        assertEquals(2, store.stuck(route, clock.instant()))
        assertEquals(0, store.stuck(route, clock.instant().minusSeconds(60)))
    }
}
