package infra.shuttle.testkit

import infra.shuttle.core.ChannelName
import infra.shuttle.core.Delivery
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
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferKind
import infra.shuttle.core.TransferState
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.time.Duration.Companion.minutes

/**
 * Spec 8.2 as one set of assertions every `StateStore` must pass. A subclass supplies the store,
 * three read-side views the seam does not offer, and a way to poison a delivery insert.
 */
abstract class StateStoreContract {
    protected val clock = ClockFixture()
    protected abstract val store: StateStore
    protected abstract suspend fun transfer(id: TransferId): Transfer
    protected abstract suspend fun transfers(): List<Transfer>
    protected abstract suspend fun outbox(): List<Delivery>

    /** Events whose insert fails inside the transaction; the transition must roll back with them. */
    protected abstract suspend fun poisonedEvents(): List<DeliveryRequest>
    protected open fun assertInjectedFailure(e: Throwable?) = assertNotNull(e)

    protected val route = RouteName("drop")
    private val hook = ChannelName("hook")
    protected val onStored = listOf(DeliveryRequest(DeliveryMoment.STORED, hook))
    private val onAcked = listOf(DeliveryRequest(DeliveryMoment.ACKED, hook))

    protected fun identity(name: String, route: RouteName = this.route) =
        SourceIdentity(route, SourceKind.SFTP, "sftp:/in", name, 10, Instant.EPOCH)

    protected fun staged(name: String) = StagedSummary(name, 10, Instant.EPOCH, Digest(DigestAlgorithm.MD5, "d-$name"), null)
    protected fun ref(key: String) = TargetRef("memory", "bucket", key, "v1", 10)

    protected suspend fun storedTransfer(name: String, events: List<DeliveryRequest> = emptyList(), route: RouteName = this.route): Transfer {
        val t = store.seen(identity(name, route), TransferKind.OBJECT)
        store.fetched(t.id, staged(name), emptyList())
        store.processed(t.id, mapOf("k" to "v"))
        store.stored(t.id, ref(name), events)
        return transfer(t.id)
    }

    @Test
    fun seen_creates_a_SEEN_row_and_returns_the_existing_row_for_a_known_identity() = runTest {
        assertNull(store.find(identity("a")))
        val first = store.seen(identity("a"), TransferKind.OBJECT)
        assertEquals(TransferState.SEEN, first.state)
        assertEquals(first, store.seen(identity("a"), TransferKind.OBJECT))
        assertEquals(first, store.find(identity("a")))
    }

    @Test
    fun the_happy_path_walks_the_states_and_stamps_the_clock() = runTest {
        val t = storedTransfer("a", onStored)
        assertEquals(TransferState.STORED, t.state)
        assertEquals(ref("a"), t.target)
        assertEquals(mapOf("k" to "v"), t.attributes)
        assertEquals(Digest(DigestAlgorithm.MD5, "d-a"), t.sourceDigest)
        assertEquals(clock.instant(), t.updatedAt)

        clock.advance(1.minutes)
        store.acked(t.id, onAcked)
        val acked = transfer(t.id)
        assertEquals(TransferState.ACKED, acked.state)
        assertEquals(clock.instant(), acked.ackedAt)
        assertEquals(listOf(DeliveryMoment.STORED, DeliveryMoment.ACKED), outbox().map { it.moment })
        assertTrue(outbox().all { it.state == DeliveryState.PENDING && it.transferId == t.id })
    }

    @Test
    fun I11_a_failing_delivery_insert_leaves_the_transfer_state_unchanged() = runTest {
        val t = store.seen(identity("a"), TransferKind.OBJECT)
        store.fetched(t.id, staged("a"), emptyList())
        assertInjectedFailure(runCatching { store.stored(t.id, ref("a"), poisonedEvents()) }.exceptionOrNull())
        assertEquals(TransferState.FETCHED, transfer(t.id).state)
        assertNull(transfer(t.id).target)
        assertTrue(outbox().isEmpty())
        // the retry commits both
        store.stored(t.id, ref("a"), onStored)
        assertEquals(TransferState.STORED, transfer(t.id).state)
        assertEquals(1, outbox().size)
    }

    @Test
    fun I20_a_notification_row_exists_iff_its_transition_committed() = runTest {
        val t = store.seen(identity("a"), TransferKind.OBJECT)
        assertInjectedFailure(runCatching { store.fetched(t.id, staged("a"), poisonedEvents()) }.exceptionOrNull())
        assertEquals(TransferState.SEEN, transfer(t.id).state)
        assertTrue(outbox().isEmpty())
        store.fetched(t.id, staged("a"), listOf(DeliveryRequest(DeliveryMoment.FETCHED, hook)))
        assertEquals(TransferState.FETCHED, transfer(t.id).state)
        assertEquals(listOf(DeliveryMoment.FETCHED), outbox().map { it.moment })

        store.processed(t.id, emptyMap())
        store.stored(t.id, ref("a"), emptyList())
        assertInjectedFailure(runCatching { store.acked(t.id, poisonedEvents()) }.exceptionOrNull())
        assertEquals(TransferState.STORED, transfer(t.id).state)
        assertEquals(1, outbox().size)
        store.acked(t.id, onAcked)
        assertEquals(TransferState.ACKED, transfer(t.id).state)
        assertEquals(listOf(DeliveryMoment.FETCHED, DeliveryMoment.ACKED), outbox().map { it.moment })
    }

    /**
     * Spec 4.4's "next trigger does a full run" for a crash after fetch: the FETCHED transition runs again for the same
     * row, and the 8.1 index allows one row per transfer, moment and channel, so the transition keeps the row it
     * already created, whatever state the notifier has moved it to (I20, measured by ticket 20 on Oracle).
     */
    @Test
    fun I20_a_transition_run_again_after_a_crash_keeps_its_existing_notification_row() = runTest {
        val t = store.seen(identity("a"), TransferKind.OBJECT)
        val onFetched = listOf(DeliveryRequest(DeliveryMoment.FETCHED, hook))
        store.fetched(t.id, staged("a"), onFetched)
        val row = outbox().single()
        store.delivered(row.id, "r-1")

        store.fetched(t.id, staged("a"), onFetched)

        assertEquals(TransferState.FETCHED, transfer(t.id).state)
        assertEquals(listOf(row.id), outbox().map { it.id }, "the row already there is the transition's row")
        assertEquals(DeliveryState.DELIVERED, outbox().single().state)
    }

    @Test
    fun I17_acked_with_no_deliveries_pending_goes_straight_to_DONE() = runTest {
        val t = storedTransfer("a")
        store.acked(t.id, emptyList())
        assertEquals(TransferState.DONE, transfer(t.id).state)
        assertNotNull(transfer(t.id).completedAt)
        assertTrue(outbox().isEmpty())
    }

    @Test
    fun delivered_flips_DONE_when_the_last_delivery_lands_and_deliveryFailed_never_does() = runTest {
        val t = storedTransfer("a", onStored)
        store.acked(t.id, onAcked)
        val (d1, d2) = outbox()
        store.deliveryFailed(d1.id, "400", "bad")
        assertEquals(TransferState.ACKED, transfer(t.id).state)
        store.delivered(d2.id, "ref-2")
        assertEquals(TransferState.ACKED, transfer(t.id).state) // one FAILED, so never DONE (D9)
        store.redriveDelivery(d1.id)
        assertEquals(DeliveryState.PENDING, outbox().first { it.id == d1.id }.state)
        store.delivered(d1.id, "ref-1")
        assertEquals(TransferState.DONE, transfer(t.id).state)
        assertEquals(listOf("ref-1", "ref-2"), outbox().map { it.reference })
    }

    @Test
    fun due_orders_by_next_attempt_excludes_ids_and_honours_the_limit() = runTest {
        val t = storedTransfer("a")
        store.acked(t.id, listOf("c1", "c2", "c3", "c4").map { DeliveryRequest(DeliveryMoment.ACKED, ChannelName(it)) })
        val (d1, d2, d3, d4) = outbox()
        val later = clock.instant().plusSeconds(60)
        store.retryLater(d1.id, later, "503", "busy")
        store.retryLater(d3.id, clock.instant().plusSeconds(30), "503", "busy")
        assertEquals(listOf(d2.id, d4.id, d3.id), store.due(later, emptySet(), 3).map { it.id })
        assertEquals(listOf(d4.id, d3.id), store.due(later, setOf(d2.id), 2).map { it.id })
        assertEquals(listOf(d2.id, d4.id), store.due(clock.instant(), emptySet(), 10).map { it.id })
        assertEquals(1, outbox().first { it.id == d1.id }.attempts)
        assertEquals("503", outbox().first { it.id == d1.id }.lastStatus)
    }

    @Test
    fun unlisted_is_exactly_the_STORED_rows_older_than_the_instant_and_not_listed() = runTest {
        val old = storedTransfer("old")
        val old2 = storedTransfer("old2")
        val listed = storedTransfer("listed")
        val otherRoute = storedTransfer("x", route = RouteName("other"))
        val notStored = store.seen(identity("seen"), TransferKind.OBJECT)
        clock.advance(1.minutes)
        val pollStart = clock.instant()
        val young = storedTransfer("young")
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
        assertEquals(TransferState.SEEN, transfer(t.id).state)
        assertEquals(0, transfer(t.id).attempts)
    }

    /** D40 measures `recheckFinished` from `updated_at`, so a re-ack must advance it; the row is otherwise untouched, whatever its state (SPEC2). */
    @Test
    fun reacked_advances_updated_at_and_changes_nothing_else() = runTest {
        val done = storedTransfer("a").also { store.acked(it.id, emptyList()) }.let { transfer(it.id) }
        val acked = storedTransfer("b").also { store.acked(it.id, onAcked) }.let { transfer(it.id) }
        assertEquals(listOf(TransferState.DONE, TransferState.ACKED), listOf(done.state, acked.state))
        clock.advance(1.minutes)

        store.reacked(done.id)
        store.reacked(acked.id)

        assertEquals(done.copy(updatedAt = clock.instant()), transfer(done.id))
        assertEquals(acked.copy(updatedAt = clock.instant()), transfer(acked.id))
        assertEquals(listOf(acked.id), outbox().map { it.transferId }, "no outbox row written")
    }

    @Test
    fun rejected_is_terminal_until_redrive() = runTest {
        val t = store.seen(identity("a"), TransferKind.OBJECT)
        store.rejected(t.id, "unreadable")
        assertEquals(TransferState.REJECTED, transfer(t.id).state)
        assertEquals("unreadable", transfer(t.id).lastError)
    }

    @Test
    fun I24_supersede_creates_the_next_revision_and_leaves_the_finished_row_untouched() = runTest {
        val t = storedTransfer("a")
        store.acked(t.id, emptyList())
        val done = transfer(t.id)
        val next = store.supersede(t.id, TransferKind.OBJECT)
        assertEquals(2, next.identity.revision)
        assertEquals(t.id, next.supersedesId)
        assertEquals(TransferState.SEEN, next.state)
        assertEquals(done, transfer(t.id))
        assertEquals(next, store.find(identity("a"))) // the listing's identity resolves to the latest revision
    }

    @Test
    fun children_replace_earlier_children_and_the_parent_is_STORED_when_the_last_child_is() = runTest {
        val parent = store.seen(identity("set.json"), TransferKind.MESSAGE)
        store.fetched(parent.id, staged("set.json"), emptyList())
        store.children(parent.id, listOf(staged("stale")))
        val children = store.children(parent.id, listOf(staged("c1"), staged("c2")))
        assertEquals(2, transfers().count { it.parentId == parent.id })
        assertTrue(children.all { it.kind == TransferKind.CHILD && it.state == TransferState.FETCHED })

        store.stored(children[0].id, ref("c1"), onStored)
        assertEquals(TransferState.FETCHED, transfer(parent.id).state)
        assertTrue(outbox().isEmpty())
        store.stored(children[1].id, ref("c2"), onStored)
        assertEquals(TransferState.STORED, transfer(parent.id).state)
        assertEquals(listOf(parent.id), outbox().map { it.transferId })

        store.acked(parent.id, emptyList())
        assertTrue(transfers().filter { it.parentId == parent.id }.all { it.state == TransferState.ACKED })
        assertEquals(TransferState.ACKED, transfer(parent.id).state) // the stored delivery is still PENDING
        store.delivered(outbox().single().id, "r")
        assertTrue(transfers().filter { it.parentId == parent.id }.all { it.state == TransferState.DONE })
        assertEquals(TransferState.DONE, transfer(parent.id).state)
    }

    @Test
    fun childrenOf_lists_a_parents_children_in_id_order_and_nothing_for_a_row_without_children() = runTest {
        val parent = store.seen(identity("set.json"), TransferKind.MESSAGE)
        val single = storedTransfer("a")
        assertEquals(emptyList<Transfer>(), store.childrenOf(parent.id))
        assertEquals(emptyList<Transfer>(), store.childrenOf(single.id))
        val children = store.children(parent.id, listOf(staged("c2"), staged("c1")))
        store.stored(children[0].id, ref("c2"), emptyList())
        assertEquals(listOf("c2", "c1"), store.childrenOf(parent.id).map { it.identity.sourceName })
        assertEquals(listOf(TransferState.STORED, TransferState.FETCHED), store.childrenOf(parent.id).map { it.state })
        assertEquals(ref("c2"), store.childrenOf(parent.id).first().target)
        assertEquals(emptyList<Transfer>(), store.childrenOf(children[0].id), "a child has no children")
    }

    /** D42: siblings store concurrently; the parent flips once, with one set of rows. */
    @Test
    fun D42_children_completing_concurrently_leave_exactly_one_parent_STORED_write() = runTest {
        val parent = store.seen(identity("set.json"), TransferKind.MESSAGE)
        val children = store.children(parent.id, (1..8).map { staged("c$it") })
        coroutineScope { children.map { c -> async { store.stored(c.id, ref(c.identity.sourceName), onStored) } }.awaitAll() }
        assertEquals(TransferState.STORED, transfer(parent.id).state)
        assertTrue(transfers().filter { it.parentId == parent.id }.all { it.state == TransferState.STORED })
        assertEquals(listOf(parent.id), outbox().map { it.transferId })
    }

    /**
     * B3: identity (spec 5.2) is a source row's; a child's is its parent's plus its own name (spec 4.5), so two
     * parents naming one shared object each expand their own child row and neither is reachable by `find`.
     */
    @Test
    fun B3_two_parents_may_expand_a_child_with_the_same_identity() = runTest {
        val first = store.seen(identity("set-1.json"), TransferKind.MESSAGE)
        val second = store.seen(identity("set-2.json"), TransferKind.MESSAGE)

        val one = store.children(first.id, listOf(staged("shared.png"))).single()
        val other = store.children(second.id, listOf(staged("shared.png"))).single()

        assertEquals(one.identity, other.identity)
        assertNotEquals(one.id, other.id)
        assertEquals(listOf(one.id), store.childrenOf(first.id).map { it.id })
        assertEquals(listOf(other.id), store.childrenOf(second.id).map { it.id })
        assertNull(store.find(one.identity), "a child is reached through its parent, never by identity")

        store.stored(one.id, ref("a/shared.png"), onStored)
        store.stored(other.id, ref("b/shared.png"), onStored)
        assertEquals(TransferState.STORED, transfer(first.id).state)
        assertEquals(TransferState.STORED, transfer(second.id).state)
    }

    @Test
    fun I16_a_child_at_maxAttempts_fails_the_parent() = runTest {
        val parent = store.seen(identity("set.json"), TransferKind.MESSAGE)
        val child = store.children(parent.id, listOf(staged("c1"))).single()
        store.failedAttempt(child.id, "boom", 1)
        assertEquals(TransferState.FAILED, transfer(parent.id).state)
    }

    @Test
    fun stuck_counts_the_rows_of_a_route_before_ACKED_older_than_the_instant() = runTest {
        store.seen(identity("a"), TransferKind.OBJECT)
        storedTransfer("b")
        store.acked(storedTransfer("c").id, emptyList())
        store.seen(identity("d", RouteName("other")), TransferKind.OBJECT)
        clock.advance(1.minutes)
        assertEquals(2, store.stuck(route, clock.instant()))
        assertEquals(0, store.stuck(route, clock.instant().minusSeconds(60)))
    }

    @Test
    fun byId_returns_the_row_in_any_state_and_null_for_an_unknown_id() = runTest {
        val seen = store.seen(identity("a"), TransferKind.OBJECT)
        val rejected = store.seen(identity("b"), TransferKind.OBJECT).also { store.rejected(it.id, "no") }
        assertEquals(seen, store.byId(seen.id))
        assertEquals(TransferState.REJECTED, store.byId(rejected.id)!!.state)
        assertNull(store.byId(TransferId(seen.id.value + rejected.id.value + 1000)))
    }

    @Test
    fun outboxPending_lists_exactly_the_PENDING_rows() = runTest {
        val a = storedTransfer("a", onStored)
        storedTransfer("b", onStored)
        val delivered = outbox().first { it.transferId == a.id }
        store.delivered(delivered.id, "ref")
        val pending = store.outboxPending()
        assertEquals(outbox().filter { it.state == DeliveryState.PENDING }.map { it.id }.toSet(), pending.map { it.id }.toSet())
        assertEquals(1, pending.size)
        assertTrue(pending.none { it.id == delivered.id })
    }
}
