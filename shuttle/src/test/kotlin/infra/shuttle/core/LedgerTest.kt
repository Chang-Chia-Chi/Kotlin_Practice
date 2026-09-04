package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.ScriptedSource
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.time.Duration.Companion.minutes

/** Spec 4.2 and 9.1 on the fake store: one route's ledger writes each transition with exactly that moment's rows, then wakes once. */
class LedgerTest {
    private val clock = ClockFixture()
    private val store = InMemoryStateStore(clock)
    private var wakes = 0
    private val staged = StagedSummary("a.csv", 1, clock.instant(), Digest(DigestAlgorithm.MD5, "d"), null)
    private val ref = TargetRef("memory", "landing", "a.csv", "v1", 1)

    private fun ledger(vararg notify: Notify) = Ledger(store, notify.toList()) { wakes++ }

    private suspend fun storedRow(): TransferId {
        val t = store.seen(ScriptedSource.identity("a.csv"), TransferKind.OBJECT)
        store.fetched(t.id, staged, emptyList())
        store.processed(t.id, emptyMap())
        store.stored(t.id, ref, emptyList())
        return t.id
    }

    @Test
    fun acked_writes_the_row_creates_exactly_the_routes_acked_rows_and_wakes_once() = runTest {
        val id = storedRow()

        ledger(Notify(DeliveryMoment.ACKED, "downstream"), Notify(DeliveryMoment.STORED, "audit"), Notify(DeliveryMoment.ACKED, "second")).acked(id)

        assertEquals(TransferState.ACKED, store.transfer(id).state)
        assertEquals(listOf("downstream", "second"), store.outbox.map { it.channel.value }, "the acked channels, in the route's order, and no STORED row")
        assertTrue(store.outbox.all { it.transferId == id && it.moment == DeliveryMoment.ACKED && it.state == DeliveryState.PENDING })
        assertEquals(1, wakes)
    }

    @Test
    fun fetched_and_stored_create_their_own_moments_rows_and_a_moment_nobody_listens_to_wakes_nobody() = runTest {
        val id = store.seen(ScriptedSource.identity("a.csv"), TransferKind.OBJECT).id
        val ledger = ledger(Notify(DeliveryMoment.STORED, "audit"))

        ledger.fetched(id, staged)
        assertEquals(TransferState.FETCHED, store.transfer(id).state)
        assertTrue(store.outbox.isEmpty(), "nobody listens to fetched")
        assertEquals(0, wakes)

        ledger.stored(id, ref)
        assertEquals(TransferState.STORED, store.transfer(id).state)
        assertEquals(listOf(DeliveryMoment.STORED to ChannelName("audit")), store.outbox.map { it.moment to it.channel })
        assertEquals(1, wakes)
    }

    @Test
    fun I11_a_failing_transaction_leaves_the_row_and_the_outbox_as_they_were_and_wakes_nobody() = runTest {
        val id = storedRow()
        store.failNextDeliveryInsert = true

        val thrown = try { ledger(Notify(DeliveryMoment.ACKED, "downstream")).acked(id); null } catch (e: IOException) { e }
        assertTrue(thrown != null, "the failed transaction is the caller's error, not swallowed")

        assertEquals(TransferState.STORED, store.transfer(id).state)
        assertTrue(store.outbox.isEmpty())
        assertEquals(0, wakes)
    }

    @Test
    fun reacked_touches_the_row_creates_nothing_and_wakes_nobody() = runTest {
        val id = storedRow()
        ledger(Notify(DeliveryMoment.ACKED, "downstream")).acked(id)
        val before = store.transfer(id)
        clock.advance(1.minutes)

        ledger(Notify(DeliveryMoment.ACKED, "downstream")).reacked(id)

        assertEquals(before.copy(updatedAt = clock.instant()), store.transfer(id))
        assertEquals(1, store.outbox.size, "no new row")
        assertEquals(1, wakes, "no second wake")
    }
}
