package infra.shuttle.testkit

import infra.shuttle.core.DeliveryRequest
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferKind
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException

class InMemoryStateStoreTest : StateStoreContract() {
    override val store = InMemoryStateStore(clock)
    override suspend fun transfer(id: TransferId) = store.transfer(id)
    override suspend fun transfers() = store.transfers
    override suspend fun outbox() = store.outbox

    override suspend fun poisonedEvents(): List<DeliveryRequest> {
        store.failNextDeliveryInsert = true
        return onStored
    }

    override fun assertInjectedFailure(e: Throwable?) = assertTrue(e is IOException, "expected the injected IOException, got $e")

    @Test
    fun every_call_is_recorded_in_order() = runTest {
        store.find(identity("a"))
        store.seen(identity("a"), TransferKind.OBJECT)
        store.seen(identity("a"), TransferKind.OBJECT)
        store.find(identity("a"))
        assertEquals(listOf("find", "seen", "seen", "find"), store.calls.map { it.method })
    }
}
