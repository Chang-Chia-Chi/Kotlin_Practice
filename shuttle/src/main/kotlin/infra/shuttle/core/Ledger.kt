package infra.shuttle.core

/**
 * One route's ledger over the state store (spec 4.2, 9.1): the transitions that may create outbox rows -
 * `fetched`, `stored`, `acked` - and the `reacked` touch of 4.3. The route's `notify` entries for a moment
 * ride the seam call, so the rows are in the transition's own transaction (I11), and the notifier is woken
 * once after it, only when rows were created (D7). Stage 4, reconciliation (4.6) and the operator ack (14.1)
 * all write ACKED through here and hold neither the request list nor the wake. Every other transition of the
 * row is the store's own, reached through [store].
 */
class Ledger(val store: StateStore, notify: List<Notify>, private val wake: () -> Unit) {
    private val requests = DeliveryMoment.entries.associateWith { moment ->
        notify.filter { it.on == moment }.map { DeliveryRequest(moment, ChannelName(it.channel)) }
    }

    suspend fun fetched(id: TransferId, staged: StagedSummary) = transition(DeliveryMoment.FETCHED) { store.fetched(id, staged, it) }

    suspend fun stored(id: TransferId, target: TargetRef, stored: StagedSummary) = transition(DeliveryMoment.STORED) { store.stored(id, target, stored, it) }

    suspend fun acked(id: TransferId) = transition(DeliveryMoment.ACKED) { store.acked(id, it) }

    /** No state change, no rows, no wake: ACKED already proved it (ticket 23). */
    suspend fun reacked(id: TransferId) = store.reacked(id)

    private suspend fun transition(moment: DeliveryMoment, write: suspend (List<DeliveryRequest>) -> Unit) {
        val events = requests.getValue(moment)
        write(events)
        if (events.isNotEmpty()) wake()
    }
}
