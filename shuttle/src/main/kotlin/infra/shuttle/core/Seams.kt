package infra.shuttle.core

import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Path
import java.time.Instant

/** Spec 8.2. Every method is one transaction. */
interface StateStore {
    suspend fun find(identity: SourceIdentity): Transfer?
    /** The row a delivery points to; the notifier renders from it (spec 9.1). */
    suspend fun byId(id: TransferId): Transfer?
    suspend fun seen(identity: SourceIdentity, kind: TransferKind): Transfer
    suspend fun supersede(finished: TransferId, kind: TransferKind): Transfer
    suspend fun fetched(id: TransferId, staged: StagedSummary, events: List<DeliveryRequest>)
    suspend fun processed(id: TransferId, attributes: Map<String, String>)
    suspend fun children(id: TransferId, staged: List<StagedSummary>): List<Transfer>
    /** A parent's child rows in id order, empty for a row without children; what `verify` and S28 read (spec 4.3). */
    suspend fun childrenOf(id: TransferId): List<Transfer>
    /** [stored] is the object as it went to the target: after a rename or zip the row's `stored_name`, `digest` and `stored_mtime` are its, not the source's (ticket 45). */
    suspend fun stored(id: TransferId, target: TargetRef, stored: StagedSummary, events: List<DeliveryRequest>)
    suspend fun acked(id: TransferId, events: List<DeliveryRequest>)
    /** Spec 4.3's `reacked`: a finished row acked again; `updated_at` advances so D40's window restarts, nothing else changes. */
    suspend fun reacked(id: TransferId)
    suspend fun rejected(id: TransferId, reason: String)
    suspend fun failedAttempt(id: TransferId, error: String, maxAttempts: Int): Transfer
    suspend fun unlisted(route: RouteName, olderThan: Instant, listed: Set<SourceIdentity>): List<TransferId>
    suspend fun due(now: Instant, excluding: Set<DeliveryId>, limit: Int): List<Delivery>
    /** Every PENDING row, for the outbox gauges of spec 14.2; read-only. */
    suspend fun outboxPending(): List<Delivery>
    suspend fun delivered(id: DeliveryId, reference: String?)
    suspend fun retryLater(id: DeliveryId, at: Instant, status: String?, error: String)
    suspend fun deliveryFailed(id: DeliveryId, status: String?, error: String)
    suspend fun redrive(id: TransferId)
    suspend fun redriveDelivery(id: DeliveryId)
    suspend fun stuck(route: RouteName, olderThan: Instant): Int
}

/** Spec 7.1. After `store`, the current object at `key` is the one just written; nothing is ever deleted. */
interface ObjectStoreTarget {
    suspend fun store(key: String, file: Path, metadata: Map<String, String>): TargetRef
    suspend fun verify(ref: TargetRef): Boolean
    suspend fun probe()
}

/** Spec 9.2. `CancellationException` is never caught or converted. */
interface DeliveryChannel {
    val name: ChannelName
    val policy: DeliveryPolicy
    suspend fun deliver(event: DeliveryEvent): DeliveryOutcome
}

/** Spec 4.4: the named interleaving points, spelled as the crash matrix spells them. */
@Suppress("EnumEntryName")
enum class HookPoint { afterFetch, afterProcess, afterStore, afterLedgerStored, afterAck, afterLedgerAcked, afterDeliverySent }

interface Hook {
    suspend fun at(point: HookPoint, transfer: TransferId)

    /** The production runner: every point is a no-op. */
    object None : Hook {
        override suspend fun at(point: HookPoint, transfer: TransferId) = Unit
    }
}

/** Spec 9.6: a named bean the renderer calls, not the pipeline. */
fun interface Provider {
    suspend fun provide(transfer: Transfer): JsonNode
}
