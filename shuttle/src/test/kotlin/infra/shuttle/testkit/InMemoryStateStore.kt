package infra.shuttle.testkit

import infra.shuttle.core.Delivery
import infra.shuttle.core.DeliveryId
import infra.shuttle.core.DeliveryRequest
import infra.shuttle.core.DeliveryState
import infra.shuttle.core.RouteName
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.StagedSummary
import infra.shuttle.core.StateStore
import infra.shuttle.core.TargetRef
import infra.shuttle.core.Transfer
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferKind
import infra.shuttle.core.TransferState
import infra.shuttle.core.TransferState.ACKED
import infra.shuttle.core.TransferState.DONE
import infra.shuttle.core.TransferState.FAILED
import infra.shuttle.core.TransferState.FETCHED
import infra.shuttle.core.TransferState.PROCESSED
import infra.shuttle.core.TransferState.REJECTED
import infra.shuttle.core.TransferState.SEEN
import infra.shuttle.core.TransferState.STORED
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.IOException
import java.time.Clock
import java.time.Instant

/**
 * Spec 8.2 in memory. Every method is one transaction: the two tables are snapshotted on entry and
 * restored when the method throws, so a failing delivery insert (`failNextDeliveryInsert`, one-shot)
 * leaves the transfer row exactly as it was (I11, I20). Every call is appended to [calls].
 *
 * Identity: `find`, `seen` and `unlisted` compare identities without `revision` and resolve to the
 * latest revision, because a listing always carries revision 1 and the runner must see the row that
 * `supersede` created. Children are never found by identity; they are reached through `parentId`.
 */
class InMemoryStateStore(private val clock: Clock) : StateStore {
    data class Call(val method: String, val args: List<Any?>)

    val calls = mutableListOf<Call>()
    @Volatile var failNextDeliveryInsert = false

    private val mutex = Mutex()
    private val rows = LinkedHashMap<TransferId, Transfer>()
    private val outboxRows = LinkedHashMap<DeliveryId, Delivery>()
    private var nextTransfer = 1L
    private var nextDelivery = 1L

    val transfers: List<Transfer> get() = rows.values.toList()
    val outbox: List<Delivery> get() = outboxRows.values.toList()
    fun transfer(id: TransferId): Transfer = rows.getValue(id)

    override suspend fun find(identity: SourceIdentity) = tx("find", identity) { latest(identity) }

    override suspend fun seen(identity: SourceIdentity, kind: TransferKind) = tx("seen", identity, kind) {
        latest(identity) ?: insert(identity, kind)
    }

    override suspend fun supersede(finished: TransferId, kind: TransferKind) = tx("supersede", finished, kind) {
        val old = rows.getValue(finished)
        insert(old.identity.copy(revision = old.identity.revision + 1), kind, supersedes = finished)
    }

    override suspend fun fetched(id: TransferId, staged: StagedSummary, events: List<DeliveryRequest>) = tx("fetched", id, staged, events) {
        update(id) { copy(state = FETCHED, sourceDigest = staged.digest, digest = staged.digest, storedName = staged.name, storedMtime = staged.mtime) }
        insertDeliveries(id, events)
    }

    override suspend fun processed(id: TransferId, attributes: Map<String, String>) = tx("processed", id, attributes) {
        update(id) { copy(state = PROCESSED, attributes = attributes) }
        Unit
    }

    override suspend fun children(id: TransferId, staged: List<StagedSummary>) = tx("children", id, staged) {
        val parent = rows.getValue(id)
        rows.values.removeIf { it.parentId == id }
        staged.map {
            insert(parent.identity.copy(sourceName = it.name, sourceSize = it.size, sourceMtime = it.mtime), TransferKind.CHILD, parent = id)
                .let { child -> update(child.id) { copy(state = FETCHED, sourceDigest = it.digest, digest = it.digest, storedName = it.name, storedMtime = it.mtime) } }
        }
    }

    /** One update on the row, then the conditional parent update that fires when no sibling is left unstored (D42). */
    override suspend fun stored(id: TransferId, target: TargetRef, events: List<DeliveryRequest>) = tx("stored", id, target, events) {
        val row = update(id) { copy(state = STORED, target = target) }
        val parent = row.parentId
        when {
            parent == null -> insertDeliveries(id, events)
            rows.values.none { it.parentId == parent && it.state != STORED } -> {
                update(parent) { copy(state = STORED) }
                insertDeliveries(parent, events)
            }
            else -> Unit
        }
    }

    override suspend fun acked(id: TransferId, events: List<DeliveryRequest>) = tx("acked", id, events) {
        val at = clock.instant()
        update(id) { copy(state = ACKED, ackedAt = at) }
        childrenOf(id).forEach { update(it.id) { copy(state = ACKED, ackedAt = at) } }
        insertDeliveries(id, events)
        finishWhenAllDelivered(id)
    }

    override suspend fun rejected(id: TransferId, reason: String) = tx("rejected", id, reason) {
        update(id) { copy(state = REJECTED, lastError = reason) }
        Unit
    }

    override suspend fun failedAttempt(id: TransferId, error: String, maxAttempts: Int) = tx("failedAttempt", id, error, maxAttempts) {
        val row = update(id) { copy(attempts = attempts + 1, lastError = error, state = if (attempts + 1 >= maxAttempts) FAILED else state) }
        if (row.state == FAILED) row.parentId?.let { update(it) { copy(state = FAILED, lastError = "child ${id.value} failed: $error") } }
        row
    }

    override suspend fun unlisted(route: RouteName, olderThan: Instant, listed: Set<SourceIdentity>) = tx("unlisted", route, olderThan, listed) {
        val keys = listed.map { it.key() }.toSet()
        rows.values.filter {
            it.kind != TransferKind.CHILD && it.identity.route == route && it.state == STORED &&
                it.updatedAt < olderThan && it.identity.key() !in keys
        }.map { it.id }
    }

    override suspend fun due(now: Instant, excluding: Set<DeliveryId>, limit: Int) = tx("due", now, excluding, limit) {
        outboxRows.values.filter { it.state == DeliveryState.PENDING && it.nextAttemptAt <= now && it.id !in excluding }
            .sortedBy { it.nextAttemptAt }.take(limit)
    }

    override suspend fun delivered(id: DeliveryId, reference: String?) = tx("delivered", id, reference) {
        val row = updateDelivery(id) { copy(state = DeliveryState.DELIVERED, reference = reference, deliveredAt = clock.instant()) }
        finishWhenAllDelivered(row.transferId)
    }

    override suspend fun retryLater(id: DeliveryId, at: Instant, status: String?, error: String) = tx("retryLater", id, at, status, error) {
        updateDelivery(id) { copy(nextAttemptAt = at, lastStatus = status, lastError = error) }
        Unit
    }

    override suspend fun deliveryFailed(id: DeliveryId, status: String?, error: String) = tx("deliveryFailed", id, status, error) {
        updateDelivery(id) { copy(state = DeliveryState.FAILED, lastStatus = status, lastError = error) }
        Unit
    }

    override suspend fun redrive(id: TransferId) = tx("redrive", id) {
        update(id) { copy(state = SEEN, attempts = 0, lastError = null) }
        Unit
    }

    override suspend fun redriveDelivery(id: DeliveryId) = tx("redriveDelivery", id) {
        outboxRows[id] = outboxRows.getValue(id).copy(state = DeliveryState.PENDING, attempts = 0, nextAttemptAt = clock.instant())
    }

    override suspend fun stuck(route: RouteName, olderThan: Instant) = tx("stuck", route, olderThan) {
        rows.values.count { it.identity.route == route && it.state in BEFORE_ACKED && it.updatedAt < olderThan }
    }

    // ponytail: whole-table snapshot per transaction; fine for test-sized tables, an undo log if they ever grow.
    private suspend fun <T> tx(method: String, vararg args: Any?, block: () -> T): T = mutex.withLock {
        calls += Call(method, args.toList())
        val (savedRows, savedOutbox, savedIds) = Triple(LinkedHashMap(rows), LinkedHashMap(outboxRows), nextTransfer to nextDelivery)
        try {
            block()
        } catch (e: Throwable) {
            rows.clear(); rows.putAll(savedRows)
            outboxRows.clear(); outboxRows.putAll(savedOutbox)
            nextTransfer = savedIds.first; nextDelivery = savedIds.second
            throw e
        }
    }

    private fun insert(identity: SourceIdentity, kind: TransferKind, parent: TransferId? = null, supersedes: TransferId? = null): Transfer {
        val now = clock.instant()
        val row = Transfer(TransferId(nextTransfer++), identity, kind, SEEN, parentId = parent, supersedesId = supersedes, firstSeenAt = now, updatedAt = now)
        rows[row.id] = row
        return row
    }

    private fun insertDeliveries(id: TransferId, events: List<DeliveryRequest>) {
        if (failNextDeliveryInsert) {
            failNextDeliveryInsert = false
            throw IOException("injected: delivery insert failed")
        }
        val now = clock.instant()
        events.forEach {
            val row = Delivery(DeliveryId(nextDelivery++), id, it.moment, it.channel, DeliveryState.PENDING, 0, nextAttemptAt = now, createdAt = now)
            outboxRows[row.id] = row
        }
    }

    private fun finishWhenAllDelivered(id: TransferId) {
        val mine = outboxRows.values.filter { it.transferId == id }
        if (rows.getValue(id).state == ACKED && mine.all { it.state == DeliveryState.DELIVERED }) {
            val at = clock.instant()
            update(id) { copy(state = DONE, completedAt = at) }
            childrenOf(id).forEach { update(it.id) { copy(state = DONE, completedAt = at) } }
        }
    }

    private fun update(id: TransferId, change: Transfer.() -> Transfer): Transfer =
        rows.getValue(id).change().copy(updatedAt = clock.instant()).also { rows[id] = it }

    private fun updateDelivery(id: DeliveryId, change: Delivery.() -> Delivery): Delivery =
        outboxRows.getValue(id).change().let { it.copy(attempts = it.attempts + 1) }.also { outboxRows[id] = it }

    private fun childrenOf(id: TransferId) = rows.values.filter { it.parentId == id }
    private fun latest(identity: SourceIdentity) =
        rows.values.filter { it.kind != TransferKind.CHILD && it.identity.key() == identity.key() }.maxByOrNull { it.identity.revision }
    private fun SourceIdentity.key() = copy(revision = 1)

    private companion object {
        val BEFORE_ACKED = setOf(SEEN, FETCHED, PROCESSED, STORED)
    }
}
