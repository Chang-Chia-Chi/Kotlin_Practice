package infra.shuttle.jdbi

import com.fasterxml.jackson.databind.ObjectMapper
import infra.shuttle.core.ChannelName
import infra.shuttle.core.Delivery
import infra.shuttle.core.DeliveryId
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
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.sql.ResultSet
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.Timestamp
import java.sql.Types
import java.time.Clock
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Calendar
import java.util.TimeZone

/**
 * Spec 8.2 on Oracle through JDBI. Every method is one transaction on [dispatcher], the module's
 * bounded IO view; the transitions that create outbox rows insert them inside that transaction, so a
 * failing insert rolls the transition back (I11, I20). Semantics match the test kit's in-memory store
 * row for row: identities resolve to their latest revision, a child's `stored` events attach to the
 * parent and are created when the parent flips, the outbox counts attempts on every outcome write.
 */
class JdbiStateStore(private val jdbi: Jdbi, private val dispatcher: CoroutineDispatcher, private val clock: Clock) : StateStore {
    private val json = ObjectMapper()

    override suspend fun find(identity: SourceIdentity) = tx { it.latest(identity) }

    override suspend fun byId(id: TransferId) = tx { h ->
        h.createQuery("SELECT * FROM file_transfer WHERE id = :id").bind("id", id.value).map { rs, _ -> transferRow(rs) }.findOne().orElse(null)
    }

    override suspend fun seen(identity: SourceIdentity, kind: TransferKind) = tx { h ->
        h.latest(identity) ?: try {
            h.insert(identity, kind)
        } catch (e: UnableToExecuteStatementException) {
            // a sibling pipeline won the race on uq_file_transfer_identity: its row is the answer
            if (e.cause is SQLIntegrityConstraintViolationException) h.latest(identity)!! else throw e
        }
    }

    override suspend fun supersede(finished: TransferId, kind: TransferKind) = tx { h ->
        val old = h.transfer(finished)
        h.insert(old.identity.copy(revision = old.identity.revision + 1), kind, supersedes = finished)
    }

    override suspend fun fetched(id: TransferId, staged: StagedSummary, events: List<DeliveryRequest>) = tx { h ->
        h.update(
            "UPDATE file_transfer SET state = 'FETCHED', source_digest = :d, digest = :d, digest_algo = :algo, stored_name = :name, stored_mtime = :mtime, updated_at = :now WHERE id = :id",
        ) { bind("d", staged.digest.hex).bind("algo", staged.digest.algorithm.name).bind("name", staged.name).bind("mtime", staged.mtime.ts()).bind("id", id.value) }
        h.insertDeliveries(id, events)
    }

    override suspend fun processed(id: TransferId, attributes: Map<String, String>) = tx { h ->
        h.update("UPDATE file_transfer SET state = 'PROCESSED', attributes = :attrs, updated_at = :now WHERE id = :id") {
            bind("attrs", json.writeValueAsString(attributes)).bind("id", id.value)
        }
        Unit
    }

    override suspend fun children(id: TransferId, staged: List<StagedSummary>) = tx { h ->
        val parent = h.transfer(id)
        h.update("DELETE FROM file_transfer WHERE parent_id = :id") { bind("id", id.value) }
        staged.forEach {
            val identity = parent.identity.copy(sourceName = it.name, sourceSize = it.size, sourceMtime = it.mtime)
            val child = h.insert(identity, TransferKind.CHILD, parent = id)
            h.update(
                "UPDATE file_transfer SET state = 'FETCHED', source_digest = :d, digest = :d, digest_algo = :algo, stored_name = :name, stored_mtime = :mtime WHERE id = :id",
            ) { bind("d", it.digest.hex).bind("algo", it.digest.algorithm.name).bind("name", it.name).bind("mtime", it.mtime.ts()).bind("id", child.id.value) }
        }
        h.childrenOf(id)
    }

    override suspend fun childrenOf(id: TransferId) = tx { it.childrenOf(id) }

    /**
     * D42: one update on the child's row, then the parent. Touching the parent row first takes its row
     * lock for the tail of this transaction, so two last children serialise there and the one that waits
     * re-reads its sibling committed; a zero-row conditional update would lock nothing and both would
     * see the other unstored, leaving the parent FETCHED for ever. The lock never spans I/O: the upload
     * happened before this call.
     */
    override suspend fun stored(id: TransferId, target: TargetRef, stored: StagedSummary, events: List<DeliveryRequest>) = tx { h ->
        h.update(
            "UPDATE file_transfer SET state = 'STORED', digest = :d, digest_algo = :algo, stored_name = :name, stored_mtime = :mtime, " +
                "target_kind = :tk, target_location = :tl, target_key = :key, target_ref = :ref, target_size = :size, updated_at = :now WHERE id = :id",
        ) {
            bind("d", stored.digest.hex).bind("algo", stored.digest.algorithm.name).bind("name", stored.name).bind("mtime", stored.mtime.ts())
                .bind("tk", target.kind).bind("tl", target.location).bind("key", target.key).bind("ref", target.ref).bind("size", target.size).bind("id", id.value)
        }
        val parent = h.transfer(id).parentId
        if (parent == null) {
            h.insertDeliveries(id, events)
        } else {
            h.update("UPDATE file_transfer SET updated_at = :now WHERE id = :id") { bind("id", parent.value) }
            val flipped = h.update(
                "UPDATE file_transfer p SET state = 'STORED', updated_at = :now WHERE p.id = :id AND p.state <> 'STORED' " +
                    "AND NOT EXISTS (SELECT 1 FROM file_transfer c WHERE c.parent_id = p.id AND c.state <> 'STORED')",
            ) { bind("id", parent.value) }
            if (flipped == 1) h.insertDeliveries(parent, events)
        }
    }

    override suspend fun acked(id: TransferId, events: List<DeliveryRequest>) = tx { h ->
        h.update("UPDATE file_transfer SET state = 'ACKED', acked_at = :now, updated_at = :now WHERE id = :id OR parent_id = :id") { bind("id", id.value) }
        h.insertDeliveries(id, events)
        h.finishWhenAllDelivered(id)
    }

    override suspend fun reacked(id: TransferId) = tx { h ->
        h.update("UPDATE file_transfer SET updated_at = :now WHERE id = :id") { bind("id", id.value) }
        Unit
    }

    override suspend fun rejected(id: TransferId, reason: String) = tx { h ->
        h.update("UPDATE file_transfer SET state = 'REJECTED', last_error = :e, updated_at = :now WHERE id = :id") { bind("e", reason).bind("id", id.value) }
        Unit
    }

    override suspend fun failedAttempt(id: TransferId, error: String, maxAttempts: Int) = tx { h ->
        h.update(
            "UPDATE file_transfer SET attempts = attempts + 1, last_error = :e, state = CASE WHEN attempts + 1 >= :max THEN 'FAILED' ELSE state END, updated_at = :now WHERE id = :id",
        ) { bind("e", error).bind("max", maxAttempts).bind("id", id.value) }
        val row = h.transfer(id)
        if (row.state == TransferState.FAILED && row.parentId != null) {
            h.update("UPDATE file_transfer SET state = 'FAILED', last_error = :e, updated_at = :now WHERE id = :id") {
                bind("e", "child ${id.value} failed: $error").bind("id", row.parentId.value)
            }
        }
        row
    }

    // ponytail: one select of the route's STORED rows, filtered here; STORED-but-unacked rows are few by design.
    override suspend fun unlisted(route: RouteName, olderThan: Instant, listed: Set<SourceIdentity>) = tx { h ->
        val keys = listed.map { it.key() }.toSet()
        h.createQuery("SELECT * FROM file_transfer WHERE route = :r AND kind <> 'CHILD' AND state = 'STORED' AND updated_at < :t")
            .bind("r", route.value).bind("t", olderThan.ts())
            .map { rs, _ -> transferRow(rs) }.list()
            .filter { it.identity.key() !in keys }.map { it.id }
    }

    override suspend fun due(now: Instant, excluding: Set<DeliveryId>, limit: Int) = tx { h ->
        val exclusion = if (excluding.isEmpty()) "" else " AND id NOT IN (<ex>)"
        // Oracle refuses FOR UPDATE over an ordered inline view (ORA-02014); locking the plain table by an id subquery is the sanctioned shape
        val q = h.createQuery(
            "SELECT * FROM delivery_outbox WHERE id IN (SELECT id FROM (SELECT id FROM delivery_outbox WHERE notification_state = 'PENDING' AND next_attempt_at <= :now$exclusion " +
                "ORDER BY next_attempt_at, id) WHERE ROWNUM <= :limit) ORDER BY next_attempt_at, id FOR UPDATE SKIP LOCKED",
        ).bind("now", now.ts()).bind("limit", limit)
        if (excluding.isNotEmpty()) q.bindList("ex", excluding.map { it.value })
        q.map { rs, _ -> deliveryRow(rs) }.list()
    }

    // ponytail: full PENDING scan per notifier pass, as the in-memory store does; an aggregate per channel is the upgrade if the outbox grows large.
    override suspend fun outboxPending() = tx { h ->
        h.createQuery("SELECT * FROM delivery_outbox WHERE notification_state = 'PENDING' ORDER BY id").map { rs, _ -> deliveryRow(rs) }.list()
    }

    override suspend fun delivered(id: DeliveryId, reference: String?) = tx { h ->
        h.update("UPDATE delivery_outbox SET notification_state = 'DELIVERED', reference = :ref, delivered_at = :now, attempts = attempts + 1 WHERE id = :id") {
            bind("ref", reference).bind("id", id.value)
        }
        val transfer = h.createQuery("SELECT file_transfer_id FROM delivery_outbox WHERE id = :id").bind("id", id.value).mapTo(Long::class.java).one()
        h.finishWhenAllDelivered(TransferId(transfer))
    }

    override suspend fun retryLater(id: DeliveryId, at: Instant, status: String?, error: String) = tx { h ->
        h.update("UPDATE delivery_outbox SET next_attempt_at = :at, last_status = :s, last_error = :e, attempts = attempts + 1 WHERE id = :id") {
            bind("at", at.ts()).bind("s", status).bind("e", error).bind("id", id.value)
        }
        Unit
    }

    override suspend fun deliveryFailed(id: DeliveryId, status: String?, error: String) = tx { h ->
        h.update("UPDATE delivery_outbox SET notification_state = 'FAILED', last_status = :s, last_error = :e, attempts = attempts + 1 WHERE id = :id") {
            bind("s", status).bind("e", error).bind("id", id.value)
        }
        Unit
    }

    override suspend fun redrive(id: TransferId) = tx { h ->
        h.update("UPDATE file_transfer SET state = 'SEEN', attempts = 0, last_error = NULL, updated_at = :now WHERE id = :id") { bind("id", id.value) }
        Unit
    }

    override suspend fun redriveDelivery(id: DeliveryId) = tx { h ->
        h.update("UPDATE delivery_outbox SET notification_state = 'PENDING', attempts = 0, next_attempt_at = :now WHERE id = :id") { bind("id", id.value) }
        Unit
    }

    override suspend fun stuck(route: RouteName, olderThan: Instant) = tx { h ->
        h.createQuery("SELECT COUNT(*) FROM file_transfer WHERE route = :r AND state IN ('SEEN', 'FETCHED', 'PROCESSED', 'STORED') AND updated_at < :t")
            .bind("r", route.value).bind("t", olderThan.ts()).mapTo(Int::class.java).one()
    }

    /**
     * Spec 14.1's listing (D57), two statements: the page of parents, then their children in one `IN` read.
     * Oracle does the filtering and the paging; nothing whole-table crosses the seam.
     */
    override suspend fun transfers(route: RouteName?, state: TransferState?, limit: Int) = tx { h ->
        val where = listOfNotNull("parent_id IS NULL", route?.let { "route = :route" }, state?.let { "state = :state" }).joinToString(" AND ")
        val parents = h.createQuery("SELECT * FROM file_transfer WHERE $where ORDER BY id DESC FETCH FIRST :limit ROWS ONLY")
            .bind("limit", limit)
            .apply { route?.let { bind("route", it.value) }; state?.let { bind("state", it.name) } }
            .map { rs, _ -> transferRow(rs) }.list()
        val children = if (parents.isEmpty()) emptyList() else {
            h.createQuery("SELECT * FROM file_transfer WHERE parent_id IN (<ids>) ORDER BY id")
                .bindList("ids", parents.map { it.id.value }).map { rs, _ -> transferRow(rs) }.list()
        }
        parents.map { parent -> parent to children.filter { it.parentId == parent.id } }
    }

    override suspend fun deliveries(transfer: TransferId) = tx { h ->
        h.createQuery("SELECT * FROM delivery_outbox WHERE file_transfer_id = :id ORDER BY id").bind("id", transfer.value)
            .map { rs, _ -> deliveryRow(rs) }.list()
    }

    override suspend fun delivery(id: DeliveryId): Delivery? = tx { h ->
        h.createQuery("SELECT * FROM delivery_outbox WHERE id = :id").bind("id", id.value).map { rs, _ -> deliveryRow(rs) }.findOne().orElse(null)
    }

    override suspend fun countsByState(route: RouteName) = tx { h ->
        h.createQuery("SELECT state, COUNT(*) AS n FROM file_transfer WHERE route = :r GROUP BY state").bind("r", route.value)
            .map { rs, _ -> TransferState.valueOf(rs.getString("state")) to rs.getInt("n") }.list().toMap()
    }

    /** Read side for the tests; not part of the seam, and production reads nothing whole-table (progress 10). */
    suspend fun transfer(id: TransferId): Transfer = tx { it.transfer(id) }
    suspend fun transfers(): List<Transfer> = tx { it.createQuery("SELECT * FROM file_transfer ORDER BY id").map { rs, _ -> transferRow(rs) }.list() }
    suspend fun outbox(): List<Delivery> = tx { it.createQuery("SELECT * FROM delivery_outbox ORDER BY id").map { rs, _ -> deliveryRow(rs) }.list() }

    private suspend fun <T> tx(block: (Handle) -> T): T = withContext(dispatcher) { jdbi.inTransaction<T, Exception> { h -> block(h) } }

    private fun Handle.update(sql: String, binder: org.jdbi.v3.core.statement.Update.() -> Unit = {}): Int =
        createUpdate(sql).bind("now", clock.instant().ts()).apply(binder).execute()

    private fun Handle.transfer(id: TransferId): Transfer =
        createQuery("SELECT * FROM file_transfer WHERE id = :id").bind("id", id.value).map { rs, _ -> transferRow(rs) }.one()

    private fun Handle.childrenOf(id: TransferId): List<Transfer> =
        createQuery("SELECT * FROM file_transfer WHERE parent_id = :id ORDER BY id").bind("id", id.value).map { rs, _ -> transferRow(rs) }.list()

    private fun Handle.latest(identity: SourceIdentity): Transfer? {
        val size = if (identity.sourceSize == null) "source_size IS NULL" else "source_size = :size"
        val mtime = if (identity.sourceMtime == null) "source_mtime IS NULL" else "source_mtime = :mtime"
        return createQuery(
            "SELECT * FROM file_transfer WHERE route = :route AND source_ref = :ref AND source_name = :name AND kind <> 'CHILD' AND $size AND $mtime " +
                "ORDER BY revision DESC FETCH FIRST 1 ROW ONLY",
        ).bind("route", identity.route.value).bind("ref", identity.sourceRef).bind("name", identity.sourceName)
            .apply { identity.sourceSize?.let { bind("size", it) }; identity.sourceMtime?.let { bind("mtime", it.ts()) } }
            .map { rs, _ -> transferRow(rs) }.findOne().orElse(null)
    }

    private fun Handle.insert(identity: SourceIdentity, kind: TransferKind, parent: TransferId? = null, supersedes: TransferId? = null): Transfer {
        val id = createQuery("SELECT file_transfer_seq.NEXTVAL FROM dual").mapTo(Long::class.java).one()
        update(
            "INSERT INTO file_transfer (id, route, parent_id, kind, source_kind, source_ref, source_name, source_size, source_mtime, revision, supersedes_id, state, attempts, first_seen_at, updated_at) " +
                "VALUES (:id, :route, :parent, :kind, :sk, :ref, :name, :size, :mtime, :rev, :sup, 'SEEN', 0, :now, :now)",
        ) {
            bind("id", id).bind("route", identity.route.value).bind("parent", parent?.value).bind("kind", kind.name).bind("sk", identity.sourceKind.name)
                .bind("ref", identity.sourceRef).bind("name", identity.sourceName).bind("size", identity.sourceSize).bind("mtime", identity.sourceMtime.ts())
                .bind("rev", identity.revision).bind("sup", supersedes?.value)
        }
        return transfer(TransferId(id))
    }

    /** One row per transfer, moment and channel (`uq_delivery_on_state_channel`): a transition run again after a crash keeps its row (I20). */
    private fun Handle.insertDeliveries(id: TransferId, events: List<DeliveryRequest>) = events.forEach {
        update(
            "INSERT INTO delivery_outbox (id, file_transfer_id, on_state, channel, notification_state, attempts, next_attempt_at, created_at) " +
                "SELECT delivery_outbox_seq.NEXTVAL, :tid, :moment, :channel, 'PENDING', 0, :now, :now FROM dual " +
                "WHERE NOT EXISTS (SELECT 1 FROM delivery_outbox WHERE file_transfer_id = :tid AND on_state = :moment AND channel = :channel)",
        ) { bind("tid", id.value).bind("moment", it.moment.name).bind("channel", it.channel.value) }
    }

    private fun Handle.finishWhenAllDelivered(id: TransferId) {
        val done = update(
            "UPDATE file_transfer SET state = 'DONE', completed_at = :now, updated_at = :now WHERE id = :id AND state = 'ACKED' " +
                "AND NOT EXISTS (SELECT 1 FROM delivery_outbox WHERE file_transfer_id = :id AND notification_state <> 'DELIVERED')",
        ) { bind("id", id.value) }
        if (done == 1) update("UPDATE file_transfer SET state = 'DONE', completed_at = :now, updated_at = :now WHERE parent_id = :id") { bind("id", id.value) }
    }

    private fun transferRow(rs: ResultSet): Transfer {
        val algo = rs.getString("digest_algo")?.let { DigestAlgorithm.valueOf(it) }
        fun digest(column: String) = rs.getString(column)?.let { Digest(algo!!, it) }
        return Transfer(
            id = TransferId(rs.getLong("id")),
            identity = SourceIdentity(
                RouteName(rs.getString("route")), SourceKind.valueOf(rs.getString("source_kind")), rs.getString("source_ref"), rs.getString("source_name"),
                rs.longOrNull("source_size"), rs.instant("source_mtime"), rs.getInt("revision"),
            ),
            kind = TransferKind.valueOf(rs.getString("kind")),
            state = TransferState.valueOf(rs.getString("state")),
            parentId = rs.longOrNull("parent_id")?.let(::TransferId),
            supersedesId = rs.longOrNull("supersedes_id")?.let(::TransferId),
            sourceDigest = digest("source_digest"),
            digest = digest("digest"),
            storedName = rs.getString("stored_name"),
            storedMtime = rs.instant("stored_mtime"),
            attempts = rs.getInt("attempts"),
            lastError = rs.getString("last_error"),
            attributes = rs.getString("attributes")?.let { json.readValue(it, MAP) } ?: emptyMap(),
            target = rs.getString("target_kind")?.let {
                TargetRef(it, rs.getString("target_location"), rs.getString("target_key"), rs.getString("target_ref"), rs.getLong("target_size"))
            },
            firstSeenAt = rs.instant("first_seen_at")!!,
            updatedAt = rs.instant("updated_at")!!,
            ackedAt = rs.instant("acked_at"),
            completedAt = rs.instant("completed_at"),
        )
    }

    private fun deliveryRow(rs: ResultSet) = Delivery(
        id = DeliveryId(rs.getLong("id")),
        transferId = TransferId(rs.getLong("file_transfer_id")),
        moment = DeliveryMoment.valueOf(rs.getString("on_state")),
        channel = ChannelName(rs.getString("channel")),
        state = DeliveryState.valueOf(rs.getString("notification_state")),
        attempts = rs.getInt("attempts"),
        nextAttemptAt = rs.instant("next_attempt_at")!!,
        lastStatus = rs.getString("last_status"),
        lastError = rs.getString("last_error"),
        reference = rs.getString("reference"),
        createdAt = rs.instant("created_at")!!,
        deliveredAt = rs.instant("delivered_at"),
    )

    private fun ResultSet.longOrNull(column: String): Long? = getObject(column)?.let { (it as Number).toLong() }
    private fun ResultSet.instant(column: String): Instant? = getTimestamp(column, utc())?.toInstant()

    /**
     * D50: an instant crosses the JDBC edge as UTC wall time, named on both sides. An 8.1 TIMESTAMP
     * keeps a date and a time and no offset, so whoever names the zone decides the digits; unnamed,
     * the driver takes the process default, and a default with DST gives one local time to two
     * instants an hour apart. Six fractional digits is all the column keeps, so truncating on the way
     * in makes what is read equal what was written.
     */
    private fun Instant?.ts(): Argument = Argument { position, statement, _ ->
        if (this == null) statement.setNull(position, Types.TIMESTAMP)
        else statement.setTimestamp(position, Timestamp.from(truncatedTo(ChronoUnit.MICROS)), utc())
    }

    private fun SourceIdentity.key() = copy(revision = 1, sourceMtime = sourceMtime?.truncatedTo(ChronoUnit.MICROS))

    private companion object {
        val MAP = object : com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {}
        val UTC: TimeZone = TimeZone.getTimeZone("UTC")

        /** The driver reads and writes the calendar it is handed, so each call gets its own. */
        fun utc(): Calendar = Calendar.getInstance(UTC)
    }
}
