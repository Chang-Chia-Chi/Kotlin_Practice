package infra.shuttle.jdbi

import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryRequest
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferKind
import infra.shuttle.testkit.StateStoreContract
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.oracle.OracleContainer
import java.time.Duration
import java.time.Instant
import java.util.TimeZone

/**
 * The spec 8.2 contract on a real Oracle. Excluded by default (`excludedGroups=oracle` in the pom);
 * run with `-DexcludedGroups=none -Dtest=JdbiStateStoreTest`. One container per class, the 8.1 DDL
 * applied once, both tables emptied before every test.
 */
@Tag("oracle")
class JdbiStateStoreTest : StateStoreContract() {

    companion object {
        // the faststart image takes about 90 s to say ready on a loaded workstation; the log wait's 60 s default never got there
        // (`withStartupTimeoutSeconds` is the JDBC field OracleContainer's own wait strategy ignores)
        private val container = OracleContainer("gvenzl/oracle-free:23-slim-faststart").withStartupTimeout(Duration.ofMinutes(10))
        private lateinit var jdbi: Jdbi

        @JvmStatic @BeforeAll
        fun start() {
            container.start()
            jdbi = Jdbi.create(container.jdbcUrl, container.username, container.password)
            jdbi.useHandle<Exception> { h -> StateStoreSchema.statements().forEach { h.execute(it) } }
        }

        @JvmStatic @AfterAll
        fun stop() = container.stop()
    }

    override val store = JdbiStateStore(jdbi, Dispatchers.IO, clock)
    override suspend fun transfer(id: TransferId) = store.transfer(id)
    override suspend fun transfers() = store.transfers()
    override suspend fun outbox() = store.outbox()

    /** `channel` is VARCHAR2(64): a longer name fails the outbox insert inside the transaction. */
    override suspend fun poisonedEvents() = listOf(DeliveryRequest(DeliveryMoment.STORED, ChannelName("x".repeat(65))))

    @BeforeEach
    fun empty() = jdbi.useHandle<Exception> { h ->
        h.execute("DELETE FROM delivery_outbox")
        h.execute("DELETE FROM file_transfer")
    }

    @Test
    fun seen_returns_the_existing_row_when_the_unique_identity_constraint_fires() = runTest {
        // the row a sibling committed a moment earlier, under the identity's own key
        val first = store.seen(identity("a"), TransferKind.OBJECT)
        val second = store.seen(identity("a"), TransferKind.OBJECT)
        assertEquals(first, second)
        assertEquals(1, transfers().size)
        // the constraint itself is the one the DDL declares, not a lookup: a direct duplicate insert is refused
        val refused = runCatching {
            jdbi.useHandle<Exception> { h ->
                h.execute(
                    "INSERT INTO file_transfer (id, route, kind, source_kind, source_ref, source_name, source_size, source_mtime, revision, state, attempts, first_seen_at, updated_at) " +
                        "SELECT file_transfer_seq.NEXTVAL, route, kind, source_kind, source_ref, source_name, source_size, source_mtime, revision, state, attempts, first_seen_at, updated_at FROM file_transfer WHERE id = ${first.id.value}",
                )
            }
        }
        assertTrue(refused.exceptionOrNull()?.cause is java.sql.SQLIntegrityConstraintViolationException, "expected ORA-00001, got ${refused.exceptionOrNull()}")
    }

    @Test
    fun due_skips_rows_another_session_holds_locked() = runTest {
        val t = storedTransfer("a")
        store.acked(t.id, listOf("c1", "c2").map { DeliveryRequest(DeliveryMoment.ACKED, ChannelName(it)) })
        val (d1, d2) = outbox()
        // a second session locks d1 and keeps its transaction open
        val other = jdbi.open()
        other.begin()
        other.createQuery("SELECT id FROM delivery_outbox WHERE id = :id FOR UPDATE").bind("id", d1.id.value).mapTo(Long::class.java).one()
        try {
            assertEquals(listOf(d2.id), store.due(clock.instant(), emptySet(), 10).map { it.id })
        } finally {
            other.rollback()
            other.close()
        }
        assertEquals(listOf(d1.id, d2.id), store.due(clock.instant(), emptySet(), 10).map { it.id })
    }

    /**
     * B6. Berlin shows 2026-10-25 02:30 twice: first at 00:30Z on CEST, then an hour later at 01:30Z
     * on CET. An instant bound through the default zone into a TIMESTAMP keeps the local digits and
     * loses which pass it was, so the two instants collapse onto one row value and every comparison on
     * `updated_at` -- reconciliation, the stuck gauge, D40's re-check window -- reads an hour off.
     */
    @Test
    fun B6_a_dst_fall_back_hour_does_not_shift_updated_at_on_oracle() = inZone("Europe/Berlin") {
        val storedAt = Instant.parse("2026-10-25T00:30:00Z")
        clock.set(storedAt)
        val t = storedTransfer("dst")

        assertEquals(storedAt, transfer(t.id).updatedAt)
        assertEquals(listOf(t.id), store.unlisted(route, Instant.parse("2026-10-25T01:30:00Z"), emptySet()))
        assertEquals(1, store.stuck(route, Instant.parse("2026-10-25T01:30:00Z")))
    }

    /** The same hour under `source_mtime` (spec 5.2): two mtimes an hour apart are two identities, not one. */
    @Test
    fun B6_source_mtime_identity_separates_the_two_passes_of_the_fall_back_hour() = inZone("Europe/Berlin") {
        val early = identity("dst").copy(sourceMtime = Instant.parse("2026-10-25T00:30:00Z"))
        val late = early.copy(sourceMtime = Instant.parse("2026-10-25T01:30:00Z"))

        val first = store.seen(early, TransferKind.OBJECT)

        assertEquals(early.sourceMtime, transfer(first.id).identity.sourceMtime)
        assertEquals(first, store.find(early))
        assertNull(store.find(late), "a later mtime is a different object, not this row")
        assertNotEquals(first.id, store.seen(late, TransferKind.OBJECT).id)
    }

    /** The zone the process runs in, for the length of one test, restored however it ends. */
    private fun inZone(zone: String, body: suspend () -> Unit) = runTest {
        val previous = TimeZone.getDefault()
        TimeZone.setDefault(TimeZone.getTimeZone(zone))
        try {
            body()
        } finally {
            TimeZone.setDefault(previous)
        }
    }
}
