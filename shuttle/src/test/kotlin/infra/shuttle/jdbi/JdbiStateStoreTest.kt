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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.oracle.OracleContainer
import java.time.Duration

/**
 * The spec 8.2 contract on a real Oracle. Excluded by default (`excludedGroups=oracle` in the pom);
 * run with `-DexcludedGroups=none -Dtest=JdbiStateStoreTest`. One container per class, the 8.1 DDL
 * applied once, both tables emptied before every test.
 */
@Tag("oracle")
class JdbiStateStoreTest : StateStoreContract() {

    companion object {
        // the image initialises its database on first start; 60 s is not enough on a loaded machine
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
}
