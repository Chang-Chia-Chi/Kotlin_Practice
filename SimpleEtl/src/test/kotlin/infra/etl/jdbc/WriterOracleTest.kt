package infra.etl.jdbc

import infra.etl.CountingConnections
import infra.etl.Scratch
import infra.etl.Scratch.STEP
import infra.etl.duckdb.CreateTable
import infra.etl.duckdb.DuckDbTableWriter
import infra.etl.jdbc.JdbcStatementWriter
import infra.etl.jdbc.JdbcTableWriter
import infra.etl.pipe.Row
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDateTime
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
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer

/**
 * The half of P2 that needs a real Oracle: the NOT NULL branch of AUTO DDL, and both JDBC
 * writers. One container for the whole class - the image is multi-GB.
 *
 * Why AUTO DDL is tested here at all: duckdb_jdbc 1.1.3 reports columnNullable for every column,
 * NOT NULL included, so a DuckDB-sourced AUTO DDL test can never reach "NOT NULL columns keep
 * their natural mapping". Oracle reports nullability truthfully, so this is the only source that
 * can. It is also the only source that can produce a NUMBER(38,10), which is where a bare
 * DECIMAL in the generated DDL silently truncates to DECIMAL(18,3).
 */
@Testcontainers
@Tag("oracle")
class WriterOracleTest {

    companion object {

        private const val BIG = "1234567890123456789012345678.1234567891"

        @Container
        @JvmStatic
        val oracle: OracleContainer = OracleContainer("gvenzl/oracle-free:slim-faststart")

        private lateinit var connection: Connection

        private fun connect(): Connection =
            DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)

        @BeforeAll
        @JvmStatic
        fun createFixture() {
            connection = connect()
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                    create table wip_src (
                        lot_id    NUMBER(18)    not null,
                        lot_code  VARCHAR2(20)  not null,
                        upd_ts    TIMESTAMP     not null,
                        is_active BOOLEAN       not null,
                        ratio     BINARY_DOUBLE not null,
                        big_qty   NUMBER(38,10) not null,
                        qty       NUMBER(18,3),
                        note      VARCHAR2(20)
                    )
                    """,
                )
                // The target's column order deliberately differs from the source's. A positional
                // writer would put lot_id into note here, and Oracle would coerce the number to
                // text without complaint - the silent corruption this phase exists to prevent.
                statement.execute(
                    """
                    create table wip_tgt (
                        note     VARCHAR2(20),
                        qty      NUMBER(18,3),
                        lot_code VARCHAR2(20) not null,
                        lot_id   NUMBER(18)   not null
                    )
                    """,
                )
                // Column-listed inserts, never positional: the framework exists because
                // positional column mapping misaligns silently, and a fixture is not exempt.
                statement.execute(
                    """
                    insert into wip_src (lot_id, lot_code, upd_ts, is_active, ratio, big_qty, qty, note)
                    values (7, 'L1', TIMESTAMP '2024-01-02 03:04:05', TRUE, 2.5, $BIG, 1.5, 'note-1')
                    """,
                )
                statement.execute(
                    """
                    insert into wip_src (lot_id, lot_code, upd_ts, is_active, ratio, big_qty, qty, note)
                    values (8, 'L2', TIMESTAMP '2024-01-02 03:04:05', FALSE, 0, 0, NULL, NULL)
                    """,
                )
            }
            // No commit(): ojdbc opens the connection with autoCommit on and ORA-17273s an
            // explicit commit.
        }

        @AfterAll
        @JvmStatic
        fun closeFixture() {
            if (::connection.isInitialized) connection.close()
        }
    }

    private lateinit var counting: CountingConnections
    private lateinit var jdbi: Jdbi

    @BeforeEach
    fun freshTarget() {
        Scratch.exec(connection, "delete from wip_tgt")
        counting = CountingConnections { connect() }
        jdbi = Jdbi.create(counting)
    }

    private fun source(sql: String) = Scratch.read(connection, sql)

    private val allColumns =
        "select lot_id, lot_code, upd_ts, is_active, ratio, big_qty, note from wip_src where lot_id = 7"

    /**
     * Done-when 1, NOT NULL half: a column the source states is NOT NULL keeps its natural
     * mapping - BOOLEAN stays BOOLEAN and BINARY_DOUBLE stays DOUBLE, neither of which a nullable
     * column may use, because their append overloads are primitive. `note` is the control: it is
     * the one nullable column and must come through as VARCHAR either way.
     */
    @Test
    fun `AUTO DDL gives a NOT NULL source column its natural DuckDB mapping`() {
        val source = source(allColumns)
        assertEquals(listOf("note"), source.columns.filter { it.nullable }.map { it.name }) {
            "Oracle reports nullability truthfully; the NOT NULL branch is only reachable from here"
        }

        Scratch.open().use { duck ->
            DuckDbTableWriter(duck, "wip_stg", CreateTable.AUTO, STEP).use { it.open(source.columns) }

            assertEquals(
                listOf(
                    "lot_id" to "DECIMAL(18,0)",
                    "lot_code" to "VARCHAR",
                    "upd_ts" to "TIMESTAMP",
                    "is_active" to "BOOLEAN",
                    "ratio" to "DOUBLE",
                    "big_qty" to "DECIMAL(38,10)",
                    "note" to "VARCHAR",
                ),
                Scratch.declaredTypes(duck, "wip_stg"),
            )
        }
    }

    /**
     * The silent-truncation case P1 flagged. CanonicalType.DECIMAL.duckDbType is the bare string
     * "DECIMAL", which DuckDB resolves to DECIMAL(18,3), so an Oracle NUMBER(38,10) generated
     * unqualified loses 25 digits of integer part and 7 of scale. Asserting the value rather than
     * the DDL is the point: this is a data-loss test, not a formatting test.
     */
    @Test
    fun `a high precision Oracle NUMBER survives the AUTO DDL round trip exactly`() {
        val source = source(allColumns)

        Scratch.open().use { duck ->
            DuckDbTableWriter(duck, "wip_stg", CreateTable.AUTO, STEP).use {
                it.open(source.columns)
                assertEquals(1, it.write(source.rows))
            }

            val row = Scratch.read(duck, "select big_qty, is_active, ratio, upd_ts, note from wip_stg").rows.single()
            assertAll(
                {
                    assertTrue(row.decimal("big_qty")?.compareTo(BigDecimal(BIG)) == 0) {
                        "expected $BIG by comparison, was ${row.decimal("big_qty")}"
                    }
                },
                // The primitive append path, reachable only from a source that reports NOT NULL.
                { assertTrue(row.bool("is_active") == true) { "is_active was ${row.bool("is_active")}" } },
                { assertEquals(2.5, row.double("ratio")) },
                { assertEquals(LocalDateTime.parse("2024-01-02T03:04:05"), row.dateTime("upd_ts")) },
                { assertEquals("note-1", row.string("note")) },
            )
        }
    }

    /**
     * Done-when 3 for the JDBC target, plus done-when 2's null rule and done-when 7's happy path.
     * The source hands four columns in one order and the target declares them in another; a
     * positional bind would land lot_id in note.
     *
     * The null row also pins how a null is bound: Oracle rejects setObject(i, null) with no type,
     * so a writer that does not take the type from catalog metadata fails here rather than in
     * production.
     */
    @Test
    fun `JdbcTableWriter maps by column name against catalog metadata and writes real nulls`() {
        val source = source("select lot_id, lot_code, qty, note from wip_src order by lot_id")

        JdbcTableWriter(jdbi, "wip_tgt", STEP).use {
            it.open(source.columns)
            assertEquals(2, it.write(source.rows))
        }

        val back = Scratch.read(connection, "select lot_id, lot_code, qty, note from wip_tgt order by lot_id").rows
        assertAll(
            {
                assertTrue(back[0].decimal("lot_id")?.compareTo(BigDecimal("7")) == 0) {
                    "expected 7 by comparison, was ${back[0].decimal("lot_id")}"
                }
            },
            { assertEquals("L1", back[0].string("lot_code")) },
            {
                assertTrue(back[0].decimal("qty")?.compareTo(BigDecimal("1.5")) == 0) {
                    "expected 1.5 by comparison, was ${back[0].decimal("qty")}"
                }
            },
            { assertEquals("note-1", back[0].string("note")) },
        )

        assertAll(
            { assertEquals("L2", back[1].string("lot_code")) },
            { assertNotEquals(BigDecimal.ZERO, back[1].decimal("qty")) },
            { assertNull(back[1].decimal("qty")) { "was ${back[1].decimal("qty")}" } },
            { assertNotEquals("", back[1].string("note")) },
            { assertNull(back[1].string("note")) { "was ${back[1].string("note")}" } },
        )

        counting.assertCatalogReadBalanced("successful table write")
    }

    @Test
    fun `JdbcStatementWriter binds Row keys by name`() {
        val source = source("select lot_id, lot_code, qty, note from wip_src where lot_id = 7")

        JdbcStatementWriter(
            jdbi,
            "insert into wip_tgt (lot_id, lot_code, qty, note) values (:lot_id, :lot_code, :qty, :note)",
            STEP,
        ).use {
            it.open(source.columns)
            assertEquals(1, it.write(source.rows))
        }

        val row = Scratch.read(connection, "select lot_id, lot_code, qty, note from wip_tgt").rows.single()
        assertAll(
            {
                assertTrue(row.decimal("lot_id")?.compareTo(BigDecimal("7")) == 0) {
                    "expected 7 by comparison, was ${row.decimal("lot_id")}"
                }
            },
            { assertEquals("note-1", row.string("note")) },
        )

        counting.assertBalanced("successful statement write")
    }

    /**
     * Done-when 6. Every missing name must be listed, not just the first one found: an author
     * fixing a MERGE one name per 30-minute run is the failure mode this rules out. The check is
     * allowed no earlier than the first chunk, so the assertion is on what is reported, not on
     * which call reports it.
     */
    @Test
    fun `JdbcStatementWriter lists every missing bind name and writes nothing`() {
        val source = source("select lot_id, lot_code, qty, note from wip_src where lot_id = 7")

        val thrown = assertThrows<Throwable> {
            JdbcStatementWriter(
                jdbi,
                "insert into wip_tgt (lot_id, lot_code, qty, note) " +
                    "values (:lot_id, :lot_code, :missing_qty, :missing_note)",
                STEP,
            ).use {
                it.open(source.columns)
                it.write(source.rows)
            }
        }

        assertAll(
            { assertTrue(thrown.message?.contains(STEP) == true) { "message was: ${thrown.message}" } },
            {
                assertTrue(thrown.message?.contains("missing_qty") == true) {
                    "every missing bind name must be listed; message was: ${thrown.message}"
                }
            },
            {
                assertTrue(thrown.message?.contains("missing_note") == true) {
                    "every missing bind name must be listed; message was: ${thrown.message}"
                }
            },
            { assertEquals(0L, Scratch.rowCount(connection, "wip_tgt")) },
        )
        counting.assertNothingLeaked("missing bind names")
    }

    /**
     * Done-when 7, the path that proves the fixture is not vacuous: the write throws inside
     * execute, so only a writer that closes on the exception path balances. A 40 character note
     * into a VARCHAR2(20) column is an ORA-12899, which is a data error and not transient, so no
     * retry ambiguity is involved.
     */
    @Test
    fun `statements and connections are closed when the write throws mid chunk`() {
        val source = source("select lot_id, lot_code, qty, rpad('x', 40, 'x') as note from wip_src where lot_id = 7")

        assertThrows<Throwable> {
            JdbcTableWriter(jdbi, "wip_tgt", STEP).use {
                it.open(source.columns)
                it.write(source.rows)
            }
        }

        counting.assertCatalogReadBalanced("failed table write")

        // Return to a usable state: the target still takes a good chunk afterwards.
        val good = source("select lot_id, lot_code, qty, note from wip_src where lot_id = 7")
        JdbcTableWriter(jdbi, "wip_tgt", STEP).use {
            it.open(good.columns)
            assertEquals(1, it.write(good.rows))
        }
        assertEquals(1L, Scratch.rowCount(connection, "wip_tgt"))
    }

    @Test
    fun `closing a JDBC writer that was never opened leaks nothing`() {
        JdbcTableWriter(jdbi, "wip_tgt", STEP).close()
        JdbcStatementWriter(jdbi, "insert into wip_tgt (lot_id) values (:lot_id)", STEP).close()

        counting.assertNothingLeaked("never opened")
    }
}
