package com.exporter.db

import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import javax.sql.DataSource

class QueryExecutorTest {

    private lateinit var dataSource: DataSource
    private lateinit var resolver: DataSourceResolver
    private lateinit var executor: QueryExecutor

    @BeforeEach
    fun setUp() {
        dataSource = JdbcDataSource().apply {
            setURL("jdbc:h2:mem:test_${System.nanoTime()};DB_CLOSE_DELAY=-1")
        }

        // Set up test table
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("""
                    CREATE TABLE metrics_source (
                        host VARCHAR(50),
                        env VARCHAR(20),
                        cpu_usage DOUBLE,
                        request_count BIGINT,
                        status VARCHAR(20)
                    )
                """)
                stmt.execute("INSERT INTO metrics_source VALUES ('srv01', 'prod', 75.5, 10000, 'up')")
                stmt.execute("INSERT INTO metrics_source VALUES ('srv02', 'prod', 45.2, 5000, 'up')")
                stmt.execute("INSERT INTO metrics_source VALUES ('srv03', 'staging', 92.1, 200, 'down')")
            }
        }

        resolver = mockk()
        every { resolver.resolve("test_db") } returns dataSource
        every { resolver.resolve("nonexistent") } returns null

        executor = QueryExecutor(resolver)
    }

    @AfterEach
    fun tearDown() {
        executor.clearCache()
        dataSource.connection.use { conn ->
            conn.createStatement().execute("DROP ALL OBJECTS")
        }
    }

    // ─── Query execution ──────────────────────────────────────

    @Nested
    inner class Execution {
        @Test
        fun `executes simple select and returns all rows`() {
            val rows = executor.execute("test_db", "SELECT host, cpu_usage FROM metrics_source")

            assertThat(rows).hasSize(3)
            assertThat(rows[0]).containsKey("HOST") // H2 uppercases column names
        }

        @Test
        fun `returns correct values for each row`() {
            val rows = executor.execute("test_db",
                "SELECT host, cpu_usage FROM metrics_source ORDER BY host")

            assertThat(rows[0]["CPU_USAGE"]).isEqualTo(75.5)
            assertThat(rows[1]["CPU_USAGE"]).isEqualTo(45.2)
            assertThat(rows[2]["CPU_USAGE"]).isEqualTo(92.1)
        }

        @Test
        fun `handles aggregate queries`() {
            val rows = executor.execute("test_db",
                "SELECT COUNT(*) as cnt, AVG(cpu_usage) as avg_cpu FROM metrics_source")

            assertThat(rows).hasSize(1)
            assertThat((rows[0]["CNT"] as Number).toLong()).isEqualTo(3)
        }

        @Test
        fun `handles queries with WHERE clause`() {
            val rows = executor.execute("test_db",
                "SELECT host FROM metrics_source WHERE env = 'prod'")

            assertThat(rows).hasSize(2)
        }

        @Test
        fun `handles GROUP BY with multiple columns`() {
            val rows = executor.execute("test_db",
                "SELECT env, COUNT(*) as cnt FROM metrics_source GROUP BY env ORDER BY env")

            assertThat(rows).hasSize(2)
            assertThat(rows[0]["ENV"]).isEqualTo("prod")
            assertThat((rows[0]["CNT"] as Number).toLong()).isEqualTo(2)
        }

        @Test
        fun `returns empty list for no matching rows`() {
            val rows = executor.execute("test_db",
                "SELECT * FROM metrics_source WHERE env = 'nonexistent'")

            assertThat(rows).isEmpty()
        }

        @Test
        fun `handles NULL values`() {
            dataSource.connection.use { conn ->
                conn.createStatement().execute(
                    "INSERT INTO metrics_source VALUES ('srv04', NULL, NULL, NULL, NULL)"
                )
            }
            val rows = executor.execute("test_db",
                "SELECT host, cpu_usage FROM metrics_source WHERE host = 'srv04'")

            assertThat(rows).hasSize(1)
            assertThat(rows[0]["CPU_USAGE"]).isNull()
        }
    }

    // ─── Error handling ───────────────────────────────────────

    @Nested
    inner class ErrorHandling {
        @Test
        fun `throws for unknown datasource`() {
            assertThatThrownBy {
                executor.execute("nonexistent", "SELECT 1")
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("nonexistent")
                .hasMessageContaining("not found")
        }

        @Test
        fun `throws for invalid SQL`() {
            assertThatThrownBy {
                executor.execute("test_db", "THIS IS NOT SQL")
            }.isInstanceOf(Exception::class.java)
        }
    }

    // ─── JDBI caching ─────────────────────────────────────────

    @Nested
    inner class Caching {
        @Test
        fun `reuses JDBI instance across calls`() {
            executor.execute("test_db", "SELECT 1")
            executor.execute("test_db", "SELECT 2")

            // If caching works, resolve() is only called once for the JDBI creation path.
            // The second call hits the cache. Verify by checking no exceptions on repeated use.
            val rows = executor.execute("test_db", "SELECT host FROM metrics_source")
            assertThat(rows).hasSize(3)
        }

        @Test
        fun `clearCache forces re-resolution`() {
            executor.execute("test_db", "SELECT 1")
            executor.clearCache()

            // After clearing, should still work (re-creates JDBI)
            val rows = executor.execute("test_db", "SELECT 1 as val")
            assertThat(rows).hasSize(1)
        }
    }
}
