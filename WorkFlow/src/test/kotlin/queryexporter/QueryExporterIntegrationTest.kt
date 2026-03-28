package com.workflow.queryexporter

import com.workflow.engine.OracleTestContainer
import com.workflow.queryexporter.config.ExporterConfig
import com.workflow.queryexporter.config.MetricConfig
import com.workflow.queryexporter.config.MetricType
import com.workflow.queryexporter.config.QueryConfig
import com.workflow.queryexporter.config.ScheduleConfig
import com.workflow.queryexporter.spi.DataSourceProvider
import com.workflow.queryexporter.spi.LeaderGuard
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.PrintWriter
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryExporterIntegrationTest {

    private lateinit var dataSource: DataSource
    private lateinit var registry: SimpleMeterRegistry
    private lateinit var bootstrap: QueryExporterBootstrap

    @BeforeAll
    fun initContainer() {
        // Trigger lazy schema creation via shared singleton (Constraint 9)
        OracleTestContainer.jdbi

        val url = OracleTestContainer.oracle.jdbcUrl
        val user = OracleTestContainer.oracle.username
        val pass = OracleTestContainer.oracle.password
        dataSource = object : DataSource {
            override fun getConnection(): Connection = DriverManager.getConnection(url, user, pass)
            override fun getConnection(u: String?, p: String?): Connection = DriverManager.getConnection(url, u, p)
            override fun getLogWriter(): PrintWriter? = null
            override fun setLogWriter(out: PrintWriter?) {}
            override fun setLoginTimeout(seconds: Int) {}
            override fun getLoginTimeout(): Int = 0
            override fun getParentLogger(): Logger = Logger.getLogger("test")
            override fun <T : Any?> unwrap(iface: Class<T>?): T = throw UnsupportedOperationException()
            override fun isWrapperFor(iface: Class<*>?): Boolean = false
        }
    }

    @BeforeEach
    fun setUp() {
        registry = SimpleMeterRegistry()
        cleanTables()
    }

    @AfterEach
    fun tearDown() {
        runBlocking {
            if (::bootstrap.isInitialized) {
                bootstrap.stop()
            }
        }
        if (::registry.isInitialized) {
            registry.close()
        }
        cleanTables()
    }

    // -- Helpers ----------------------------------------------------------------

    private fun cleanTables() {
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            conn.prepareStatement("DELETE FROM task").use { it.executeUpdate() }
            conn.prepareStatement("DELETE FROM workflow").use { it.executeUpdate() }
            conn.commit()
        }
    }

    private fun insertWorkflow(
        conn: Connection,
        id: String = UUID.randomUUID().toString(),
        status: String = "RUNNING",
        updatedAt: LocalDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS),
    ) {
        val deadlineAt = updatedAt.plusHours(1)
        conn.prepareStatement(
            """INSERT INTO workflow (id, definition, current_sequence, version, status, created_at, updated_at, deadline_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, """{"name":"test"}""")
            ps.setInt(3, 0)
            ps.setInt(4, 0)
            ps.setString(5, status)
            ps.setObject(6, updatedAt)
            ps.setObject(7, updatedAt)
            ps.setObject(8, deadlineAt)
            ps.executeUpdate()
        }
    }

    private fun insertTask(
        conn: Connection,
        workflowId: String,
        status: String = "PENDING",
        id: String = UUID.randomUUID().toString(),
    ) {
        conn.prepareStatement(
            """INSERT INTO task (id, workflow_id, sequence_number, status, handler_key, retry_count, max_retries)
               VALUES (?, ?, ?, ?, ?, ?, ?)"""
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, workflowId)
            ps.setInt(3, 0)
            ps.setString(4, status)
            ps.setString(5, "test-handler")
            ps.setInt(6, 0)
            ps.setInt(7, 3)
            ps.executeUpdate()
        }
    }

    private fun startBootstrap(config: ExporterConfig) {
        startBootstrapWithGuard(config, LeaderGuard.ALWAYS)
    }

    private fun startBootstrapWithGuard(config: ExporterConfig, guard: LeaderGuard) {
        bootstrap = QueryExporterBootstrap(
            config = config,
            dataSourceProvider = DataSourceProvider { dataSource },
            meterRegistry = registry,
            leaderGuard = guard,
        )
        bootstrap.start()
    }

    private fun workflowByStatusConfig() = ExporterConfig(
        queries = mapOf(
            "workflow_by_status" to QueryConfig(
                sql = """SELECT status, COUNT(*) AS cnt FROM workflow GROUP BY status""",
                datasource = "default",
                schedule = ScheduleConfig(interval = Duration.ofMillis(500)),
                metrics = listOf(
                    MetricConfig(
                        name = "workflow_by_status",
                        type = MetricType.GAUGE,
                        valueColumn = "cnt",
                        tagColumns = listOf("status"),
                    ),
                ),
            ),
        ),
    )

    private fun taskByStatusConfig() = ExporterConfig(
        queries = mapOf(
            "task_by_status" to QueryConfig(
                sql = """SELECT status, COUNT(*) AS cnt FROM task GROUP BY status""",
                datasource = "default",
                schedule = ScheduleConfig(interval = Duration.ofMillis(500)),
                metrics = listOf(
                    MetricConfig(
                        name = "task_by_status",
                        type = MetricType.GAUGE,
                        valueColumn = "cnt",
                        tagColumns = listOf("status"),
                    ),
                ),
            ),
        ),
    )

    private fun stuckWorkflowConfig() = ExporterConfig(
        queries = mapOf(
            "stuck_workflows" to QueryConfig(
                sql = """SELECT COUNT(*) AS cnt FROM workflow
                         WHERE status = 'RUNNING'
                         AND updated_at < (SYSTIMESTAMP - INTERVAL '1' HOUR)""",
                datasource = "default",
                schedule = ScheduleConfig(interval = Duration.ofMillis(500)),
                metrics = listOf(
                    MetricConfig(
                        name = "workflow_stuck_count",
                        type = MetricType.GAUGE,
                        valueColumn = "cnt",
                    ),
                ),
            ),
        ),
    )

    // ==========================================================================
    // 1. GAUGE with workflow data
    // ==========================================================================

    @Nested
    inner class WorkflowGauge {

        @Test
        fun `workflow_by_status gauge reflects correct counts per status`() {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                insertWorkflow(conn, status = "RUNNING")
                insertWorkflow(conn, status = "RUNNING")
                insertWorkflow(conn, status = "COMPLETED")
                insertWorkflow(conn, status = "FAILED")
                conn.commit()
            }

            startBootstrap(workflowByStatusConfig())

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val running = registry.find("workflow_by_status").tag("status", "RUNNING").gauge()
                val completed = registry.find("workflow_by_status").tag("status", "COMPLETED").gauge()
                val failed = registry.find("workflow_by_status").tag("status", "FAILED").gauge()

                assertNotNull(running, "RUNNING gauge should be registered")
                assertNotNull(completed, "COMPLETED gauge should be registered")
                assertNotNull(failed, "FAILED gauge should be registered")
                assertEquals(2.0, running.value(), "Two RUNNING workflows")
                assertEquals(1.0, completed.value(), "One COMPLETED workflow")
                assertEquals(1.0, failed.value(), "One FAILED workflow")
            }
        }

        @Test
        fun `workflow_by_status gauge updates after data changes`() {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                insertWorkflow(conn, id = "wf-1", status = "RUNNING")
                conn.commit()
            }

            startBootstrap(workflowByStatusConfig())

            // Wait for initial metric to appear
            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val running = registry.find("workflow_by_status").tag("status", "RUNNING").gauge()
                assertNotNull(running)
                assertEquals(1.0, running.value())
            }

            // Now add more workflows
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                insertWorkflow(conn, status = "RUNNING")
                insertWorkflow(conn, status = "RUNNING")
                conn.commit()
            }

            // Wait for metric to reflect the update
            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val running = registry.find("workflow_by_status").tag("status", "RUNNING").gauge()
                assertNotNull(running)
                assertEquals(3.0, running.value())
            }
        }
    }

    // ==========================================================================
    // 2. GAUGE with task data
    // ==========================================================================

    @Nested
    inner class TaskGauge {

        @Test
        fun `task_by_status gauge reflects correct counts per status`() {
            val wfId = UUID.randomUUID().toString()
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                insertWorkflow(conn, id = wfId, status = "RUNNING")
                insertTask(conn, workflowId = wfId, status = "PENDING")
                insertTask(conn, workflowId = wfId, status = "PENDING")
                insertTask(conn, workflowId = wfId, status = "PROCESSING")
                insertTask(conn, workflowId = wfId, status = "COMPLETED")
                insertTask(conn, workflowId = wfId, status = "FAILED")
                conn.commit()
            }

            startBootstrap(taskByStatusConfig())

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val pending = registry.find("task_by_status").tag("status", "PENDING").gauge()
                val processing = registry.find("task_by_status").tag("status", "PROCESSING").gauge()
                val completed = registry.find("task_by_status").tag("status", "COMPLETED").gauge()
                val failed = registry.find("task_by_status").tag("status", "FAILED").gauge()

                assertNotNull(pending, "PENDING gauge should be registered")
                assertNotNull(processing, "PROCESSING gauge should be registered")
                assertNotNull(completed, "COMPLETED gauge should be registered")
                assertNotNull(failed, "FAILED gauge should be registered")
                assertEquals(2.0, pending.value(), "Two PENDING tasks")
                assertEquals(1.0, processing.value(), "One PROCESSING task")
                assertEquals(1.0, completed.value(), "One COMPLETED task")
                assertEquals(1.0, failed.value(), "One FAILED task")
            }
        }
    }

    // ==========================================================================
    // 3. GAUGE with no data (empty tables)
    // ==========================================================================

    @Nested
    inner class EmptyTableGauge {

        @Test
        fun `workflow query runs without error when tables are empty`() {
            // Tables are already clean from setUp
            startBootstrap(workflowByStatusConfig())

            // Let at least 2 scheduler cycles run (500ms interval) without error
            await().atMost(5, TimeUnit.SECONDS).pollDelay(Duration.ofSeconds(1)).until { true }

            // Insert data OUTSIDE the assertion block — only once
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                insertWorkflow(conn, status = "RUNNING")
                conn.commit()
            }

            // Verify the exporter is still running and picks up the new data
            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val running = registry.find("workflow_by_status").tag("status", "RUNNING").gauge()
                assertNotNull(running, "Gauge should appear after data is inserted")
                assertEquals(1.0, running.value())
            }
        }

        @Test
        fun `task query runs without error when tables are empty`() {
            startBootstrap(taskByStatusConfig())

            // Let at least 2 scheduler cycles run (500ms interval) without error
            await().atMost(5, TimeUnit.SECONDS).pollDelay(Duration.ofSeconds(1)).until { true }

            // Insert data OUTSIDE the assertion block — only once
            val wfId = UUID.randomUUID().toString()
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                insertWorkflow(conn, id = wfId, status = "RUNNING")
                insertTask(conn, workflowId = wfId, status = "PENDING")
                conn.commit()
            }

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val pending = registry.find("task_by_status").tag("status", "PENDING").gauge()
                assertNotNull(pending, "Gauge should appear after data is inserted")
                assertEquals(1.0, pending.value())
            }
        }
    }

    // ==========================================================================
    // 4. Stuck workflow count
    // ==========================================================================

    @Nested
    inner class StuckWorkflowGauge {

        @Test
        fun `workflow_stuck_count gauge detects workflows with old updated_at`() {
            val oldTimestamp = LocalDateTime.now()
                .minusHours(2)
                .truncatedTo(ChronoUnit.MICROS)

            dataSource.connection.use { conn ->
                conn.autoCommit = false
                // Two stuck workflows (updated_at > 1 hour ago)
                insertWorkflow(conn, status = "RUNNING", updatedAt = oldTimestamp)
                insertWorkflow(conn, status = "RUNNING", updatedAt = oldTimestamp)
                // One recent workflow (should not be counted)
                insertWorkflow(conn, status = "RUNNING")
                // One completed workflow with old timestamp (should not be counted - not RUNNING)
                insertWorkflow(conn, status = "COMPLETED", updatedAt = oldTimestamp)
                conn.commit()
            }

            startBootstrap(stuckWorkflowConfig())

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val stuck = registry.find("workflow_stuck_count").gauge()
                assertNotNull(stuck, "workflow_stuck_count gauge should be registered")
                assertEquals(2.0, stuck.value(), "Only RUNNING workflows with old updated_at should be counted")
            }
        }

        @Test
        fun `workflow_stuck_count is zero when no stuck workflows exist`() {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                // Recent RUNNING workflows - not stuck
                insertWorkflow(conn, status = "RUNNING")
                insertWorkflow(conn, status = "RUNNING")
                conn.commit()
            }

            startBootstrap(stuckWorkflowConfig())

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val stuck = registry.find("workflow_stuck_count").gauge()
                assertNotNull(stuck, "workflow_stuck_count gauge should be registered")
                assertEquals(0.0, stuck.value(), "No stuck workflows should be counted")
            }
        }
    }

    // ==========================================================================
    // 5. Leader-gated behavior — queries only run when leader
    // ==========================================================================

    @Nested
    inner class LeaderGatedBehavior {

        @Test
        fun `no gauges registered while not leader, gauges appear after becoming leader`() {
            val leaderFlow = MutableStateFlow(false)
            val guard = object : LeaderGuard {
                override val leaderState: StateFlow<Boolean> = leaderFlow.asStateFlow()
            }

            // Insert data so gauges will appear once queries run
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                insertWorkflow(conn, status = "RUNNING")
                insertWorkflow(conn, status = "COMPLETED")
                conn.commit()
            }

            startBootstrapWithGuard(workflowByStatusConfig(), guard)

            // Wait long enough for multiple scheduler cycles to pass while NOT leader
            await().atMost(5, TimeUnit.SECONDS).pollDelay(Duration.ofSeconds(1)).until { true }

            // Assert no gauges registered — scheduler should not have executed any queries
            assertNull(
                registry.find("workflow_by_status").tag("status", "RUNNING").gauge(),
                "No RUNNING gauge should be registered while not leader",
            )
            assertNull(
                registry.find("workflow_by_status").tag("status", "COMPLETED").gauge(),
                "No COMPLETED gauge should be registered while not leader",
            )

            // Flip to leader
            leaderFlow.value = true

            // Gauges should appear now
            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val running = registry.find("workflow_by_status").tag("status", "RUNNING").gauge()
                val completed = registry.find("workflow_by_status").tag("status", "COMPLETED").gauge()

                assertNotNull(running, "RUNNING gauge should appear after becoming leader")
                assertNotNull(completed, "COMPLETED gauge should appear after becoming leader")
                assertEquals(1.0, running.value(), "One RUNNING workflow")
                assertEquals(1.0, completed.value(), "One COMPLETED workflow")
            }
        }
    }
}
