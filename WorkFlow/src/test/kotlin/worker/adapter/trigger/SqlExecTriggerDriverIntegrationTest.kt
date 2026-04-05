package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.PrintWriter
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlExecTriggerDriverIntegrationTest {

    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private lateinit var dataSource: DataSource
    private lateinit var driver: SqlExecTriggerDriver

    @BeforeAll
    fun initContainer() {
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
        val provider = DataSourceProvider { dataSource }
        driver = SqlExecTriggerDriver(provider, objectMapper, maxConcurrent = 2)
    }

    private fun makeRef(sql: String, taskId: String = "t-int-1") = DeferredTaskRef(
        taskId = taskId,
        workflowId = "wf-1",
        sequenceNumber = 1,
        triggerType = TriggerTypes.SQL_EXEC,
        triggerMeta = objectMapper.writeValueAsString(
            mapOf("datasource" to "default", "sql" to sql, "params" to emptyMap<String, Any>()),
        ),
        deadlineAt = Instant.now().plusSeconds(3600),
        retryCount = 0,
        maxRetries = 3,
    )

    private fun awaitPollResults(expectedCount: Int, atMostSeconds: Long = 10): List<TriggerResult> {
        val accumulated = mutableListOf<TriggerResult>()
        await().atMost(atMostSeconds, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .untilAsserted {
                accumulated += runBlocking { driver.poll() }
                assertTrue(accumulated.size >= expectedCount,
                    "Expected $expectedCount results but got ${accumulated.size}")
            }
        return accumulated
    }

    @Test
    fun `executes SELECT against Oracle and returns rows as JSON`() = runTest {
        val ref = makeRef("SELECT 42 AS ANSWER FROM DUAL")
        driver.start(listOf(ref))

        val results = awaitPollResults(expectedCount = 1)
        assertTrue(results[0] is TriggerResult.Succeeded)
        val json = (results[0] as TriggerResult.Succeeded).result!!
        val rows = objectMapper.readValue<List<Map<String, Any>>>(json)
        assertEquals(1, rows.size)
        assertEquals(42, (rows[0]["ANSWER"] as Number).toInt())
    }

    @Test
    fun `invalid SQL returns Failed`() = runTest {
        val ref = makeRef("SELECT * FROM nonexistent_table_xyz_99")
        driver.start(listOf(ref))

        val results = awaitPollResults(expectedCount = 1)
        assertTrue(results[0] is TriggerResult.Failed)
    }
}
