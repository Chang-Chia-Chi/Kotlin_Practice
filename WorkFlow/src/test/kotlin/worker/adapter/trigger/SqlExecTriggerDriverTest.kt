package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.time.Instant
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class SqlExecTriggerDriverTest {

    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private lateinit var dataSourceProvider: DataSourceProvider
    private lateinit var driver: SqlExecTriggerDriver

    @BeforeEach
    fun setUp() {
        dataSourceProvider = mock()
        driver = SqlExecTriggerDriver(dataSourceProvider, objectMapper, maxConcurrent = 2)
    }

    // ── Factories ───────────────────────────────────────────────────────

    private fun makeMeta(
        datasource: String = "warehouse",
        sql: String = "SELECT 1 FROM DUAL",
        params: Map<String, Any?> = emptyMap(),
    ): String = objectMapper.writeValueAsString(
        mapOf("datasource" to datasource, "sql" to sql, "params" to params),
    )

    private fun makeRef(
        taskId: String = "t-1",
        meta: String = makeMeta(),
    ) = DeferredTaskRef(
        taskId = taskId,
        workflowId = "wf-1",
        sequenceNumber = 1,
        triggerType = TriggerTypes.SQL_EXEC,
        triggerMeta = meta,
        deadlineAt = Instant.now().plusSeconds(3600),
        retryCount = 0,
        maxRetries = 3,
    )

    private fun mockDataSource(
        resultValue: String = "1",
        columnLabel: String = "RESULT",
    ): DataSource {
        val metaData = mock<ResultSetMetaData> {
            on { columnCount } doReturn 1
            on { getColumnLabel(1) } doReturn columnLabel
        }
        val resultSet = mock<ResultSet> {
            var called = false
            on { next() } doAnswer {
                if (!called) { called = true; true } else false
            }
            on { getObject(1) } doReturn resultValue
            on { this.metaData } doReturn metaData
        }
        val stmt = mock<PreparedStatement> {
            on { executeQuery() } doReturn resultSet
        }
        val conn = mock<Connection> {
            on { prepareStatement(any()) } doReturn stmt
        }
        return mock<DataSource> {
            on { connection } doReturn conn
        }
    }

    private fun mockTransientDataSource(
        failCount: Int,
        exception: SQLException = SQLException("Connection refused"),
        resultValue: String = "ok",
    ): DataSource {
        val metaData = mock<ResultSetMetaData> {
            on { columnCount } doReturn 1
            on { getColumnLabel(1) } doReturn "RESULT"
        }
        val resultSet = mock<ResultSet> {
            var called = false
            on { next() } doAnswer {
                if (!called) { called = true; true } else false
            }
            on { getObject(1) } doReturn resultValue
            on { this.metaData } doReturn metaData
        }
        var attempts = 0
        val stmt = mock<PreparedStatement> {
            on { executeQuery() } doAnswer {
                attempts++
                if (attempts <= failCount) throw exception
                resultSet
            }
        }
        val conn = mock<Connection> {
            on { prepareStatement(any()) } doReturn stmt
        }
        return mock<DataSource> {
            on { connection } doReturn conn
        }
    }

    /**
     * Polls the driver via Awaitility until [expectedCount] results accumulate.
     * Uses real-time polling since the driver runs on Dispatchers.IO.
     */
    private fun awaitPollResults(
        target: SqlExecTriggerDriver = driver,
        expectedCount: Int,
        atMostSeconds: Long = 5,
    ): List<TriggerResult> {
        val accumulated = mutableListOf<TriggerResult>()
        await().atMost(atMostSeconds, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .untilAsserted {
                accumulated += runBlocking { target.poll() }
                assertTrue(accumulated.size >= expectedCount,
                    "Expected $expectedCount results but got ${accumulated.size}")
            }
        return accumulated
    }

    // ── 1. type() ───────────────────────────────────────────────────────

    @Test
    fun `type returns sql-exec`() {
        assertEquals(TriggerTypes.SQL_EXEC, driver.type())
    }

    // ── 7. start submits query to pool ──────────────────────────────────

    @Test
    fun `start with mocked DS increases trackedCount`() = runTest {
        val ds = mockDataSource()
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))

        assertEquals(1, driver.trackedCount())
    }

    // ── 8. poll after query completes returns Succeeded with rows JSON ──

    @Test
    fun `start and poll returns Succeeded with JSON rows`() = runTest {
        val ds = mockDataSource(resultValue = "42", columnLabel = "ANSWER")
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef(meta = makeMeta(sql = "SELECT 42 AS ANSWER FROM DUAL"))
        driver.start(listOf(ref))

        val results = awaitPollResults(expectedCount = 1)
        val succeeded = results[0] as TriggerResult.Succeeded
        assertEquals("t-1", succeeded.taskId)
        assertTrue(succeeded.result!!.contains("ANSWER"))
        assertTrue(succeeded.result!!.contains("42"))
    }

    // ── 9. poll after query error returns Failed ────────────────────────

    @Test
    fun `poll returns Failed when query throws non-transient error`() = runTest {
        val stmt = mock<PreparedStatement> {
            on { executeQuery() } doAnswer {
                throw SQLException("ORA-00942: table or view does not exist")
            }
        }
        val conn = mock<Connection> {
            on { prepareStatement(any()) } doReturn stmt
        }
        val ds = mock<DataSource> {
            on { connection } doReturn conn
        }
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))

        val results = awaitPollResults(expectedCount = 1)
        val failed = results[0] as TriggerResult.Failed
        assertEquals("t-1", failed.taskId)
        assertTrue(failed.reason.contains("ORA-00942"))
    }

    // ── 10. Transient failure retried then succeeds ─────────────────────

    @Test
    fun `transient failure retried then succeeds`() = runTest {
        val ds = mockTransientDataSource(
            failCount = 2,
            exception = SQLException("Connection refused"),
            resultValue = "recovered",
        )
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))

        // backoff: 1s + 2s + execution time
        val results = awaitPollResults(expectedCount = 1, atMostSeconds = 10)
        assertTrue(results[0] is TriggerResult.Succeeded, "Should succeed after transient retries")
    }

    @Test
    fun `transient failure exhausts all retries then returns Failed`() = runTest {
        val ds = mockTransientDataSource(
            failCount = 3,
            exception = SQLException("Connection timeout"),
        )
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))

        // backoff: 1s + 2s then fail on 3rd attempt (no 3rd backoff)
        val results = awaitPollResults(expectedCount = 1, atMostSeconds = 10)
        val failed = results[0] as TriggerResult.Failed
        assertEquals("t-1", failed.taskId)
        assertTrue(failed.reason.contains("timeout", ignoreCase = true))
    }

    // ── 11. sqlMaxConcurrent respected ──────────────────────────────────

    @Test
    fun `maxConcurrent bounds parallel execution`() = runTest {
        val boundedDriver = SqlExecTriggerDriver(dataSourceProvider, objectMapper, maxConcurrent = 1)

        val concurrentCount = java.util.concurrent.atomic.AtomicInteger(0)
        val maxObserved = java.util.concurrent.atomic.AtomicInteger(0)

        val metaData = mock<ResultSetMetaData> {
            on { columnCount } doReturn 1
            on { getColumnLabel(1) } doReturn "X"
        }

        val ds = mock<DataSource> {
            on { connection } doAnswer {
                val current = concurrentCount.incrementAndGet()
                maxObserved.updateAndGet { prev -> maxOf(prev, current) }
                val resultSet = mock<ResultSet> {
                    var called = false
                    on { next() } doAnswer {
                        if (!called) { called = true; true } else false
                    }
                    on { getObject(1) } doReturn "1"
                    on { this.metaData } doReturn metaData
                }
                val stmt = mock<PreparedStatement> {
                    on { executeQuery() } doAnswer {
                        Thread.sleep(200) // simulate work inside mock callback
                        concurrentCount.decrementAndGet()
                        resultSet
                    }
                }
                mock<Connection> {
                    on { prepareStatement(any()) } doReturn stmt
                }
            }
        }
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val refs = listOf(
            makeRef(taskId = "t-1"),
            makeRef(taskId = "t-2"),
        )
        boundedDriver.start(refs)

        val results = awaitPollResults(target = boundedDriver, expectedCount = 2, atMostSeconds = 5)
        assertEquals(2, results.size)
        assertTrue(maxObserved.get() <= 1, "Expected max concurrency 1, observed ${maxObserved.get()}")
    }

    // ── Idempotent start ────────────────────────────────────────────────

    @Test
    fun `start with already-tracked task does not re-submit`() = runTest {
        val ds = mockDataSource()
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))
        driver.start(listOf(ref))

        val results = awaitPollResults(expectedCount = 1)
        assertEquals(1, results.size)
    }

    // ── Datasource not found ────────────────────────────────────────────

    @Test
    fun `poll returns Failed when datasource not found`() = runTest {
        whenever(dataSourceProvider.resolve("unknown")).thenThrow(
            IllegalArgumentException("DataSource 'unknown' not found"),
        )

        val ref = makeRef(meta = makeMeta(datasource = "unknown"))
        driver.start(listOf(ref))

        val results = awaitPollResults(expectedCount = 1)
        val failed = results[0] as TriggerResult.Failed
        assertTrue(failed.reason.contains("unknown"))
    }

    // ── cancel removes tracked task ─────────────────────────────────────

    @Test
    fun `cancel removes tracked task`() = runTest {
        val stmt = mock<PreparedStatement> {
            on { executeQuery() } doAnswer {
                Thread.sleep(10_000) // simulate slow query
                mock()
            }
        }
        val conn = mock<Connection> {
            on { prepareStatement(any()) } doReturn stmt
        }
        val ds = mock<DataSource> {
            on { connection } doReturn conn
        }
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))
        assertEquals(1, driver.trackedCount())

        driver.cancel("t-1")

        assertEquals(0, driver.trackedCount())
    }

    @Test
    fun `cancel on unknown taskId is a no-op`() = runTest {
        driver.cancel("nonexistent")
        assertEquals(0, driver.trackedCount())
    }

    // ── close cleans up all tracked tasks ───────────────────────────────

    @Test
    fun `close cleans up all tracked tasks`() = runTest {
        val stmt = mock<PreparedStatement> {
            on { executeQuery() } doAnswer {
                Thread.sleep(10_000)
                mock()
            }
        }
        val conn = mock<Connection> {
            on { prepareStatement(any()) } doReturn stmt
        }
        val ds = mock<DataSource> {
            on { connection } doReturn conn
        }
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        driver.start(listOf(makeRef(taskId = "t-1"), makeRef(taskId = "t-2")))
        assertEquals(2, driver.trackedCount())

        driver.close()

        assertEquals(0, driver.trackedCount())
    }

    @Test
    fun `close is idempotent`() = runTest {
        driver.close()
        driver.close()
        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    // ── poll edge cases ─────────────────────────────────────────────────

    @Test
    fun `poll with no events returns empty list`() = runTest {
        val results = driver.poll()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `poll drains all queued results`() = runTest {
        val ds = mockDataSource()
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        driver.start(listOf(makeRef(taskId = "t-1"), makeRef(taskId = "t-2")))

        val results = awaitPollResults(expectedCount = 2)
        assertEquals(2, results.size)

        val results2 = driver.poll()
        assertTrue(results2.isEmpty())
    }

    // ── start() removes stale tracked tasks ─────────────────────────────

    @Test
    fun `start removes tracked tasks no longer in the deferred list`() = runTest {
        val stmt = mock<PreparedStatement> {
            on { executeQuery() } doAnswer {
                Thread.sleep(10_000)
                mock()
            }
        }
        val conn = mock<Connection> {
            on { prepareStatement(any()) } doReturn stmt
        }
        val ds = mock<DataSource> {
            on { connection } doReturn conn
        }
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        driver.start(listOf(makeRef()))
        assertEquals(1, driver.trackedCount())

        driver.start(emptyList())

        assertEquals(0, driver.trackedCount())
    }

    // ── Multi-row result ────────────────────────────────────────────────

    @Test
    fun `result contains multiple rows as JSON array`() = runTest {
        val metaData = mock<ResultSetMetaData> {
            on { columnCount } doReturn 2
            on { getColumnLabel(1) } doReturn "ID"
            on { getColumnLabel(2) } doReturn "NAME"
        }
        var rowIndex = -1
        val rows = listOf(
            arrayOf<Any>("1", "Alice"),
            arrayOf<Any>("2", "Bob"),
        )
        val resultSet = mock<ResultSet> {
            on { next() } doAnswer {
                rowIndex++
                rowIndex < rows.size
            }
            on { getObject(1) } doAnswer { rows[rowIndex][0] }
            on { getObject(2) } doAnswer { rows[rowIndex][1] }
            on { this.metaData } doReturn metaData
        }
        val stmt = mock<PreparedStatement> {
            on { executeQuery() } doReturn resultSet
        }
        val conn = mock<Connection> {
            on { prepareStatement(any()) } doReturn stmt
        }
        val ds = mock<DataSource> {
            on { connection } doReturn conn
        }
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))

        val results = awaitPollResults(expectedCount = 1)
        val succeeded = results[0] as TriggerResult.Succeeded
        assertTrue(succeeded.result!!.contains("Alice"))
        assertTrue(succeeded.result!!.contains("Bob"))
    }
}
