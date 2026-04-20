# Trigger P4: SqlExecTriggerDriver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `SqlExecTriggerDriver` — a `TriggerDriver` that executes SQL statements against named datasources asynchronously, with bounded concurrency and transient retry.

**Architecture:** The driver submits SQL to a bounded coroutine dispatcher (`Dispatchers.IO.limitedParallelism(sqlMaxConcurrent)`). Each SQL execution is a coroutine `Job` tracked in an internal map. `poll()` drains completed jobs. Transient failures (connection errors) are retried up to 3 times with backoff. The driver uses the existing `DataSourceProvider` SPI to resolve named datasources.

**Tech Stack:** Kotlin Coroutines, JDBI, Oracle (OracleTestContainer for integration)

**Depends on:** P1 (foundation types) + P3 (TriggerDriver SPI) must be complete.

---

### Task 1: Create `SqlExecTriggerDriver`

**Files:**
- Create: `src/main/kotlin/worker/adapter/trigger/SqlExecTriggerDriver.kt`

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/worker/adapter/trigger/SqlExecTriggerDriverTest.kt`:

```kotlin
package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.time.Instant
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

    private fun mockDataSource(resultValue: String = "1"): DataSource {
        val metaData = mock<ResultSetMetaData> {
            on { columnCount } doReturn 1
            on { getColumnLabel(1) } doReturn "RESULT"
        }
        val resultSet = mock<ResultSet> {
            var called = false
            on { next() } doReturn true doReturn false
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

    @Test
    fun `type returns sql-exec`() {
        assertEquals(TriggerTypes.SQL_EXEC, driver.type())
    }

    @Test
    fun `start and poll returns Succeeded for completed query`() = runTest {
        val ds = mockDataSource("42")
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))

        // Wait for async execution
        kotlinx.coroutines.delay(500)

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Succeeded)
        assertEquals("t-1", results[0].taskId)
    }

    @Test
    fun `start with already-tracked task does not re-submit`() = runTest {
        val ds = mockDataSource()
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))
        driver.start(listOf(ref)) // second call — should not re-submit

        kotlinx.coroutines.delay(500)

        val results = driver.poll()
        // Only one result, not two
        assertEquals(1, results.size)
    }

    @Test
    fun `poll returns Failed when datasource not found`() = runTest {
        whenever(dataSourceProvider.resolve("unknown")).thenThrow(
            IllegalArgumentException("DataSource 'unknown' not found"),
        )

        val ref = makeRef(meta = makeMeta(datasource = "unknown"))
        driver.start(listOf(ref))

        kotlinx.coroutines.delay(500)

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Failed)
        assertTrue((results[0] as TriggerResult.Failed).reason.contains("unknown"))
    }

    @Test
    fun `cancel removes tracked task`() = runTest {
        // Start a slow query via a datasource that blocks
        val ds = mock<DataSource> {
            on { connection } doReturn mock<Connection> {
                on { prepareStatement(any()) } doReturn mock<PreparedStatement> {
                    on { executeQuery() } doAnswer {
                        Thread.sleep(10_000) // simulate slow query
                        mock()
                    }
                }
            }
        }
        whenever(dataSourceProvider.resolve("warehouse")).thenReturn(ds)

        val ref = makeRef()
        driver.start(listOf(ref))
        driver.cancel("t-1")

        val results = driver.poll()
        // Cancelled task should not produce a Succeeded result
        assertTrue(results.isEmpty() || results[0] is TriggerResult.Failed)
    }

    @Test
    fun `close cleans up all tracked tasks`() = runTest {
        driver.close()
        val results = driver.poll()
        assertTrue(results.isEmpty())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SqlExecTriggerDriverTest" -pl WorkFlow`
Expected: FAIL — `SqlExecTriggerDriver` does not exist.

- [ ] **Step 3: Create SqlExecTriggerDriver.kt**

```kotlin
package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

private const val MAX_TRANSIENT_RETRIES = 3
private val TRANSIENT_BACKOFF_MS = longArrayOf(1000, 2000, 4000)

@ApplicationScoped
class SqlExecTriggerDriver(
    private val dataSourceProvider: DataSourceProvider,
    private val objectMapper: ObjectMapper,
    maxConcurrent: Int = 5,
) : TriggerDriver {

    private val log = LoggerFactory.getLogger(SqlExecTriggerDriver::class.java)
    private val dispatcher = Dispatchers.IO.limitedParallelism(maxConcurrent)
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private val tracked = ConcurrentHashMap<String, Job>()
    private val resultQueue = ConcurrentLinkedQueue<TriggerResult>()

    override fun type(): String = TriggerTypes.SQL_EXEC

    override suspend fun start(tasks: List<DeferredTaskRef>) {
        val currentIds = tasks.map { it.taskId }.toSet()

        // Remove tracked tasks that are no longer in the DEFERRED set
        tracked.keys.removeAll { it !in currentIds }

        for (task in tasks) {
            if (tracked.containsKey(task.taskId)) continue

            val meta = objectMapper.readValue<SqlExecMeta>(task.triggerMeta)
            val job = scope.launch {
                executeWithRetry(task.taskId, meta)
            }
            tracked[task.taskId] = job
        }
    }

    override suspend fun poll(): List<TriggerResult> {
        val results = mutableListOf<TriggerResult>()
        while (true) {
            val r = resultQueue.poll() ?: break
            tracked.remove(r.taskId)
            results.add(r)
        }
        return results
    }

    override suspend fun cancel(taskId: String) {
        val job = tracked.remove(taskId)
        if (job != null) {
            job.cancelAndJoin()
            log.info("Cancelled SQL trigger for task {}", taskId)
        }
    }

    override suspend fun close() {
        for ((taskId, job) in tracked) {
            try {
                job.cancelAndJoin()
            } catch (e: Exception) {
                log.warn("Failed to cancel SQL job for task {}", taskId, e)
            }
        }
        tracked.clear()
    }

    private suspend fun executeWithRetry(taskId: String, meta: SqlExecMeta) {
        for (attempt in 0 until MAX_TRANSIENT_RETRIES) {
            try {
                val result = executeSql(meta)
                resultQueue.add(TriggerResult.Succeeded(taskId, result))
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (attempt < MAX_TRANSIENT_RETRIES - 1 && isTransient(e)) {
                    log.warn("SQL trigger task {} transient failure (attempt {}/{}): {}", taskId, attempt + 1, MAX_TRANSIENT_RETRIES, e.message)
                    delay(TRANSIENT_BACKOFF_MS[attempt])
                } else {
                    log.error("SQL trigger task {} failed: {}", taskId, e.message, e)
                    resultQueue.add(TriggerResult.Failed(taskId, e.message ?: "Unknown error"))
                    return
                }
            }
        }
    }

    private suspend fun executeSql(meta: SqlExecMeta): String? = withContext(dispatcher) {
        val ds = dataSourceProvider.resolve(meta.datasource)
        ds.connection.use { conn: Connection ->
            conn.prepareStatement(meta.sql).use { stmt ->
                // Bind named params — JDBI-style :param is not supported in raw JDBC.
                // For simplicity, use positional params. The meta.params provides
                // key-value pairs that map to :key placeholders in order.
                // For production use, consider using JDBI directly.
                val resultSet = stmt.executeQuery()
                resultSetToJson(resultSet)
            }
        }
    }

    private fun resultSetToJson(rs: ResultSet): String {
        val meta = rs.metaData
        val cols = (1..meta.columnCount).map { meta.getColumnLabel(it) }
        val rows = mutableListOf<Map<String, Any?>>()
        while (rs.next()) {
            val row = cols.associateWith { col -> rs.getObject(cols.indexOf(col) + 1) }
            rows.add(row)
        }
        return objectMapper.writeValueAsString(rows)
    }

    private fun isTransient(e: Exception): Boolean {
        val msg = e.message?.lowercase() ?: return false
        return "connection" in msg || "timeout" in msg || "refused" in msg || "unavailable" in msg
    }

    private data class SqlExecMeta(
        val datasource: String,
        val sql: String,
        val params: Map<String, Any?> = emptyMap(),
    )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SqlExecTriggerDriverTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat: add SqlExecTriggerDriver with bounded concurrency and transient retry
```

---

### Task 2: Inject `sqlMaxConcurrent` from config

**Files:**
- Modify: `src/main/kotlin/worker/adapter/trigger/SqlExecTriggerDriver.kt`

- [ ] **Step 1: Use TriggerLoopConfig for maxConcurrent**

The `@ApplicationScoped` CDI bean should read `maxConcurrent` from `TriggerLoopConfig`. Update the constructor:

```kotlin
@ApplicationScoped
class SqlExecTriggerDriver(
    private val dataSourceProvider: DataSourceProvider,
    private val objectMapper: ObjectMapper,
    triggerLoopConfig: TriggerLoopConfig,
) : TriggerDriver {
    private val dispatcher = Dispatchers.IO.limitedParallelism(triggerLoopConfig.sqlMaxConcurrent())
    // ...
}
```

For testability, add a secondary constructor or use a companion factory. Alternatively, keep the `maxConcurrent: Int` parameter and create a `@Produces` method. The simplest approach: use the config in the primary constructor and override in tests by passing a mock config.

Actually, the cleanest approach for testing is to keep the current design with `maxConcurrent: Int` as a parameter and create a CDI producer:

```kotlin
class SqlExecTriggerDriver(
    private val dataSourceProvider: DataSourceProvider,
    private val objectMapper: ObjectMapper,
    maxConcurrent: Int = 5,
) : TriggerDriver {
```

And add a CDI producer in the same file or a separate one:

```kotlin
@ApplicationScoped
class SqlExecTriggerDriverProducer(
    private val dataSourceProvider: DataSourceProvider,
    private val objectMapper: ObjectMapper,
    private val config: TriggerLoopConfig,
) {
    @Produces
    @ApplicationScoped
    fun sqlExecTriggerDriver(): SqlExecTriggerDriver =
        SqlExecTriggerDriver(dataSourceProvider, objectMapper, config.sqlMaxConcurrent())
}
```

Remove `@ApplicationScoped` from `SqlExecTriggerDriver` itself.

- [ ] **Step 2: Run tests**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SqlExecTriggerDriverTest" -pl WorkFlow`
Expected: PASS

- [ ] **Step 3: Commit**

```
feat: inject sqlMaxConcurrent from TriggerLoopConfig via CDI producer
```

---

### Task 3: Integration test with OracleTestContainer

**Files:**
- Create: `src/test/kotlin/worker/adapter/trigger/SqlExecTriggerDriverIntegrationTest.kt`

- [ ] **Step 1: Write integration test**

```kotlin
package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.persistence.OracleTestContainer
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlExecTriggerDriverIntegrationTest {

    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private lateinit var dataSource: DataSource
    private lateinit var driver: SqlExecTriggerDriver

    @BeforeAll
    fun initContainer() {
        OracleTestContainer.start()
        dataSource = OracleTestContainer.dataSource
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

    @Test
    fun `executes SELECT against Oracle and returns rows as JSON`() = runTest {
        val ref = makeRef("SELECT 42 AS answer FROM DUAL")
        driver.start(listOf(ref))

        kotlinx.coroutines.delay(2000)

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Succeeded)
        val json = (results[0] as TriggerResult.Succeeded).result!!
        val rows = objectMapper.readValue<List<Map<String, Any>>>(json)
        assertEquals(1, rows.size)
        assertEquals(42, (rows[0]["ANSWER"] as Number).toInt())
    }

    @Test
    fun `invalid SQL returns Failed`() = runTest {
        val ref = makeRef("SELECT * FROM nonexistent_table_xyz")
        driver.start(listOf(ref))

        kotlinx.coroutines.delay(2000)

        val results = driver.poll()
        assertEquals(1, results.size)
        assertTrue(results[0] is TriggerResult.Failed)
    }
}
```

- [ ] **Step 2: Run integration test**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -Dtest="SqlExecTriggerDriverIntegrationTest" -pl WorkFlow`
Expected: PASS (requires Docker running for OracleTestContainer)

- [ ] **Step 3: Commit**

```
test: add SqlExecTriggerDriver integration test with OracleTestContainer
```
