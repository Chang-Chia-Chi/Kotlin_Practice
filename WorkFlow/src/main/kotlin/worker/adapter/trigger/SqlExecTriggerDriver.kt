package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import com.workflow.worker.config.TriggerLoopConfig
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
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
private val TRANSIENT_BACKOFF_MS = longArrayOf(1_000, 2_000, 4_000)

/**
 * Trigger driver that executes SQL statements against named datasources asynchronously.
 *
 * Each task is deserialized into a [SqlExecMeta] and submitted to a bounded coroutine
 * dispatcher. Transient failures (connection errors, timeouts) are retried up to
 * [MAX_TRANSIENT_RETRIES] times with exponential backoff. Results are collected via
 * a lock-free [ConcurrentLinkedQueue] drained on each [poll] call.
 *
 * ## Concurrency model
 * - Bounded dispatcher: `Dispatchers.IO.limitedParallelism(maxConcurrent)`
 * - One coroutine [Job] per task, tracked in a [ConcurrentHashMap]
 * - [start] performs diff-based reconciliation against incoming DEFERRED tasks
 */
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

    /**
     * Diffs [tasks] against [tracked]. Removes stale entries no longer in the DEFERRED
     * set, skips already-tracked tasks, and launches a coroutine for each new task.
     */
    override suspend fun start(tasks: List<DeferredTaskRef>) {
        val incomingIds = tasks.map { it.taskId }.toSet()

        // Remove tracked entries no longer in the DEFERRED set
        val staleIds = tracked.keys.filter { it !in incomingIds }
        for (taskId in staleIds) {
            val job = tracked.remove(taskId)
            job?.cancelAndJoin()
        }

        for (task in tasks) {
            if (tracked.containsKey(task.taskId)) continue

            val meta = objectMapper.readValue<SqlExecMeta>(task.triggerMeta)
            val job = scope.launch {
                executeWithRetry(task.taskId, meta)
            }
            tracked[task.taskId] = job
        }
    }

    /**
     * Drains completed results from [resultQueue]. Removes corresponding entries
     * from [tracked]. Non-blocking.
     */
    override suspend fun poll(): List<TriggerResult> {
        val results = mutableListOf<TriggerResult>()
        while (true) {
            val r = resultQueue.poll() ?: break
            tracked.remove(r.taskId)
            results.add(r)
        }
        return results
    }

    /** Best-effort cancellation: removes from [tracked] and cancels the coroutine. */
    override suspend fun cancel(taskId: String) {
        val job = tracked.remove(taskId)
        if (job != null) {
            job.cancelAndJoin()
            log.info("Cancelled SQL trigger for task {}", taskId)
        }
    }

    /** Cancels all tracked coroutines and clears state. Idempotent. */
    override suspend fun close() {
        for ((taskId, job) in tracked) {
            try {
                job.cancelAndJoin()
            } catch (e: Exception) {
                log.warn("Failed to cancel SQL job for task {}", taskId, e)
            }
        }
        tracked.clear()
        resultQueue.clear()
    }

    /** Test accessor. */
    internal fun trackedCount(): Int = tracked.size

    // -- Private implementation -----------------------------------------------

    /**
     * Executes SQL with retry. Transient failures are retried up to [MAX_TRANSIENT_RETRIES]
     * times with backoff. Non-transient failures and exhausted retries enqueue [TriggerResult.Failed].
     */
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
                    log.warn(
                        "SQL trigger task {} transient failure (attempt {}/{}): {}",
                        taskId, attempt + 1, MAX_TRANSIENT_RETRIES, e.message,
                    )
                    delay(TRANSIENT_BACKOFF_MS[attempt])
                } else {
                    log.error("SQL trigger task {} failed: {}", taskId, e.message, e)
                    resultQueue.add(TriggerResult.Failed(taskId, e.message ?: "Unknown error"))
                    return
                }
            }
        }
    }

    /**
     * Resolves the datasource, executes the SQL query, and serializes the result set
     * to a JSON array string.
     *
     * Note: [SqlExecMeta.params] is accepted in the JSON payload for forward compatibility
     * but is not bound to the statement. Named parameter binding (`:param` style) requires
     * JDBI or a custom binder — raw JDBC only supports positional `?` placeholders.
     * Callers should use literal SQL until JDBI-based execution is added.
     */
    private suspend fun executeSql(meta: SqlExecMeta): String = withContext(dispatcher) {
        val ds = dataSourceProvider.resolve(meta.datasource)
        ds.connection.use { conn: Connection ->
            conn.prepareStatement(meta.sql).use { stmt ->
                val resultSet = stmt.executeQuery()
                resultSetToJson(resultSet)
            }
        }
    }

    /** Converts a [ResultSet] to a JSON array of row objects using column labels as keys. */
    private fun resultSetToJson(rs: ResultSet): String {
        val meta = rs.metaData
        val cols = (1..meta.columnCount).map { meta.getColumnLabel(it) }
        val rows = mutableListOf<Map<String, Any?>>()
        while (rs.next()) {
            val row = cols.mapIndexed { index, label ->
                label to rs.getObject(index + 1)
            }.toMap()
            rows.add(row)
        }
        return objectMapper.writeValueAsString(rows)
    }

    /** Returns true if the exception message suggests a transient connectivity issue. */
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

/** CDI producer that wires [SqlExecTriggerDriver] with config-driven concurrency. */
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
