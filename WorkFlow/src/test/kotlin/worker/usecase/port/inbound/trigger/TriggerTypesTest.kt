package com.workflow.worker.usecase.port.inbound.trigger

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TriggerTypesTest {

    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `TriggerTypes constants have expected values`() {
        assertEquals("k8s-job", TriggerTypes.K8S_JOB)
        assertEquals("sql-exec", TriggerTypes.SQL_EXEC)
    }

    @Test
    fun `deferK8sJob creates Defer with correct type and meta`() {
        val result = deferK8sJob(
            jobName = "my-batch-job",
            namespace = "production",
        )
        assertEquals(TriggerTypes.K8S_JOB, result.triggerType)

        val meta: Map<String, Any?> = objectMapper.readValue(result.triggerMeta)
        assertEquals("my-batch-job", meta["jobName"])
        assertEquals("production", meta["namespace"])
    }

    @Test
    fun `deferSqlExec creates Defer with correct type and meta`() {
        val result = deferSqlExec(
            objectMapper = objectMapper,
            datasource = "oracle-main",
            sql = "UPDATE orders SET status = :status WHERE id = :id",
            params = mapOf("status" to "SHIPPED", "id" to 42),
        )
        assertEquals(TriggerTypes.SQL_EXEC, result.triggerType)

        val meta: Map<String, Any?> = objectMapper.readValue(result.triggerMeta)
        assertEquals("oracle-main", meta["datasource"])
        assertEquals("UPDATE orders SET status = :status WHERE id = :id", meta["sql"])
        @Suppress("UNCHECKED_CAST")
        val params = meta["params"] as Map<String, Any?>
        assertEquals("SHIPPED", params["status"])
        assertEquals(42, params["id"])
    }

    @Test
    fun `deferSqlExec with empty params`() {
        val result = deferSqlExec(
            objectMapper = objectMapper,
            datasource = "postgres-replica",
            sql = "SELECT 1",
        )
        assertEquals(TriggerTypes.SQL_EXEC, result.triggerType)

        val meta: Map<String, Any?> = objectMapper.readValue(result.triggerMeta)
        assertEquals("postgres-replica", meta["datasource"])
        assertEquals("SELECT 1", meta["sql"])
        @Suppress("UNCHECKED_CAST")
        val params = meta["params"] as Map<String, Any?>
        assertTrue(params.isEmpty())
    }
}
