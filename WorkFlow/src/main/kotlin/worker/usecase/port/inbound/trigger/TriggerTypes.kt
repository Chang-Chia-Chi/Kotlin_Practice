package com.workflow.worker.usecase.port.inbound.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult

object TriggerTypes {
    const val K8S_JOB = "k8s-job"
    const val SQL_EXEC = "sql-exec"
}

fun deferK8sJob(objectMapper: ObjectMapper, jobName: String, namespace: String): HandlerResult.Defer =
    HandlerResult.Defer(
        triggerType = TriggerTypes.K8S_JOB,
        triggerMeta = objectMapper.writeValueAsString(mapOf("jobName" to jobName, "namespace" to namespace)),
    )

fun deferSqlExec(
    objectMapper: ObjectMapper,
    datasource: String,
    sql: String,
    params: Map<String, Any?> = emptyMap(),
): HandlerResult.Defer =
    HandlerResult.Defer(
        triggerType = TriggerTypes.SQL_EXEC,
        triggerMeta = objectMapper.writeValueAsString(mapOf("datasource" to datasource, "sql" to sql, "params" to params)),
    )
