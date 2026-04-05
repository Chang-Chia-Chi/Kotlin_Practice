package com.workflow.worker.usecase.port.inbound.trigger

import com.workflow.worker.usecase.port.inbound.execution.HandlerResult

object TriggerTypes {
    const val K8S_JOB = "k8s-job"
}

fun deferK8sJob(jobName: String, namespace: String): HandlerResult.Defer =
    HandlerResult.Defer(
        triggerType = TriggerTypes.K8S_JOB,
        triggerMeta = """{"jobName":"$jobName","namespace":"$namespace"}""",
    )
