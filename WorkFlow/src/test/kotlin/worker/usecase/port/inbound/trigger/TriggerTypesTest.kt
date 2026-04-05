package com.workflow.worker.usecase.port.inbound.trigger

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlin.test.Test
import kotlin.test.assertEquals

class TriggerTypesTest {

    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `TriggerTypes constants have expected values`() {
        assertEquals("k8s-job", TriggerTypes.K8S_JOB)
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
}
