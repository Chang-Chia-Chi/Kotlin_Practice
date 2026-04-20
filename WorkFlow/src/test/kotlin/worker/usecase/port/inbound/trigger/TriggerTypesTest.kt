package com.workflow.worker.usecase.port.inbound.trigger

import kotlin.test.Test
import kotlin.test.assertEquals

class TriggerTypesTest {

    @Test
    fun `TriggerTypes constants have expected values`() {
        assertEquals("k8s-job", TriggerTypes.K8S_JOB)
    }
}
