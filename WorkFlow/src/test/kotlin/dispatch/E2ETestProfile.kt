package com.workflow.dispatch

import io.quarkus.test.junit.QuarkusTestProfile

/**
 * Enables the WorkerLoop auto-start for E2E tests that need real task processing.
 * All other tests disable auto-start to prevent background workers from claiming
 * tasks inserted by non-Quarkus integration tests (e.g. WorkflowWatchdogTest).
 */
class E2ETestProfile : QuarkusTestProfile {
    override fun getConfigOverrides(): Map<String, String> = mapOf(
        "framework.worker.auto-start" to "true",
        "framework.trigger.auto-start" to "true",
    )
}
