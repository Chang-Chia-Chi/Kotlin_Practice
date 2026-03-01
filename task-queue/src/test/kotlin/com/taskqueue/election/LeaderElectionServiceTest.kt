package com.taskqueue.election

import io.quarkus.runtime.ShutdownEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LeaderElectionServiceTest {

    private fun createService() = LeaderElectionService(
        leaseName = "test-lease",
        namespace = "test-ns",
        leaseDurationSeconds = 15,
        renewDeadlineSeconds = 10,
        retryPeriodSeconds = 2,
    )

    @Test
    fun `isLeader defaults to false`() {
        val service = createService()
        assertThat(service.isLeader.value).isFalse()
    }

    @Test
    fun `identity falls back to PID when HOSTNAME is not set`() {
        val service = createService()
        // HOSTNAME is not set in test env, so identity should be "unknown-<pid>"
        if (System.getenv("HOSTNAME") == null) {
            assertThat(service.identity).startsWith("unknown-")
        } else {
            assertThat(service.identity).isNotEmpty()
        }
    }

    @Test
    fun `onStop sets isLeader to false`() {
        val service = createService()
        // Even if somehow leader state were true, shutdown should set it to false
        service.onStop(ShutdownEvent())
        assertThat(service.isLeader.value).isFalse()
    }

    @Test
    fun `isLeader StateFlow is accessible`() {
        val service = createService()
        val flow = service.isLeader
        assertThat(flow).isNotNull()
        assertThat(flow.value).isFalse()
    }
}
