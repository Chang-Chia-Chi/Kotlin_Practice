package com.workflow.infrastructure.leader

import org.eclipse.microprofile.health.HealthCheckResponse
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LeaderHealthCheckTest {

    private val leaderElectionConfig = mock<LeaderElectionConfig>().also {
        whenever(it.healthThreshold()).thenReturn(Duration.ofSeconds(45))
    }

    @Test
    fun `follower always returns UP`() {
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(false)
        }
        val check = LeaderHealthCheck(leaderElection, leaderElectionConfig)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
        assertEquals("leader-election", response.name)
    }

    @Test
    fun `leader with fresh heartbeat returns UP`() {
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(true)
            whenever(it.lastHeartbeat).thenReturn(Instant.now())
        }
        val check = LeaderHealthCheck(leaderElection, leaderElectionConfig)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
    }

    @Test
    fun `leader with stale heartbeat returns DOWN`() {
        val staleTime = Instant.now().minus(Duration.ofSeconds(60))
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(true)
            whenever(it.lastHeartbeat).thenReturn(staleTime)
        }
        val check = LeaderHealthCheck(leaderElection, leaderElectionConfig)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.DOWN, response.status)
        assertEquals("leader-election", response.name)
        assertTrue(
            response.data.isPresent,
            "DOWN response should include diagnostic data",
        )
    }

    @Test
    fun `leader at just under threshold returns UP`() {
        // age = 44s, threshold = 45s -> UP
        val justUnder = Instant.now().minus(Duration.ofSeconds(44))
        val leaderElection = mock<LeaderElection>().also {
            whenever(it.isActive).thenReturn(true)
            whenever(it.lastHeartbeat).thenReturn(justUnder)
        }
        val check = LeaderHealthCheck(leaderElection, leaderElectionConfig)

        val response = check.call()

        assertEquals(HealthCheckResponse.Status.UP, response.status)
    }
}
