package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.reaper.StaleTaskReaper
import com.mapreduce.queue.worker.WorkerLoop
import com.mapreduce.shutdown.ShutdownCoordinator
import com.mapreduce.shutdown.ShutdownState
import org.eclipse.microprofile.health.HealthCheckResponse
import org.jdbi.v3.core.HandleCallback
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.kotlin.any
import org.mockito.kotlin.mock

class HealthContributorTest {

    // ── ShutdownHealthContributor ──────────────────────────────────

    @Nested
    inner class ShutdownHealth {

        private val shutdownCoordinator: ShutdownCoordinator = mock()
        private val workerLoop: WorkerLoop = mock()
        private val contributor = ShutdownHealthContributor(shutdownCoordinator, workerLoop)

        @Test
        fun `UP when state is RUNNING`() {
            `when`(shutdownCoordinator.state).thenReturn(ShutdownState.RUNNING)

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            assertEquals("shutdown", response.name)
        }

        @Test
        fun `DOWN when state is DRAINING with state and inFlightTasks data`() {
            `when`(shutdownCoordinator.state).thenReturn(ShutdownState.DRAINING)
            `when`(workerLoop.inFlightTasks).thenReturn(3)

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
            assertEquals("DRAINING", response.data.get()["state"])
            assertEquals("3", response.data.get()["inFlightTasks"])
        }

        @Test
        fun `DOWN when state is TERMINATED`() {
            `when`(shutdownCoordinator.state).thenReturn(ShutdownState.TERMINATED)
            `when`(workerLoop.inFlightTasks).thenReturn(0)

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
            assertEquals("TERMINATED", response.data.get()["state"])
        }
    }

    // ── LeaderElectionHealthContributor ─────────────────────────────

    @Nested
    inner class LeaderElectionHealth {

        private val leaderManager: LeaderManager = mock()
        private val config: FrameworkConfig = mock()
        private val contributor = LeaderElectionHealthContributor(leaderManager, config)

        @Test
        fun `UP with dev mode when KUBERNETES_SERVICE_HOST is not set`() {
            // In a test environment, KUBERNETES_SERVICE_HOST is null,
            // so the dev-mode early-return path is always exercised.
            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            assertEquals("leader-election", response.name)
            assertEquals("dev", response.data.get()["mode"])
        }
    }

    // ── WorkerLoopHealthContributor ────────────────────────────────

    @Nested
    inner class WorkerLoopHealth {

        private val workerLoop: WorkerLoop = mock()
        private val handlerRegistry: HandlerRegistry = mock()
        private val config: FrameworkConfig = mock()
        private val healthConfig: FrameworkConfig.HealthConfig = mock()

        private val contributor = WorkerLoopHealthContributor(workerLoop, handlerRegistry, config)

        @Test
        fun `UP when lastPollTimestamp is within threshold`() {
            `when`(config.health()).thenReturn(healthConfig)
            `when`(healthConfig.workerLoopStaleThreshold()).thenReturn(java.time.Duration.ofSeconds(6))
            `when`(workerLoop.lastPollTimestamp).thenReturn(java.time.Instant.now())
            `when`(handlerRegistry.registeredHandlers()).thenReturn(setOf("handler-a"))

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            assertEquals("worker-loop", response.name)
            assertEquals("1", response.data.get()["handlers"])
        }

        @Test
        fun `DOWN when lastPollTimestamp is beyond threshold`() {
            `when`(config.health()).thenReturn(healthConfig)
            `when`(healthConfig.workerLoopStaleThreshold()).thenReturn(java.time.Duration.ofSeconds(6))
            `when`(workerLoop.lastPollTimestamp).thenReturn(java.time.Instant.now().minusSeconds(60))

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
            assertTrue(response.data.get().containsKey("reason"))
        }
    }

    // ── OracleHealthContributor ────────────────────────────────────

    @Nested
    inner class OracleHealth {

        private val jdbi: Jdbi = mock()
        private val config: FrameworkConfig = mock()
        private val healthConfig: FrameworkConfig.HealthConfig = mock()

        private val contributor = OracleHealthContributor(jdbi, config)

        @Test
        fun `UP when query succeeds`() {
            `when`(config.health()).thenReturn(healthConfig)
            `when`(healthConfig.oracleCheckTimeout()).thenReturn(java.time.Duration.ofSeconds(5))
            `when`(jdbi.withHandle<Int, Exception>(any<HandleCallback<Int, Exception>>())).thenReturn(1)

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            assertEquals("oracle", response.name)
        }

        @Test
        fun `DOWN with database unreachable when query throws exception`() {
            `when`(config.health()).thenReturn(healthConfig)
            `when`(healthConfig.oracleCheckTimeout()).thenReturn(java.time.Duration.ofSeconds(5))
            `when`(jdbi.withHandle<Int, Exception>(any<HandleCallback<Int, Exception>>()))
                .thenThrow(RuntimeException("Connection refused"))

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
            assertEquals("oracle", response.name)
            assertEquals("Database unreachable", response.data.get()["reason"])
        }
    }

    // ── StaleReaperHealthContributor ───────────────────────────────

    @Nested
    inner class StaleReaperHealth {

        private val staleTaskReaper: StaleTaskReaper = mock()
        private val leaderManager: LeaderManager = mock()

        private val contributor = StaleReaperHealthContributor(staleTaskReaper, leaderManager)

        @Test
        fun `UP with not-leader mode when instance is not leader`() {
            `when`(leaderManager.isActive).thenReturn(false)

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            assertEquals("stale-reaper", response.name)
            assertEquals("not-leader", response.data.get()["mode"])
        }

        @Test
        fun `UP when leader and scan is recent`() {
            `when`(leaderManager.isActive).thenReturn(true)
            `when`(staleTaskReaper.scanInterval).thenReturn(java.time.Duration.ofSeconds(30))
            `when`(staleTaskReaper.lastScanTimestamp).thenReturn(java.time.Instant.now())

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.UP, response.status)
            assertEquals("stale-reaper", response.name)
        }

        @Test
        fun `DOWN when leader and scan is stale`() {
            `when`(leaderManager.isActive).thenReturn(true)
            `when`(staleTaskReaper.scanInterval).thenReturn(java.time.Duration.ofSeconds(30))
            // threshold = 30s * 3 = 90s; elapsed = 600s >> 90s
            `when`(staleTaskReaper.lastScanTimestamp)
                .thenReturn(java.time.Instant.now().minusSeconds(600))

            val response = contributor.call()

            assertEquals(HealthCheckResponse.Status.DOWN, response.status)
            assertTrue(response.data.get().containsKey("reason"))
        }
    }
}
