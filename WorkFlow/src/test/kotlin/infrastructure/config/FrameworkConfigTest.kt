package com.workflow.infrastructure.config

import com.workflow.infrastructure.leader.LeaderElectionConfig
import com.workflow.infrastructure.shutdown.ShutdownConfig
import com.workflow.worker.config.WorkerLoopConfig
import com.workflow.workflow.config.SweeperConfig
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Tests for per-domain config interfaces -- validates SmallRye ConfigMapping defaults,
 * overrides via test profile, config resolution, and CDI wiring.
 */
@QuarkusTest
@TestProfile(ConfigOnlyTestProfile::class)
class FrameworkConfigDefaultsTest {

    @Inject
    lateinit var config: FrameworkConfig

    @Inject
    lateinit var workerConfig: WorkerLoopConfig

    @Inject
    lateinit var leaderElectionConfig: LeaderElectionConfig

    @Inject
    lateinit var shutdownConfig: ShutdownConfig

    @Inject
    lateinit var sweeperConfig: SweeperConfig

    // -- 1. Default values --------------------------------------------------

    @Test
    fun `worker id defaults to localhost`() {
        assertEquals("localhost", workerConfig.id())
    }

    @Test
    fun `worker pollInterval defaults to 1 second`() {
        assertEquals(Duration.ofSeconds(1), workerConfig.pollInterval())
    }

    @Test
    fun `worker concurrency defaults to 4`() {
        assertEquals(4, workerConfig.concurrency())
    }

    @Test
    fun `worker batchSize defaults to 1`() {
        assertEquals(1, workerConfig.batchSize())
    }

    @Test
    fun `leaderElection namespace defaults to default`() {
        assertEquals("default", leaderElectionConfig.namespace())
    }

    @Test
    fun `leaderElection leaseName defaults to workflow-leader`() {
        assertEquals("workflow-leader", leaderElectionConfig.leaseName())
    }

    @Test
    fun `leaderElection leaseDuration defaults to 15 seconds`() {
        assertEquals(Duration.ofSeconds(15), leaderElectionConfig.leaseDuration())
    }

    @Test
    fun `leaderElection renewDeadline defaults to 10 seconds`() {
        assertEquals(Duration.ofSeconds(10), leaderElectionConfig.renewDeadline())
    }

    @Test
    fun `leaderElection retryPeriod defaults to 2 seconds`() {
        assertEquals(Duration.ofSeconds(2), leaderElectionConfig.retryPeriod())
    }

    @Test
    fun `shutdown globalTimeout defaults to 30 seconds`() {
        assertEquals(Duration.ofSeconds(30), shutdownConfig.globalTimeout())
    }

    @Test
    fun `shutdown leaderTeardownTimeout defaults to 10 seconds`() {
        assertEquals(Duration.ofSeconds(10), shutdownConfig.leaderTeardownTimeout())
    }

    @Test
    fun `sweeper interval defaults to 30 seconds`() {
        assertEquals(Duration.ofSeconds(30), sweeperConfig.interval())
    }

    @Test
    fun `sweeper gracePeriod defaults to 2 minutes`() {
        assertEquals(Duration.ofMinutes(2), sweeperConfig.gracePeriod())
    }

    // -- 1b. New event-driven dispatch defaults -----------------------------

    @Test
    fun `worker fallbackPollInterval defaults to 5 seconds`() {
        assertEquals(Duration.ofSeconds(5), workerConfig.fallbackPollInterval())
    }

    @Test
    fun `worker maxBatchSize defaults to 16`() {
        assertEquals(16, workerConfig.maxBatchSize())
    }

    @Test
    fun `worker podIp defaults to localhost`() {
        assertEquals("localhost", workerConfig.podIp())
    }

    @Test
    fun `serviceName defaults to workflow-engine`() {
        assertEquals("workflow-engine", config.serviceName())
    }

    // -- 3. Config resolution ----------------------------------------

    @Test
    fun `worker config is non-null`() {
        assertNotNull(workerConfig)
    }

    @Test
    fun `leaderElection config is non-null`() {
        assertNotNull(leaderElectionConfig)
    }

    @Test
    fun `shutdown config is non-null`() {
        assertNotNull(shutdownConfig)
    }

    @Test
    fun `sweeper config is non-null`() {
        assertNotNull(sweeperConfig)
    }

    // -- 4. CDI wiring integration ------------------------------------------

    @Test
    fun `FrameworkConfig is injectable`() {
        assertNotNull(config)
    }
}

// -- 2. Override test via TestProfile ---------------------------------------

class FrameworkConfigOverrideProfile : QuarkusTestProfile {
    override fun getConfigOverrides(): Map<String, String> = mapOf(
        "quarkus.arc.exclude-types" to "com.workflow.workflow.**,com.workflow.dispatch.**,com.workflow.worker.**,com.workflow.infrastructure.queryexporter.**",
        "framework.worker.id" to "worker-42",
        "framework.worker.poll-interval" to "PT5S",
        "framework.worker.concurrency" to "16",
        "framework.worker.batch-size" to "8",
        "framework.worker.fallback-poll-interval" to "PT10S",
        "framework.worker.max-batch-size" to "32",
        "framework.worker.pod-ip" to "10.0.0.42",
        "framework.service-name" to "custom-engine",
        "quarkus.datasource.jdbc.max-size" to "32",
        "framework.leader-election.namespace" to "prod",
        "framework.leader-election.lease-name" to "custom-lease",
        "framework.leader-election.lease-duration" to "PT30S",
        "framework.leader-election.renew-deadline" to "PT20S",
        "framework.leader-election.retry-period" to "PT5S",
        "framework.shutdown.global-timeout" to "PT1M",
        "framework.shutdown.leader-teardown-timeout" to "PT20S",
        "framework.sweeper.interval" to "PT1M",
        "framework.sweeper.grace-period" to "PT5M",
    )
}

@QuarkusTest
@TestProfile(FrameworkConfigOverrideProfile::class)
class FrameworkConfigOverrideTest {

    @Inject
    lateinit var config: FrameworkConfig

    @Inject
    lateinit var workerConfig: WorkerLoopConfig

    @Inject
    lateinit var leaderElectionConfig: LeaderElectionConfig

    @Inject
    lateinit var shutdownConfig: ShutdownConfig

    @Inject
    lateinit var sweeperConfig: SweeperConfig

    @Test
    fun `worker id is overridden`() {
        assertEquals("worker-42", workerConfig.id())
    }

    @Test
    fun `worker pollInterval is overridden`() {
        assertEquals(Duration.ofSeconds(5), workerConfig.pollInterval())
    }

    @Test
    fun `worker concurrency is overridden`() {
        assertEquals(16, workerConfig.concurrency())
    }

    @Test
    fun `worker batchSize is overridden`() {
        assertEquals(8, workerConfig.batchSize())
    }

    @Test
    fun `leaderElection namespace is overridden`() {
        assertEquals("prod", leaderElectionConfig.namespace())
    }

    @Test
    fun `leaderElection leaseName is overridden`() {
        assertEquals("custom-lease", leaderElectionConfig.leaseName())
    }

    @Test
    fun `leaderElection leaseDuration is overridden`() {
        assertEquals(Duration.ofSeconds(30), leaderElectionConfig.leaseDuration())
    }

    @Test
    fun `leaderElection renewDeadline is overridden`() {
        assertEquals(Duration.ofSeconds(20), leaderElectionConfig.renewDeadline())
    }

    @Test
    fun `leaderElection retryPeriod is overridden`() {
        assertEquals(Duration.ofSeconds(5), leaderElectionConfig.retryPeriod())
    }

    @Test
    fun `shutdown globalTimeout is overridden`() {
        assertEquals(Duration.ofMinutes(1), shutdownConfig.globalTimeout())
    }

    @Test
    fun `shutdown leaderTeardownTimeout is overridden`() {
        assertEquals(Duration.ofSeconds(20), shutdownConfig.leaderTeardownTimeout())
    }

    @Test
    fun `sweeper interval is overridden`() {
        assertEquals(Duration.ofMinutes(1), sweeperConfig.interval())
    }

    @Test
    fun `sweeper gracePeriod is overridden`() {
        assertEquals(Duration.ofMinutes(5), sweeperConfig.gracePeriod())
    }

    @Test
    fun `worker fallbackPollInterval is overridden`() {
        assertEquals(Duration.ofSeconds(10), workerConfig.fallbackPollInterval())
    }

    @Test
    fun `worker maxBatchSize is overridden`() {
        assertEquals(32, workerConfig.maxBatchSize())
    }

    @Test
    fun `worker podIp is overridden`() {
        assertEquals("10.0.0.42", workerConfig.podIp())
    }

    @Test
    fun `serviceName is overridden`() {
        assertEquals("custom-engine", config.serviceName())
    }

    @Test
    fun `all configs remain non-null after override`() {
        assertNotNull(workerConfig)
        assertNotNull(leaderElectionConfig)
        assertNotNull(shutdownConfig)
        assertNotNull(sweeperConfig)
    }
}
