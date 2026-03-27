package com.workflow.config

import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.QuarkusTestProfile
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Tests for [FrameworkConfig] -- validates SmallRye ConfigMapping defaults,
 * overrides via test profile, nested config resolution, and CDI wiring.
 */
@QuarkusTest
@TestProfile(ConfigOnlyTestProfile::class)
class FrameworkConfigDefaultsTest {

    @Inject
    lateinit var config: FrameworkConfig

    // -- 1. Default values --------------------------------------------------

    @Test
    fun `worker id defaults to localhost`() {
        assertEquals("localhost", config.worker().id())
    }

    @Test
    fun `worker pollInterval defaults to 1 second`() {
        assertEquals(Duration.ofSeconds(1), config.worker().pollInterval())
    }

    @Test
    fun `worker concurrency defaults to 4`() {
        assertEquals(4, config.worker().concurrency())
    }

    @Test
    fun `worker batchSize defaults to 1`() {
        assertEquals(1, config.worker().batchSize())
    }

    @Test
    fun `leaderElection namespace defaults to default`() {
        assertEquals("default", config.leaderElection().namespace())
    }

    @Test
    fun `leaderElection leaseName defaults to workflow-leader`() {
        assertEquals("workflow-leader", config.leaderElection().leaseName())
    }

    @Test
    fun `leaderElection leaseDuration defaults to 15 seconds`() {
        assertEquals(Duration.ofSeconds(15), config.leaderElection().leaseDuration())
    }

    @Test
    fun `leaderElection renewDeadline defaults to 10 seconds`() {
        assertEquals(Duration.ofSeconds(10), config.leaderElection().renewDeadline())
    }

    @Test
    fun `leaderElection retryPeriod defaults to 2 seconds`() {
        assertEquals(Duration.ofSeconds(2), config.leaderElection().retryPeriod())
    }

    @Test
    fun `shutdown globalTimeout defaults to 30 seconds`() {
        assertEquals(Duration.ofSeconds(30), config.shutdown().globalTimeout())
    }

    @Test
    fun `shutdown leaderTeardownTimeout defaults to 10 seconds`() {
        assertEquals(Duration.ofSeconds(10), config.shutdown().leaderTeardownTimeout())
    }

    @Test
    fun `sweeper interval defaults to 30 seconds`() {
        assertEquals(Duration.ofSeconds(30), config.sweeper().interval())
    }

    @Test
    fun `sweeper gracePeriod defaults to 2 minutes`() {
        assertEquals(Duration.ofMinutes(2), config.sweeper().gracePeriod())
    }

    // -- 3. Nested config resolution ----------------------------------------

    @Test
    fun `worker config is non-null`() {
        assertNotNull(config.worker())
    }

    @Test
    fun `leaderElection config is non-null`() {
        assertNotNull(config.leaderElection())
    }

    @Test
    fun `shutdown config is non-null`() {
        assertNotNull(config.shutdown())
    }

    @Test
    fun `sweeper config is non-null`() {
        assertNotNull(config.sweeper())
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
        "quarkus.arc.exclude-types" to "com.workflow.engine.**,com.workflow.worker.**,com.workflow.queryexporter.**",
        "framework.worker.id" to "worker-42",
        "framework.worker.poll-interval" to "PT5S",
        "framework.worker.concurrency" to "16",
        "framework.worker.batch-size" to "8",
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

    @Test
    fun `worker id is overridden`() {
        assertEquals("worker-42", config.worker().id())
    }

    @Test
    fun `worker pollInterval is overridden`() {
        assertEquals(Duration.ofSeconds(5), config.worker().pollInterval())
    }

    @Test
    fun `worker concurrency is overridden`() {
        assertEquals(16, config.worker().concurrency())
    }

    @Test
    fun `worker batchSize is overridden`() {
        assertEquals(8, config.worker().batchSize())
    }

    @Test
    fun `leaderElection namespace is overridden`() {
        assertEquals("prod", config.leaderElection().namespace())
    }

    @Test
    fun `leaderElection leaseName is overridden`() {
        assertEquals("custom-lease", config.leaderElection().leaseName())
    }

    @Test
    fun `leaderElection leaseDuration is overridden`() {
        assertEquals(Duration.ofSeconds(30), config.leaderElection().leaseDuration())
    }

    @Test
    fun `leaderElection renewDeadline is overridden`() {
        assertEquals(Duration.ofSeconds(20), config.leaderElection().renewDeadline())
    }

    @Test
    fun `leaderElection retryPeriod is overridden`() {
        assertEquals(Duration.ofSeconds(5), config.leaderElection().retryPeriod())
    }

    @Test
    fun `shutdown globalTimeout is overridden`() {
        assertEquals(Duration.ofMinutes(1), config.shutdown().globalTimeout())
    }

    @Test
    fun `shutdown leaderTeardownTimeout is overridden`() {
        assertEquals(Duration.ofSeconds(20), config.shutdown().leaderTeardownTimeout())
    }

    @Test
    fun `sweeper interval is overridden`() {
        assertEquals(Duration.ofMinutes(1), config.sweeper().interval())
    }

    @Test
    fun `sweeper gracePeriod is overridden`() {
        assertEquals(Duration.ofMinutes(5), config.sweeper().gracePeriod())
    }

    @Test
    fun `all nested configs remain non-null after override`() {
        assertNotNull(config.worker())
        assertNotNull(config.leaderElection())
        assertNotNull(config.shutdown())
        assertNotNull(config.sweeper())
    }
}
