package com.workflow.infrastructure.config

import com.workflow.infrastructure.leader.LeaderElectionConfig
import com.workflow.worker.config.WorkerLoopConfig
import io.quarkus.runtime.StartupEvent
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertDoesNotThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFailsWith

class ConfigValidatorTest {

    private val workerConfig = mock<WorkerLoopConfig>()
    private val leaderConfig = mock<LeaderElectionConfig>()
    private val startupEvent = mock<StartupEvent>()

    private fun validator(poolMaxSize: Int) = ConfigValidator(workerConfig, leaderConfig, poolMaxSize)

    private fun validDefaults() {
        whenever(workerConfig.concurrency()).thenReturn(4)
        whenever(workerConfig.batchSize()).thenReturn(1)
        whenever(leaderConfig.leaseDuration()).thenReturn(Duration.ofSeconds(15))
        whenever(leaderConfig.renewDeadline()).thenReturn(Duration.ofSeconds(10))
        whenever(leaderConfig.retryPeriod()).thenReturn(Duration.ofSeconds(2))
    }

    @Nested
    inner class ConnectionPoolValidation {

        @Test
        fun `fails when pool size is less than 2x concurrency`() {
            validDefaults()
            whenever(workerConfig.concurrency()).thenReturn(10)

            val ex = assertFailsWith<IllegalStateException> {
                validator(poolMaxSize = 5).onStart(startupEvent)
            }
            assertContains(ex.message!!, "Connection pool max-size (5)")
            assertContains(ex.message!!, ">= 20")
        }

        @Test
        fun `succeeds when pool size equals 2x concurrency`() {
            validDefaults()
            whenever(workerConfig.concurrency()).thenReturn(4)

            assertDoesNotThrow { validator(poolMaxSize = 8).onStart(startupEvent) }
        }

        @Test
        fun `succeeds when pool size exceeds 2x concurrency`() {
            validDefaults()
            whenever(workerConfig.concurrency()).thenReturn(4)

            assertDoesNotThrow { validator(poolMaxSize = 20).onStart(startupEvent) }
        }
    }

    @Nested
    inner class BatchSizeValidation {

        @Test
        fun `fails when batchSize is 0`() {
            validDefaults()
            whenever(workerConfig.batchSize()).thenReturn(0)

            val ex = assertFailsWith<IllegalStateException> {
                validator(poolMaxSize = 20).onStart(startupEvent)
            }
            assertContains(ex.message!!, "batch-size")
        }

        @Test
        fun `fails when batchSize exceeds 100`() {
            validDefaults()
            whenever(workerConfig.batchSize()).thenReturn(101)

            val ex = assertFailsWith<IllegalStateException> {
                validator(poolMaxSize = 20).onStart(startupEvent)
            }
            assertContains(ex.message!!, "batch-size")
        }

        @Test
        fun `succeeds when batchSize is 1`() {
            validDefaults()
            assertDoesNotThrow { validator(poolMaxSize = 20).onStart(startupEvent) }
        }

        @Test
        fun `succeeds when batchSize is 100`() {
            validDefaults()
            whenever(workerConfig.batchSize()).thenReturn(100)
            assertDoesNotThrow { validator(poolMaxSize = 20).onStart(startupEvent) }
        }
    }

    @Nested
    inner class LeaderElectionTimingValidation {

        @Test
        fun `fails when renewDeadline equals leaseDuration`() {
            validDefaults()
            whenever(leaderConfig.renewDeadline()).thenReturn(Duration.ofSeconds(15))
            whenever(leaderConfig.leaseDuration()).thenReturn(Duration.ofSeconds(15))

            val ex = assertFailsWith<IllegalStateException> {
                validator(poolMaxSize = 20).onStart(startupEvent)
            }
            assertContains(ex.message!!, "renew-deadline")
        }

        @Test
        fun `fails when renewDeadline exceeds leaseDuration`() {
            validDefaults()
            whenever(leaderConfig.renewDeadline()).thenReturn(Duration.ofSeconds(20))
            whenever(leaderConfig.leaseDuration()).thenReturn(Duration.ofSeconds(15))

            val ex = assertFailsWith<IllegalStateException> {
                validator(poolMaxSize = 20).onStart(startupEvent)
            }
            assertContains(ex.message!!, "renew-deadline")
            assertContains(ex.message!!, "lease-duration")
        }

        @Test
        fun `fails when retryPeriod equals renewDeadline`() {
            validDefaults()
            whenever(leaderConfig.retryPeriod()).thenReturn(Duration.ofSeconds(10))
            whenever(leaderConfig.renewDeadline()).thenReturn(Duration.ofSeconds(10))

            val ex = assertFailsWith<IllegalStateException> {
                validator(poolMaxSize = 20).onStart(startupEvent)
            }
            assertContains(ex.message!!, "retry-period")
        }

        @Test
        fun `fails when retryPeriod exceeds renewDeadline`() {
            validDefaults()
            whenever(leaderConfig.retryPeriod()).thenReturn(Duration.ofSeconds(15))
            whenever(leaderConfig.renewDeadline()).thenReturn(Duration.ofSeconds(10))

            val ex = assertFailsWith<IllegalStateException> {
                validator(poolMaxSize = 20).onStart(startupEvent)
            }
            assertContains(ex.message!!, "retry-period")
            assertContains(ex.message!!, "renew-deadline")
        }
    }

    @Nested
    inner class ValidConfiguration {

        @Test
        fun `succeeds with all default values`() {
            validDefaults()
            assertDoesNotThrow { validator(poolMaxSize = 20).onStart(startupEvent) }
        }
    }
}
