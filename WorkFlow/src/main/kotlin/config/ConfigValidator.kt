package com.workflow.config

import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.event.Observes
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger

@Singleton
class ConfigValidator(
    private val config: FrameworkConfig,
    @param:ConfigProperty(name = "quarkus.datasource.jdbc.max-size", defaultValue = "20")
    private val poolMaxSize: Int,
) {
    fun onStart(@Observes event: StartupEvent) {
        val concurrency = config.worker().concurrency()
        val requiredPool = concurrency * 2

        check(poolMaxSize >= requiredPool) {
            "Connection pool max-size ($poolMaxSize) must be >= 2 * worker concurrency ($concurrency). " +
            "Set quarkus.datasource.jdbc.max-size >= $requiredPool or reduce framework.worker.concurrency."
        }

        val batchSize = config.worker().batchSize()
        check(batchSize in 1..100) {
            "framework.worker.batch-size must be between 1 and 100 (got $batchSize)"
        }

        val leaseDuration = config.leaderElection().leaseDuration()
        val renewDeadline = config.leaderElection().renewDeadline()
        val retryPeriod = config.leaderElection().retryPeriod()

        check(renewDeadline < leaseDuration) {
            "leader-election.renew-deadline ($renewDeadline) must be < lease-duration ($leaseDuration)"
        }
        check(retryPeriod < renewDeadline) {
            "leader-election.retry-period ($retryPeriod) must be < renew-deadline ($renewDeadline)"
        }

        log.infof(
            "Config validated: concurrency=%d, batchSize=%d, poolSize=%d, lease=%s/%s/%s",
            concurrency, batchSize, poolMaxSize,
            leaseDuration, renewDeadline, retryPeriod,
        )
    }

    companion object {
        private val log = Logger.getLogger(ConfigValidator::class.java)
    }
}
