package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import jakarta.enterprise.context.ApplicationScoped
import org.jdbi.v3.core.Jdbi
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Health contributor for Oracle database connectivity.
 *
 * Executes `SELECT 1 FROM DUAL` with a configurable timeout.
 * Both liveness and readiness return the same result — if the database
 * is unreachable, the pod is neither live nor ready.
 */
@ApplicationScoped
class OracleHealthContributor(
    private val jdbi: Jdbi,
    private val config: FrameworkConfig,
) : HealthContributor {

    override val name: String = "oracle"

    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "health-oracle-check").apply { isDaemon = true }
    }

    override fun liveness(): ProbeResult = checkConnectivity()

    override fun readiness(): ProbeResult = checkConnectivity()

    private fun checkConnectivity(): ProbeResult {
        val timeout = config.health().oracleCheckTimeout()
        return try {
            val future = executor.submit(Callable {
                jdbi.withHandle<Int, Exception> { h ->
                    h.createQuery("SELECT 1 FROM DUAL").mapTo(Int::class.java).one()
                }
            })
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
            ProbeResult(status = HealthStatus.UP)
        } catch (e: TimeoutException) {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf("reason" to "Query timed out after ${timeout.seconds}s"),
            )
        } catch (e: Exception) {
            ProbeResult(
                status = HealthStatus.DOWN,
                details = mapOf(
                    "reason" to "Database unreachable",
                    "error" to (e.cause?.message ?: e.message ?: "unknown"),
                ),
            )
        }
    }
}
