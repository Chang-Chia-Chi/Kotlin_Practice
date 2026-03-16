package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.jdbi.v3.core.Jdbi
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@Liveness
@ApplicationScoped
class OracleHealthContributor(
    private val jdbi: Jdbi,
    private val config: FrameworkConfig,
) : HealthCheck {

    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "health-oracle-check").apply { isDaemon = true }
    }

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("oracle")
        val timeout = config.health().oracleCheckTimeout()
        return try {
            val future = executor.submit(Callable {
                jdbi.withHandle<Int, Exception> { h ->
                    h.createQuery("SELECT 1 FROM DUAL").mapTo(Int::class.java).one()
                }
            })
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
            builder.up().build()
        } catch (e: TimeoutException) {
            builder.down()
                .withData("reason", "Query timed out after ${timeout.seconds}s")
                .build()
        } catch (e: Exception) {
            builder.down()
                .withData("reason", "Database unreachable")
                .withData("error", e.cause?.message ?: e.message ?: "unknown")
                .build()
        }
    }
}
