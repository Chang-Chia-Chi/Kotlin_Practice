package com.mapreduce.observability.health

import com.mapreduce.config.FrameworkConfig
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Liveness
import org.jdbi.v3.core.Jdbi

@Liveness
@ApplicationScoped
class OracleHealthContributor(
    private val jdbi: Jdbi,
    private val config: FrameworkConfig,
) : HealthCheck {

    override fun call(): HealthCheckResponse {
        val builder = HealthCheckResponse.named("oracle")
        val timeout = config.health().oracleCheckTimeout()
        return try {
            runBlocking {
                withTimeout(timeout.toMillis()) {
                    withContext(Dispatchers.IO) {
                        jdbi.withHandle<Int, Exception> { h ->
                            h.createQuery("SELECT 1 FROM DUAL").mapTo(Int::class.java).one()
                        }
                    }
                }
            }
            builder.up().build()
        } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
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
