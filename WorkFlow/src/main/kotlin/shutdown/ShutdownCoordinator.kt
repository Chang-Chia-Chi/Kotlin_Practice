package com.mapreduce.shutdown

import com.mapreduce.config.FrameworkConfig
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.jboss.logging.Logger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Central coordinator for the graceful shutdown protocol.
 *
 * This is the **single entry point** for shutdown — no other bean should
 * observe [ShutdownEvent] for lifecycle teardown. Components participate
 * by implementing [ShutdownParticipant]; the coordinator discovers them
 * via CDI, groups by [ShutdownParticipant.shutdownOrder], and runs each
 * group concurrently with per-participant timeouts.
 */
@ApplicationScoped
class ShutdownCoordinator(
    private val participants: Instance<ShutdownParticipant>,
    private val meterRegistry: MeterRegistry,
    private val config: FrameworkConfig,
) {
    private val log = Logger.getLogger(ShutdownCoordinator::class.java)
    private val podId = System.getenv("HOSTNAME") ?: "local-worker"

    private val _state = AtomicReference(ShutdownState.RUNNING)

    val state: ShutdownState get() = _state.get()
    val isShuttingDown: Boolean get() = _state.get() != ShutdownState.RUNNING

    init {
        meterRegistry.gauge("taskqueue_shutdown_state", this) { it.state.ordinal.toDouble() }
    }

    fun onShutdown(
        @Observes ev: ShutdownEvent,
    ) = runBlocking {
        val shutdownStart = Instant.now()

        _state.set(ShutdownState.DRAINING)
        log.info("Shutdown initiated")

        // Run participants grouped by order (lower first, concurrent within group)
        val completed = withTimeoutOrNull(config.shutdown().globalTimeout().toMillis()) {
            participants
                .sortedBy { it.shutdownOrder }
                .groupBy { it.shutdownOrder }
                .forEach { (order, group) ->
                    log.infof(
                        "Shutdown group order=%d: %s",
                        order,
                        group.joinToString { it::class.simpleName ?: "?" },
                    )
                    coroutineScope {
                        group.forEach { p ->
                            launch {
                                try {
                                    withTimeoutOrNull(p.shutdownTimeout.toMillis()) {
                                        p.shutdown()
                                    } ?: log.warnf(
                                        "%s timed out after %s",
                                        p::class.simpleName,
                                        p.shutdownTimeout,
                                    )
                                } catch (e: Exception) {
                                    log.warnf(e, "Error during shutdown of %s", p::class.simpleName)
                                }
                            }
                        }
                    }
                }
        }
        if (completed == null) {
            log.warnf("Global shutdown timeout expired after %s — forcing termination", config.shutdown().globalTimeout())
        }

        // Final
        _state.set(ShutdownState.TERMINATED)
        val totalDuration = Duration.between(shutdownStart, Instant.now())

        meterRegistry
            .timer("taskqueue_shutdown_duration_seconds", "pod", podId)
            .record(totalDuration.toMillis(), TimeUnit.MILLISECONDS)

        log.infof(
            "Shutdown complete: pod=%s, durationMs=%d",
            podId,
            totalDuration.toMillis(),
        )
    }
}
