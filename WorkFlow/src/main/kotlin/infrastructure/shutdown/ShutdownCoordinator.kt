package com.workflow.infrastructure.shutdown

import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.ShutdownEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

@ApplicationScoped
class ShutdownCoordinator(
    private val participants: Instance<ShutdownParticipant>,
    private val meterRegistry: MeterRegistry,
    private val shutdownConfig: ShutdownConfig,
) {
    private val log = LoggerFactory.getLogger(ShutdownCoordinator::class.java)
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
        if (!_state.compareAndSet(ShutdownState.RUNNING, ShutdownState.DRAINING)) return@runBlocking

        val shutdownStart = Instant.now()
        log.info("Shutdown initiated")

        // Run participants grouped by order (lower first, concurrent within group)
        val completed = withTimeoutOrNull(shutdownConfig.globalTimeout().toMillis()) {
            participants
                .sortedBy { it.shutdownOrder }
                .groupBy { it.shutdownOrder }
                .forEach { (order, group) ->
                    log.info(
                        "Shutdown group order={}: {}",
                        order,
                        group.joinToString { it::class.simpleName ?: "?" },
                    )
                    coroutineScope {
                        group.forEach { p ->
                            launch {
                                try {
                                    withTimeoutOrNull(p.shutdownTimeout.toMillis()) {
                                        p.shutdown()
                                    } ?: log.warn(
                                        "{} timed out after {}",
                                        p::class.simpleName,
                                        p.shutdownTimeout,
                                    )
                                } catch (e: Exception) {
                                    log.warn("Error during shutdown of {}", p::class.simpleName, e)
                                }
                            }
                        }
                    }
                }
        }
        if (completed == null) {
            log.warn("Global shutdown timeout expired after {} — forcing termination", shutdownConfig.globalTimeout())
        }

        // Final
        _state.set(ShutdownState.TERMINATED)
        val totalDuration = Duration.between(shutdownStart, Instant.now())

        meterRegistry
            .timer("taskqueue_shutdown_duration_seconds", "pod", podId)
            .record(totalDuration.toMillis(), TimeUnit.MILLISECONDS)

        log.info(
            "Shutdown complete: pod={}, durationMs={}",
            podId,
            totalDuration.toMillis(),
        )
    }
}
