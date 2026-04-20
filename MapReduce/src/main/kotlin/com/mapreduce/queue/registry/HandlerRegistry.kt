package com.mapreduce.queue.registry

import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.event.Observes
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers all [TaskHandler] beans at startup via CDI and supports
 * programmatic registration (used by Layer 2 patterns like workflow definitions
 * to register handlers from definitions).
 *
 * **Startup ordering contract:** This bean observes [StartupEvent] at
 * `@Priority(10)` to discover CDI handlers first. Layer 2 registrars
 * (e.g., WorkflowRegistry) must use a higher priority value (e.g., 20)
 * so they run after CDI discovery is complete.
 */
@ApplicationScoped
class HandlerRegistry(private val cdiHandlers: Instance<TaskHandler>) {

    private val log = Logger.getLogger(HandlerRegistry::class.java)
    private val registry = ConcurrentHashMap<String, TaskHandler>()

    fun onStart(@Observes @Priority(10) ev: StartupEvent) {
        cdiHandlers.forEach { register(it) }
        log.infof("CDI handler discovery complete: %s", registry.keys)
    }

    /** Programmatic registration — called by Layer 2 registrars. */
    fun register(handler: TaskHandler) {
        val prev = registry.putIfAbsent(handler.handlerName, handler)
        if (prev != null) {
            log.warnf("Duplicate handler '%s' — keeping first registration", handler.handlerName)
        } else {
            log.debugf("Registered handler: %s", handler.handlerName)
        }
    }

    fun resolve(handlerName: String): TaskHandler? = registry[handlerName]

    fun registeredHandlers(): Set<String> = registry.keys.toSet()
}
