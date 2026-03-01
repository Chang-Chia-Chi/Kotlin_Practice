package com.taskqueue.queue

import io.quarkus.runtime.Startup
import jakarta.enterprise.inject.Instance
import jakarta.inject.Singleton
import org.jboss.logging.Logger

/**
 * Discovers all [TaskHandler] CDI beans at startup and indexes them by [TaskHandler.taskType].
 *
 * Duplicate taskType registrations are caught eagerly — fail-fast at startup rather
 * than producing mysterious runtime routing errors.
 */
@Singleton
@Startup // force eager init so we fail fast on duplicate taskType
class TaskHandlerRegistry(handlers: Instance<TaskHandler>) {

    private val log = Logger.getLogger(TaskHandlerRegistry::class.java)

    private val handlersByType: Map<String, TaskHandler> = buildMap {
        for (handler in handlers) {
            val prev = put(handler.taskType, handler)
            if (prev != null) {
                throw IllegalStateException(
                    "Duplicate TaskHandler for taskType='${handler.taskType}': " +
                        "${prev::class.qualifiedName} and ${handler::class.qualifiedName}"
                )
            }
        }
    }

    init {
        log.infof("Registered %d task handler(s): %s", handlersByType.size, handlersByType.keys)
    }

    /** Resolve handler by taskType. Returns null for unknown types (logged + marked FAILED by consumer). */
    fun getHandler(taskType: String): TaskHandler? = handlersByType[taskType]

    /** All registered task types — useful for health checks and monitoring. */
    fun registeredTypes(): Set<String> = handlersByType.keys
}
