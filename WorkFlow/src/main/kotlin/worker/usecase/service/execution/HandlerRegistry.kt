package com.workflow.worker.usecase.service.execution

import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class HandlerRegistry {

    private val handlers = ConcurrentHashMap<String, TransitionHandler>()

    fun resolve(key: String): TransitionHandler =
        handlers[key] ?: throw IllegalStateException("No handler found for key: $key")

    fun register(key: String, handler: TransitionHandler) {
        handlers[key] = handler
    }
}
