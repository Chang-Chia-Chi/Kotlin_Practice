package com.workflow.worker

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
