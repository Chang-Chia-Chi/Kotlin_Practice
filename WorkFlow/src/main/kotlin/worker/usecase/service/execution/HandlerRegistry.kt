package com.workflow.worker.usecase.service.execution

import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class HandlerRegistry(
    cdiBeans: Instance<TransitionHandler>,
) {
    private val handlers = ConcurrentHashMap<String, TransitionHandler>()

    init {
        cdiBeans.forEach {
            handlers[it.key()] = it
        }
    }

    fun resolve(key: String): TransitionHandler = handlers[key] ?: throw IllegalStateException("No handler found for key: $key")

    fun register(
        key: String,
        handler: TransitionHandler,
    ) {
        handlers[key] = handler
    }
}
