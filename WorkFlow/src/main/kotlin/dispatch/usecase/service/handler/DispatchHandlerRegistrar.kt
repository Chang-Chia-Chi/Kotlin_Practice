package com.workflow.dispatch.usecase.service.handler

import com.workflow.worker.usecase.service.execution.HandlerRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes

@ApplicationScoped
class DispatchHandlerRegistrar(
    private val registry: HandlerRegistry,
    private val scatter: DispatchScatterHandler,
    private val simulation: DispatchSimulationHandler,
    private val join: DispatchJoinHandler,
) {
    fun onStart(@Observes event: StartupEvent) {
        registry.register("dispatch.scatter", scatter)
        registry.register("dispatch.simulate", simulation)
        registry.register("dispatch.join", join)
    }
}
