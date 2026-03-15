package com.mapreduce.chain.registry

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.chain.handler.ChainStepHandler
import com.mapreduce.chain.repository.ChainRepository
import com.mapreduce.chain.spi.ChainDefinition
import com.mapreduce.queue.registry.HandlerRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers all [ChainDefinition] beans at startup and registers the single
 * [ChainStepHandler] with the generic [HandlerRegistry].
 *
 * Unlike MR/DAG registrars that create per-type handlers, chains use a single
 * `"chain.step"` handler for all chain types. The handler resolves the actual
 * step handler at runtime from the definition's step list.
 */
@ApplicationScoped
class ChainRegistrar(
    private val definitions: Instance<ChainDefinition>,
    private val handlerRegistry: HandlerRegistry,
    private val chainRepository: ChainRepository,
    private val objectMapper: ObjectMapper,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(ChainRegistrar::class.java)
    private val definitionMap = ConcurrentHashMap<String, ChainDefinition>()

    fun onStart(@Observes @Priority(20) ev: StartupEvent) {
        definitions.forEach { def ->
            definitionMap[def.chainType] = def
            log.infof(
                "Registered Chain definition: %s (%d steps, handlers: [%s])",
                def.chainType,
                def.steps.size,
                def.steps.joinToString(", ") { it.handler },
            )
        }

        if (definitionMap.isNotEmpty()) {
            handlerRegistry.register(
                ChainStepHandler(this, chainRepository, handlerRegistry, objectMapper, meterRegistry),
            )
            log.infof("Registered chain.step handler for %d chain type(s)", definitionMap.size)
        }
    }

    fun getDefinition(chainType: String): ChainDefinition? = definitionMap[chainType]

    fun supportedChainTypes(): List<String> = definitionMap.keys.toList()
}
