package com.mapreduce.fanout.registry

import com.mapreduce.fanout.handler.FanoutTaskHandler
import com.mapreduce.fanout.repository.FanoutJobRepository
import com.mapreduce.fanout.spi.FanoutDefinition
import com.mapreduce.fanout.spi.unsafeCast
import com.mapreduce.queue.registry.HandlerRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers all [FanoutDefinition] beans at startup and registers
 * the auto-generated execute handler with the generic [HandlerRegistry].
 *
 * This is how the fan-out layer (Layer 2) plugs into Layer 1 without
 * modifying the queue. Only one handler per definition: `"{jobType}.execute"`.
 */
@ApplicationScoped
class FanoutRegistrar(
    private val definitions: Instance<FanoutDefinition<*, *>>,
    private val handlerRegistry: HandlerRegistry,
    private val fanoutJobRepository: FanoutJobRepository,
) {

    private val log = Logger.getLogger(FanoutRegistrar::class.java)
    private val definitionMap = ConcurrentHashMap<String, FanoutDefinition<*, *>>()

    fun onStart(@Observes @Priority(20) ev: StartupEvent) {
        definitions.forEach { def ->
            val unsafe = def.unsafeCast()
            handlerRegistry.register(FanoutTaskHandler(unsafe, fanoutJobRepository))
            definitionMap[def.jobType] = def
            log.infof("Registered fanout definition: %s → [%s.execute]",
                def.jobType, def.jobType)
        }
    }

    fun getDefinition(jobType: String): FanoutDefinition<*, *>? =
        definitionMap[jobType]

    fun supportedJobTypes(): List<String> = definitionMap.keys.toList()
}
