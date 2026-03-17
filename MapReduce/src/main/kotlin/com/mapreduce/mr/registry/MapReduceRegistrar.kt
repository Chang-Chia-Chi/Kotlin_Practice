package com.mapreduce.mr.registry

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.mr.handler.MapTaskHandler
import com.mapreduce.mr.handler.PhaseTransitionHandler
import com.mapreduce.mr.handler.ReduceTaskHandler
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.mr.spi.unsafeCast
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.TaskGroupRepository
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers all [MapReduceDefinition] beans at startup and registers
 * the auto-generated map/reduce/phase-transition handlers with the
 * generic [HandlerRegistry].
 */
@ApplicationScoped
class MapReduceRegistrar(
    private val definitions: Instance<MapReduceDefinition<*, *, *, *>>,
    private val handlerRegistry: HandlerRegistry,
    private val taskGroupRepository: TaskGroupRepository,
    private val blobStore: BlobStore,
    private val objectMapper: ObjectMapper,
) {

    private val log = Logger.getLogger(MapReduceRegistrar::class.java)
    private val definitionMap = ConcurrentHashMap<String, MapReduceDefinition<*, *, *, *>>()

    fun onStart(@Observes @Priority(20) ev: StartupEvent) {
        definitions.forEach { def ->
            val unsafe = def.unsafeCast()
            handlerRegistry.register(MapTaskHandler(unsafe, blobStore))
            handlerRegistry.register(ReduceTaskHandler(unsafe, taskGroupRepository, blobStore, objectMapper))
            handlerRegistry.register(
                PhaseTransitionHandler(
                    jobType = def.jobType,
                    taskGroupRepository = taskGroupRepository,
                    maxRetries = def.maxRetries,
                    queue = def.queue,
                    totalPartitions = 1,
                ),
            )
            definitionMap[def.jobType] = def
            log.infof("Registered MR definition: %s → [%s.map, %s.reduce, %s.__phase_complete]",
                def.jobType, def.jobType, def.jobType, def.jobType)
        }
    }

    fun getDefinition(jobType: String): MapReduceDefinition<*, *, *, *>? =
        definitionMap[jobType]

    fun supportedJobTypes(): List<String> = definitionMap.keys.toList()
}
