package com.mapreduce.mr.registry

import com.mapreduce.mr.handler.MapTaskHandler
import com.mapreduce.mr.handler.ReduceTaskHandler
import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.shuffle.BlobStore
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.mr.spi.unsafeCast
import com.mapreduce.queue.registry.HandlerRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers all [MapReduceDefinition] beans at startup and registers
 * the auto-generated map/reduce handlers with the generic [HandlerRegistry].
 *
 * This is how Layer 2 plugs into Layer 1 without modifying the queue.
 */
@ApplicationScoped
class MapReduceRegistrar(
    private val definitions: Instance<MapReduceDefinition<*, *, *, *>>,
    private val handlerRegistry: HandlerRegistry,
    private val jobRepository: JobRepository,
    private val blobStore: BlobStore,
) {

    private val log = Logger.getLogger(MapReduceRegistrar::class.java)
    private val definitionMap = ConcurrentHashMap<String, MapReduceDefinition<*, *, *, *>>()

    fun onStart(@Observes @Priority(20) ev: StartupEvent) {
        definitions.forEach { def ->
            val unsafe = def.unsafeCast()
            handlerRegistry.register(MapTaskHandler(unsafe, jobRepository, blobStore))
            handlerRegistry.register(ReduceTaskHandler(unsafe, jobRepository, blobStore))
            definitionMap[def.jobType] = def
            log.infof("Registered MR definition: %s → [%s.map, %s.reduce]",
                def.jobType, def.jobType, def.jobType)
        }
    }

    fun getDefinition(jobType: String): MapReduceDefinition<*, *, *, *>? =
        definitionMap[jobType]

    fun supportedJobTypes(): List<String> = definitionMap.keys.toList()
}
