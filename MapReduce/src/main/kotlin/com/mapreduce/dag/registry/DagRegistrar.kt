package com.mapreduce.dag.registry

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.dag.handler.DagTaskHandler
import com.mapreduce.dag.repository.DagRepository
import com.mapreduce.dag.spi.DagBlueprint
import com.mapreduce.dag.spi.DagNodeHandler
import com.mapreduce.dag.spi.TaskTypeHandler
import com.mapreduce.queue.registry.HandlerRegistry
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers all [DagBlueprint], [DagNodeHandler], and [TaskTypeHandler] beans at startup.
 *
 * For each [DagNodeHandler], creates a [DagTaskHandler] wrapper and registers
 * it with the generic [HandlerRegistry]. This is how Layer 2 (DAG) plugs
 * into Layer 1 without modifying the queue.
 *
 * For each [TaskTypeHandler], registers it in the task type registry for
 * lookup during dispatch-time payload assembly.
 */
@ApplicationScoped
class DagRegistrar(
    private val blueprints: Instance<DagBlueprint>,
    private val nodeHandlers: Instance<DagNodeHandler>,
    private val taskTypeHandlers: Instance<TaskTypeHandler>,
    private val handlerRegistry: HandlerRegistry,
    private val dagRepository: DagRepository,
    private val objectMapper: ObjectMapper,
) {
    private val log = Logger.getLogger(DagRegistrar::class.java)
    private val blueprintMap = ConcurrentHashMap<String, DagBlueprint>()
    private val taskTypeMap = ConcurrentHashMap<String, TaskTypeHandler>()

    fun onStart(
        @Observes @Priority(20) ev: StartupEvent,
    ) {
        // Register node handlers (Layer 1 task wrappers)
        nodeHandlers.forEach { handler ->
            handlerRegistry.register(DagTaskHandler(handler, dagRepository, objectMapper))
            log.infof("Registered DAG node handler: dag.%s", handler.nodeType)
        }

        // Register blueprints
        blueprints.forEach { bp ->
            blueprintMap[bp.dagId] = bp
            log.infof("Registered DAG blueprint: %s (%d nodes, namespace=%s)",
                bp.dagId, bp.nodes().size, bp.namespace.ifBlank { "<root>" })
        }

        // Register task type handlers
        taskTypeHandlers.forEach { typeHandler ->
            taskTypeMap[typeHandler.typeId] = typeHandler
            log.infof("Registered task type handler: %s", typeHandler.typeId)
        }
    }

    fun getBlueprint(dagId: String): DagBlueprint? = blueprintMap[dagId]

    fun supportedDags(): List<String> = blueprintMap.keys.toList()

    fun getTaskTypeHandler(typeId: String): TaskTypeHandler? = taskTypeMap[typeId]

    fun supportedTaskTypes(): List<String> = taskTypeMap.keys.toList()
}
