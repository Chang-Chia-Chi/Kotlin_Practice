package com.mapreduce.workflow.registry

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.queue.registry.HandlerRegistry
import com.mapreduce.queue.repository.WorkflowStepRepository
import com.mapreduce.workflow.handler.StepTransitionHandler
import com.mapreduce.workflow.spi.WorkflowDefinition
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers all [WorkflowDefinition] beans at startup and registers
 * one [StepTransitionHandler] per workflow type with the generic [HandlerRegistry].
 *
 * Validates at startup that:
 * - Step names within each pipeline are unique.
 * - Handler names in each step spec are non-blank.
 */
@ApplicationScoped
class WorkflowRegistry(
    private val definitions: Instance<WorkflowDefinition<*>>,
    private val handlerRegistry: HandlerRegistry,
    private val workflowStepRepository: WorkflowStepRepository,
    private val config: FrameworkConfig,
) {

    private val log = Logger.getLogger(WorkflowRegistry::class.java)
    private val definitionMap = ConcurrentHashMap<String, WorkflowDefinition<*>>()

    fun onStart(@Observes @Priority(20) ev: StartupEvent) {
        definitions.forEach { def ->
            validate(def)
            handlerRegistry.register(
                StepTransitionHandler(
                    workflowName = def.workflowName,
                    workflowStepRepository = workflowStepRepository,
                    workflowRegistry = this,
                    config = config,
                ),
            )
            definitionMap[def.workflowName] = def
            log.infof(
                "Registered workflow definition: %s → [%s.__step_transition] (%d steps)",
                def.workflowName, def.workflowName, def.pipeline().size,
            )
        }
    }

    fun getDefinition(workflowName: String): WorkflowDefinition<*>? =
        definitionMap[workflowName]

    fun supportedWorkflows(): List<String> = definitionMap.keys.toList()

    private fun validate(def: WorkflowDefinition<*>) {
        val pipeline = def.pipeline()
        require(pipeline.isNotEmpty()) {
            "Workflow '${def.workflowName}' has an empty pipeline"
        }

        val stepNames = pipeline.map { it.name }
        val duplicates = stepNames.groupBy { it }.filter { it.value.size > 1 }.keys
        require(duplicates.isEmpty()) {
            "Workflow '${def.workflowName}' has duplicate step names: $duplicates"
        }

        pipeline.forEach { step ->
            require(step.handler.isNotBlank()) {
                "Workflow '${def.workflowName}' step '${step.name}' has a blank handler name"
            }
            if (step.compensation != null) {
                require(step.compensation.isNotBlank()) {
                    "Workflow '${def.workflowName}' step '${step.name}' has a blank compensation handler name"
                }
            }
        }
    }
}
