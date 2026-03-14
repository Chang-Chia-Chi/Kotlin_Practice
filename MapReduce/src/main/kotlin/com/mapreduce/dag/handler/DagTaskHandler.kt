package com.mapreduce.dag.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.mapreduce.dag.repository.DagRepository
import com.mapreduce.dag.spi.DagNodeHandler
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import org.jboss.logging.Logger

/**
 * Auto-generated [TaskHandler] wrapper for a [DagNodeHandler].
 *
 * Not a CDI bean — instantiated by [com.mapreduce.dag.registry.DagRegistrar]
 * and registered programmatically with the [com.mapreduce.queue.registry.HandlerRegistry].
 *
 * The wrapper captures the node's output and persists it on the
 * `dag_task_instance` row for downstream XCom consumption.
 */
class DagTaskHandler(
    private val nodeHandler: DagNodeHandler,
    private val dagRepository: DagRepository,
    private val objectMapper: ObjectMapper,
) : TaskHandler {

    private val log = Logger.getLogger(DagTaskHandler::class.java)

    override val handlerName: String = "dag.${nodeHandler.nodeType}"

    override suspend fun handle(ctx: TaskContext): TaskResult {
        val instanceId = extractInstanceId(ctx.metadata)
            ?: return TaskResult.Failure("DAG task ${ctx.taskId} missing instance_id in metadata")

        val output = nodeHandler.execute(ctx.payload)
        dagRepository.saveInstanceOutput(instanceId, output)

        log.debugf("DAG node %s completed (task=%s, instance=%s)", nodeHandler.nodeType, ctx.taskId, instanceId)
        return TaskResult.Success
    }

    private fun extractInstanceId(metadata: String?): String? {
        if (metadata == null) return null
        return try {
            objectMapper.readTree(metadata).get("instance_id")?.asText()
        } catch (_: Exception) {
            null
        }
    }
}
