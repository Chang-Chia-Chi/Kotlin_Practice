package com.mapreduce.dag.spi

/**
 * Handler for a specific type of DAG node.
 *
 * Implement this interface as a CDI bean. The framework wraps each
 * implementation in a [com.mapreduce.queue.spi.TaskHandler] and registers
 * it with handler name `"dag.{nodeType}"`.
 *
 * Node handlers are reusable across multiple DAG blueprints.
 */
interface DagNodeHandler {
    /**
     * Node type identifier — e.g. `"order.validate"`, `"payment.process"`.
     * Must be unique across all node handlers.
     */
    val nodeType: String

    /**
     * Execute the node's business logic.
     *
     * @param payload Merged JSON containing `global_context` and `upstream` outputs.
     * @return Output JSON to pass to downstream nodes (XCom), or null for no output.
     */
    suspend fun execute(payload: String): String?
}
