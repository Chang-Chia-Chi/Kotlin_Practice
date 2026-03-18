package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance

/**
 * Immutable middleware pipeline, built once at startup.
 *
 * Middlewares are CDI-discovered, sorted by [Middleware.order], and
 * composed into an onion-layer chain via `foldRight`. The handler
 * sits at the terminal of the chain.
 */
@ApplicationScoped
class TaskPipeline(middlewares: Instance<Middleware>) {

    private val sorted: List<Middleware> = middlewares.toList().sortedBy { it.order }

    /**
     * Execute [handler] through the middleware chain with the given [context].
     */
    suspend fun execute(context: TaskExecutionContext, handler: TaskHandler): TaskResult {
        val terminal: suspend (TaskExecutionContext) -> TaskResult = { ctx ->
            handler.handle(ctx.taskContext)
        }
        val chain = sorted.foldRight(terminal) { middleware, next ->
            { ctx -> middleware.invoke(ctx, next) }
        }
        return chain(context)
    }
}
