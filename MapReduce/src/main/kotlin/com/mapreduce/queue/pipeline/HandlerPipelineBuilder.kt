package com.mapreduce.queue.pipeline

import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.spi.TaskHandler
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Instance
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap

/**
 * Constructs and caches per-handler middleware chains.
 *
 * The chain is a composed suspend function: each middleware wraps the next,
 * with the handler invocation as the innermost call. Built once per handler
 * on first use (lazy), then reused for every invocation. No per-invocation allocation.
 *
 * Middlewares are discovered via CDI ([Instance]) and sorted by [HandlerMiddleware.order].
 */
@ApplicationScoped
class HandlerPipelineBuilder(
    private val middlewares: Instance<HandlerMiddleware>,
) {

    private val log = Logger.getLogger(HandlerPipelineBuilder::class.java)

    /** Cached chains keyed by handler name. */
    private val cache = ConcurrentHashMap<String, suspend (TaskExecutionContext) -> TaskResult>()

    /** Sorted middleware list, computed once on first access. */
    private val sortedMiddlewares: List<HandlerMiddleware> by lazy {
        middlewares.toList().sortedBy { it.order }.also { list ->
            log.infof(
                "Pipeline middlewares (ordered): %s",
                list.joinToString(" → ") { "${it::class.simpleName}(${it.order})" },
            )
        }
    }

    /**
     * Get or build the cached pipeline chain for a handler.
     *
     * The returned suspend function accepts a [TaskExecutionContext] and returns
     * a [TaskResult] after passing through all middlewares and the handler.
     */
    fun chainFor(handler: TaskHandler): suspend (TaskExecutionContext) -> TaskResult =
        cache.computeIfAbsent(handler.handlerName) { buildChain(handler) }

    private fun buildChain(handler: TaskHandler): suspend (TaskExecutionContext) -> TaskResult {
        // Innermost: invoke the handler itself
        val innermost: suspend (TaskExecutionContext) -> TaskResult = { ctx ->
            handler.handle(ctx.taskContext)
        }

        // Fold middlewares from innermost to outermost (foldRight so that
        // the lowest-order middleware is the outermost wrapper)
        return sortedMiddlewares.foldRight(innermost) { middleware, next ->
            { ctx -> middleware.invoke(ctx, next) }
        }
    }
}
