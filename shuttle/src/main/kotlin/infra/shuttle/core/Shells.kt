package infra.shuttle.core

import kotlinx.coroutines.flow.Flow

/*
 * G0 shells: the names later phases fill in. Every method throws until its phase lands
 * (G6 runner and supervisor, G16 expand).
 */

/** The collector around the pipeline and the end-of-poll reconciliation (G6). */
class RouteRunner {
    suspend fun run(events: Flow<RouteEvent>): Nothing = throw NotImplementedError()
}

/** Spec 10: restart with backoff, readiness (G6). */
class RouteSupervisor {
    suspend fun run(): Nothing = throw NotImplementedError()
}

/** Spec 6.3 `expand`: one child per listed path, fetched through the context (G16). */
class ExpandProcessor(val spec: ProcessorSpec.Expand) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}
