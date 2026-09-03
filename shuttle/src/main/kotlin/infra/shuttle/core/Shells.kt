package infra.shuttle.core

import com.fasterxml.jackson.databind.JsonNode
import kotlinx.coroutines.flow.Flow

/*
 * G0 shells: the names later phases fill in. Every method throws until its phase lands
 * (G3 renderer, G4 processors, G5 pipeline, G6 runner and supervisor, G8 notifier).
 */

/** Spec 4.1 stages 0 to 4 for one source object (G5). */
class TransferPipeline {
    suspend fun run(event: RouteEvent.Seen, fetch: Fetcher): Nothing = throw NotImplementedError()
}

/** The collector around the pipeline and the end-of-poll reconciliation (G6). */
class RouteRunner {
    suspend fun run(events: Flow<RouteEvent>): Nothing = throw NotImplementedError()
}

/** Spec 9.4 (G8). */
class Notifier {
    suspend fun run(): Nothing = throw NotImplementedError()
}

/** Spec 10: restart with backoff, readiness (G6). */
class RouteSupervisor {
    suspend fun run(): Nothing = throw NotImplementedError()
}

/** Spec 9.6: a pure function from a transfer row plus attributes to a JSON tree (G3). */
class MappingRenderer {
    suspend fun render(table: MappingTable, transfer: Transfer, event: DeliveryEvent?): JsonNode = throw NotImplementedError()
    fun check(table: MappingTable, declaredAttributes: Set<String>): List<String> = throw NotImplementedError()
}

/** Spec 6.3 built-ins (G4); each is the others' second implementation of the seam. */
class QualityProcessor : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}

class RenameProcessor(val spec: ProcessorSpec.Rename) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}

class ZipProcessor : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}

class UnzipProcessor(val spec: ProcessorSpec.Unzip) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}

class ExtractProcessor(val spec: ProcessorSpec.Extract) : Processor {
    override val produces = spec.produces
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}

class ExpandProcessor(val spec: ProcessorSpec.Expand) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}

class VerifyDigestProcessor(val spec: ProcessorSpec.VerifyDigest) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}
