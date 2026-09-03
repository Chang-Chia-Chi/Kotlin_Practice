package infra.shuttle.core

/*
 * G0 shells: the names later phases fill in. Every method throws until its phase lands (G16 expand).
 */

/** Spec 6.3 `expand`: one child per listed path, fetched through the context (G16). */
class ExpandProcessor(val spec: ProcessorSpec.Expand) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw NotImplementedError()
}
