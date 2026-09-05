package infra.shuttle.quarkus

import infra.shuttle.core.Outcome
import infra.shuttle.core.Payload
import infra.shuttle.core.ProcessContext
import infra.shuttle.core.Processor

/**
 * Spec 13.1's `custom: imageResizer` as a test bean: it passes the payload through and remembers every
 * `config` it was configured with, so a test can read what the host and `shuttle try` handed it. It answers
 * itself rather than a configured copy because a copy would be the thing the chain holds and the test would
 * have nothing to read.
 */
class RecordingProcessor : Processor {
    val configs = mutableListOf<Map<String, Any?>>()
    override val produces = emptySet<String>()
    override fun configured(config: Map<String, Any?>): Processor = also { configs += config }
    override suspend fun process(payload: Payload, ctx: ProcessContext) = Outcome.Continue(payload)
}
