package infra.shuttle.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext

/**
 * One place that reads a transfer's row, renders `bodies[channel]` for a moment (spec 9.6, D19) and calls
 * the channel (9.2): the notifier's worker and the pipeline's `callback` ack (5.3) both go through it, and
 * the attribute freeze check of 6.4 reads the same tables. One per process, built by the host; it holds no
 * loop, no policy and no in-flight set - those stay the notifier's. `CancellationException` passes through.
 */
class Deliverer(
    private val store: StateStore,
    channels: Collection<DeliveryChannel>,
    private val bodies: Map<ChannelName, MappingTable>,
    private val renderer: MappingRenderer = MappingRenderer(),
) {
    val channels: Map<ChannelName, DeliveryChannel> = channels.associateBy { it.name }

    /**
     * Renders and delivers [moment] of transfer [id] on [channel], then runs [record] on the outcome. Everything,
     * [record] included, runs with the transfer, its route and the channel in the MDC (spec 3.2), so what the
     * caller logs about the outcome names them too. The outcome is the channel's own, or the classification
     * of what stopped the call: a row that cannot render is a `Reject`, anything else that threw a `Retry`;
     * a state store failure reading the row is a `Retry` as well, with the route absent from the MDC.
     */
    suspend fun <T> deliver(id: TransferId, channel: ChannelName, moment: DeliveryMoment, attempt: Int, record: suspend (DeliveryOutcome) -> T): T {
        val loaded = try {
            Result.success(store.byId(id))
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
        val mdc = buildMap {
            put(Mdc.TRANSFER_ID, id.value.toString())
            put(Mdc.CHANNEL, channel.value)
            loaded.getOrNull()?.let { put(Mdc.ROUTE, it.identity.route.value) }
        }
        return withContext(MDCContext(mdc)) {
            val target = channels[channel]
            val outcome = if (target == null) {
                DeliveryOutcome.Reject(null, "no channel named ${channel.value}")
            } else {
                try {
                    val transfer = loaded.getOrThrow() ?: throw MappingFailure("", "transfer ${id.value} not found")
                    val body = renderer.render(bodies[channel] ?: MappingTable(emptyList()), transfer, moment, attempt)
                    target.deliver(DeliveryEvent(id, moment, channel, attempt, body))
                } catch (e: CancellationException) {
                    throw e
                } catch (e: MappingFailure) {
                    DeliveryOutcome.Reject(null, e.message ?: "mapping failure")
                } catch (e: Exception) {
                    DeliveryOutcome.Retry(null, e.toString())
                }
            }
            record(outcome)
        }
    }

    /** Spec 6.4 at attribute freeze: every named channel's table must be satisfiable by [attributes]; a provider resolves iff the renderer's does. */
    fun checkAttributes(attributes: Map<String, String>, channels: Collection<ChannelName>) =
        ProcessingChain.checkMappings(attributes, channels.mapNotNull { bodies[it] }, renderer::hasProvider)
}
