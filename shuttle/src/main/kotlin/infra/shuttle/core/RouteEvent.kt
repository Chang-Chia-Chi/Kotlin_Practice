package infra.shuttle.core

import java.time.Instant

/** Spec 3.1: what a trigger emits. The test kit's scripted source produces these directly (D20). */
sealed class RouteEvent {
    class Seen(
        val identity: SourceIdentity,
        val source: SourceView,
        val ack: suspend () -> Unit,
        val nack: suspend (redeliver: Boolean) -> Unit,
    ) : RouteEvent()

    data class PollCompleted(val startedAt: Instant, val listed: Set<SourceIdentity>, val truncated: Boolean) : RouteEvent()
    data class PollFailed(val cause: Throwable) : RouteEvent()
    data object PollSkipped : RouteEvent()
    data class RouteDown(val cause: Throwable) : RouteEvent()
}
