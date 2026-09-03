package infra.shuttle.core

/** Spec 14.2, verbatim. Tags are `route`, `channel`, `store`; never a name, id or key. */
object ShuttleMetrics {
    const val TRANSFERS = "shuttle_transfers_total"
    const val STAGE_SECONDS = "shuttle_stage_seconds"
    const val INFLIGHT = "shuttle_inflight"
    const val CHILDREN = "shuttle_children_total"
    const val STUCK_TRANSFERS = "shuttle_stuck_transfers"
    const val RECONCILED = "shuttle_reconciled_total"
    const val RECONCILE_SKIPPED = "shuttle_reconcile_skipped_total"
    const val POLLS = "shuttle_poll_total"
    const val ROUTE_UP = "shuttle_route_up"
    const val ROUTE_RESTARTS = "shuttle_route_restarts_total"
    const val DELIVERIES = "shuttle_delivery_total"
    const val DELIVERY_SECONDS = "shuttle_delivery_seconds"
    const val OUTBOX_PENDING = "shuttle_outbox_pending"
    const val OUTBOX_OLDEST_SECONDS = "shuttle_outbox_oldest_seconds"
    const val NOTIFIER_INFLIGHT = "shuttle_notifier_inflight"
    const val SUPERSEDES = "shuttle_supersedes_total"
    const val STAGING_FREE_BYTES = "shuttle_staging_free_bytes"
    const val STAGING_DEFERRED = "shuttle_staging_deferred_total"

    val names: Set<String> = setOf(
        TRANSFERS, STAGE_SECONDS, INFLIGHT, CHILDREN, STUCK_TRANSFERS, RECONCILED, RECONCILE_SKIPPED, POLLS, ROUTE_UP,
        ROUTE_RESTARTS, DELIVERIES, DELIVERY_SECONDS, OUTBOX_PENDING, OUTBOX_OLDEST_SECONDS, NOTIFIER_INFLIGHT, SUPERSEDES,
        STAGING_FREE_BYTES, STAGING_DEFERRED,
    )
}
