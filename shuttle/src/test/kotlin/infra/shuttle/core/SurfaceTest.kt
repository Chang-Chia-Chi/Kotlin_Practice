package infra.shuttle.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** The numbers and names the spec fixes, read back from the model rather than from a document. */
class SurfaceTest {

    @Test
    fun defaults_of_spec_9_3_and_10() {
        val policy = DeliveryPolicy()
        assertEquals(50, policy.maxAttempts)
        assertEquals(24.hours, policy.giveUpAfter)
        assertEquals(Backoff(initial = 5.seconds, max = 15.minutes, factor = 2.0), policy.backoff)
        assertEquals(true, policy.fullJitter)
        assertEquals(10.seconds, policy.timeout)

        val bare = shuttle { route("mirror") {} }
        assertEquals(Backoff(initial = 30.seconds, max = 15.minutes), bare.supervision.restartBackoff)
        assertEquals(Readiness.AllRoutesDown, bare.supervision.readiness)
        assertEquals(1, bare.routes.single().parallelism)
        assertEquals(5, bare.routes.single().maxAttempts)
        assertEquals(60.seconds, bare.drainTimeout)
        assertEquals(30.seconds, bare.notifier.sweepEvery)
        assertEquals(DigestAlgorithm.MD5, bare.digest)

        // v0.4 (D40, D41)
        assertEquals(24.hours, bare.routes.single().recheckFinished)
        assertEquals(1L shl 30, Staging(java.nio.file.Path.of("stage")).minFree)
        assertEquals(ProcessorSpec.Unzip(maxEntries = 10_000, maxBytes = 10L shl 30), unzip())
    }

    @Test
    fun metric_names_of_spec_14_2_verbatim() = assertEquals(
        setOf(
            "shuttle_transfers_total", "shuttle_stage_seconds", "shuttle_inflight", "shuttle_children_total",
            "shuttle_stuck_transfers", "shuttle_reconciled_total", "shuttle_reconcile_skipped_total", "shuttle_poll_total",
            "shuttle_route_up", "shuttle_route_restarts_total", "shuttle_delivery_total", "shuttle_delivery_seconds",
            "shuttle_outbox_pending", "shuttle_outbox_oldest_seconds", "shuttle_notifier_inflight", "shuttle_supersedes_total",
            "shuttle_staging_free_bytes", "shuttle_staging_deferred_total",
        ),
        ShuttleMetrics.names,
    )

    @Test
    fun hook_points_of_spec_4_4_in_order() = assertEquals(
        listOf("afterFetch", "afterProcess", "afterStore", "afterLedgerStored", "afterAck", "afterLedgerAcked", "afterDeliverySent"),
        HookPoint.entries.map { it.name },
    )
}
