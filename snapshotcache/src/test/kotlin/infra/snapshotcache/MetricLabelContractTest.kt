package infra.snapshotcache

import infra.snapshotcache.api.AcquireUnavailableReason
import infra.snapshotcache.api.RefreshPhase
import infra.snapshotcache.api.RefreshResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * `name.lowercase()` on these enums is the spec 12 metric label verbatim, which is what
 * every alert rule in spec 12.6 is written against. Whole-set equality, so adding,
 * removing or renaming a constant fails here rather than silently in an alert.
 */
class MetricLabelContractTest {

    @Test
    fun `RefreshResult constants are the spec 12_2 result labels`() {
        assertThat(RefreshResult.entries.map { it.name.lowercase() })
            .containsExactlyInAnyOrder(
                "success", "verify_failed", "source_error", "disk_error",
                "shutdown_aborted", "skipped_overlap", "blocked_by_k",
            )
    }

    @Test
    fun `RefreshPhase constants are the spec 12_2 phase labels`() {
        assertThat(RefreshPhase.entries.map { it.name.lowercase() })
            .containsExactlyInAnyOrder("query", "fetch", "append", "checkpoint", "verify", "publish")
    }

    @Test
    fun `AcquireUnavailableReason constants are the spec 12_3 reason labels`() {
        assertThat(AcquireUnavailableReason.entries.map { it.name.lowercase() })
            .containsExactlyInAnyOrder("not_ready", "timeout", "shutting_down")
    }
}
