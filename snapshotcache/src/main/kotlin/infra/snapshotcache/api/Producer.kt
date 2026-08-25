package infra.snapshotcache.api

import java.sql.Connection
import java.time.Instant

/**
 * Caller-injected "how to pull the data" (spec 5.2).
 *
 * The framework owns generation management, leases, the verify gate and reclamation;
 * this is the only seam that knows about the source system. Switching to delta mode
 * later replaces this implementation and nothing else (spec 14.1).
 */
fun interface GenerationSource {
    /**
     * Populates the candidate generation. All tables in the group must be read inside
     * one source read transaction (spec 7.1) and streamed into [BuildContext.target].
     */
    fun refresh(ctx: BuildContext)
}

/** Everything a [GenerationSource] needs to build one candidate generation (spec 5.2). */
data class BuildContext(
    val group: GroupId,
    val generation: Long,
    /** Write connection to the candidate generation file. */
    val target: Connection,
    /** Source point in time, recorded when the source read transaction opened. */
    val dataAsOf: Instant,
    /** Reserved for delta mode; always null while full reload is the strategy (spec 14.1, D6). */
    val previous: Snapshot? = null,
)

/**
 * A verification rule run against a candidate before it can be published (spec 5.2, 8).
 *
 * Built-in rules are a fixed list inside the framework; this is the single extension point.
 */
fun interface GenerationCheck {
    fun verify(candidate: Connection, previous: GenerationInfo?): VerifyResult
}

/** Outcome of one [GenerationCheck]. Any [Fail] aborts the candidate and leaves current untouched. */
sealed interface VerifyResult {
    data object Pass : VerifyResult

    /** [rule] identifies the check for `snapshot_verify_failed_total{rule}`; [detail] goes to the log. */
    data class Fail(val rule: String, val detail: String) : VerifyResult
}
