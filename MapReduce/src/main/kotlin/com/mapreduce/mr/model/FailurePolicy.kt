package com.mapreduce.mr.model

enum class FailurePolicy {
    FAIL_GROUP, THRESHOLD, BEST_EFFORT
}

/**
 * Evaluate whether a failure policy is violated.
 * Returns a failure reason string if the policy is breached, or `null` if it passes.
 */
fun evaluateFailurePolicy(
    policy: FailurePolicy,
    failed: Int,
    total: Int,
    failureThreshold: Double,
): String? = when (policy) {
    FailurePolicy.FAIL_GROUP ->
        if (failed > 0) "FAIL_GROUP: $failed task(s) failed" else null

    FailurePolicy.THRESHOLD -> {
        if (total == 0) null
        else {
            val rate = failed.toDouble() / total
            if (rate > failureThreshold)
                "THRESHOLD: %.1f%% > %.1f%%".format(rate * 100, failureThreshold * 100)
            else null
        }
    }

    FailurePolicy.BEST_EFFORT -> null
}
