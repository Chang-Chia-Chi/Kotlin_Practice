package com.mapreduce.leader

/**
 * Thrown by the [FencedLeaderInterceptor] when the current pod is not the leader,
 * or when leadership was lost during execution of a fenced method.
 *
 * Expected on follower pods — callers (e.g., @Scheduled methods) should catch
 * this silently rather than logging it as an error.
 */
class NotLeaderException(
    message: String = "This pod is not the leader",
) : RuntimeException(message)
