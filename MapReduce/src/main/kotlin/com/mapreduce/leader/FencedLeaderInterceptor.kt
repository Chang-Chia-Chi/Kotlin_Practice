package com.mapreduce.leader

import jakarta.annotation.Priority
import jakarta.interceptor.AroundInvoke
import jakarta.interceptor.Interceptor
import jakarta.interceptor.InvocationContext
import org.jboss.logging.Logger

/**
 * CDI interceptor that enforces the fenced leader election pattern.
 *
 * Intercepts methods annotated with [@FencedLeader][FencedLeader] and provides
 * three layers of zombie protection:
 *
 * 1. **Pre-check** — rejects the call if this pod is not the leader (fast-fail).
 * 2. **Token propagation** — sets the fencing epoch on [FencingTokenHolder]
 *    so repository code can include it in SQL WHERE guards.
 * 3. **Post-check** — detects leadership loss during execution (e.g., GC pause).
 *
 * The pre/post checks are optimizations. The real safety comes from the
 * DB fence (`WHERE last_epoch <= :epoch`) in each SQL write.
 */
@FencedLeader
@Interceptor
@Priority(Interceptor.Priority.APPLICATION)
class FencedLeaderInterceptor(
    private val leaderManager: LeaderManager,
) {

    private val log = Logger.getLogger(FencedLeaderInterceptor::class.java)

    @AroundInvoke
    fun intercept(ctx: InvocationContext): Any? {
        // Pre-check: is this pod the leader?
        if (!leaderManager.isActive) {
            throw NotLeaderException()
        }

        val epoch = leaderManager.token

        // Propagate epoch via ThreadLocal and execute
        return FencingTokenHolder.withToken(epoch) {
            val result = ctx.proceed()

            // Post-check: still leader? (detects GC pauses > leaseDuration)
            if (!leaderManager.isActive) {
                throw NotLeaderException("Leadership lost during execution of ${ctx.method.name}")
            }

            // Post-check: epoch unchanged? (warn but don't throw — DB fence already protected)
            val currentEpoch = leaderManager.token
            if (currentEpoch != epoch) {
                log.warnf(
                    "Epoch changed during execution of %s (%d → %d) — DB fence protected individual writes",
                    ctx.method.name, epoch, currentEpoch,
                )
            }

            result
        }
    }
}
