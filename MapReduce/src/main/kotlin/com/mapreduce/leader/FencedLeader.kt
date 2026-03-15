package com.mapreduce.leader

import jakarta.interceptor.InterceptorBinding

/**
 * Marks a method (or all methods on a class) as leader-only with fencing.
 *
 * The [FencedLeaderInterceptor] intercepts calls to annotated methods and:
 * 1. **Pre-check**: verifies this pod is the leader.
 * 2. **Propagate**: sets the fencing epoch on both [FencingTokenHolder] (ThreadLocal)
 *    and [FencingContext] (CoroutineContext) so downstream repository code can read it.
 * 3. **Execute**: proceeds with the method body.
 * 4. **Post-check**: verifies leadership was not lost during execution.
 *
 * On follower pods, throws [NotLeaderException] immediately. Callers
 * (e.g., @Scheduled methods) should catch this silently.
 */
@InterceptorBinding
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class FencedLeader
