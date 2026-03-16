package com.mapreduce.leader

import jakarta.interceptor.InvocationContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class FencedLeaderInterceptorTest {

    private val leaderManager = mock<LeaderManager>()
    private val interceptor = FencedLeaderInterceptor(leaderManager)
    private val ctx = mock<InvocationContext>()

    @AfterEach
    fun cleanup() {
        FencingTokenHolder.clear()
    }

    private fun stubMethod() {
        val method = FencedLeaderInterceptorTest::class.java.getMethod("cleanup")
        whenever(ctx.method).thenReturn(method)
    }

    @Test
    fun `pre-check throws NotLeaderException when not leader`() {
        whenever(leaderManager.isActive).thenReturn(false)

        assertThrows<NotLeaderException> {
            interceptor.intercept(ctx)
        }
    }

    @Test
    fun `sets FencingTokenHolder before calling ctx proceed()`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(leaderManager.token).thenReturn(42L)
        stubMethod()

        whenever(ctx.proceed()).thenAnswer {
            // Verify the token is available during proceed()
            assertEquals(42L, FencingTokenHolder.require())
            "result"
        }

        interceptor.intercept(ctx)
        verify(ctx).proceed()
    }

    @Test
    fun `clears FencingTokenHolder after execution`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(leaderManager.token).thenReturn(10L)
        stubMethod()
        whenever(ctx.proceed()).thenReturn("ok")

        interceptor.intercept(ctx)

        assertNull(FencingTokenHolder.get())
    }

    @Test
    fun `post-check throws NotLeaderException if leadership lost during execution`() {
        whenever(leaderManager.token).thenReturn(5L)
        stubMethod()
        whenever(ctx.proceed()).thenReturn("ok")

        // Leader before proceed, not leader after
        whenever(leaderManager.isActive).thenReturn(true, false)

        assertThrows<NotLeaderException> {
            interceptor.intercept(ctx)
        }

        // Token must still be cleared
        assertNull(FencingTokenHolder.get())
    }

    @Test
    fun `warns but does not throw if epoch changed during execution`() {
        stubMethod()
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(ctx.proceed()).thenReturn("ok")

        // Epoch changes from 5 to 6 between the initial read and the post-check
        whenever(leaderManager.token).thenReturn(5L, 6L)

        val result = interceptor.intercept(ctx)

        // Should return normally (warning logged but no exception)
        assertEquals("ok", result)
    }

    @Test
    fun `returns result from ctx proceed()`() {
        whenever(leaderManager.isActive).thenReturn(true)
        whenever(leaderManager.token).thenReturn(1L)
        stubMethod()
        whenever(ctx.proceed()).thenReturn("expected-result")

        val result = interceptor.intercept(ctx)

        assertEquals("expected-result", result)
    }
}
