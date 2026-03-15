package com.mapreduce.leader

import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test

class FencingContextTest {

    @Test
    fun `current returns epoch when in context`() = runTest {
        withContext(FencingContext(42L)) {
            assertEquals(42L, FencingContext.current())
        }
    }

    @Test
    fun `current throws when not in context`() = runTest {
        try {
            FencingContext.current()
            fail("Expected IllegalStateException")
        } catch (e: IllegalStateException) {
            // expected
        }
    }

    @Test
    fun `currentOrNull returns null when not in context`() = runTest {
        assertNull(FencingContext.currentOrNull())
    }

    @Test
    fun `currentOrNull returns epoch when in context`() = runTest {
        withContext(FencingContext(99L)) {
            assertEquals(99L, FencingContext.currentOrNull())
        }
    }

    @Test
    fun `propagates across suspension points`() = runTest {
        withContext(FencingContext(7L)) {
            // First suspension point
            withContext(coroutineContext) {
                assertEquals(7L, FencingContext.current())
            }
            // Second suspension point — still propagated
            withContext(coroutineContext) {
                assertEquals(7L, FencingContext.current())
            }
        }
    }

    @Test
    fun `child coroutine inherits context`() = runTest {
        withContext(FencingContext(55L)) {
            val job = launch {
                assertEquals(55L, FencingContext.current())
            }
            job.join()
        }
    }

    @Test
    fun `different coroutines with different epochs are isolated`() = runTest {
        val results = mutableListOf<Long>()

        val job1 = launch(FencingContext(100L)) {
            results.add(FencingContext.current())
        }
        val job2 = launch(FencingContext(200L)) {
            results.add(FencingContext.current())
        }

        job1.join()
        job2.join()

        assert(results.contains(100L)) { "Should contain epoch 100" }
        assert(results.contains(200L)) { "Should contain epoch 200" }
    }

    @Test
    fun `nested context - inner overrides outer`() = runTest {
        withContext(FencingContext(10L)) {
            assertEquals(10L, FencingContext.current())
            withContext(FencingContext(20L)) {
                assertEquals(20L, FencingContext.current())
            }
            // Outer context is restored
            assertEquals(10L, FencingContext.current())
        }
    }

    @Test
    fun `context element has correct key`() {
        val ctx = FencingContext(1L)
        assertEquals(FencingContext, ctx.key)
    }
}
