package com.mapreduce.leader

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertNull

class FencingTokenHolderTest {

    @AfterEach
    fun cleanup() {
        FencingTokenHolder.clear()
    }

    @Test
    fun `set-get-clear lifecycle`() {
        assertNull(FencingTokenHolder.get())

        FencingTokenHolder.set(10L)
        assertEquals(10L, FencingTokenHolder.get())

        FencingTokenHolder.clear()
        assertNull(FencingTokenHolder.get())
    }

    @Test
    fun `require() returns value when set`() {
        FencingTokenHolder.set(42L)
        assertEquals(42L, FencingTokenHolder.require())
    }

    @Test
    fun `require() throws when not set`() {
        assertThrows<IllegalStateException> {
            FencingTokenHolder.require()
        }
    }

    @Test
    fun `withToken sets, executes block, and clears in finally`() {
        assertNull(FencingTokenHolder.get())

        val result = FencingTokenHolder.withToken(55L) {
            assertEquals(55L, FencingTokenHolder.require())
            "done"
        }

        assertEquals("done", result)
        assertNull(FencingTokenHolder.get())
    }

    @Test
    fun `withToken clears even on exception`() {
        assertThrows<RuntimeException> {
            FencingTokenHolder.withToken(77L) {
                assertEquals(77L, FencingTokenHolder.require())
                throw RuntimeException("boom")
            }
        }

        assertNull(FencingTokenHolder.get())
    }

    @Test
    fun `thread isolation - different threads see different values`() {
        val otherThreadValue = AtomicReference<Long?>(null)
        val latch = CountDownLatch(1)

        FencingTokenHolder.set(100L)

        val thread = Thread {
            // Other thread should not see the value set on this thread
            otherThreadValue.set(FencingTokenHolder.get())
            latch.countDown()
        }
        thread.start()
        latch.await()

        assertEquals(100L, FencingTokenHolder.get())
        assertNull(otherThreadValue.get())
    }

    @Test
    fun `nested withToken clears rather than restoring outer value`() {
        FencingTokenHolder.withToken(1L) {
            assertEquals(1L, FencingTokenHolder.require())

            FencingTokenHolder.withToken(2L) {
                assertEquals(2L, FencingTokenHolder.require())
            }

            // Inner withToken called clear() in its finally block,
            // so the outer value is gone -- this is the known behavior
            assertNull(FencingTokenHolder.get())
        }

        assertNull(FencingTokenHolder.get())
    }
}
