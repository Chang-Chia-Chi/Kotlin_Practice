package com.mapreduce.leader

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class FencingContextTest {

    @Test
    fun `current() returns epoch when present in coroutine context`() = runTest {
        withContext(FencingContext(42L)) {
            assertEquals(42L, FencingContext.current())
        }
    }

    @Test
    fun `current() throws when not in context`() = runTest {
        assertThrows<IllegalStateException> {
            FencingContext.current()
        }
    }

    @Test
    fun `currentOrNull() returns null when not in context`() = runTest {
        assertNull(FencingContext.currentOrNull())
    }

    @Test
    fun `epoch propagates through withContext dispatcher switches`() = runTest {
        withContext(FencingContext(99L)) {
            withContext(Dispatchers.Default) {
                assertEquals(99L, FencingContext.current())
            }
        }
    }

    @Test
    fun `child coroutines inherit the fencing context`() = runTest {
        withContext(FencingContext(7L)) {
            val deferred = async {
                FencingContext.current()
            }
            assertEquals(7L, deferred.await())
        }
    }

    @Test
    fun `nested FencingContext overrides parent`() = runTest {
        withContext(FencingContext(1L)) {
            assertEquals(1L, FencingContext.current())

            withContext(FencingContext(2L)) {
                assertEquals(2L, FencingContext.current())
            }

            // Parent scope is restored after inner withContext exits
            assertEquals(1L, FencingContext.current())
        }
    }
}
