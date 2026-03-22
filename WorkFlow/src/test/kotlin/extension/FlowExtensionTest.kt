package com.workflow.extension

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FlowExtensionTest {

    // -- A. indefinitelyRepeat ------------------------------------------------

    @Test
    fun `indefinitelyRepeat emits value multiple times`() = runTest {
        val results = indefinitelyRepeat(42).take(5).toList()

        assertEquals(listOf(42, 42, 42, 42, 42), results)
    }

    @Test
    fun `indefinitelyRepeat works with different types`() = runTest {
        val results = indefinitelyRepeat("tick").take(3).toList()

        assertEquals(listOf("tick", "tick", "tick"), results)
    }

    @Test
    fun `indefinitelyRepeat with null value emits nulls`() = runTest {
        val results = indefinitelyRepeat(null as String?).take(3).toList()

        assertEquals(listOf(null, null, null), results)
    }

    // -- B. unorderedMapAsync -------------------------------------------------

    @Test
    fun `unorderedMapAsync transforms all elements`() = runTest {
        val results = (1..5).asFlow()
            .unorderedMapAsync(concurrency = 3) { it * 2 }
            .toList()
            .sorted()

        assertEquals(listOf(2, 4, 6, 8, 10), results)
    }

    @Test
    fun `unorderedMapAsync respects concurrency limit`() = runTest {
        val maxConcurrent = AtomicInteger(0)
        val currentConcurrent = AtomicInteger(0)
        val concurrencyLimit = 2

        (1..10).asFlow()
            .unorderedMapAsync(concurrency = concurrencyLimit) {
                val current = currentConcurrent.incrementAndGet()
                maxConcurrent.updateAndGet { max -> maxOf(max, current) }
                delay(10) // simulate work
                currentConcurrent.decrementAndGet()
                it
            }
            .toList()

        assertTrue(maxConcurrent.get() <= concurrencyLimit)
    }

    @Test
    fun `unorderedMapAsync with concurrency 1 processes sequentially`() = runTest {
        val maxConcurrent = AtomicInteger(0)
        val currentConcurrent = AtomicInteger(0)

        (1..5).asFlow()
            .unorderedMapAsync(concurrency = 1) {
                val current = currentConcurrent.incrementAndGet()
                maxConcurrent.updateAndGet { max -> maxOf(max, current) }
                delay(1)
                currentConcurrent.decrementAndGet()
                it
            }
            .toList()

        assertEquals(1, maxConcurrent.get())
    }

    @Test
    fun `unorderedMapAsync exception in transform does not kill the flow`() = runTest {
        val results = (1..5).asFlow()
            .unorderedMapAsync(concurrency = 2) { value ->
                if (value == 3) throw RuntimeException("bad element")
                value
            }
            .toList()
            .sorted()

        // Element 3 was dropped due to exception; others should be present
        assertTrue(results.containsAll(listOf(1, 2, 4, 5)))
        assertTrue(3 !in results)
    }

    @Test
    fun `unorderedMapAsync with empty flow produces empty result`() = runTest {
        val results = emptyList<Int>().asFlow()
            .unorderedMapAsync(concurrency = 3) { it * 2 }
            .toList()

        assertTrue(results.isEmpty())
    }

    @Test
    fun `unorderedMapAsync semaphore is released after exception`() = runTest {
        // If semaphore isn't released on error, later elements would deadlock
        val processedCount = AtomicInteger(0)

        (1..5).asFlow()
            .unorderedMapAsync(concurrency = 1) { value ->
                if (value == 1) throw RuntimeException("fail early")
                processedCount.incrementAndGet()
                value
            }
            .toList()

        // Elements 2-5 should still process since semaphore is released in finally
        assertEquals(4, processedCount.get())
    }

    // -- C. takeUntilSignal ---------------------------------------------------

    @Test
    fun `takeUntilSignal collects until signal then stops`() = runTest {
        val signal = Channel<Unit>(1)

        val upstream = flow {
            for (i in 1..100) {
                emit(i)
                if (i == 5) signal.send(Unit)
                delay(10)
            }
        }

        val collected = upstream.takeUntilSignal(signal).toList()

        assertTrue(collected.isNotEmpty())
        assertTrue(collected.size < 100) // stopped before collecting all
    }

    @Test
    fun `takeUntilSignal upstream completes before signal completes normally`() = runTest {
        val signal = Channel<Unit>(1)

        val results = (1..3).asFlow().takeUntilSignal(signal).toList()

        assertEquals(listOf(1, 2, 3), results)
    }

    @Test
    fun `takeUntilSignal signal sent before collection yields empty or minimal flow`() = runTest {
        val signal = Channel<Unit>(1)
        signal.send(Unit) // signal before collection

        val upstream = flow {
            delay(100) // delay before emitting
            emit(1)
            emit(2)
        }

        val results = upstream.takeUntilSignal(signal).toList()

        assertTrue(results.size < 2) // should have stopped early
    }

    @Test
    fun `takeUntilSignal collects all elements emitted before signal`() = runTest {
        val signal = Channel<Unit>(1)
        val collected = mutableListOf<Int>()

        val upstream = flow {
            emit(1)
            emit(2)
            emit(3)
            signal.send(Unit)
            delay(100)
            emit(4)
            emit(5)
        }

        upstream.takeUntilSignal(signal).toList().also { collected.addAll(it) }

        assertTrue(collected.contains(1))
        assertTrue(collected.contains(2))
        assertTrue(collected.contains(3))
    }
}
