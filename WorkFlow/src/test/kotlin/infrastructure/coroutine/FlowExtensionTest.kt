package com.workflow.infrastructure.coroutine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

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
    fun `unorderedMapAsync exception in transform propagates to collector`() = runTest {
        assertFailsWith<RuntimeException>("bad element") {
            (1..5).asFlow()
                .unorderedMapAsync(concurrency = 2) { value ->
                    if (value == 3) throw RuntimeException("bad element")
                    value
                }
                .toList()
        }
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
        // Semaphore must be released in finally even when transform throws,
        // so the flow can propagate the exception without deadlocking
        assertFailsWith<RuntimeException>("fail early") {
            (1..5).asFlow()
                .unorderedMapAsync(concurrency = 1) { value ->
                    if (value == 1) throw RuntimeException("fail early")
                    value
                }
                .toList()
        }
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

    // -- D. Exception propagation (D5) ----------------------------------------

    @Test
    fun `unorderedMapAsync non-CancellationException propagates and cancels flow`() = runTest {
        val upstream = flow {
            emit(1)
            emit(2)
            emit(3)
        }

        assertFailsWith<RuntimeException>("expected boom") {
            upstream
                .unorderedMapAsync(concurrency = 1) { value ->
                    if (value == 2) throw RuntimeException("expected boom")
                    value
                }
                .toList()
        }
    }

    // -- E. SupervisorJob failure isolation (R2.5) ----------------------------

    @Test
    fun `unorderedMapAsync failure isolation — siblings survive one failure`() = runTest {
        val completed = AtomicInteger(0)
        // Gate: the failing element waits until at least one sibling has started
        val siblingStarted = CountDownLatch(1)

        // withContext(Dispatchers.Default) so channelFlow children run with
        // real concurrency (channelFlow + SupervisorJob children don't inherit
        // the TestCoroutineScheduler, so delay() needs a real dispatcher).
        val exception = withContext(Dispatchers.Default) {
            assertFailsWith<RuntimeException> {
                (1..5).asFlow()
                    .unorderedMapAsync(concurrency = 3) { value ->
                        if (value == 3) {
                            // Wait until a sibling is running, then fail
                            siblingStarted.await()
                            delay(50) // give siblings time to complete
                            throw RuntimeException("element 3 failed")
                        }
                        siblingStarted.countDown()
                        delay(10) // simulate work
                        completed.incrementAndGet()
                        value
                    }
                    .toList()
            }
        }

        // (a) The exception from element 3 surfaces to the collector
        assertEquals("element 3 failed", exception.message)

        // (b) At least some non-failing elements completed before the flow terminated.
        // With SupervisorJob, the failing child does NOT cancel its siblings,
        // so in-flight transforms run to completion.
        assertTrue(
            completed.get() > 0,
            "Expected at least one non-failing element to complete, but none did. " +
                "In-flight siblings should complete before channel close propagates."
        )
    }

    @Test
    fun `unorderedMapAsync cancellation propagation — collector cancel terminates flow`() = runTest {
        val transformStarted = AtomicInteger(0)
        val collected = mutableListOf<Int>()

        // Real dispatcher needed: channelFlow + SupervisorJob children use real time.
        withContext(Dispatchers.Default) {
            withTimeout(5.seconds) {
                try {
                    (1..100).asFlow()
                        .unorderedMapAsync(concurrency = 3) { value ->
                            transformStarted.incrementAndGet()
                            delay(200) // slow transform
                            value
                        }
                        .collect { value ->
                            collected.add(value)
                            if (collected.size >= 3) {
                                throw kotlinx.coroutines.CancellationException("collector done")
                            }
                        }
                } catch (_: kotlinx.coroutines.CancellationException) {
                    // Expected: collector cancelled
                }
            }
        }

        // We collected at least 3 results before cancelling
        assertTrue(
            collected.size >= 3,
            "Expected at least 3 collected results, got ${collected.size}"
        )

        // The flow did NOT process all 100 elements — cancellation propagated downward
        assertTrue(
            transformStarted.get() < 100,
            "Expected fewer than 100 transforms to start (got ${transformStarted.get()}). " +
                "Cancellation should propagate downward to terminate the flow."
        )
    }
}
