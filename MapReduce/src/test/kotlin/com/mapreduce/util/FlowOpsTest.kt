package com.mapreduce.util

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

class FlowOpsTest {

    // ── indefinitelyRepeat ──────────────────────────────────────────

    @Test
    fun `indefinitelyRepeat emits value repeatedly`() = runTest {
        val items = indefinitelyRepeat("x").take(5).toList()
        assertEquals(listOf("x", "x", "x", "x", "x"), items)
    }

    // ── unorderedMapAsync ───────────────────────────────────────────

    @Test
    fun `unorderedMapAsync transforms all elements`() = runTest {
        val source = flow { for (i in 1..5) emit(i) }

        val results = source
            .unorderedMapAsync(3) { it * 2 }
            .toList()
            .sorted()

        assertEquals(listOf(2, 4, 6, 8, 10), results)
    }

    @Test
    fun `unorderedMapAsync isolates transform exceptions`() = runTest {
        val source = flow { for (i in 1..5) emit(i) }

        val results = source
            .unorderedMapAsync(3) {
                if (it == 3) throw RuntimeException("boom")
                it * 2
            }
            .toList()
            .sorted()

        // Element 3 fails and is dropped; the rest complete normally
        assertEquals(listOf(2, 4, 8, 10), results)
    }

    @Test
    fun `unorderedMapAsync respects concurrency limit`() = runTest {
        val maxConcurrent = AtomicInteger(0)
        val active = AtomicInteger(0)

        val source = flow { for (i in 1..10) emit(i) }

        source.unorderedMapAsync(3) {
            val current = active.incrementAndGet()
            maxConcurrent.updateAndGet { max -> maxOf(max, current) }
            delay(50)
            active.decrementAndGet()
            it
        }.toList()

        assertTrue(maxConcurrent.get() <= 3, "Max concurrent was ${maxConcurrent.get()}, expected <= 3")
    }

    // ── takeUntilSignal ─────────────────────────────────────────────

    @Test
    fun `takeUntilSignal stops collection on signal`() = runTest {
        val signal = Channel<Unit>(1)
        val counter = AtomicInteger(0)

        val source = flow {
            while (true) {
                emit(counter.incrementAndGet())
                delay(10)
            }
        }

        val collected = mutableListOf<Int>()
        val job = launch {
            source.takeUntilSignal(signal).collect { collected.add(it) }
        }

        delay(100)
        signal.send(Unit)
        job.join()

        assertTrue(collected.isNotEmpty(), "Should have collected some items")
        assertTrue(collected.size < 100, "Should have stopped early, got ${collected.size}")
    }
}
