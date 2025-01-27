package org.example

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.sync.Mutex
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

suspend fun main() {
    val stateFlow = MutableStateFlow(0)
    val counter = AtomicInteger(0)
    val mutex = Mutex()

    CoroutineScope(Dispatchers.IO).launch {
        val count = counter.incrementAndGet()
        stateFlow.value = count

        delay(1000)

        val count2 = counter.incrementAndGet()
        stateFlow.value = count2
    }

    (0..10)
        .map {
            CoroutineScope(Dispatchers.IO).launch {
                val delayMillis = Random.nextLong(600, 1200)
                println("delay: $delayMillis")

                delay(delayMillis)

                val count =
                    stateFlow.filter { it == counter.get() }.first()
                println("count: $count")
            }
        }.joinAll()
}
