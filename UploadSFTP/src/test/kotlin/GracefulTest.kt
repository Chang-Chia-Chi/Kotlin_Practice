import com.river.core.unorderedMapAsync
import infra.coroutine.takeUntilSignal
import junit.framework.TestCase.assertEquals
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

class GracefulTest {
    @Test
    fun `test unorderedMapAsync waits for in-flight jobs`(): Unit =
        runBlocking(Dispatchers.IO) {
            val inFlight = AtomicInteger(0)
            val completed = AtomicInteger(0)

            val signal = CompletableDeferred<Unit>()

            launch {
                delay(100) // Let some jobs start
                println("Triggering shutdown. In-flight: ${inFlight.get()}, time: ${LocalDateTime.now()}")
                signal.complete(Unit)
            }

            coroutineScope {
                testFlow(signal) { value ->
                    inFlight.incrementAndGet()
                    println("Start $value, in-flight: ${inFlight.get()}")
                    delay(1000) // Long processing
                    println("Done $value")
                    completed.incrementAndGet()
                    inFlight.decrementAndGet()
                    value
                }.collect()

                println("Final: Completed=${completed.get()}, In-flight=${inFlight.get()}, time: ${LocalDateTime.now()}")

                // If unorderedMapAsync waits: in-flight will be 0
                // If it doesn't wait: in-flight will be > 0
                assertEquals(0, inFlight.get())
            }
        }

    private fun <R> testFlow(
        signal: Deferred<Unit>,
        block: suspend (Int) -> R,
    ) = flow {
        repeat(20) {
            emit(it)
            delay(10)
        }
    }.takeUntilSignal(signal)
        .unorderedMapAsync(10) { value ->
            block(value)
        }.onCompletion { println("Flow Done!") }
}
