import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.onSuccess
import kotlinx.coroutines.selects.select

fun main() =
    runBlocking<Unit> {
        val dataChannel = Channel<String>(5)

        val shutdownDeferred = CompletableDeferred<Unit>()

        repeat(5) { id ->
            launch {
                println("Consumer $id started.")
                select<Unit> {
                    shutdownDeferred.onAwait {
                        println("Consumer $id received shutdown signal.")
                        return@onAwait
                    }

                    dataChannel.onReceiveCatching { result ->
                        result
                            .onSuccess { data ->
                                println("Consumer $id received data: $data")
                            }.onFailure { throwable ->
                                if (throwable is ClosedReceiveChannelException) {
                                    println("Consumer $id detected channel closure.")
                                } else {
                                    println("Consumer $id encountered an error: ${throwable?.message}")
                                }
                            }
                    }
                }
            }
        }

        launch {
            val items = listOf("Item1", "Item2", "Item3", "Item4", "Item5")
            for (item in items) {
                delay(500)

                dataChannel
                    .trySend(item)
                    .onSuccess { println("Producer sends: $item") }
                    .onFailure { throwable -> println("Producer sends failed: ${throwable?.message}") }
            }
            dataChannel.close()
        }

        launch {
            delay(1600)
            println("Shutdown initiated.")
            shutdownDeferred.complete(Unit)
            dataChannel.close()
        }

        delay(5000)
        println("Main coroutine completed.")
    }
