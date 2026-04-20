package jms

import aws.sdk.kotlin.services.s3.model.SessionMode
import com.river.core.indefinitelyRepeat
import com.river.core.objectPool
import infra.coroutine.unorderedMapAsync
import jakarta.jms.ConnectionFactory
import jakarta.jms.JMSContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runInterruptible
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

suspend fun ConnectionFactory.createContextSafely(): JMSContext {
    var context: JMSContext? = null
    try {
        context =
            runInterruptible {
                createContext()
            }
        return context
    } catch (e: CancellationException) {
        context?.let { runCatching { it.close() } }
        throw e
    }
}

fun ConnectionFactory.consume(
    queueName: String,
    credentials: Credentials? = null,
    sessionMode: SessionMode = SessionMode.CLIENT_ACKNOWLEDGE,
    pollingMaxWait: Duration = 10.seconds,
    concurrency: Int = 1,
): Flow<CommittableMessage> {
    val IO: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(concurrency)

    suspend fun newContext(): JMSContext = IO { newBlockingContext(sessionMode, credentials) }

    return flow {
        val queue = newContext().use { it.createQueue(queueName) }

        val contextPool =
            objectPool(
                maxSize = concurrency,
                onClose = { (context, consumer) ->
                    IO {
                        consumer.close()
                        context.close()
                    }
                },
                factory = { IO { newContext().let { it to it.createConsumer(queue) } } },
            )

        emitAll(
            indefinitelyRepeat(contextPool)
                .unorderedMapAsync(concurrency) {
                    val instance = it.borrow()
                    val (_, consumer) = instance.instance

                    IO {
                        consumer
                            .receive(pollingMaxWait.inWholeMilliseconds)
                            ?.let { message ->
                                CommittableMessage(message) {
                                    IO { message.acknowledge() }
                                    it.release(instance)
                                }
                            }?.let { listOf(it) } ?: emptyList()
                    }
                }.flattenIterable()
                .onCompletion { contextPool.close() },
        )
    }
}
