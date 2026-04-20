package jms

import com.river.core.ObjectPool
import com.river.core.flattenIterable
import com.river.core.indefinitelyRepeat
import com.river.core.objectPool
import com.river.core.unorderedMapAsync
import infra.coroutine.pauseWhile
import infra.coroutine.takeUntilSignal
import infra.fault.circuitbreakr.BreakerWatcherFlow
import jakarta.jms.ConnectionFactory
import jakarta.jms.JMSContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.invoke
import kotlinx.coroutines.withContext
import kotlin.time.Duration.Companion.seconds

suspend fun listen(connectionFactory: ConnectionFactory): Flow<CommittableMessage> =
    coroutineScope {
        val IO: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(1)

        suspend fun newContext(): JMSContext = withContext(IO) { connectionFactory.createContextSafely() }
        val signal = CompletableDeferred<Unit>()
        val concurrency = 1
        val pollingMaxWait = 1.seconds

        val contextPool = JmsPool()
        val watcher = BreakerWatcherFlow()

        coroutineScope {
            indefinitelyRepeat(contextPool)
                .takeUntilSignal(signal)
                .pauseWhile(watcher.paused)
                .unorderedMapAsync(concurrency) { pool ->
                    IO {
                        pool.withManagedConsumer { consumer ->
                            consumer
                                .receive(pollingMaxWait.inWholeMilliseconds)
                                ?.let { message ->
                                    CommittableMessage(message) {
                                        IO { message.acknowledge() }
                                    }
                                }?.let { listOf(it) } ?: emptyList()
                        }
                    }
                }.flattenIterable()
                .onCompletion { contextPool.close() }
        }

//        indefinitelyRepeat(contextPool)
//            .takeUntilSignal(signal)
//            .unorderedMapAsync(concurrency) { pool ->
//                pool
//                    .withManagedConsumer { consumer ->
//                        IO {
//                            consumer
//                                .receive(pollingMaxWait.inWholeMilliseconds)
//                                ?.let { message ->
//                                    CommittableMessage(message) {
//                                        IO { message.acknowledge() }
//                                    }
//                                }?.let { listOf(it) } ?: emptyList()
//                        }
//                    }.flattenIterable()
//                    .onCompletion { contextPool.close() }
//            }
    }
