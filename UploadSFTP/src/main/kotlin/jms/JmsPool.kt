package jms

import com.river.core.ObjectPool
import jakarta.jms.Connection
import jakarta.jms.JMSConsumer
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlin.time.Duration

class JmsPool(
    private val size: Int,
    private val maxDuration: Duration,
    initial: List<JmsListenerContextManager>,
    val factory: suspend () -> JmsListenerContextManager,
    val onClose: suspend (JmsListenerContextManager) -> Unit,
) : ObjectPool<JmsListenerContextManager> {
    private val lock = Mutex()
    private var created = 0
    private val borrowed: MutableSet<ObjectPool.ObjectHolder<JmsListenerContextManager>> = mutableSetOf()
    private val channel = Channel<ObjectPool.ObjectHolder<JmsListenerContextManager>>(size)

    init {
        initial.forEach { channel.trySend(ObjectPool.ObjectHolder(it, maxDuration)) }
        created = initial.size
    }

    suspend fun <T> withManagedConsumer(block: suspend (JMSConsumer) -> T): T {
        var holder: ObjectPool.ObjectHolder<JmsListenerContextManager>? = null
        try {
            holder = borrow()
            val manager = holder.instance
            val context = manager.getContext()
            return block(context.consumer)
        } finally {
            holder?.let { withContext(NonCancellable) { release(it) } }
        }
    }

    override suspend fun borrow(): ObjectPool.ObjectHolder<JmsListenerContextManager> {
        val instance = channel.tryReceive().getOrNull()

        val obj =
            when {
                instance != null -> {
                    instance
                }

                created >= size -> {
                    channel.receive()
                }

                else ->
                    lock.withLock {
                        created++
                        new()
                    }
            }

        return lock.withLock {
            (
                if (obj.shouldBeClosed()) {
                    onClose(obj.instance)
                    created--
                    new().also { created++ }
                } else {
                    obj
                }
            ).also { borrowed.add(it) }
        }
    }

    override suspend fun close(): Unit =
        lock.withLock {
            ((1..created).mapNotNull { channel.tryReceive().getOrNull() } + borrowed)
                .forEach {
                    onClose(it.instance)
                    created--
                }

            channel.close()
        }

    override suspend fun release(holder: ObjectPool.ObjectHolder<JmsListenerContextManager>) {
        lock.withLock { borrowed.remove(holder) }
        channel.send(holder)
    }

    private suspend fun new(): ObjectPool.ObjectHolder<JmsListenerContextManager> = ObjectPool.ObjectHolder(factory(), maxDuration)
}
