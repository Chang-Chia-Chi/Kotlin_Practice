package adapter

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.Message
import io.nats.client.PullSubscribeOptions
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import java.time.Duration as JavaDuration

class NatsAdapter(
    private val conn: Connection,
    private val subject: String,
    private val stream: String,
    private val durable: String,
    private val batch: Int = 1,
    private val fetchExpire: Duration = 200.milliseconds,
    private val idleBackoff: Duration = 25.milliseconds,
) {
    fun consumeAsFlow(): Flow<Message> =
        flow {
            val js: JetStream = conn.jetStream()
            val opts =
                PullSubscribeOptions
                    .builder()
                    .stream(stream)
                    .durable(durable)
                    .build()
            val sub = js.subscribe(subject, opts)

            try {
                while (currentCoroutineContext().isActive) {
                    val msgs: List<Message> =
                        withContext(Dispatchers.IO.limitedParallelism(4)) {
                            sub.fetch(batch, JavaDuration.ofNanos(fetchExpire.inWholeNanoseconds))
                        }
                    if (msgs.isEmpty()) {
                        delay(idleBackoff)
                    } else {
                        for (m in msgs) {
                            emit(m)
                        }
                    }
                }
            } finally {
                try {
                    sub.drain(JavaDuration.ofSeconds(1))
                } catch (_: Exception) {
                }
                try {
                    sub.unsubscribe()
                } catch (_: Exception) {
                }
            }
        }
}
