package adapter

import aws.smithy.kotlin.runtime.io.IOException
import io.nats.client.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retryWhen

class NatsAdapter(
    private val conn: Connection,
    private val subject: String,
    private val stream: String,
    private val durable: String,
) {
    private val js: JetStream = conn.jetStream()

    fun subscriptionAsFlow(): Flow<JetStreamSubscription> =
        flow {
            val opts =
                PullSubscribeOptions
                    .builder()
                    .stream(stream)
                    .durable(durable)
                    .build()
            val sub = js.subscribe(subject, opts)
            emit(sub)
        }.retryWhen { cause, _ -> cause is IOException }
}
