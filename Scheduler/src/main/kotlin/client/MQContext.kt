package org.example.mockConstructor.config.client

import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.LinkedBlockingQueue

@ApplicationScoped
class MQContext {
    private val queueMap: Map<String, LinkedBlockingQueue<String>> =
        mapOf(
            "event" to LinkedBlockingQueue<String>(),
        )

    fun sendMessage(
        msg: String,
        event: String,
    ) = fetchQueue(event).put(msg)

    fun getNextMessage(event: String): String = fetchQueue(event).take()

    private fun fetchQueue(event: String): LinkedBlockingQueue<String> =
        queueMap[event]
            ?: throw Exception("Event $event is not existed...")
}
