package org.example.mockConstructor.config.constructor.client

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

class MQContext {
    val logger = LoggerFactory.getLogger(MQContext::class.java)

    private val queueMap: Map<String, ConcurrentLinkedQueue<String>> =
        mapOf(
            "event" to ConcurrentLinkedQueue<String>(),
        )

    fun sendMessage(
        msg: String,
        event: String,
    ) = when (fetchQueue(event).add(msg)) {
        true -> logger.info("MQContext send message $msg to event $event successful")
        false -> throw Exception("MQContext send msg $msg to event $event failed")
    }

    fun getNextMessage(event: String): String {
        while (true) {
            fetchQueue(event)
                .poll()
                ?: Thread.sleep(100)
        }
    }

    private fun fetchQueue(event: String): ConcurrentLinkedQueue<String> =
        queueMap[event]
            ?: throw Exception("Event $event is not existed...")
}
