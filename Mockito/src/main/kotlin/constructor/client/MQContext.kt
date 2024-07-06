package org.example.mockConstructor.config.constructor.client

import java.util.*

class MQContext {
    fun sendMessage(msg: String) = println("$msg is send in context")

    fun getNextMessage(event: String) = "$event id: ${UUID.randomUUID()}"
}
