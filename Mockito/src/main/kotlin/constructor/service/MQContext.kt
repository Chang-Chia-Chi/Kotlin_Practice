package org.example.mockConstructor.config.constructor.service

class MQContext {
    fun sendMessage(msg: String) = println("$msg is send in context")
}
