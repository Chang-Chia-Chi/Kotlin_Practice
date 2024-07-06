package org.example.mockConstructor.config.constructor.service

import org.example.mockConstructor.config.constructor.client.MQClient

open class MQService(
    open val client: MQClient,
) {
    fun sendMessage(msg: String) = client.sendMessage(msg)

    fun getNextMessage(event: String) = client.getNextMessage(event)
}
