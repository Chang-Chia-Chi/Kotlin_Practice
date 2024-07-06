package org.example.mockConstructor.config.constructor.service

import org.example.mockConstructor.config.constructor.client.MQClient

class SendMQService(
    override val client: MQClient,
) : MQService(client) {
    fun sendMessage(msg: String) = client.sendMessage(msg)
}
