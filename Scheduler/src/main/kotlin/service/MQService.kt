package org.example.mockConstructor.config.service

import org.example.mockConstructor.config.client.MQClient

class MQService(
    val client: MQClient,
) {
    fun sendMessage(msg: String) = client.sendMessage(msg)

    fun getNextMessage() = client.getNextMessage()
}
