package org.example.mockConstructor.config.client

import org.example.mockConstructor.config.config.MQConfig

class MQClient(
    val mqConfig: MQConfig,
    val mqContext: MQContext,
) {
    fun sendMessage(msg: String) {
        mqContext.sendMessage(msg, mqConfig.event)
    }

    fun getNextMessage() = mqContext.getNextMessage(mqConfig.event)
}
