package org.example.mockConstructor.config.constructor.client

import org.example.mockConstructor.config.constructor.config.MQConfig

class MQClient(
    val mqConfig: MQConfig,
) {
    val mqContext: MQContext by lazy { this.getContext() }

    fun sendMessage(msg: String) {
        mqContext.sendMessage(msg, mqConfig.event)
    }

    fun getNextMessage() = mqContext.getNextMessage(mqConfig.event)

    private fun getContext(): MQContext = MQContext()
}
