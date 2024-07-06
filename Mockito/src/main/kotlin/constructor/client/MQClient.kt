package org.example.mockConstructor.config.constructor.client

import org.example.mockConstructor.config.constructor.config.MQConfig

class MQClient(
    val mqConfig: MQConfig,
) {
    val mqContext: MQContext by lazy { this.getContext(mqConfig) }

    fun sendMessage(msg: String) {
        mqContext.sendMessage(msg)
    }

    fun getNextMessage(event: String) = mqContext.getNextMessage(event)

    private fun getContext(mqConfig: MQConfig): MQContext = MQContext()
}
