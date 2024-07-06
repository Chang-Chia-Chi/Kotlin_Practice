package org.example.mockConstructor.config.constructor.client

import org.example.mockConstructor.config.constructor.config.MQConfig
import org.example.mockConstructor.config.constructor.service.MQContext

class MQClient(
    val MQConfig: MQConfig,
) {
    val mqContext: MQContext by lazy { this.getContext(MQConfig) }

    fun sendMessage(msg: String) {
        mqContext.sendMessage(msg)
    }

    fun getContext(MQConfig: MQConfig): MQContext = MQContext()
}
