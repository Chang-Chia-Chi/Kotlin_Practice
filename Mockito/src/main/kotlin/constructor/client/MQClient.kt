package org.example.mockConstructor.config.constructor.client

import org.example.mockConstructor.config.constructor.config.Config
import org.example.mockConstructor.config.constructor.service.MQContext

class MQClient(
    val config: Config,
) {
    val mqContext: MQContext by lazy { this.getContext(config) }

    fun sendMessage(msg: String) {
        mqContext.sendMessage(msg)
    }

    fun getContext(config: Config): MQContext = MQContext()
}
