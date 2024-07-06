package org.example.mockConstructor.config.constructor.service

import jakarta.enterprise.context.ApplicationScoped
import org.example.mockConstructor.config.constructor.client.MQClient
import org.example.mockConstructor.config.constructor.config.MyConfig

@ApplicationScoped
class SendMQService(
    override val config: MyConfig,
) : MQService(config) {
    fun sendMessage(msg: String) = MQClient(config).sendMessage(msg)
}
