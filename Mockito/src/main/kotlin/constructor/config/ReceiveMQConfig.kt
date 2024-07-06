package org.example.mockConstructor.config.constructor.config

import org.eclipse.microprofile.config.inject.ConfigProperty

class ReceiveMQConfig : MQConfig {
    @ConfigProperty(name = "config.url")
    override lateinit var url: String

    @ConfigProperty(name = "config.pwd")
    override lateinit var pwd: String
}
