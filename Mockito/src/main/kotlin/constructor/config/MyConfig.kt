package org.example.mockConstructor.config.constructor.config

import org.eclipse.microprofile.config.inject.ConfigProperty

class MyConfig : Config {
    @ConfigProperty(name = "config.url")
    override lateinit var url: String

    @ConfigProperty(name = "config.pwd")
    override lateinit var pwd: String
}
