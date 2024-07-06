package org.example.mockConstructor.config.constructor

import jakarta.enterprise.context.ApplicationScoped
import org.example.mockConstructor.config.constructor.config.MyConfig

@ApplicationScoped
class TestService(
    override val config: MyConfig,
) : Service(config) {
    fun sendMessage(msg: String) = TestClient(config).sendMessage(msg)
}
