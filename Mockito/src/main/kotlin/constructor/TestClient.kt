package org.example.mockConstructor.config.constructor

import org.example.mockConstructor.config.constructor.config.Config

class TestClient(
    val config: Config,
) {
    val testContext: TestContext by lazy { this.getContext(config) }

    fun sendMessage(msg: String) {
        testContext.sendMessage(msg)
    }

    fun getContext(config: Config): TestContext = TestContext()
}
