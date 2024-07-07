package org.example.mockConstructor.config.constructor.usecase

import org.example.mockConstructor.config.constructor.service.MQService
import org.slf4j.LoggerFactory

class SendUseCase(
    val svc: MQService,
) {
    val logger = LoggerFactory.getLogger(SendUseCase::class.java)

    var flag: Boolean = true

    fun run(msg: String) =
        when (flag) {
            true -> {
                svc.sendMessage((msg))
                logger.info("Send message $msg")
            }
            false -> println("Do nothing...")
        }
}
