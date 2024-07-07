package org.example.mockConstructor.config.constructor.usecase

import org.example.mockConstructor.config.constructor.service.MQService
import org.slf4j.LoggerFactory
import java.util.*

class SendUseCase(
    val svc: MQService,
) {
    val logger = LoggerFactory.getLogger(SendUseCase::class.java)
    var flag: Boolean = true

    fun run() =
        when (flag) {
            true -> {
                val message = "Message at time ${System.currentTimeMillis()}"
                svc.sendMessage(message)
                logger.info("Send message $message")
            }
            false -> logger.info("Flag close, do nothing...")
        }
}
