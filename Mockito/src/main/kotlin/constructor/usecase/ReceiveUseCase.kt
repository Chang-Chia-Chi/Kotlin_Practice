package org.example.mockConstructor.config.constructor.usecase

import org.example.mockConstructor.config.constructor.service.MQService
import org.slf4j.LoggerFactory

class ReceiveUseCase(
    val svc: MQService,
) {
    val logger = LoggerFactory.getLogger(ReceiveUseCase::class.java)

    val event = "receive"

    fun run() {
        while (true) {
            val nxtMessage = svc.getNextMessage(event)
            logger.info("Receive message $nxtMessage")
            Thread.sleep(10000)
        }
    }
}
