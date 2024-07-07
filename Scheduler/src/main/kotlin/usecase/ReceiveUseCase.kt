package org.example.mockConstructor.config.usecase

import org.example.mockConstructor.config.service.MQService
import org.slf4j.LoggerFactory

class ReceiveUseCase(
    val svc: MQService,
) {
    val logger = LoggerFactory.getLogger(ReceiveUseCase::class.java)

    fun run() {
        while (true) {
            val nxtMessage = svc.getNextMessage()
            logger.info("Receive message $nxtMessage")
            Thread.sleep(10000)
        }
    }
}
