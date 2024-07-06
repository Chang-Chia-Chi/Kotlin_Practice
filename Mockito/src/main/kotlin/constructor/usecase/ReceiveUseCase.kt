package org.example.mockConstructor.config.constructor.usecase

import org.example.mockConstructor.config.constructor.service.MQService

class ReceiveUseCase(
    val svc: MQService,
) {
    val event = "receive"

    fun run() {
        while (true) {
            val nxtMessage = svc.getNextMessage(event)
            println("Get message $nxtMessage")
            Thread.sleep(10000)
        }
    }
}
