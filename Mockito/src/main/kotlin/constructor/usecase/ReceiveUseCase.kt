package org.example.mockConstructor.config.constructor.usecase

import org.example.mockConstructor.config.constructor.service.ReceiveMQService

class ReceiveUseCase(
    val svc: ReceiveMQService,
) {
    val event = "receive"

    fun run() {
        while (true) {
            val nxtMessage = svc.getNextMessage(event)
            println("Get message $nxtMessage")
            Thread.sleep(1000)
        }
    }
}
