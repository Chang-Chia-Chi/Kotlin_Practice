package org.example.mockConstructor.config.constructor.usecase

import org.example.mockConstructor.config.constructor.service.SendMQService

class CronUseCase(
    val svc: SendMQService,
) {
    var flag: Boolean = true

    fun run(msg: String) =
        when (flag) {
            true -> svc.sendMessage((msg))
            false -> println("Do nothing...")
        }
}
