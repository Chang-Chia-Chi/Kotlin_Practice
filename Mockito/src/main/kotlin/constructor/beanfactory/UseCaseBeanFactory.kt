package org.example.mockConstructor.config.constructor.beanfactory

import io.quarkus.scheduler.Scheduler
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.example.mockConstructor.config.constructor.client.MQClient
import org.example.mockConstructor.config.constructor.config.ReceiveMQConfig
import org.example.mockConstructor.config.constructor.config.SendMQConfig
import org.example.mockConstructor.config.constructor.service.MQService
import org.example.mockConstructor.config.constructor.usecase.ReceiveUseCase
import org.example.mockConstructor.config.constructor.usecase.SchedulerUseCase
import org.example.mockConstructor.config.constructor.usecase.SendUseCase

@ApplicationScoped
class UseCaseBeanFactory(
    val scheduler: Scheduler,
) {
    @Produces
    fun getSchedulerUseCase(): SchedulerUseCase = SchedulerUseCase(scheduler)

    @Produces
    fun getSendUseCase(): SendUseCase =
        SendUseCase(
            MQService(
                MQClient(
                    SendMQConfig(),
                ),
            ),
        )

    @Produces
    fun getReceiveUseCase(): ReceiveUseCase =
        ReceiveUseCase(
            MQService(
                MQClient(
                    ReceiveMQConfig(),
                ),
            ),
        )
}
