package org.example.mockConstructor.config.beanfactory

import io.quarkus.scheduler.Scheduler
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.example.mockConstructor.config.client.MQClient
import org.example.mockConstructor.config.config.MQConfig
import org.example.mockConstructor.config.service.MQService
import org.example.mockConstructor.config.usecase.ReceiveUseCase
import org.example.mockConstructor.config.usecase.SchedulerUseCase
import org.example.mockConstructor.config.usecase.SendUseCase

@ApplicationScoped
class UseCaseBeanFactory(
    val scheduler: Scheduler,
) {
    @Produces
    fun getSchedulerUseCase(): SchedulerUseCase = SchedulerUseCase(scheduler)

    @Produces
    fun getSendUseCase(
        @ConfigProperty(name = "config.url") url: String,
        @ConfigProperty(name = "config.pwd") pwd: String,
        @ConfigProperty(name = "config.event") event: String,
    ): SendUseCase =
        SendUseCase(
            MQService(
                MQClient(
                    MQConfig(
                        url = url,
                        pwd = pwd,
                        event = event,
                    ),
                ),
            ),
        )

    @Produces
    fun getReceiveUseCase(
        @ConfigProperty(name = "config.url") url: String,
        @ConfigProperty(name = "config.pwd") pwd: String,
        @ConfigProperty(name = "config.event") event: String,
    ): ReceiveUseCase =
        ReceiveUseCase(
            MQService(
                MQClient(
                    MQConfig(
                        url = url,
                        pwd = pwd,
                        event = event,
                    ),
                ),
            ),
        )
}
