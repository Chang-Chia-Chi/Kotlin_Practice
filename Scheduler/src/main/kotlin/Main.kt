package org.example.mockConstructor.config

import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.enterprise.context.ApplicationScoped
import org.example.mockConstructor.config.usecase.ReceiveUseCase
import org.example.mockConstructor.config.usecase.SchedulerUseCase
import org.example.mockConstructor.config.usecase.SendUseCase

@QuarkusMain
@ApplicationScoped
class Main(
    val schedulerUC: SchedulerUseCase,
    val sendUC: SendUseCase,
    val recvUC: ReceiveUseCase,
) : QuarkusApplication {
    override fun run(vararg args: String?): Int {
        println("Start cronjobs...")
        schedulerUC
            .register(
                "Send message",
                crontab = "0/5 * * * * ?",
                runnable =
                    {
                        sendUC.run()
                    },
            ).run()

        println("Start receive infinite loop...")
        recvUC.run()
        return 0
    }
}
