package org.example.mockConstructor.config

import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.enterprise.context.ApplicationScoped
import org.example.mockConstructor.config.constructor.usecase.ReceiveUseCase
import org.example.mockConstructor.config.constructor.usecase.SchedulerUseCase
import org.example.mockConstructor.config.constructor.usecase.SendUseCase

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
                crontab = "0/10 * * * * ?",
                runnable =
                    {
                        val msg = "Message at time ${System.currentTimeMillis()}"
                        sendUC.run(msg)
                    },
            ).run()
        println("Start cronjobs success...")

        println("Start receive infinite loop...")
        Thread {
            recvUC.run()
        }.start()
        println("Start receive infinite loop success...")
        return 0
    }
}

fun main(args: Array<String>) {
    Quarkus.run(Main::class.java, *args)
}
