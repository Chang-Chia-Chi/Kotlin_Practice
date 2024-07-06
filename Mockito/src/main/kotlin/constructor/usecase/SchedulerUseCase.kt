package org.example.mockConstructor.config.constructor.usecase

import io.quarkus.scheduler.Scheduler
import jakarta.enterprise.context.ApplicationScoped
import org.example.mockConstructor.config.constructor.model.CronTaskModel
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

@ApplicationScoped
class SchedulerUseCase(
    val scheduler: Scheduler,
) {
    val taskMap: ConcurrentMap<String, CronTaskModel> = ConcurrentHashMap()

    fun register(
        name: String,
        runnable: Runnable,
        crontab: String,
    ): SchedulerUseCase {
        taskMap[name] =
            CronTaskModel(
                crontab = crontab,
                runnable = runnable,
            )
        return this
    }

    fun run() =
        taskMap.forEach { (name, task) ->
            println("Trigger cronjob for event $name")
            scheduler
                .newJob(name)
                .setCron(task.crontab)
                .setTask {
                    task.runnable.run()
                }.schedule()
        }
}
