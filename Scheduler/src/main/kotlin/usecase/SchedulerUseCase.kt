package org.example.mockConstructor.config.usecase

import io.quarkus.scheduler.Scheduler
import org.example.mockConstructor.config.model.CronTaskModel
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

class SchedulerUseCase(
    val scheduler: Scheduler,
) {
    val logger = LoggerFactory.getLogger(SchedulerUseCase::class.java)

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

    fun run() {
        taskMap.forEach { (name, task) ->
            println("Trigger cronjob for event $name")
            scheduler
                .newJob(name)
                .setCron(task.crontab)
                .setTask {
                    task.runnable.run()
                }.schedule()
        }
        logger.info("Jobs scheduled ${scheduler.getScheduledJobs()}")
    }
}
