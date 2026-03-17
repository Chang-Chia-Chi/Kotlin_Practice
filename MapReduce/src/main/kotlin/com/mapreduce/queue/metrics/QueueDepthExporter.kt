package com.mapreduce.queue.metrics

import com.mapreduce.leader.NotLeader
import com.mapreduce.queue.repository.TaskRepository
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Leader-only scheduled task that publishes per-queue depth gauges for HPA autoscaling.
 *
 * This is a generic queue-layer concern — it knows nothing about MapReduce
 * or any specific handler type.
 */
@ApplicationScoped
class QueueDepthExporter(
    private val taskRepository: TaskRepository,
    private val meterRegistry: MeterRegistry,
) {

    private val log = Logger.getLogger(QueueDepthExporter::class.java)
    private val queueDepths = ConcurrentHashMap<String, AtomicLong>()

    @Scheduled(
        every = "{taskqueue.metrics.queue-depth-interval}",
        delayed = "{taskqueue.metrics.queue-depth-interval}",
        concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
        skipExecutionIf = NotLeader::class,
    )
    fun pollQueueDepth() {
        val counts = taskRepository.countPendingByQueue()
        for ((queue, count) in counts) {
            queueDepths.computeIfAbsent(queue) { q ->
                AtomicLong(0).also { gauge ->
                    meterRegistry.gauge(
                        "framework.queue.depth",
                        listOf(Tag.of("queue_name", q)),
                        gauge,
                    ) { it.toDouble() }
                }
            }.set(count.toLong())
        }
        for ((queue, depth) in queueDepths) {
            if (queue !in counts) depth.set(0)
        }
    }
}
