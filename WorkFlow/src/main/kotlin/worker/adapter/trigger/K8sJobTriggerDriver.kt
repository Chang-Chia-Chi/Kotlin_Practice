package com.workflow.worker.adapter.trigger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.workflow.worker.usecase.port.inbound.trigger.DeferredTaskRef
import com.workflow.worker.usecase.port.inbound.trigger.TriggerDriver
import com.workflow.worker.usecase.port.inbound.trigger.TriggerResult
import com.workflow.worker.usecase.port.inbound.trigger.TriggerTypes
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.api.model.DeletionPropagation
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.Watch
import io.fabric8.kubernetes.client.Watcher
import io.fabric8.kubernetes.client.WatcherException
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Trigger driver that monitors Kubernetes Job completion via the Watch API.
 *
 * Each tracked task gets its own [Watch] on the corresponding K8s Job resource.
 * Watch callbacks enqueue lightweight [WatchEvent] markers only -- no blocking I/O
 * happens inside callbacks. ConfigMap result extraction is deferred to [poll], where
 * it runs on [Dispatchers.IO].
 *
 * Reconnection after transient Watch closure is handled via [closedTaskIds]:
 * when a Watch closes unexpectedly, the task ID is recorded. On the next [start]
 * sweep the driver detects the dead Watch and re-creates it.
 */
/** Deserialized form of [DeferredTaskRef.triggerMeta] for k8s-job triggers. */
data class K8sJobMeta(val jobName: String, val namespace: String)

/** Immutable snapshot of a tracked Job and its Watch handle. */
data class TrackedJob(
    val taskId: String,
    val jobName: String,
    val namespace: String,
    val watch: Watch,
)

@ApplicationScoped
class K8sJobTriggerDriver(
    private val kubernetesClient: KubernetesClient,
    private val objectMapper: ObjectMapper,
) : TriggerDriver {

    private val log = LoggerFactory.getLogger(K8sJobTriggerDriver::class.java)

    /** Lightweight terminal marker enqueued by Watch callbacks. */
    private sealed interface WatchEvent {
        val taskId: String
        val jobName: String
        val namespace: String

        data class Completed(
            override val taskId: String,
            override val jobName: String,
            override val namespace: String,
        ) : WatchEvent

        data class Failed(
            override val taskId: String,
            override val jobName: String,
            override val namespace: String,
            val reason: String,
        ) : WatchEvent
    }

    private val tracked = ConcurrentHashMap<String, TrackedJob>()
    private val closedTaskIds: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val settledTaskIds: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val eventQueue = ConcurrentLinkedQueue<WatchEvent>()

    override fun type(): String = TriggerTypes.K8S_JOB

    /**
     * Diffs [tasks] against [tracked]. Registers new Watches, removes stale entries,
     * and re-registers Watches for tasks whose previous Watch closed unexpectedly.
     */
    override suspend fun start(tasks: List<DeferredTaskRef>) {
        val incomingIds = tasks.map { it.taskId }.toSet()

        // Remove tracked entries no longer in the DEFERRED set
        val staleIds = tracked.keys.filter { it !in incomingIds }
        for (taskId in staleIds) {
            tracked.remove(taskId)?.watch?.close()
            closedTaskIds.remove(taskId)
            settledTaskIds.remove(taskId)
        }

        for (task in tasks) {
            // Skip if already tracked with a healthy Watch
            if (tracked.containsKey(task.taskId) && task.taskId !in closedTaskIds) continue

            // If the Watch was closed (transient failure), tear down the old entry
            if (task.taskId in closedTaskIds) {
                tracked.remove(task.taskId)?.let { old ->
                    try {
                        old.watch.close()
                    } catch (_: Exception) {
                        // best-effort
                    }
                }
                closedTaskIds.remove(task.taskId)
                log.info("Re-registering Watch for task {} after transient close", task.taskId)
            }

            val meta = objectMapper.readValue<K8sJobMeta>(task.triggerMeta)
            val taskId = task.taskId

            val watcher = object : Watcher<Job> {
                override fun eventReceived(action: Watcher.Action, resource: Job) {
                    if (taskId in settledTaskIds) return
                    val conditions = resource.status?.conditions ?: return
                    for (condition in conditions) {
                        if (condition.status != "True") continue
                        when (condition.type) {
                            CONDITION_COMPLETE -> {
                                if (!settledTaskIds.add(taskId)) return
                                eventQueue.add(
                                    WatchEvent.Completed(taskId, meta.jobName, meta.namespace),
                                )
                                return
                            }

                            CONDITION_FAILED -> {
                                if (!settledTaskIds.add(taskId)) return
                                eventQueue.add(
                                    WatchEvent.Failed(
                                        taskId,
                                        meta.jobName,
                                        meta.namespace,
                                        condition.reason ?: "Unknown",
                                    ),
                                )
                                return
                            }
                        }
                    }
                }

                override fun onClose(cause: WatcherException?) {
                    closedTaskIds.add(taskId)
                    if (cause != null) {
                        log.warn(
                            "Watch closed for Job {}/{} (task {}): {}",
                            meta.namespace, meta.jobName, taskId, cause.message,
                        )
                    }
                }
            }

            val watch = kubernetesClient.batch().v1().jobs()
                .inNamespace(meta.namespace)
                .withName(meta.jobName)
                .watch(watcher)

            tracked[taskId] = TrackedJob(taskId, meta.jobName, meta.namespace, watch)
            log.info("Started watching K8s Job {}/{} for task {}", meta.namespace, meta.jobName, taskId)
        }
    }

    /**
     * Drains [eventQueue] and converts each [WatchEvent] to a [TriggerResult].
     *
     * For [WatchEvent.Completed], reads the output ConfigMap on [Dispatchers.IO]
     * (blocking Fabric8 call) to extract the result payload.
     */
    override suspend fun poll(): List<TriggerResult> {
        val events = mutableListOf<WatchEvent>()
        while (true) {
            val e = eventQueue.poll() ?: break
            events.add(e)
        }
        if (events.isEmpty()) return emptyList()

        return withContext(Dispatchers.IO) {
            events.map { event ->
                // Remove from tracked and settled on terminal event
                tracked.remove(event.taskId)?.watch?.close()
                settledTaskIds.remove(event.taskId)

                when (event) {
                    is WatchEvent.Completed -> {
                        val result = readConfigMapOutput(event.jobName, event.namespace)
                        TriggerResult.Succeeded(event.taskId, result)
                    }

                    is WatchEvent.Failed -> {
                        TriggerResult.Failed(event.taskId, event.reason)
                    }
                }
            }
        }
    }

    /**
     * Best-effort cancellation: closes the Watch and deletes the Job
     * with propagationPolicy=Background so dependent pods are cleaned up.
     */
    override suspend fun cancel(taskId: String) {
        val t = tracked.remove(taskId) ?: return
        closedTaskIds.remove(taskId)
        settledTaskIds.remove(taskId)
        t.watch.close()
        try {
            withContext(Dispatchers.IO) {
                kubernetesClient.batch().v1().jobs()
                    .inNamespace(t.namespace)
                    .withName(t.jobName)
                    .withPropagationPolicy(DeletionPropagation.BACKGROUND)
                    .delete()
            }
            log.info("Deleted K8s Job {}/{} for cancelled task {}", t.namespace, t.jobName, taskId)
        } catch (e: Exception) {
            log.warn("Failed to delete K8s Job {}/{} for task {}", t.namespace, t.jobName, taskId, e)
        }
    }

    /** Closes all Watches, clears state. Idempotent. */
    override suspend fun close() {
        for ((taskId, t) in tracked) {
            try {
                t.watch.close()
            } catch (e: Exception) {
                log.warn("Failed to close watch for task {}", taskId, e)
            }
        }
        tracked.clear()
        closedTaskIds.clear()
        settledTaskIds.clear()
        eventQueue.clear()
    }

    /**
     * Reads ConfigMap `{jobName}-output`, key `"result"`.
     * Returns null if the ConfigMap is absent or the read fails.
     *
     * Note: This is a blocking Fabric8 call. Callers must wrap in [Dispatchers.IO].
     */
    internal fun readConfigMapOutput(jobName: String, namespace: String): String? =
        try {
            kubernetesClient.configMaps()
                .inNamespace(namespace)
                .withName("$jobName-output")
                .get()
                ?.data
                ?.get("result")
        } catch (e: Exception) {
            log.warn("Failed to read output ConfigMap for Job {}/{}", namespace, jobName, e)
            null
        }

    /** Test accessor. */
    internal fun trackedCount(): Int = tracked.size

    private companion object {
        const val CONDITION_COMPLETE = "Complete"
        const val CONDITION_FAILED = "Failed"
    }
}
