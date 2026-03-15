package com.mapreduce.queue.worker

import com.mapreduce.config.FrameworkConfig
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 * Coroutine-based poll loop with bulkhead-controlled parallelism.
 *
 * Uses two separate scopes to enable graceful shutdown:
 * - [pollScope]: drives the claim loop — cancelled first to stop accepting new work.
 * - [taskScope]: runs in-flight task handlers — drained via semaphore, then force-cancelled on timeout.
 *
 * Total cluster parallelism = pods × bulkhead.
 */
@ApplicationScoped
class WorkerLoop(
    private val config: FrameworkConfig,
    private val dispatcher: TaskDispatcher,
    private val circuitBreaker: PodCircuitBreaker,
) {

    private val log = Logger.getLogger(WorkerLoop::class.java)
    private val pollScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val taskScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private lateinit var semaphore: Semaphore
    private var bulkheadSize = 0

    fun onStart(@Observes ev: StartupEvent) {
        bulkheadSize = config.worker().bulkheadSize()
        semaphore = Semaphore(bulkheadSize)
        val pollInterval = config.worker().pollInterval().toMillis()
        val workerId = config.worker().id()
        val queues = config.worker().queues()

        log.infof("Worker starting: id=%s, bulkhead=%d, poll=%dms, queues=%s",
            workerId, bulkheadSize, pollInterval, queues)

        pollScope.launch {
            while (isActive) {
                if (circuitBreaker.isTripped) {
                    delay(pollInterval)
                    continue
                }

                if (!semaphore.tryAcquire()) {
                    delay(pollInterval)
                    continue
                }

                try {
                    val task = withContext(Dispatchers.IO) { dispatcher.claimTask() }
                    if (task != null) {
                        log.debugf("Claimed task %s [handler=%s, queue=%s]",
                            task.taskId, task.handler, task.queue)
                        taskScope.launch {
                            try {
                                dispatcher.execute(task)
                            } finally {
                                semaphore.release()
                            }
                        }
                    } else {
                        semaphore.release()
                        delay(pollInterval)
                    }
                } catch (e: CancellationException) {
                    semaphore.release()
                    throw e
                } catch (e: Exception) {
                    semaphore.release()
                    log.errorf(e, "Error in worker claim loop")
                    delay(pollInterval)
                }
            }
        }
    }

    fun onStop(@Observes ev: ShutdownEvent) {
        log.info("Worker stopping — halting claim loop")

        // Phase 1: Stop claiming new tasks
        pollScope.cancel()

        // Phase 2: Drain in-flight tasks
        val inFlight = bulkheadSize - semaphore.availablePermits()
        if (inFlight > 0) {
            val timeoutSeconds = config.worker().shutdownTimeout().toSeconds()
            log.infof("Draining %d in-flight task(s) (timeout=%ds)", inFlight, timeoutSeconds)
            val drained = semaphore.tryAcquire(bulkheadSize, timeoutSeconds, TimeUnit.SECONDS)
            if (!drained) {
                val remaining = bulkheadSize - semaphore.availablePermits()
                log.warnf("Shutdown timeout: %d task(s) still running — force-cancelling", remaining)
            }
        }

        // Phase 3: Force-cancel any remaining tasks
        taskScope.cancel()
        log.info("Worker stopped")
    }
}
