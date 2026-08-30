package etlhost

import infra.etl.task.TaskEvent
import infra.etl.task.TaskRunListener
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch

/**
 * A second [TaskRunListener] bean, which is the point: the host composes every one it finds, so a
 * test can observe real runs without replacing the host's own logging.
 *
 * It records the **thread each step actually ran on**, which is what SimpleEtl spec 8.6's hand-off
 * row is about. Everything else here is a latch.
 */
@Singleton
class RecordingListener : TaskRunListener {

    val stepThreads = CopyOnWriteArrayList<Pair<String, String>>()
    val events = CopyOnWriteArrayList<String>()
    private val ended = ConcurrentHashMap<String, CountDownLatch>()

    /**
     * Set by [holdNextRunOf] to park a run on its own dispatcher.
     *
     * A listener that blocks is exactly what spec 9.2 forbids - "a listener that waits parks an ETL
     * run behind it" - which is why it is the cheapest way to *make* a run be in flight on purpose.
     * Nothing in production does this; the seam's documented hazard is the test's lever.
     */
    @Volatile
    private var hold: Pair<String, CountDownLatch>? = null

    override fun on(event: TaskEvent) {
        events += "${event.javaClass.simpleName}:${event.task.taskName}"
        when (event) {
            is TaskEvent.StepStart -> {
                stepThreads += event.step.step to Thread.currentThread().name
                hold?.takeIf { it.first == event.task.taskName }?.let {
                    hold = null
                    it.second.await(30, java.util.concurrent.TimeUnit.SECONDS)
                }
            }
            is TaskEvent.TaskEnd -> latch(event.task.taskName).countDown()
            else -> Unit
        }
    }

    /** Returns the release: the next run of [task] parks at its first step until it is counted down. */
    fun holdNextRunOf(task: String): CountDownLatch =
        CountDownLatch(1).also { hold = task to it }

    fun latch(task: String): CountDownLatch = ended.computeIfAbsent(task) { CountDownLatch(1) }

    fun clear() {
        stepThreads.clear()
        events.clear()
        ended.clear()
    }
}
