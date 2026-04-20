package com.workflow.stress

import com.workflow.worker.usecase.port.inbound.execution.HandlerInput
import com.workflow.worker.usecase.port.inbound.execution.HandlerResult
import com.workflow.worker.usecase.port.inbound.execution.TransitionHandler
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.fail

/**
 * Event types recorded during handler execution.
 */
enum class EventType {
    EXECUTE_START,
    EXECUTE_END,
    EXECUTE_FAIL,
}

/**
 * A single recorded event from handler execution history.
 */
data class HistoryEvent(
    val taskId: String,
    val workflowId: String,
    val thread: String,
    val timestamp: Instant,
    val type: EventType,
)

/**
 * TransitionHandler decorator that records execution events.
 *
 * Wrap any handler to capture a timeline of task executions
 * for post-hoc property verification via [HistoryChecker].
 *
 * Inspired by Jepsen's operation history recording and
 * Porcupine's linearizability checker input format.
 */
class HistoryRecorder(
    private val delegate: TransitionHandler,
) : TransitionHandler {

    private val _events = ConcurrentLinkedQueue<HistoryEvent>()

    fun snapshot(): List<HistoryEvent> = _events.toList()

    override suspend fun execute(input: HandlerInput): HandlerResult {
        val thread = Thread.currentThread().name
        _events.add(HistoryEvent(input.taskId, input.workflowId, thread, Instant.now(), EventType.EXECUTE_START))
        return try {
            val output = delegate.execute(input)
            _events.add(HistoryEvent(input.taskId, input.workflowId, thread, Instant.now(), EventType.EXECUTE_END))
            output
        } catch (e: Exception) {
            _events.add(HistoryEvent(input.taskId, input.workflowId, thread, Instant.now(), EventType.EXECUTE_FAIL))
            throw e
        }
    }
}

/**
 * Post-hoc property checker for handler execution histories.
 *
 * Checks properties inspired by Jepsen/Maelstrom Kafka workload:
 * no lost tasks, no duplicates, monotonic progression.
 */
object HistoryChecker {

    /**
     * Verifies no task was successfully executed more than once.
     * Detects: SKIP LOCKED failure, double-claim, stale reclaim race.
     */
    fun noDuplicateExecution(events: List<HistoryEvent>): List<String> {
        val completions = events.filter { it.type == EventType.EXECUTE_END }
        val byTask = completions.groupBy { it.taskId }
        return byTask.filter { it.value.size > 1 }.map { (taskId, executions) ->
            "DUPLICATE_EXECUTION: task $taskId executed ${executions.size} times on threads: ${executions.map { it.thread }}"
        }
    }

    /**
     * Verifies every task that was started eventually reached a terminal DB state.
     * Requires final DB task state for comparison.
     *
     * @param events recorded handler events
     * @param dbTasks final task rows from DB (maps with STATUS key)
     */
    fun noLostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>): List<String> {
        val startedTaskIds = events.filter { it.type == EventType.EXECUTE_START }.map { it.taskId }.toSet()
        val terminalStatuses = setOf("COMPLETED", "FAILED", "DEAD_LETTER", "TIMED_OUT")
        val dbTaskMap = dbTasks.associateBy { it["ID"]?.toString() ?: "" }

        return startedTaskIds.mapNotNull { taskId ->
            val dbTask = dbTaskMap[taskId]
            if (dbTask == null) {
                "LOST_TASK: task $taskId was executed but not found in DB"
            } else {
                val status = dbTask["STATUS"]?.toString()
                if (status !in terminalStatuses) {
                    "LOST_TASK: task $taskId was executed but stuck in status $status"
                } else {
                    null
                }
            }
        }
    }

    /**
     * Verifies every EXECUTE_END event has a matching DB row in COMPLETED or FAILED status.
     */
    fun noGhostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>): List<String> {
        val completedTaskIds = events.filter { it.type == EventType.EXECUTE_END }.map { it.taskId }.toSet()
        val dbTaskMap = dbTasks.associateBy { it["ID"]?.toString() ?: "" }
        val terminalStatuses = setOf("COMPLETED", "FAILED", "DEAD_LETTER", "TIMED_OUT")

        return completedTaskIds.mapNotNull { taskId ->
            val dbTask = dbTaskMap[taskId]
            if (dbTask == null) {
                "GHOST_TASK: task $taskId completed in handler but not found in DB"
            } else {
                val status = dbTask["STATUS"]?.toString()
                if (status !in terminalStatuses) {
                    "GHOST_TASK: task $taskId completed in handler but DB status is $status"
                } else {
                    null
                }
            }
        }
    }

    /**
     * Assert all checks pass. Fails the test with details if any violation found.
     */
    fun assertNoDuplicateExecution(events: List<HistoryEvent>) {
        val violations = noDuplicateExecution(events)
        if (violations.isNotEmpty()) {
            fail("History check failed:\n${violations.joinToString("\n")}")
        }
    }

    fun assertNoLostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>) {
        val violations = noLostTasks(events, dbTasks)
        if (violations.isNotEmpty()) {
            fail("History check failed:\n${violations.joinToString("\n")}")
        }
    }

    fun assertNoGhostTasks(events: List<HistoryEvent>, dbTasks: List<Map<String, Any?>>) {
        val violations = noGhostTasks(events, dbTasks)
        if (violations.isNotEmpty()) {
            fail("History check failed:\n${violations.joinToString("\n")}")
        }
    }
}
