package com.mapreduce.mr.handler

import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.model.evaluateFailurePolicy
import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.GroupStatus.ACTIVE
import com.mapreduce.queue.model.GroupStatus.COMPLETED
import com.mapreduce.queue.model.GroupStatus.FAILED
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.repository.TaskGroupRepository
import com.mapreduce.queue.spi.TaskHandler
import org.jboss.logging.Logger

/**
 * Callback handler that fires when a phase's barrier is reached.
 * Registered as `"{jobType}.__phase_complete"` by [com.mapreduce.mr.registry.MapReduceRegistrar].
 *
 * This replaces the polling-based phase transition logic that was in the old orchestrator.
 * The callback task is created atomically with barrier detection (same TX), so transitions
 * cannot be lost.
 */
class PhaseTransitionHandler(
    private val jobType: String,
    private val taskGroupRepository: TaskGroupRepository,
    private val maxRetries: Int,
    private val queue: String,
    private val totalPartitions: Int,
) : TaskHandler {

    private val log = Logger.getLogger(PhaseTransitionHandler::class.java)

    override val handlerName: String = "$jobType.__phase_complete"

    override suspend fun handle(ctx: TaskContext): TaskResult {
        val groupId = ctx.payload
        val group = taskGroupRepository.findGroup(groupId)
            ?: return TaskResult.Failure("Group $groupId not found")

        return when (group.phase) {
            "map" -> handleMapPhaseComplete(group)
            "reduce" -> handleReducePhaseComplete(group)
            else -> TaskResult.Failure("Unknown phase: ${group.phase}")
        }
    }

    private fun handleMapPhaseComplete(group: TaskGroup): TaskResult {
        val failureReason = evaluateFailurePolicy(
            FailurePolicy.valueOf(group.failurePolicy), group.tasksFailed,
            group.phaseTotal, group.failureThreshold,
        )
        if (failureReason != null) {
            val transitioned = taskGroupRepository.casGroupStatus(group.groupId, ACTIVE, FAILED, group.version)
            if (transitioned) {
                log.warnf("Group %s failed during map phase: %s", group.groupId, failureReason)
            }
            return TaskResult.Success()
        }

        val reduceTasks = (0 until totalPartitions).map { partition ->
            EnqueueRequest(
                handler = "$jobType.reduce",
                payload = "{}",
                queue = queue,
                groupId = group.groupId,
                metadata = """{"phase":"REDUCE","partition_hash":$partition}""",
                maxRetries = maxRetries,
            )
        }

        val transitioned = taskGroupRepository.transitionPhase(
            groupId = group.groupId,
            expectedVersion = group.version,
            newPhase = "reduce",
            newPhaseTotal = reduceTasks.size,
            tasks = reduceTasks,
            onCompleteHandler = handlerName,
        )
        if (transitioned) {
            log.infof("Group %s transitioned to reduce phase (%d partitions)", group.groupId, totalPartitions)
        }

        return TaskResult.Success()
    }

    private fun handleReducePhaseComplete(group: TaskGroup): TaskResult {
        val transitioned = taskGroupRepository.casGroupStatus(
            group.groupId, ACTIVE, COMPLETED, group.version,
        )
        if (transitioned) {
            log.infof("Group %s completed", group.groupId)
        }
        return TaskResult.Success()
    }
}
