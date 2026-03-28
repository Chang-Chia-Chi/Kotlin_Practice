package com.workflow.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

class ScatterPhaseStrategy(
    private val objectMapper: ObjectMapper,
) : PhaseStrategy {

    override fun resolve(context: PhaseContext): AdvancementDecision {
        context.failOrAdvance()?.let { return it }

        val scatterTask = context.tasks.firstOrNull { it.status == TaskStatus.COMPLETED }
            ?: return AdvancementDecision.Abort("No completed scatter task at sequence ${context.currentSeqInfo.sequenceNumber}")
        val scatterResult = scatterTask.resultJson
            ?: return AdvancementDecision.Abort("Scatter task ${scatterTask.id} has no result")

        val items: List<String> = objectMapper.readValue(scatterResult)
        val parallelSeq = context.currentSeqInfo.nextSequence!!
        val parallelSeqInfo = context.sequenceMap[parallelSeq]!!
        val fanOut = parallelSeqInfo.activity.fanOut
            ?: throw IllegalStateException("SCATTER phase at seq ${context.currentSeqInfo.sequenceNumber} points to PARALLEL seqInfo with no fanOut definition")
        val now = Instant.now().truncatedTo(ChronoUnit.MICROS)

        val tasks = items.map { item ->
            Task(
                id = UUID.randomUUID().toString(),
                workflowId = context.workflow.id,
                sequenceNumber = parallelSeq,
                status = TaskStatus.PENDING,
                handlerKey = fanOut.transition,
                item = item,
                resultJson = null,
                claimedBy = null,
                claimedAt = null,
                completedAt = null,
                retryCount = 0,
                maxRetries = fanOut.retries,
                deadlineAt = now.plus(fanOut.deadline),
                backoffBase = fanOut.backoffBase.seconds.toInt(),
                backoffCap = fanOut.backoffCap.seconds.toInt(),
                queueName = fanOut.queue,
            )
        }
        return AdvancementDecision.Advance(parallelSeq, tasks)
    }
}
