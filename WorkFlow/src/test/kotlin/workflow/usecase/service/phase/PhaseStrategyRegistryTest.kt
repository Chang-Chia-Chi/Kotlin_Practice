package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.PhaseContext
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.port.inbound.phase.PhaseStrategy
import com.workflow.workflow.usecase.service.phase.LinearPhaseStrategy
import com.workflow.workflow.usecase.service.phase.ParallelPhaseStrategy
import com.workflow.workflow.usecase.service.phase.PhaseStrategyRegistry
import kotlin.test.Test
import kotlin.test.assertIs

class PhaseStrategyRegistryTest {

    private val registry = PhaseStrategyRegistry()

    @Test
    fun `resolve returns LinearPhaseStrategy for LINEAR`() {
        assertIs<LinearPhaseStrategy>(registry.resolve(PhaseType.LINEAR))
    }

    @Test
    fun `resolve returns ParallelPhaseStrategy for PARALLEL`() {
        assertIs<ParallelPhaseStrategy>(registry.resolve(PhaseType.PARALLEL))
    }

    @Test
    fun `all known phase types resolve without error`() {
        PhaseType.entries.forEach { type ->
            registry.resolve(type) // should not throw
        }
    }

    @Test
    fun `register overrides existing strategy`() {
        val custom = object : PhaseStrategy {
            override fun resolve(context: PhaseContext): AdvancementDecision = AdvancementDecision.Complete
        }
        registry.register(PhaseType.LINEAR, custom)
        val resolved = registry.resolve(PhaseType.LINEAR)
        assertIs<AdvancementDecision.Complete>(resolved.resolve(
            PhaseContext(
                workflow = WorkflowRun("w", "{}", 1, 0, WorkflowStatus.RUNNING,
                    java.time.Instant.now(), java.time.Instant.now(), java.time.Instant.now()),
                definition = WorkflowDefinition(
                    activities = listOf(ActivityDefinition(name = "a", transition = "a.h")),
                ),
                currentSeqInfo = SequenceInfo(1, 0,
                    ActivityDefinition(name = "a", transition = "a.h"),
                    PhaseType.LINEAR, null),
                sequenceMap = emptyMap(),
                failedCount = 0,
                totalCount = 0,
            ),
        ))
    }
}
