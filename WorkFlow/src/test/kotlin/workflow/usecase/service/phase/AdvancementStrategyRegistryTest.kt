package com.workflow.workflow.usecase.service.phase

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.AdvancementDecision
import com.workflow.workflow.model.PhaseContext
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.WorkflowRun
import com.workflow.workflow.model.WorkflowStatus
import com.workflow.workflow.usecase.port.inbound.phase.AdvancementStrategy
import com.workflow.workflow.usecase.service.phase.LinearAdvancementStrategy
import com.workflow.workflow.usecase.service.phase.ParallelAdvancementStrategy
import com.workflow.workflow.usecase.service.phase.AdvancementStrategyRegistry
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertIs

class AdvancementStrategyRegistryTest {

    private val registry = AdvancementStrategyRegistry()

    @Test
    fun `resolve returns LinearAdvancementStrategy for LINEAR`() {
        assertIs<LinearAdvancementStrategy>(registry.resolve(PhaseType.LINEAR))
    }

    @Test
    fun `resolve returns ParallelAdvancementStrategy for PARALLEL`() {
        assertIs<ParallelAdvancementStrategy>(registry.resolve(PhaseType.PARALLEL))
    }

    @Test
    fun `LINEAR and PARALLEL resolve without error`() {
        registry.resolve(PhaseType.LINEAR)
        registry.resolve(PhaseType.PARALLEL)
    }

    @Test
    fun `SCATTER throws because no strategy is registered yet`() {
        assertThrows<IllegalStateException> {
            registry.resolve(PhaseType.SCATTER)
        }
    }

    @Test
    fun `register overrides existing strategy`() {
        val custom = object : AdvancementStrategy {
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
