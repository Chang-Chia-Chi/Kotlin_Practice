package com.workflow.engine

import com.workflow.dsl.ActivityDefinition
import com.workflow.dsl.FanOutDefinition
import com.workflow.dsl.WorkflowDefinition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SequenceModelTest {

    @Test
    fun `single linear activity produces one entry with nextSequence null`() {
        val def = WorkflowDefinition(
            activities = listOf(ActivityDefinition(name = "a", transition = "a.handler")),
        )
        val map = buildSequenceMap(def)

        assertEquals(1, map.size)
        val seq1 = map[1]!!
        assertEquals(PhaseType.LINEAR, seq1.phaseType)
        assertEquals(0, seq1.activityIndex)
        assertEquals("a", seq1.activity.name)
        assertEquals(1, seq1.sequenceNumber)
        assertNull(seq1.nextSequence, "Last sequence should have null nextSequence")
        assertNull(seq1.branchSequences)
    }

    @Test
    fun `two linear activities produce seq 1 next 2, seq 2 next null`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(name = "b", transition = "b.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        assertEquals(2, map[1]!!.nextSequence)
        assertNull(map[2]!!.nextSequence)
    }

    @Test
    fun `fan-out activity produces SCATTER then PARALLEL`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "scatter-activity",
                    transition = "scatter.handler",
                    fanOut = FanOutDefinition(transition = "parallel.handler"),
                ),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        val scatter = map[1]!!
        assertEquals(PhaseType.SCATTER, scatter.phaseType)
        assertEquals(1, scatter.sequenceNumber)
        assertEquals(2, scatter.nextSequence, "SCATTER next should point to PARALLEL")

        val parallel = map[2]!!
        assertEquals(PhaseType.PARALLEL, parallel.phaseType)
        assertEquals(2, parallel.sequenceNumber)
        assertNull(parallel.nextSequence, "Last PARALLEL should have null nextSequence")
    }

    @Test
    fun `fan-out then linear produces SCATTER 1 next 2, PARALLEL 2 next 3, LINEAR 3 next null`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(
                    name = "scatter-activity",
                    transition = "scatter.handler",
                    fanOut = FanOutDefinition(transition = "parallel.handler"),
                ),
                ActivityDefinition(name = "final", transition = "final.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(3, map.size)
        assertEquals(2, map[1]!!.nextSequence) // SCATTER -> PARALLEL
        assertEquals(3, map[2]!!.nextSequence) // PARALLEL -> LINEAR
        assertNull(map[3]!!.nextSequence)       // LINEAR -> end
    }

    @Test
    fun `linear then fan-out then linear produces correct chain`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "step1", transition = "step1.handler"),
                ActivityDefinition(
                    name = "scatter-activity",
                    transition = "scatter.handler",
                    fanOut = FanOutDefinition(transition = "parallel.handler"),
                ),
                ActivityDefinition(name = "step3", transition = "step3.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(4, map.size)
        // step1: LINEAR seq 1 -> 2
        assertEquals(PhaseType.LINEAR, map[1]!!.phaseType)
        assertEquals(2, map[1]!!.nextSequence)
        // scatter: SCATTER seq 2 -> 3
        assertEquals(PhaseType.SCATTER, map[2]!!.phaseType)
        assertEquals(3, map[2]!!.nextSequence)
        // parallel: PARALLEL seq 3 -> 4
        assertEquals(PhaseType.PARALLEL, map[3]!!.phaseType)
        assertEquals(4, map[3]!!.nextSequence)
        // step3: LINEAR seq 4 -> null
        assertEquals(PhaseType.LINEAR, map[4]!!.phaseType)
        assertNull(map[4]!!.nextSequence)
    }

    @Test
    fun `sequenceNumber field matches map key`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(
                    name = "b",
                    transition = "b.handler",
                    fanOut = FanOutDefinition(transition = "b.fan.handler"),
                ),
            ),
        )
        val map = buildSequenceMap(def)
        map.forEach { (key, info) ->
            assertEquals(key, info.sequenceNumber, "Map key should match sequenceNumber")
        }
    }

    @Test
    fun `branchSequences is null for all current phase types`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(
                    name = "b",
                    transition = "b.handler",
                    fanOut = FanOutDefinition(transition = "b.fan.handler"),
                ),
            ),
        )
        val map = buildSequenceMap(def)
        map.values.forEach { info ->
            assertNull(info.branchSequences, "branchSequences should be null for ${info.phaseType}")
        }
    }
}
