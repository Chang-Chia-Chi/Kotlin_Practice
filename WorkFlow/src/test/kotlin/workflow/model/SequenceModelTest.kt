package com.workflow.workflow.model

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.buildSequenceMap
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
        assertNull(seq1.nextSequence)
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
    fun `fan-out activity is PARALLEL when referenced by another activity`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "scatter", transition = "scatter.handler", fanOut = "parallel"),
                ActivityDefinition(name = "parallel", transition = "parallel.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        val scatter = map[1]!!
        assertEquals(PhaseType.LINEAR, scatter.phaseType)
        assertEquals("scatter", scatter.activity.name)
        assertEquals(2, scatter.nextSequence)

        val parallel = map[2]!!
        assertEquals(PhaseType.PARALLEL, parallel.phaseType)
        assertEquals("parallel", parallel.activity.name)
        assertNull(parallel.nextSequence)
    }

    @Test
    fun `scatter then parallel then join produces LINEAR PARALLEL LINEAR`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "scatter", transition = "scatter.handler", fanOut = "parallel"),
                ActivityDefinition(name = "parallel", transition = "parallel.handler"),
                ActivityDefinition(name = "join", transition = "join.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(3, map.size)
        assertEquals(PhaseType.LINEAR, map[1]!!.phaseType)
        assertEquals(2, map[1]!!.nextSequence)
        assertEquals(PhaseType.PARALLEL, map[2]!!.phaseType)
        assertEquals(3, map[2]!!.nextSequence)
        assertEquals(PhaseType.LINEAR, map[3]!!.phaseType)
        assertNull(map[3]!!.nextSequence)
    }

    @Test
    fun `linear then scatter then parallel then join produces correct chain`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "step1", transition = "step1.handler"),
                ActivityDefinition(name = "scatter", transition = "scatter.handler", fanOut = "parallel"),
                ActivityDefinition(name = "parallel", transition = "parallel.handler"),
                ActivityDefinition(name = "step3", transition = "step3.handler"),
            ),
        )
        val map = buildSequenceMap(def)

        assertEquals(4, map.size)
        assertEquals(PhaseType.LINEAR, map[1]!!.phaseType)
        assertEquals(2, map[1]!!.nextSequence)
        assertEquals(PhaseType.LINEAR, map[2]!!.phaseType)
        assertEquals(3, map[2]!!.nextSequence)
        assertEquals(PhaseType.PARALLEL, map[3]!!.phaseType)
        assertEquals(4, map[3]!!.nextSequence)
        assertEquals(PhaseType.LINEAR, map[4]!!.phaseType)
        assertNull(map[4]!!.nextSequence)
    }

    @Test
    fun `sequenceNumber field matches map key`() {
        val def = WorkflowDefinition(
            activities = listOf(
                ActivityDefinition(name = "a", transition = "a.handler"),
                ActivityDefinition(name = "b", transition = "b.handler", fanOut = "c"),
                ActivityDefinition(name = "c", transition = "c.handler"),
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
                ActivityDefinition(name = "b", transition = "b.handler", fanOut = "c"),
                ActivityDefinition(name = "c", transition = "c.handler"),
            ),
        )
        val map = buildSequenceMap(def)
        map.values.forEach { info ->
            assertNull(info.branchSequences, "branchSequences should be null for ${info.phaseType}")
        }
    }
}
