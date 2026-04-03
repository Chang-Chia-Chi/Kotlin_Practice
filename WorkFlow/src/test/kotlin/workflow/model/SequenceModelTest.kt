package com.workflow.workflow.model

import com.workflow.workflow.dsl.workflow
import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.buildSequenceMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SequenceModelTest {

    // ── Spec item 1: Linear chain ─────────────────────────────────────────

    @Test
    fun `linear chain produces correct sequence numbers and predecessors`() {
        val def = workflow {
            activity("a") { transition("a.h"); next("b") }
            activity("b") { transition("b.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(2, map.size)
        val a = map[1]!!
        assertEquals("a", a.activityName)
        assertEquals(PhaseType.LINEAR, a.phaseType)
        assertEquals(emptyList(), a.predecessorSequences)

        val b = map[2]!!
        assertEquals("b", b.activityName)
        assertEquals(PhaseType.LINEAR, b.phaseType)
        assertEquals(listOf(1), b.predecessorSequences)
    }

    // ── Spec item 2: Fork ─────────────────────────────────────────────────

    @Test
    fun `fork gives B and C different seq numbers, D predecessors are both`() {
        val def = workflow {
            activity("a") { transition("a.h"); next("b"); next("c") }
            activity("b") { transition("b.h"); next("d") }
            activity("c") { transition("c.h"); next("d") }
            activity("d") { transition("d.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(4, map.size)
        val seqA = map.values.first { it.activityName == "a" }.sequenceNumber
        val seqB = map.values.first { it.activityName == "b" }.sequenceNumber
        val seqC = map.values.first { it.activityName == "c" }.sequenceNumber
        val seqD = map.values.first { it.activityName == "d" }.sequenceNumber

        assertTrue(seqB != seqC, "B and C must have different sequence numbers")
        assertEquals(setOf(seqB, seqC), map[seqD]!!.predecessorSequences.toSet())
    }

    // ── Spec item 3: Conditional — same shape as fork, edge labels recorded

    @Test
    fun `conditional edges have correct labels on successors`() {
        val def = workflow {
            activity("validate") {
                transition("v.h")
                on("OK") { next("charge") }
                on("INVALID") { next("reject") }
            }
            activity("charge") { transition("c.h") }
            activity("reject") { transition("r.h") }
        }
        val map = buildSequenceMap(def)
        assertEquals(3, map.size)

        val validate = def.activities["validate"]!!
        val okEdge = validate.successors.first { it.target == "charge" }
        val invalidEdge = validate.successors.first { it.target == "reject" }
        assertEquals("OK", okEdge.label)
        assertEquals("INVALID", invalidEdge.label)
    }

    // ── Spec item 4: Fan-out → SCATTER at N, PARALLEL at N+1 ─────────────

    @Test
    fun `fan-out produces SCATTER at N and PARALLEL at N+1`() {
        val def = workflow {
            activity("scatter") {
                transition("s.h")
                fanOut { transition("p.h"); joinPolicy(JoinPolicy.All) }
                next("join")
            }
            activity("join") { transition("j.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(3, map.size)
        val scatter = map.values.first { it.activityName == "scatter" }
        val parallel = map.values.first { it.activityName == "scatter.__parallel__" }
        val join = map.values.first { it.activityName == "join" }

        assertEquals(PhaseType.SCATTER, scatter.phaseType)
        assertEquals(PhaseType.PARALLEL, parallel.phaseType)
        assertEquals(scatter.sequenceNumber + 1, parallel.sequenceNumber)
        assertEquals(listOf(parallel.sequenceNumber), join.predecessorSequences)
    }

    // ── Spec item 5: Fan-out inside DAG ───────────────────────────────────

    @Test
    fun `fan-out inside DAG has correct predecessor chain`() {
        val def = workflow {
            activity("start") { transition("s.h"); next("scatter") }
            activity("scatter") {
                transition("sc.h")
                fanOut { transition("p.h") }
                next("end")
            }
            activity("end") { transition("e.h") }
        }
        val map = buildSequenceMap(def)

        assertEquals(4, map.size) // start, scatter(SCATTER), scatter(PARALLEL), end
        val startSeq = map.values.first { it.activityName == "start" }.sequenceNumber
        val scatterSeq = map.values.first { it.activityName == "scatter" }.sequenceNumber
        val parallelSeq = map.values.first { it.activityName == "scatter.__parallel__" }.sequenceNumber
        val endSeq = map.values.first { it.activityName == "end" }.sequenceNumber

        assertEquals(listOf(startSeq), map[scatterSeq]!!.predecessorSequences)
        assertEquals(listOf(scatterSeq), map[parallelSeq]!!.predecessorSequences)
        assertEquals(listOf(parallelSeq), map[endSeq]!!.predecessorSequences)
    }

    // ── Spec item 6: Cycle detection ──────────────────────────────────────

    @Test
    fun `cycle in activity graph is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition("a", "a.h", successors = listOf(Edge("b"))),
                    "b" to ActivityDefinition("b", "b.h", successors = listOf(Edge("a"))),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 7: Unreachable activity rejected ────────────────────────

    @Test
    fun `unreachable activity is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition("a", "a.h"),
                    "orphan" to ActivityDefinition("orphan", "orphan.h"),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 8: Unknown edge target rejected ─────────────────────────

    @Test
    fun `unknown edge target is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition("a", "a.h", successors = listOf(Edge("nonexistent"))),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 9: BEST_EFFORT + on() rejected ──────────────────────────

    @Test
    fun `BEST_EFFORT with conditional successors is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition(
                        "a", "a.h",
                        failurePolicy = FailurePolicy.BEST_EFFORT,
                        successors = listOf(Edge("b", "OK")),
                    ),
                    "b" to ActivityDefinition("b", "b.h"),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 10: fanOut + on() rejected ──────────────────────────────

    @Test
    fun `fanOut with conditional successors is rejected at build time`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition(
                        "a", "a.h",
                        fanOut = FanOutDefinition("p.h"),
                        successors = listOf(Edge("b", "OK")),
                    ),
                    "b" to ActivityDefinition("b", "b.h"),
                ),
                start = "a",
            )
        }
    }

    // ── Spec item 11: no start → reject ───────────────────────────────────

    @Test
    fun `missing start activity is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf("a" to ActivityDefinition("a", "a.h")),
                start = "nonexistent",
            )
        }
    }

    // ── Spec item 12: no terminal activity → reject ──────────────────────

    @Test
    fun `workflow with no terminal activity is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            WorkflowDefinition(
                activities = mapOf(
                    "a" to ActivityDefinition("a", "a.h", successors = listOf(Edge("b"))),
                    "b" to ActivityDefinition("b", "b.h", successors = listOf(Edge("a"))),
                ),
                start = "a",
            )
        }
    }
}
