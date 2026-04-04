package com.workflow.workflow.usecase.service.orchestration

import com.workflow.workflow.model.ActivityDefinition
import com.workflow.workflow.model.DEFAULT_BRANCH
import com.workflow.workflow.model.Edge
import com.workflow.workflow.model.FailurePolicy
import com.workflow.workflow.model.FanOutDefinition
import com.workflow.workflow.model.JoinPolicy
import com.workflow.workflow.model.PhaseType
import com.workflow.workflow.model.SequenceInfo
import com.workflow.workflow.model.Task
import com.workflow.workflow.model.TaskStatus
import com.workflow.workflow.model.TaskStatusCounts
import com.workflow.workflow.model.WorkflowDefinition
import com.workflow.workflow.model.buildSequenceMap
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DagRouterTest {

    // -- Test helpers ----------------------------------------------------------

    private val now: Instant = Instant.parse("2026-01-01T00:00:00Z")

    private fun activity(
        name: String,
        transition: String = "$name.handler",
        successors: List<Edge> = emptyList(),
        failurePolicy: FailurePolicy = FailurePolicy.ABORT,
    ) = ActivityDefinition(
        name = name,
        transition = transition,
        successors = successors,
        failurePolicy = failurePolicy,
    )

    private fun task(
        id: String = "t-1",
        workflowId: String = "wf-1",
        activityName: String = "",
        seq: Int = 1,
        status: TaskStatus = TaskStatus.COMPLETED,
        handlerKey: String = "h",
        resultJson: String? = null,
    ) = Task(
        id = id,
        workflowId = workflowId,
        activityName = activityName,
        sequenceNumber = seq,
        status = status,
        handlerKey = handlerKey,
        resultJson = resultJson,
        claimedBy = null,
        claimedAt = null,
        completedAt = if (status.isTerminal) now else null,
        retryCount = 0,
        maxRetries = 0,
        deadlineAt = null,
    )

    private fun emptySnapshot(
        workflowId: String = "wf-1",
        definition: WorkflowDefinition,
        allCounts: Map<Int, TaskStatusCounts> = emptyMap(),
        tasksBySeq: Map<Int, List<Task>> = emptyMap(),
        resultBranches: Map<String, String?> = emptyMap(),
    ): GateSnapshot {
        val sequenceMap = buildSequenceMap(definition)
        val seqByName = sequenceMap.values
            .filter { it.phaseType != PhaseType.PARALLEL }
            .associateBy { it.activityName }
        return GateSnapshot(
            workflowId = workflowId,
            definition = definition,
            sequenceMap = sequenceMap,
            seqByName = seqByName,
            allCounts = allCounts,
            tasksBySeq = tasksBySeq,
            resultBranches = resultBranches,
            now = now,
        )
    }

    // =========================================================================
    // evaluateJoinPolicy
    // =========================================================================

    @Nested
    inner class EvaluateJoinPolicyTest {

        @Test
        fun `All policy passes when completed equals total`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.All, completedCount = 5, totalCount = 5))
        }

        @Test
        fun `All policy fails when completed less than total`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.All, completedCount = 4, totalCount = 5))
        }

        @Test
        fun `Threshold policy passes when completed meets threshold`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.Threshold(3), completedCount = 3, totalCount = 5))
        }

        @Test
        fun `Threshold policy fails when completed below threshold`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.Threshold(3), completedCount = 2, totalCount = 5))
        }

        @Test
        fun `Threshold policy passes when completed exceeds threshold`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.Threshold(3), completedCount = 5, totalCount = 5))
        }

        @Test
        fun `Percentage policy passes at exact boundary`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.Percentage(80), completedCount = 4, totalCount = 5))
        }

        @Test
        fun `Percentage policy fails below boundary`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.Percentage(80), completedCount = 3, totalCount = 5))
        }

        @Test
        fun `Percentage policy handles zero total`() {
            assertFalse(evaluateJoinPolicy(JoinPolicy.Percentage(50), completedCount = 0, totalCount = 0))
        }

        @Test
        fun `All policy handles zero total - zero completed equals zero`() {
            assertTrue(evaluateJoinPolicy(JoinPolicy.All, completedCount = 0, totalCount = 0))
        }
    }

    // =========================================================================
    // isEdgeTaken
    // =========================================================================

    @Nested
    inner class IsEdgeTakenTest {

        @Test
        fun `COMPLETED task with DEFAULT_BRANCH edge is taken`() {
            assertTrue(isEdgeTaken(TaskStatus.COMPLETED, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `COMPLETED task with matching branch label is taken`() {
            assertTrue(isEdgeTaken(TaskStatus.COMPLETED, "OK", "OK", FailurePolicy.ABORT))
        }

        @Test
        fun `COMPLETED task with non-matching branch label is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.COMPLETED, "OK", "FAIL", FailurePolicy.ABORT))
        }

        @Test
        fun `FAILED task with BEST_EFFORT takes DEFAULT_BRANCH`() {
            assertTrue(isEdgeTaken(TaskStatus.FAILED, null, DEFAULT_BRANCH, FailurePolicy.BEST_EFFORT))
        }

        @Test
        fun `FAILED task with BEST_EFFORT does not take conditional edge`() {
            assertFalse(isEdgeTaken(TaskStatus.FAILED, null, "OK", FailurePolicy.BEST_EFFORT))
        }

        @Test
        fun `FAILED task with ABORT is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.FAILED, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `non-terminal task is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.PROCESSING, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `COMPLETED task with null branch and conditional edge is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.COMPLETED, null, "SOME_LABEL", FailurePolicy.ABORT))
        }

        @Test
        fun `PENDING task is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.PENDING, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `SKIPPED task is not taken for default edge`() {
            // SKIPPED is terminal but neither COMPLETED nor FAILED-with-BEST_EFFORT
            assertFalse(isEdgeTaken(TaskStatus.SKIPPED, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }

        @Test
        fun `TIMED_OUT task with ABORT is not taken`() {
            assertFalse(isEdgeTaken(TaskStatus.TIMED_OUT, null, DEFAULT_BRANCH, FailurePolicy.ABORT))
        }
    }

    // =========================================================================
    // isAnyEdgeTaken
    // =========================================================================

    @Nested
    inner class IsAnyEdgeTakenTest {

        @Test
        fun `returns true when predecessor completed with default edge`() {
            val predActivity = activity("pred", successors = listOf(Edge("succ")))
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf("pred" to predActivity, "succ" to succActivity),
                start = "pred",
            )
            val sequenceMap = mapOf(
                1 to SequenceInfo(1, "pred", predActivity, PhaseType.LINEAR, emptyList()),
                2 to SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1)),
            )
            val predTask = task(id = "t-pred", seq = 1, status = TaskStatus.COMPLETED)
            val tasksBySeq = mapOf(1 to listOf(predTask))
            val resultBranches = mapOf("t-pred" to null as String?)

            assertTrue(isAnyEdgeTaken(tasksBySeq, resultBranches, sequenceMap.getValue(2), sequenceMap, definition))
        }

        @Test
        fun `returns false when predecessor not completed`() {
            val predActivity = activity("pred", successors = listOf(Edge("succ")))
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf("pred" to predActivity, "succ" to succActivity),
                start = "pred",
            )
            val sequenceMap = mapOf(
                1 to SequenceInfo(1, "pred", predActivity, PhaseType.LINEAR, emptyList()),
                2 to SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1)),
            )
            val predTask = task(id = "t-pred", seq = 1, status = TaskStatus.PROCESSING)
            val tasksBySeq = mapOf(1 to listOf(predTask))
            val resultBranches = emptyMap<String, String?>()

            assertFalse(isAnyEdgeTaken(tasksBySeq, resultBranches, sequenceMap.getValue(2), sequenceMap, definition))
        }

        @Test
        fun `returns true when predecessor completed with matching conditional edge`() {
            val predActivity = activity("pred", successors = listOf(Edge("succ", "OK")))
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf("pred" to predActivity, "succ" to succActivity),
                start = "pred",
            )
            val sequenceMap = mapOf(
                1 to SequenceInfo(1, "pred", predActivity, PhaseType.LINEAR, emptyList()),
                2 to SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1)),
            )
            val predTask = task(id = "t-pred", seq = 1, status = TaskStatus.COMPLETED)
            val tasksBySeq = mapOf(1 to listOf(predTask))
            val resultBranches = mapOf("t-pred" to "OK")

            assertTrue(isAnyEdgeTaken(tasksBySeq, resultBranches, sequenceMap.getValue(2), sequenceMap, definition))
        }

        @Test
        fun `returns false when predecessor completed with non-matching conditional edge`() {
            val predActivity = activity("pred", successors = listOf(Edge("succ", "OK")))
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf("pred" to predActivity, "succ" to succActivity),
                start = "pred",
            )
            val sequenceMap = mapOf(
                1 to SequenceInfo(1, "pred", predActivity, PhaseType.LINEAR, emptyList()),
                2 to SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1)),
            )
            val predTask = task(id = "t-pred", seq = 1, status = TaskStatus.COMPLETED)
            val tasksBySeq = mapOf(1 to listOf(predTask))
            val resultBranches = mapOf("t-pred" to "FAIL")

            assertFalse(isAnyEdgeTaken(tasksBySeq, resultBranches, sequenceMap.getValue(2), sequenceMap, definition))
        }

        @Test
        fun `returns false when no predecessor tasks exist for sequence`() {
            val predActivity = activity("pred", successors = listOf(Edge("succ")))
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf("pred" to predActivity, "succ" to succActivity),
                start = "pred",
            )
            val sequenceMap = mapOf(
                1 to SequenceInfo(1, "pred", predActivity, PhaseType.LINEAR, emptyList()),
                2 to SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1)),
            )
            val tasksBySeq = emptyMap<Int, List<Task>>()
            val resultBranches = emptyMap<String, String?>()

            assertFalse(isAnyEdgeTaken(tasksBySeq, resultBranches, sequenceMap.getValue(2), sequenceMap, definition))
        }
    }

    // =========================================================================
    // hasDefaultBranchEdge
    // =========================================================================

    @Nested
    inner class HasDefaultBranchEdgeTest {

        @Test
        fun `returns true when predecessor has default edge to successor`() {
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf(
                    "pred" to activity("pred", successors = listOf(Edge("succ"))),
                    "succ" to succActivity,
                ),
                start = "pred",
            )
            val successor = SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1))
            assertTrue(hasDefaultBranchEdge(successor, definition))
        }

        @Test
        fun `returns false when predecessor has only conditional edges to successor`() {
            val succActivity = activity("succ")
            val definition = WorkflowDefinition(
                activities = mapOf(
                    "pred" to activity("pred", successors = listOf(Edge("succ", "OK"))),
                    "succ" to succActivity,
                ),
                start = "pred",
            )
            val successor = SequenceInfo(2, "succ", succActivity, PhaseType.LINEAR, listOf(1))
            assertFalse(hasDefaultBranchEdge(successor, definition))
        }

        @Test
        fun `returns false when no predecessor edges target successor`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a")),
                start = "a",
            )
            val orphan = SequenceInfo(99, "orphan", activity("orphan"), PhaseType.LINEAR, emptyList())
            assertFalse(hasDefaultBranchEdge(orphan, def))
        }
    }

    // =========================================================================
    // successorsOf
    // =========================================================================

    @Nested
    inner class SuccessorsOfTest {

        @Test
        fun `linear chain returns single successor`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"))),
                    "b" to activity("b"),
                ),
                start = "a",
            )
            val sequenceMap = buildSequenceMap(def)
            val seqByName = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
            val seqInfo = sequenceMap.getValue(1)

            val successors = successorsOf(seqInfo, seqByName, def)
            assertEquals(1, successors.size)
            assertEquals("b", successors[0].activityName)
        }

        @Test
        fun `scatter activity returns scatter sequence as successor`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("done")))
                .copy(fanOut = FanOutDefinition(transition = "par.h"))
            val def = WorkflowDefinition(
                activities = mapOf(
                    "start" to activity("start", successors = listOf(Edge("scatter"))),
                    "scatter" to scatterAct,
                    "done" to activity("done"),
                ),
                start = "start",
            )
            val sequenceMap = buildSequenceMap(def)
            val seqByName = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
            val startSeq = sequenceMap.values.first { it.activityName == "start" }

            val successors = successorsOf(startSeq, seqByName, def)
            assertEquals(1, successors.size)
            assertEquals("scatter", successors[0].activityName)
            assertEquals(PhaseType.SCATTER, successors[0].phaseType)
        }

        @Test
        fun `terminal activity has no successors`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a")),
                start = "a",
            )
            val sequenceMap = buildSequenceMap(def)
            val seqByName = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
            val seqInfo = sequenceMap.getValue(1)

            val successors = successorsOf(seqInfo, seqByName, def)
            assertTrue(successors.isEmpty())
        }

        @Test
        fun `parallel suffix stripping resolves successors correctly`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("done")))
                .copy(fanOut = FanOutDefinition(transition = "par.h"))
            val def = WorkflowDefinition(
                activities = mapOf(
                    "scatter" to scatterAct,
                    "done" to activity("done"),
                ),
                start = "scatter",
            )
            val sequenceMap = buildSequenceMap(def)
            val seqByName = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
            // The parallel node is scatter.__parallel__ — successorsOf should strip suffix and resolve
            val parallelSeq = sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }

            val successors = successorsOf(parallelSeq, seqByName, def)
            assertEquals(1, successors.size)
            assertEquals("done", successors[0].activityName)
        }

        @Test
        fun `fork returns multiple successors`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"), Edge("c"))),
                    "b" to activity("b"),
                    "c" to activity("c"),
                ),
                start = "a",
            )
            val sequenceMap = buildSequenceMap(def)
            val seqByName = sequenceMap.values
                .filter { it.phaseType != PhaseType.PARALLEL }
                .associateBy { it.activityName }
            val seqInfo = sequenceMap.values.first { it.activityName == "a" }

            val successors = successorsOf(seqInfo, seqByName, def)
            assertEquals(2, successors.size)
            val names = successors.map { it.activityName }.toSet()
            assertTrue(names.contains("b"))
            assertTrue(names.contains("c"))
        }
    }

    // =========================================================================
    // resolvePhaseDecision
    // =========================================================================

    @Nested
    inner class ResolvePhaseDecisionTest {

        @Test
        fun `LINEAR COMPLETED returns Normal`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a")),
                start = "a",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.getValue(1)
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.Normal, decision)
        }

        @Test
        fun `LINEAR FAILED with ABORT returns Abort`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a", failurePolicy = FailurePolicy.ABORT)),
                start = "a",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.getValue(1)
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.FAILED, scatterItems = null)
            assertEquals(PhaseDecision.Abort, decision)
        }

        @Test
        fun `LINEAR FAILED with BEST_EFFORT returns Normal`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a", failurePolicy = FailurePolicy.BEST_EFFORT)),
                start = "a",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.getValue(1)
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.FAILED, scatterItems = null)
            assertEquals(PhaseDecision.Normal, decision)
        }

        @Test
        fun `LINEAR SKIPPED returns Normal`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a")),
                start = "a",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.getValue(1)
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.SKIPPED, scatterItems = null)
            assertEquals(PhaseDecision.Normal, decision)
        }

        @Test
        fun `SCATTER COMPLETED returns ScatterExpand`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h"))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.SCATTER }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = listOf("a", "b"))
            assertTrue(decision is PhaseDecision.ScatterExpand)
            assertEquals(listOf("a", "b"), (decision as PhaseDecision.ScatterExpand).items)
        }

        @Test
        fun `SCATTER FAILED with ABORT returns Abort`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h"))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.SCATTER }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.FAILED, scatterItems = null)
            assertEquals(PhaseDecision.Abort, decision)
        }

        @Test
        fun `PARALLEL join passed returns Normal`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h", joinPolicy = JoinPolicy.All))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(2 to TaskStatusCounts(total = 3, completed = 3, nonTerminal = 0, failed = 0)),
            )
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.Normal, decision)
        }

        @Test
        fun `PARALLEL join failed with ABORT returns Abort`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(
                    fanOut = FanOutDefinition(
                        transition = "par.h",
                        joinPolicy = JoinPolicy.All,
                        failurePolicy = FailurePolicy.ABORT,
                    ),
                )
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(2 to TaskStatusCounts(total = 3, completed = 2, nonTerminal = 0, failed = 1)),
            )
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.Abort, decision)
        }

        @Test
        fun `PARALLEL join failed with BEST_EFFORT returns ForceDefaultBranch`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(
                    fanOut = FanOutDefinition(
                        transition = "par.h",
                        joinPolicy = JoinPolicy.All,
                        failurePolicy = FailurePolicy.BEST_EFFORT,
                    ),
                )
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(2 to TaskStatusCounts(total = 3, completed = 2, nonTerminal = 0, failed = 1)),
            )
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.ForceDefaultBranch, decision)
        }

        @Test
        fun `PARALLEL threshold join passes with enough completions`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("join")))
                .copy(fanOut = FanOutDefinition(transition = "par.h", joinPolicy = JoinPolicy.Threshold(2)))
            val joinAct = activity("join")
            val def = WorkflowDefinition(
                activities = mapOf("scatter" to scatterAct, "join" to joinAct),
                start = "scatter",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(2 to TaskStatusCounts(total = 5, completed = 2, nonTerminal = 0, failed = 3)),
            )
            val seqInfo = snap.sequenceMap.values.first { it.phaseType == PhaseType.PARALLEL }
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.COMPLETED, scatterItems = null)
            assertEquals(PhaseDecision.Normal, decision)
        }

        @Test
        fun `LINEAR TIMED_OUT with ABORT returns Abort`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a", failurePolicy = FailurePolicy.ABORT)),
                start = "a",
            )
            val snap = emptySnapshot(definition = def)
            val seqInfo = snap.sequenceMap.getValue(1)
            val decision = resolvePhaseDecision(snap, seqInfo, TaskStatus.TIMED_OUT, scatterItems = null)
            assertEquals(PhaseDecision.Abort, decision)
        }
    }

    // =========================================================================
    // dispatchSuccessors
    // =========================================================================

    @Nested
    inner class DispatchSuccessorsTest {

        @Test
        fun `linear chain dispatches next activity`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"))),
                    "b" to activity("b"),
                ),
                start = "a",
            )
            val predTask = task(id = "t-a", seq = 1, status = TaskStatus.COMPLETED)
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-a" to null),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            assertEquals(1, result.tasksToInsert.size)
            assertEquals(TaskStatus.PENDING, result.tasksToInsert[0].status)
            assertTrue(result.signalQueues.contains("default"))
        }

        @Test
        fun `conditional routing skips unmatched branch`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b", "OK"), Edge("c", "FAIL"))),
                    "b" to activity("b"),
                    "c" to activity("c"),
                ),
                start = "a",
            )
            val predTask = task(
                id = "t-a",
                seq = 1,
                status = TaskStatus.COMPLETED,
                resultJson = """{"branch":"OK"}""",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-a" to "OK"),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            val pending = result.tasksToInsert.filter { it.status == TaskStatus.PENDING }
            val skipped = result.tasksToInsert.filter { it.status == TaskStatus.SKIPPED }
            assertEquals(1, pending.size)
            assertEquals(1, skipped.size)
            assertEquals("b", pending[0].activityName)
            assertEquals("c", skipped[0].activityName)
        }

        @Test
        fun `diamond join waits for all predecessors`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"), Edge("c"))),
                    "b" to activity("b", successors = listOf(Edge("join"))),
                    "c" to activity("c", successors = listOf(Edge("join"))),
                    "join" to activity("join"),
                ),
                start = "a",
            )
            // Only b completed, c not yet
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(
                    1 to TaskStatusCounts(1, 1, 0, 0),
                    2 to TaskStatusCounts(1, 1, 0, 0),
                    // seq 3 (c) has no counts — not yet dispatched or still pending
                ),
                tasksBySeq = mapOf(
                    1 to listOf(task(id = "t-a", seq = 1)),
                    2 to listOf(task(id = "t-b", seq = 2)),
                ),
                resultBranches = mapOf("t-a" to null, "t-b" to null),
            )
            val seqInfo = snap.sequenceMap.getValue(2) // b completing
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            // join should NOT be dispatched (c not resolved)
            assertTrue(result.tasksToInsert.isEmpty())
        }

        @Test
        fun `cascade skip propagates through chain`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b", "OK"), Edge("x", "NO"))),
                    "b" to activity("b", successors = listOf(Edge("c"))),
                    "c" to activity("c"),
                    "x" to activity("x"),
                ),
                start = "a",
            )
            val predTask = task(
                id = "t-a",
                seq = 1,
                status = TaskStatus.COMPLETED,
                resultJson = """{"branch":"NO"}""",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-a" to "NO"),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            val names = result.tasksToInsert.map { it.activityName to it.status }
            assertTrue(names.contains("b" to TaskStatus.SKIPPED))
            assertTrue(names.contains("c" to TaskStatus.SKIPPED))
            assertTrue(names.contains("x" to TaskStatus.PENDING))
        }

        @Test
        fun `scatter skip cascades to companion parallel node`() {
            val scatterAct = activity("scatter", successors = listOf(Edge("done")))
                .copy(fanOut = FanOutDefinition(transition = "par.h"))
            val def = WorkflowDefinition(
                activities = mapOf(
                    "route" to activity("route", successors = listOf(Edge("scatter", "RUN"), Edge("done", "SKIP"))),
                    "scatter" to scatterAct,
                    "done" to activity("done"),
                ),
                start = "route",
            )
            val predTask = task(
                id = "t-route",
                seq = 1,
                status = TaskStatus.COMPLETED,
                resultJson = """{"branch":"SKIP"}""",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-route" to "SKIP"),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            val skipped = result.tasksToInsert.filter { it.status == TaskStatus.SKIPPED }
            assertTrue(skipped.any { it.activityName == "scatter" })
            assertTrue(skipped.any { it.activityName == "scatter.__parallel__" })
        }

        @Test
        fun `terminal activity produces empty result`() {
            val def = WorkflowDefinition(
                activities = mapOf("a" to activity("a")),
                start = "a",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            assertTrue(result.tasksToInsert.isEmpty())
            assertTrue(result.signalQueues.isEmpty())
            assertFalse(result.hasTerminalCompletion)
        }

        @Test
        fun `forceDefault mode dispatches only default-edged successors`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"), Edge("c", "OK"))),
                    "b" to activity("b"),
                    "c" to activity("c"),
                ),
                start = "a",
            )
            // No tasksBySeq or resultBranches needed — forceDefault uses hasDefaultBranchEdge
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = true)

            val pending = result.tasksToInsert.filter { it.status == TaskStatus.PENDING }
            val skipped = result.tasksToInsert.filter { it.status == TaskStatus.SKIPPED }
            assertEquals(1, pending.size)
            assertEquals("b", pending[0].activityName)
            assertEquals(1, skipped.size)
            assertEquals("c", skipped[0].activityName)
        }

        @Test
        fun `diamond join dispatches when all predecessors resolved`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"), Edge("c"))),
                    "b" to activity("b", successors = listOf(Edge("join"))),
                    "c" to activity("c", successors = listOf(Edge("join"))),
                    "join" to activity("join"),
                ),
                start = "a",
            )
            // Both b and c completed
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(
                    1 to TaskStatusCounts(1, 1, 0, 0),
                    2 to TaskStatusCounts(1, 1, 0, 0),
                    3 to TaskStatusCounts(1, 1, 0, 0),
                ),
                tasksBySeq = mapOf(
                    1 to listOf(task(id = "t-a", seq = 1)),
                    2 to listOf(task(id = "t-b", seq = 2)),
                    3 to listOf(task(id = "t-c", seq = 3)),
                ),
                resultBranches = mapOf("t-a" to null, "t-b" to null, "t-c" to null),
            )
            val seqInfo = snap.sequenceMap.getValue(3) // c completing (b already resolved)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            assertEquals(1, result.tasksToInsert.size)
            assertEquals("join", result.tasksToInsert[0].activityName)
            assertEquals(TaskStatus.PENDING, result.tasksToInsert[0].status)
        }

        @Test
        fun `skip of terminal successor sets hasTerminalCompletion`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("term", "OK"), Edge("other"))),
                    "term" to activity("term"),
                    "other" to activity("other"),
                ),
                start = "a",
            )
            val predTask = task(id = "t-a", seq = 1, status = TaskStatus.COMPLETED)
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(predTask)),
                resultBranches = mapOf("t-a" to null),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            // "term" gets skipped (conditional edge "OK" not taken since branch is null)
            // "other" gets dispatched (default edge)
            val skippedNames = result.tasksToInsert
                .filter { it.status == TaskStatus.SKIPPED }
                .map { it.activityName }
            assertTrue(skippedNames.contains("term"))
            assertTrue(result.hasTerminalCompletion)
        }

        @Test
        fun `already-dispatched successor is not re-dispatched`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"))),
                    "b" to activity("b"),
                ),
                start = "a",
            )
            // b already has tasks (total > 0), meaning it was already dispatched
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(
                    1 to TaskStatusCounts(1, 1, 0, 0),
                    2 to TaskStatusCounts(1, 0, 1, 0),
                ),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            assertTrue(result.tasksToInsert.isEmpty())
        }

        @Test
        fun `multi-hop chain dispatches all pending successors`() {
            val def = WorkflowDefinition(
                activities = mapOf(
                    "a" to activity("a", successors = listOf(Edge("b"))),
                    "b" to activity("b", successors = listOf(Edge("c"))),
                    "c" to activity("c"),
                ),
                start = "a",
            )
            val snap = emptySnapshot(
                definition = def,
                allCounts = mapOf(1 to TaskStatusCounts(1, 1, 0, 0)),
                tasksBySeq = mapOf(1 to listOf(task(id = "t-a", seq = 1))),
                resultBranches = mapOf("t-a" to null),
            )
            val seqInfo = snap.sequenceMap.getValue(1)
            val result = dispatchSuccessors(snap, seqInfo, forceDefault = false)

            // Only b should be dispatched; c waits for b to complete
            assertEquals(1, result.tasksToInsert.size)
            assertEquals("b", result.tasksToInsert[0].activityName)
            assertEquals(TaskStatus.PENDING, result.tasksToInsert[0].status)
        }
    }
}
