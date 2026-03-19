package com.mapreduce.mr.handler

import com.mapreduce.queue.model.EnqueueRequest
import com.mapreduce.queue.model.GroupStatus
import com.mapreduce.queue.model.TaskContext
import com.mapreduce.queue.model.TaskGroup
import com.mapreduce.queue.model.TaskResult
import com.mapreduce.queue.repository.TaskGroupRepository
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class PhaseTransitionHandlerTest {

    private lateinit var taskGroupRepository: TaskGroupRepository
    private lateinit var handler: PhaseTransitionHandler

    private val jobType = "test"
    private val maxRetries = 3
    private val queue = "default"
    private val totalPartitions = 4

    private fun ctx(groupId: String = "g-1") = TaskContext(
        taskId = "t-1",
        handler = "$jobType.__phase_complete",
        queue = "default",
        payload = groupId,
    )

    private fun group(
        groupId: String = "g-1",
        phase: String = "map",
        tasksFailed: Int = 0,
        failurePolicy: String = "FAIL_GROUP",
        failureThreshold: Double = 0.0,
        phaseTotal: Int = 10,
        version: Long = 1,
    ) = TaskGroup(
        groupId = groupId,
        groupType = jobType,
        status = GroupStatus.ACTIVE,
        phase = phase,
        phaseTotal = phaseTotal,
        tasksFailed = tasksFailed,
        failurePolicy = failurePolicy,
        failureThreshold = failureThreshold,
        version = version,
    )

    @BeforeEach
    fun setUp() {
        taskGroupRepository = mock()
        handler = PhaseTransitionHandler(
            jobType = jobType,
            taskGroupRepository = taskGroupRepository,
            maxRetries = maxRetries,
            queue = queue,
            totalPartitions = totalPartitions,
        )
    }

    @Test
    fun `handlerName follows jobType dot __phase_complete convention`() {
        assertEquals("test.__phase_complete", handler.handlerName)
    }

    @Nested
    inner class `group lookup` {

        @Test
        fun `returns Failure when group is not found`() = runTest {
            whenever(taskGroupRepository.findGroup("g-missing")).thenReturn(null)

            val result = handler.handle(ctx("g-missing"))

            val failure = assertInstanceOf(TaskResult.Failure::class.java, result)
            assertEquals("Group g-missing not found", failure.message)
        }

        @Test
        fun `returns Failure for unknown phase`() = runTest {
            whenever(taskGroupRepository.findGroup("g-1"))
                .thenReturn(group(phase = "shuffle"))

            val result = handler.handle(ctx())

            val failure = assertInstanceOf(TaskResult.Failure::class.java, result)
            assertEquals("Unknown phase: shuffle", failure.message)
        }
    }

    @Nested
    inner class `map phase -- FAIL_GROUP policy` {

        @Test
        fun `transitions to reduce phase when zero failures`() = runTest {
            val g = group(tasksFailed = 0, failurePolicy = "FAIL_GROUP")
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.transitionPhase(
                    groupId = eq("g-1"),
                    expectedVersion = eq(1L),
                    newPhase = eq("reduce"),
                    newPhaseTotal = eq(totalPartitions),
                    tasks = any(),
                    onCompleteHandler = eq("test.__phase_complete"),
                ),
            ).thenReturn(true)

            val result = handler.handle(ctx())

            assertInstanceOf(TaskResult.Success::class.java, result)
            verify(taskGroupRepository).transitionPhase(
                groupId = eq("g-1"),
                expectedVersion = eq(1L),
                newPhase = eq("reduce"),
                newPhaseTotal = eq(totalPartitions),
                tasks = argThat { size == totalPartitions },
                onCompleteHandler = eq("test.__phase_complete"),
            )
            verify(taskGroupRepository, never()).casGroupStatus(any(), any(), any(), any(), any())
        }

        @Test
        fun `marks group FAILED when failures are greater than zero`() = runTest {
            val g = group(tasksFailed = 3, failurePolicy = "FAIL_GROUP")
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.casGroupStatus("g-1", GroupStatus.ACTIVE, GroupStatus.FAILED, 1L, null),
            ).thenReturn(true)

            val result = handler.handle(ctx())

            assertInstanceOf(TaskResult.Success::class.java, result)
            verify(taskGroupRepository).casGroupStatus("g-1", GroupStatus.ACTIVE, GroupStatus.FAILED, 1L, null)
            verify(taskGroupRepository, never()).transitionPhase(any(), any(), any(), any(), any(), any())
        }
    }

    @Nested
    inner class `map phase -- THRESHOLD policy` {

        @Test
        fun `transitions to reduce when failure rate is below threshold`() = runTest {
            // 1 out of 10 = 10%, threshold = 20% => passes
            val g = group(
                tasksFailed = 1, phaseTotal = 10,
                failurePolicy = "THRESHOLD", failureThreshold = 0.2,
            )
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.transitionPhase(
                    groupId = any(), expectedVersion = any(),
                    newPhase = any(), newPhaseTotal = any(),
                    tasks = any(), onCompleteHandler = any(),
                ),
            ).thenReturn(true)

            val result = handler.handle(ctx())

            assertInstanceOf(TaskResult.Success::class.java, result)
            verify(taskGroupRepository).transitionPhase(
                groupId = eq("g-1"),
                expectedVersion = eq(1L),
                newPhase = eq("reduce"),
                newPhaseTotal = eq(totalPartitions),
                tasks = any(),
                onCompleteHandler = eq("test.__phase_complete"),
            )
        }

        @Test
        fun `marks group FAILED when failure rate exceeds threshold`() = runTest {
            // 5 out of 10 = 50%, threshold = 20% => fails
            val g = group(
                tasksFailed = 5, phaseTotal = 10,
                failurePolicy = "THRESHOLD", failureThreshold = 0.2,
            )
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.casGroupStatus("g-1", GroupStatus.ACTIVE, GroupStatus.FAILED, 1L, null),
            ).thenReturn(true)

            val result = handler.handle(ctx())

            assertInstanceOf(TaskResult.Success::class.java, result)
            verify(taskGroupRepository).casGroupStatus("g-1", GroupStatus.ACTIVE, GroupStatus.FAILED, 1L, null)
            verify(taskGroupRepository, never()).transitionPhase(any(), any(), any(), any(), any(), any())
        }
    }

    @Nested
    inner class `map phase -- BEST_EFFORT policy` {

        @Test
        fun `always transitions to reduce regardless of failures`() = runTest {
            val g = group(tasksFailed = 8, phaseTotal = 10, failurePolicy = "BEST_EFFORT")
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.transitionPhase(
                    groupId = any(), expectedVersion = any(),
                    newPhase = any(), newPhaseTotal = any(),
                    tasks = any(), onCompleteHandler = any(),
                ),
            ).thenReturn(true)

            val result = handler.handle(ctx())

            assertInstanceOf(TaskResult.Success::class.java, result)
            verify(taskGroupRepository).transitionPhase(
                groupId = eq("g-1"),
                expectedVersion = eq(1L),
                newPhase = eq("reduce"),
                newPhaseTotal = eq(totalPartitions),
                tasks = any(),
                onCompleteHandler = eq("test.__phase_complete"),
            )
            verify(taskGroupRepository, never()).casGroupStatus(any(), any(), any(), any(), any())
        }
    }

    @Nested
    inner class `reduce phase` {

        @Test
        fun `marks group COMPLETED on reduce phase complete`() = runTest {
            val g = group(phase = "reduce")
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.casGroupStatus("g-1", GroupStatus.ACTIVE, GroupStatus.COMPLETED, 1L, null),
            ).thenReturn(true)

            val result = handler.handle(ctx())

            assertInstanceOf(TaskResult.Success::class.java, result)
            verify(taskGroupRepository).casGroupStatus("g-1", GroupStatus.ACTIVE, GroupStatus.COMPLETED, 1L, null)
        }
    }

    @Nested
    inner class `reduce task fan-out` {

        @Test
        fun `enqueues exactly totalPartitions reduce tasks`() = runTest {
            val g = group(tasksFailed = 0, failurePolicy = "FAIL_GROUP")
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.transitionPhase(
                    groupId = any(), expectedVersion = any(),
                    newPhase = any(), newPhaseTotal = any(),
                    tasks = any(), onCompleteHandler = any(),
                ),
            ).thenReturn(true)

            handler.handle(ctx())

            verify(taskGroupRepository).transitionPhase(
                groupId = any(),
                expectedVersion = any(),
                newPhase = any(),
                newPhaseTotal = eq(totalPartitions),
                tasks = argThat<List<EnqueueRequest>> { size == totalPartitions },
                onCompleteHandler = any(),
            )
        }

        @Test
        fun `reduce tasks have correct handler, queue, groupId, and maxRetries`() = runTest {
            val g = group(tasksFailed = 0, failurePolicy = "FAIL_GROUP")
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.transitionPhase(
                    groupId = any(), expectedVersion = any(),
                    newPhase = any(), newPhaseTotal = any(),
                    tasks = any(), onCompleteHandler = any(),
                ),
            ).thenReturn(true)

            handler.handle(ctx())

            verify(taskGroupRepository).transitionPhase(
                groupId = any(),
                expectedVersion = any(),
                newPhase = any(),
                newPhaseTotal = any(),
                tasks = argThat<List<EnqueueRequest>> {
                    all { task ->
                        task.handler == "test.reduce" &&
                            task.queue == queue &&
                            task.groupId == "g-1" &&
                            task.maxRetries == maxRetries
                    }
                },
                onCompleteHandler = any(),
            )
        }

        @Test
        fun `reduce tasks contain sequential partition_hash in metadata`() = runTest {
            val g = group(tasksFailed = 0, failurePolicy = "FAIL_GROUP")
            whenever(taskGroupRepository.findGroup("g-1")).thenReturn(g)
            whenever(
                taskGroupRepository.transitionPhase(
                    groupId = any(), expectedVersion = any(),
                    newPhase = any(), newPhaseTotal = any(),
                    tasks = any(), onCompleteHandler = any(),
                ),
            ).thenReturn(true)

            handler.handle(ctx())

            verify(taskGroupRepository).transitionPhase(
                groupId = any(),
                expectedVersion = any(),
                newPhase = any(),
                newPhaseTotal = any(),
                tasks = argThat<List<EnqueueRequest>> {
                    mapIndexed { index, task ->
                        task.metadata == """{"phase":"REDUCE","partition_hash":$index}"""
                    }.all { it }
                },
                onCompleteHandler = any(),
            )
        }
    }
}
