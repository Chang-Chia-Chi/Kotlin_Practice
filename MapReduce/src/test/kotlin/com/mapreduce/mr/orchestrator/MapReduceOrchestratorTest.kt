package com.mapreduce.mr.orchestrator

import com.mapreduce.config.FrameworkConfig
import com.mapreduce.leader.LeaderManager
import com.mapreduce.mr.model.FailurePolicy
import com.mapreduce.mr.model.Job
import com.mapreduce.mr.model.JobStatus
import com.mapreduce.mr.registry.MapReduceRegistrar
import com.mapreduce.mr.repository.JobRepository
import com.mapreduce.mr.spi.MapReduceDefinition
import com.mapreduce.queue.model.Task
import com.mapreduce.queue.model.TaskStatus
import com.mapreduce.queue.repository.TaskRepository
import com.mapreduce.shutdown.ShutdownCoordinator
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.quarkus.runtime.StartupEvent
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.any
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.TimeUnit

class MapReduceOrchestratorTest {

    private lateinit var config: FrameworkConfig
    private lateinit var leaderConfig: FrameworkConfig.LeaderConfig
    private lateinit var jobRepository: JobRepository
    private lateinit var taskRepository: TaskRepository
    private lateinit var registrar: MapReduceRegistrar
    private lateinit var leaderManager: LeaderManager
    private lateinit var shutdownCoordinator: ShutdownCoordinator
    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var orchestrator: MapReduceOrchestrator

    @BeforeEach
    fun setUp() {
        config = mock()
        leaderConfig = mock()
        whenever(config.leader()).thenReturn(leaderConfig)
        whenever(leaderConfig.monitorInterval()).thenReturn(Duration.ofMillis(50))

        jobRepository = mock()
        taskRepository = mock()
        registrar = mock()
        leaderManager = mock()
        shutdownCoordinator = mock()
        meterRegistry = SimpleMeterRegistry()

        doNothing().whenever(shutdownCoordinator).registerLeaderScopeCallback(any())

        whenever(leaderManager.isActive).thenReturn(true)
        whenever(leaderManager.token).thenReturn(1L)

        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(emptyList())
        whenever(jobRepository.findJobsByStatus(JobStatus.REDUCING)).thenReturn(emptyList())

        orchestrator = MapReduceOrchestrator(
            config, jobRepository, taskRepository, registrar,
            leaderManager, shutdownCoordinator, meterRegistry,
        )
    }

    @Test
    fun `RUNNING job with all tasks completed transitions to REDUCING and dispatches reduce`() {
        val job = runningJob(
            jobId = "j-1", completedTasks = 10, totalTasks = 10,
            failurePolicy = FailurePolicy.FAIL_JOB,
        )
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(listOf(job))
        whenever(taskRepository.countByGroupAndStatus("j-1", TaskStatus.DEAD_LETTER)).thenReturn(0)

        val definition: MapReduceDefinition<*, *, *, *> = mock()
        whenever(definition.maxRetries).thenReturn(3)
        whenever(definition.queue).thenReturn("mr")
        whenever(registrar.getDefinition("wc")).thenReturn(definition)
        whenever(jobRepository.transitionToReducing("j-1", 0, "wc", 3, "mr", 1)).thenReturn(true)

        startAndAwait {
            verify(jobRepository).transitionToReducing("j-1", 0, "wc", 3, "mr", 1)
        }
    }

    @Test
    fun `RUNNING job with FAIL_JOB policy and dead-lettered tasks transitions to FAILED`() {
        val job = runningJob(
            jobId = "j-fail", completedTasks = 9, totalTasks = 10,
            failurePolicy = FailurePolicy.FAIL_JOB,
        )
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(listOf(job))
        whenever(taskRepository.countByGroupAndStatus("j-fail", TaskStatus.DEAD_LETTER)).thenReturn(1)
        whenever(jobRepository.casJobStatus("j-fail", JobStatus.RUNNING, JobStatus.FAILED, 0)).thenReturn(true)

        startAndAwait {
            verify(jobRepository).casJobStatus("j-fail", JobStatus.RUNNING, JobStatus.FAILED, 0)
            verify(jobRepository, never()).insertReduceTasks(any(), any(), any(), any(), any())
        }
    }

    @Test
    fun `RUNNING job with BEST_EFFORT policy and dead-lettered tasks transitions to REDUCING`() {
        val job = runningJob(
            jobId = "j-be", completedTasks = 7, totalTasks = 10,
            failurePolicy = FailurePolicy.BEST_EFFORT,
        )
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(listOf(job))
        whenever(taskRepository.countByGroupAndStatus("j-be", TaskStatus.DEAD_LETTER)).thenReturn(3)

        val definition: MapReduceDefinition<*, *, *, *> = mock()
        whenever(definition.maxRetries).thenReturn(3)
        whenever(definition.queue).thenReturn("mr")
        whenever(registrar.getDefinition("wc")).thenReturn(definition)
        whenever(jobRepository.transitionToReducing("j-be", 0, "wc", 3, "mr", 1)).thenReturn(true)

        startAndAwait {
            verify(jobRepository).transitionToReducing("j-be", 0, "wc", 3, "mr", 1)
        }
    }

    @Test
    fun `RUNNING job with THRESHOLD policy and rate below threshold transitions to REDUCING`() {
        val job = runningJob(
            jobId = "j-th-ok", completedTasks = 8, totalTasks = 10,
            failurePolicy = FailurePolicy.THRESHOLD, failureThreshold = 0.5,
        )
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(listOf(job))
        whenever(taskRepository.countByGroupAndStatus("j-th-ok", TaskStatus.DEAD_LETTER)).thenReturn(2)

        val definition: MapReduceDefinition<*, *, *, *> = mock()
        whenever(definition.maxRetries).thenReturn(3)
        whenever(definition.queue).thenReturn("mr")
        whenever(registrar.getDefinition("wc")).thenReturn(definition)
        whenever(jobRepository.transitionToReducing("j-th-ok", 0, "wc", 3, "mr", 1)).thenReturn(true)

        startAndAwait {
            verify(jobRepository).transitionToReducing("j-th-ok", 0, "wc", 3, "mr", 1)
        }
    }

    @Test
    fun `RUNNING job with THRESHOLD policy and rate above threshold transitions to FAILED`() {
        val job = runningJob(
            jobId = "j-th-bad", completedTasks = 3, totalTasks = 10,
            failurePolicy = FailurePolicy.THRESHOLD, failureThreshold = 0.5,
        )
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(listOf(job))
        whenever(taskRepository.countByGroupAndStatus("j-th-bad", TaskStatus.DEAD_LETTER)).thenReturn(7)
        whenever(jobRepository.casJobStatus("j-th-bad", JobStatus.RUNNING, JobStatus.FAILED, 0))
            .thenReturn(true).thenReturn(false)

        startAndAwait {
            verify(jobRepository, atLeast(1)).casJobStatus("j-th-bad", JobStatus.RUNNING, JobStatus.FAILED, 0)
        }
    }

    @Test
    fun `REDUCING job with all reduce tasks completed transitions to COMPLETED`() {
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(emptyList())

        val job = reducingJob(jobId = "j-done")
        whenever(jobRepository.findJobsByStatus(JobStatus.REDUCING)).thenReturn(listOf(job))

        val completedTask = task("rt-1", "wc.reduce", "j-done", TaskStatus.COMPLETED)
        whenever(taskRepository.findAllByGroupAndHandler("j-done", "wc.reduce")).thenReturn(listOf(completedTask))
        whenever(jobRepository.casJobStatus("j-done", JobStatus.REDUCING, JobStatus.COMPLETED, 0))
            .thenReturn(true).thenReturn(false)

        startAndAwait {
            verify(jobRepository, atLeast(1)).casJobStatus("j-done", JobStatus.REDUCING, JobStatus.COMPLETED, 0)
        }
    }

    @Test
    fun `REDUCING job with no reduce tasks dispatches recovery reduce`() {
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(emptyList())

        val job = reducingJob(jobId = "j-recover")
        whenever(jobRepository.findJobsByStatus(JobStatus.REDUCING)).thenReturn(listOf(job))
        whenever(taskRepository.findAllByGroupAndHandler("j-recover", "wc.reduce")).thenReturn(emptyList())

        val definition: MapReduceDefinition<*, *, *, *> = mock()
        whenever(definition.maxRetries).thenReturn(3)
        whenever(definition.queue).thenReturn("mr")
        whenever(registrar.getDefinition("wc")).thenReturn(definition)

        startAndAwait {
            verify(jobRepository).insertReduceTasks("j-recover", "wc", 3, "mr", 1)
        }
    }

    @Test
    fun `REDUCING job with dead-lettered reduce task transitions to FAILED`() {
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(emptyList())

        val job = reducingJob(jobId = "j-rfail")
        whenever(jobRepository.findJobsByStatus(JobStatus.REDUCING)).thenReturn(listOf(job))

        val deadTask = task("rt-d", "wc.reduce", "j-rfail", TaskStatus.DEAD_LETTER)
        whenever(taskRepository.findAllByGroupAndHandler("j-rfail", "wc.reduce")).thenReturn(listOf(deadTask))
        whenever(jobRepository.casJobStatus("j-rfail", JobStatus.REDUCING, JobStatus.FAILED, 0))
            .thenReturn(true).thenReturn(false)

        startAndAwait {
            verify(jobRepository, atLeast(1)).casJobStatus("j-rfail", JobStatus.REDUCING, JobStatus.FAILED, 0)
        }
    }

    @Test
    fun `barrier not met does not trigger any transition`() {
        val job = runningJob(
            jobId = "j-wait", completedTasks = 3, totalTasks = 10,
            failurePolicy = FailurePolicy.FAIL_JOB,
        )
        whenever(jobRepository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(listOf(job))
        whenever(taskRepository.countByGroupAndStatus("j-wait", TaskStatus.DEAD_LETTER)).thenReturn(0)

        startAndAwait {
            verify(jobRepository).findJobsByStatus(JobStatus.RUNNING)
        }

        verify(jobRepository, never()).casJobStatus(any(), any(), any(), any())
        verify(jobRepository, never()).transitionToReducing(any(), any(), any(), any(), any(), any())
        verify(jobRepository, never()).insertReduceTasks(any(), any(), any(), any(), any())
    }

    // ── Helpers ──────────────────────────────────────────────────

    private fun startAndAwait(assertions: () -> Unit) {
        val startupEvent = mock<StartupEvent>()
        orchestrator.onStart(startupEvent)

        await.atMost(3, TimeUnit.SECONDS).untilAsserted(assertions)
    }

    private fun runningJob(
        jobId: String,
        completedTasks: Int,
        totalTasks: Int,
        failurePolicy: FailurePolicy,
        failureThreshold: Double = 0.0,
    ) = Job(
        jobId = jobId,
        jobType = "wc",
        status = JobStatus.RUNNING,
        jobParams = "{}",
        totalTasks = totalTasks,
        completedTasks = completedTasks,
        failedTasks = 0,
        failurePolicy = failurePolicy,
        failureThreshold = failureThreshold,
        totalPartitions = 1,
        version = 0,
    )

    private fun reducingJob(jobId: String) = Job(
        jobId = jobId,
        jobType = "wc",
        status = JobStatus.REDUCING,
        jobParams = "{}",
        totalTasks = 10,
        completedTasks = 10,
        failedTasks = 0,
        failurePolicy = FailurePolicy.FAIL_JOB,
        totalPartitions = 1,
        version = 0,
    )

    private fun task(taskId: String, handler: String, groupId: String, status: TaskStatus) = Task(
        taskId = taskId,
        handler = handler,
        payload = "{}",
        status = status,
        groupId = groupId,
    )
}
