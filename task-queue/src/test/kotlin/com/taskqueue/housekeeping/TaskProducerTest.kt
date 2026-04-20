package com.taskqueue.housekeeping

import com.taskqueue.election.LeaderElectionService
import com.taskqueue.queue.TaskQueueDao
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.flow.MutableStateFlow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class TaskProducerTest {

    private lateinit var dao: TaskQueueDao
    private lateinit var leaderElection: LeaderElectionService
    private lateinit var jobs: Instance<TaskProducerJob>
    private val isLeader = MutableStateFlow(false)

    @BeforeEach
    fun setUp() {
        dao = mockk(relaxed = true)
        leaderElection = mockk()
        every { leaderElection.isLeader } returns isLeader
        jobs = mockk()
    }

    private fun createProducer(vararg producerJobs: TaskProducerJob): TaskProducer {
        every { jobs.iterator() } returns producerJobs.toMutableList().iterator()
        return TaskProducer(dao, leaderElection, jobs)
    }

    @Test
    fun `produceAll inserts root tasks when leader`() {
        isLeader.value = true
        val job = object : TaskProducerJob {
            override val name = "test-job"
            override fun produce() = listOf(
                RootTaskRequest(taskType = "TYPE_A", priority = 3),
                RootTaskRequest(taskType = "TYPE_B", payload = "data"),
            )
        }
        every { dao.insertRootTask(any(), any(), any(), any()) } returns 1L

        val producer = createProducer(job)
        producer.produceAll()

        verify(exactly = 1) { dao.insertRootTask("TYPE_A", null, 3, null) }
        verify(exactly = 1) { dao.insertRootTask("TYPE_B", "data", 5, null) }
    }

    @Test
    fun `produceAll skips when not leader`() {
        isLeader.value = false
        val job = object : TaskProducerJob {
            override val name = "test-job"
            override fun produce() = listOf(RootTaskRequest(taskType = "TYPE_A"))
        }

        val producer = createProducer(job)
        producer.produceAll()

        verify(exactly = 0) { dao.insertRootTask(any(), any(), any(), any()) }
    }

    @Test
    fun `produceAll uses unique insert for tasks with uniqueKey`() {
        isLeader.value = true
        val uniqueKey = TaskQueueDao.generateUniqueKey("DEDUP", null)
        val job = object : TaskProducerJob {
            override val name = "dedup-job"
            override fun produce() = listOf(
                RootTaskRequest(taskType = "DEDUP", uniqueKey = uniqueKey),
            )
        }
        every { dao.insertRootTaskUnique(any(), any(), any(), any(), any()) } returns 1L

        val producer = createProducer(job)
        producer.produceAll()

        verify { dao.insertRootTaskUnique("DEDUP", null, 5, null, uniqueKey) }
        verify(exactly = 0) { dao.insertRootTask(any(), any(), any(), any()) }
    }

    @Test
    fun `produceAll handles null from unique insert (duplicate skipped)`() {
        isLeader.value = true
        val job = object : TaskProducerJob {
            override val name = "dup-job"
            override fun produce() = listOf(
                RootTaskRequest(taskType = "DUP", uniqueKey = "key123"),
            )
        }
        every { dao.insertRootTaskUnique(any(), any(), any(), any(), any()) } returns null

        val producer = createProducer(job)
        producer.produceAll() // should not throw
    }

    @Test
    fun `produceAll continues with other jobs when one fails`() {
        isLeader.value = true
        val failingJob = object : TaskProducerJob {
            override val name = "failing"
            override fun produce(): List<RootTaskRequest> = throw RuntimeException("broken")
        }
        val goodJob = object : TaskProducerJob {
            override val name = "good"
            override fun produce() = listOf(RootTaskRequest(taskType = "OK"))
        }
        every { dao.insertRootTask(any(), any(), any(), any()) } returns 1L

        val producer = createProducer(failingJob, goodJob)
        producer.produceAll()

        verify(exactly = 1) { dao.insertRootTask("OK", null, 5, null) }
    }

    @Test
    fun `produceAll with no jobs is a no-op`() {
        isLeader.value = true

        val producer = createProducer() // no jobs
        producer.produceAll()

        verify(exactly = 0) { dao.insertRootTask(any(), any(), any(), any()) }
    }
}
