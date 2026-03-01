package com.taskqueue.housekeeping

import com.taskqueue.election.LeaderElectionService
import com.taskqueue.queue.TaskQueueDao
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.MutableStateFlow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class LeaderCronJobsTest {

    private lateinit var dao: TaskQueueDao
    private lateinit var leaderElection: LeaderElectionService
    private lateinit var taskProducer: TaskProducer
    private lateinit var cronJobs: LeaderCronJobs
    private val isLeader = MutableStateFlow(false)

    @BeforeEach
    fun setUp() {
        dao = mockk(relaxed = true)
        leaderElection = mockk()
        every { leaderElection.isLeader } returns isLeader
        taskProducer = mockk(relaxed = true)

        cronJobs = LeaderCronJobs(
            dao = dao,
            leaderElection = leaderElection,
            taskProducer = taskProducer,
            staleMinutes = 5,
            retentionDays = 7,
        )
    }

    // ── promoteScheduledTasks ──

    @Test
    fun `promoteScheduledTasks runs when leader`() {
        isLeader.value = true
        every { dao.promoteScheduledTasks() } returns 3

        cronJobs.promoteScheduledTasks()

        verify(exactly = 1) { dao.promoteScheduledTasks() }
    }

    @Test
    fun `promoteScheduledTasks skips when not leader`() {
        isLeader.value = false

        cronJobs.promoteScheduledTasks()

        verify(exactly = 0) { dao.promoteScheduledTasks() }
    }

    @Test
    fun `promoteScheduledTasks handles exception gracefully`() {
        isLeader.value = true
        every { dao.promoteScheduledTasks() } throws RuntimeException("DB error")

        cronJobs.promoteScheduledTasks() // should not throw
    }

    // ── reclaimStaleTasks ──

    @Test
    fun `reclaimStaleTasks runs when leader with configured stale minutes`() {
        isLeader.value = true
        every { dao.reclaimStaleTasks(5) } returns 2

        cronJobs.reclaimStaleTasks()

        verify { dao.reclaimStaleTasks(5) }
    }

    @Test
    fun `reclaimStaleTasks skips when not leader`() {
        isLeader.value = false

        cronJobs.reclaimStaleTasks()

        verify(exactly = 0) { dao.reclaimStaleTasks(any()) }
    }

    @Test
    fun `reclaimStaleTasks handles exception gracefully`() {
        isLeader.value = true
        every { dao.reclaimStaleTasks(any()) } throws RuntimeException("DB error")

        cronJobs.reclaimStaleTasks() // should not throw
    }

    // ── expireOverdueTasks ──

    @Test
    fun `expireOverdueTasks runs when leader`() {
        isLeader.value = true
        every { dao.expireOverdueTasks() } returns 1

        cronJobs.expireOverdueTasks()

        verify(exactly = 1) { dao.expireOverdueTasks() }
    }

    @Test
    fun `expireOverdueTasks skips when not leader`() {
        isLeader.value = false

        cronJobs.expireOverdueTasks()

        verify(exactly = 0) { dao.expireOverdueTasks() }
    }

    // ── purgeOldTasks ──

    @Test
    fun `purgeOldTasks runs when leader with configured retention`() {
        isLeader.value = true
        every { dao.purgeOldTasks(7) } returns 100

        cronJobs.purgeOldTasks()

        verify { dao.purgeOldTasks(7) }
    }

    @Test
    fun `purgeOldTasks skips when not leader`() {
        isLeader.value = false

        cronJobs.purgeOldTasks()

        verify(exactly = 0) { dao.purgeOldTasks(any()) }
    }

    // ── logQueueMetrics ──

    @Test
    fun `logQueueMetrics runs when leader`() {
        isLeader.value = true
        every { dao.countByStatus() } returns mapOf("PENDING" to 5L, "DONE" to 10L)

        cronJobs.logQueueMetrics()

        verify(exactly = 1) { dao.countByStatus() }
    }

    @Test
    fun `logQueueMetrics skips when not leader`() {
        isLeader.value = false

        cronJobs.logQueueMetrics()

        verify(exactly = 0) { dao.countByStatus() }
    }

    // ── produceTasks ──

    @Test
    fun `produceTasks delegates to TaskProducer`() {
        cronJobs.produceTasks()

        verify(exactly = 1) { taskProducer.produceAll() }
    }
}
