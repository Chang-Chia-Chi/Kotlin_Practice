package com.mapreduce.leader

import io.quarkus.scheduler.ScheduledExecution
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class NotLeaderTest {

    private val leaderManager: LeaderManager = mock()
    private val execution: ScheduledExecution = mock()
    private val notLeader = NotLeader(leaderManager)

    @Test
    fun `skips execution when not leader`() {
        whenever(leaderManager.isActive).thenReturn(false)
        assertTrue(notLeader.test(execution))
    }

    @Test
    fun `allows execution when leader`() {
        whenever(leaderManager.isActive).thenReturn(true)
        assertFalse(notLeader.test(execution))
    }
}
