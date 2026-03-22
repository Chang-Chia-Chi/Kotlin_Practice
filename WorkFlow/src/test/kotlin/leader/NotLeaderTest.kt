package com.workflow.leader

import io.quarkus.scheduler.ScheduledExecution
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NotLeaderTest {

    @Test
    fun `test returns false when leader is active`() {
        val leaderElection = mock<LeaderElection>()
        whenever(leaderElection.isActive).thenReturn(true)
        val notLeader = NotLeader(leaderElection)

        assertFalse(notLeader.test(mock<ScheduledExecution>()))
    }

    @Test
    fun `test returns true when leader is not active`() {
        val leaderElection = mock<LeaderElection>()
        whenever(leaderElection.isActive).thenReturn(false)
        val notLeader = NotLeader(leaderElection)

        assertTrue(notLeader.test(mock<ScheduledExecution>()))
    }
}
