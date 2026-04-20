package com.workflow.infrastructure.queryexporter

import com.workflow.infrastructure.leader.LeaderManager
import com.workflow.infrastructure.queryexporter.adapter.LeaderManagerGuardAdapter
import com.workflow.infrastructure.queryexporter.adapter.QuarkusDataSourceProvider
import com.workflow.infrastructure.queryexporter.spi.LeaderGuard
import io.agroal.api.AgroalDataSource
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import kotlin.test.assertSame

class QuarkusAdapterTest {

    // ==========================================================================
    // 1. QuarkusDataSourceProvider — default datasource branch
    // ==========================================================================

    @Nested
    inner class QuarkusDataSourceProviderTest {

        private val mockDataSource: AgroalDataSource = mock()
        private val provider = QuarkusDataSourceProvider(mockDataSource)

        @Test
        fun `resolve with 'default' returns injected datasource`() {
            val result = provider.resolve("default")
            assertSame(mockDataSource, result)
        }

        @Test
        fun `resolve with empty string returns injected datasource`() {
            val result = provider.resolve("")
            assertSame(mockDataSource, result)
        }

        @Test
        fun `resolve with blank string returns injected datasource`() {
            val result = provider.resolve("   ")
            assertSame(mockDataSource, result)
        }
    }

    // ==========================================================================
    // 2. LeaderManagerGuardAdapter — delegates to LeaderManager.leaderState
    // ==========================================================================

    @Nested
    inner class LeaderManagerGuardAdapterTest {

        @Test
        fun `leaderState delegates to LeaderManager leaderState`() = runTest {
            val flow = MutableStateFlow(false)
            val mockManager: LeaderManager = mock()
            whenever(mockManager.leaderState).thenReturn(flow.asStateFlow())

            val adapter = LeaderManagerGuardAdapter(mockManager)
            val result: StateFlow<Boolean> = adapter.leaderState

            assertSame(mockManager.leaderState, result, "Adapter should return the same StateFlow from LeaderManager")
        }

        @Test
        fun `isLeader reflects current leaderState value`() = runTest {
            val flow = MutableStateFlow(false)
            val mockManager: LeaderManager = mock()
            whenever(mockManager.leaderState).thenReturn(flow.asStateFlow())

            val guard: LeaderGuard = LeaderManagerGuardAdapter(mockManager)

            kotlin.test.assertFalse(guard.isLeader, "Should not be leader initially")

            flow.value = true
            kotlin.test.assertTrue(guard.isLeader, "Should be leader after state flip")
        }
    }
}
