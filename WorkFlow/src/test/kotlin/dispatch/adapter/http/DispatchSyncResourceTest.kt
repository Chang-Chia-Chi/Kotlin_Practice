package com.workflow.dispatch.adapter.http

import com.workflow.dispatch.adapter.persistence.SyncRepository
import com.workflow.dispatch.adapter.persistence.SyncResult
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DispatchSyncResourceTest {

    @Test
    fun `sync endpoint delegates to SyncRepository and maps response`() = runTest {
        val syncRepo = mock<SyncRepository>()
        whenever(syncRepo.syncFromProd(listOf("cfg1", "cfg2"))).thenReturn(
            SyncResult(listOf("cfg1", "cfg2"), 5, 120),
        )

        val resource = DispatchSyncResource(syncRepo)
        val response = resource.sync(SyncRequest(configIds = listOf("cfg1", "cfg2")))

        assertEquals(listOf("cfg1", "cfg2"), response.syncedConfigs)
        assertEquals(5, response.batchesCopied)
        assertEquals(120, response.eventsCopied)
        verify(syncRepo).syncFromProd(listOf("cfg1", "cfg2"))
    }

    @Test
    fun `sync endpoint maps single config correctly`() = runTest {
        val syncRepo = mock<SyncRepository>()
        whenever(syncRepo.syncFromProd(listOf("only-one"))).thenReturn(
            SyncResult(listOf("only-one"), 1, 3),
        )

        val resource = DispatchSyncResource(syncRepo)
        val response = resource.sync(SyncRequest(configIds = listOf("only-one")))

        assertEquals(listOf("only-one"), response.syncedConfigs)
        assertEquals(1, response.batchesCopied)
        assertEquals(3, response.eventsCopied)
    }

    @Test
    fun `sync endpoint maps zero-result correctly`() = runTest {
        val syncRepo = mock<SyncRepository>()
        whenever(syncRepo.syncFromProd(listOf("no-match"))).thenReturn(
            SyncResult(listOf("no-match"), 0, 0),
        )

        val resource = DispatchSyncResource(syncRepo)
        val response = resource.sync(SyncRequest(configIds = listOf("no-match")))

        assertEquals(listOf("no-match"), response.syncedConfigs)
        assertEquals(0, response.batchesCopied)
        assertEquals(0, response.eventsCopied)
    }

    @Test
    fun `sync endpoint propagates repository exceptions`() = runTest {
        val syncRepo = mock<SyncRepository>()
        whenever(syncRepo.syncFromProd(listOf("cfg1"))).thenThrow(RuntimeException("db connection lost"))

        val resource = DispatchSyncResource(syncRepo)

        assertFailsWith<RuntimeException>("db connection lost") {
            resource.sync(SyncRequest(configIds = listOf("cfg1")))
        }
    }
}
