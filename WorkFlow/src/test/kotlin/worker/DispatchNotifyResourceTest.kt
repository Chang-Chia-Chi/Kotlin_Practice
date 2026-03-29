package com.workflow.worker

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify

class DispatchNotifyResourceTest {

    private lateinit var notifier: DispatchNotifier
    private lateinit var resource: DispatchNotifyResource

    @BeforeEach
    fun setup() {
        notifier = mock()
        resource = DispatchNotifyResource(notifier)
    }

    @Test
    fun `POST with queue param calls onRemoteSignal with that queue`() {
        resource.notify("myqueue")

        verify(notifier).onRemoteSignal("myqueue")
    }

    @Test
    fun `POST with default queue calls onRemoteSignal with default`() {
        resource.notify("default")

        verify(notifier).onRemoteSignal("default")
    }
}
