package com.workflow.worker

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

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

    @Test
    fun `notify method queue parameter has DefaultValue annotation with default`() {
        val method = DispatchNotifyResource::class.java.getMethod("notify", String::class.java)
        val paramAnnotations = method.parameterAnnotations[0]
        val defaultValue = paramAnnotations.filterIsInstance<jakarta.ws.rs.DefaultValue>().firstOrNull()
        assertNotNull(defaultValue, "queue param should have @DefaultValue")
        assertEquals("default", defaultValue.value, "@DefaultValue should be 'default'")
    }
}
