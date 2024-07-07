package service

import org.example.mockConstructor.config.client.MQClient
import org.example.mockConstructor.config.service.MQService
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mock
import org.mockito.Mockito.times
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import kotlin.test.Test

class MQServiceTest {
    @Mock
    lateinit var client: MQClient
    lateinit var svc: MQService

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        svc = MQService(client)
    }

    @Test
    fun `test client call`() {
        val msg = "test message"
        svc.sendMessage(msg)
        verify(client, times(1)).sendMessage(eq(msg))
    }
}
