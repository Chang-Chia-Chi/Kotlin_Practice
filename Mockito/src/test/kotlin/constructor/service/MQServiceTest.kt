package constructor.service

import org.example.mockConstructor.config.constructor.client.MQClient
import org.example.mockConstructor.config.constructor.service.SendMQService
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
    lateinit var svc: SendMQService

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        svc = SendMQService(client)
    }

    @Test
    fun `test client call`() {
        val msg = "test message"
        svc.sendMessage(msg)
        verify(client, times(1)).sendMessage(eq(msg))
    }
}
