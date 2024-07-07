package client

import org.example.mockConstructor.config.client.MQClient
import org.example.mockConstructor.config.client.MQContext
import org.example.mockConstructor.config.config.MQConfig
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito.times
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify

class MQClientTest {
    @Mock
    lateinit var context: MQContext

    lateinit var mQConfig: MQConfig
    lateinit var client: MQClient

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        mQConfig = MQConfig(event = "test_event")
        client = MQClient(mQConfig, context)
    }

    @Test
    fun `mock context`() {
        val msg = "test message"
        client.sendMessage(msg)
        verify(context, times(1)).sendMessage(eq(msg), eq("test_event"))
    }
}
