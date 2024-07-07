package client

import org.example.mockConstructor.config.client.MQClient
import org.example.mockConstructor.config.client.MQContext
import org.example.mockConstructor.config.config.MQConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.MockedConstruction
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify

class MQClientTest {
    lateinit var mQConfig: MQConfig
    lateinit var client: MQClient
    lateinit var context: MQContext
    lateinit var mockConstructor: MockedConstruction<MQContext>

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        mockConstructor =
            Mockito.mockConstruction(MQContext::class.java) { mocked, ctx ->
                context = mocked
            }
        mQConfig = MQConfig(event = "test_event")
        client = MQClient(mQConfig)
    }

    @AfterEach
    fun tearDown() {
        mockConstructor.close()
    }

    @Test
    fun `mock context`() {
        val msg = "test message"
        client.sendMessage(msg)
        verify(context, times(1)).sendMessage(eq(msg), eq("test_event"))
    }
}
