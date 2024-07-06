package constructor.client

import org.example.mockConstructor.config.constructor.client.MQClient
import org.example.mockConstructor.config.constructor.client.MQContext
import org.example.mockConstructor.config.constructor.config.MQConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.MockedConstruction
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify

class MQClientTest {
    @Mock
    lateinit var MQConfig: MQConfig
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
        client = MQClient(MQConfig)
    }

    @AfterEach
    fun tearDown() {
        mockConstructor.close()
    }

    @Test
    fun `mock context`() {
        val msg = "test message"
        client.sendMessage(msg)
        verify(context, times(1)).sendMessage(eq(msg))
    }
}
