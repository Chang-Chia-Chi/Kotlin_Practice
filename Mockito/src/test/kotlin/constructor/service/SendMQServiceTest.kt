package constructor.service

import org.example.mockConstructor.config.constructor.client.MQClient
import org.example.mockConstructor.config.constructor.config.MyConfig
import org.example.mockConstructor.config.constructor.service.SendMQService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mock
import org.mockito.MockedConstruction
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import kotlin.test.Test

class SendMQServiceTest {
    lateinit var svc: SendMQService
    lateinit var client: MQClient
    lateinit var mockConstructor: MockedConstruction<MQClient>

    @Mock
    lateinit var config: MyConfig

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        mockConstructor =
            Mockito
                .mockConstruction(MQClient::class.java) { mocked, ctx ->
                    client = mocked
                }
        svc = SendMQService(config)
    }

    @AfterEach
    fun tearDown() {
        mockConstructor.close()
    }

    @Test
    fun `test correct config`() {
        val myConfig = MyConfig()
        myConfig.pwd = "mockito_test"
        myConfig.url = "localhost_test"
    }

    @Test
    fun `test client call`() {
        val msg = "test message"
        svc.sendMessage(msg)
        verify(client, times(1)).sendMessage(eq(msg))
    }
}
