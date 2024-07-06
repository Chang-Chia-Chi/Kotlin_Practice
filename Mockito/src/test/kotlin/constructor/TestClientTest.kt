package constructor

import org.example.mockConstructor.config.constructor.TestClient
import org.example.mockConstructor.config.constructor.TestContext
import org.example.mockConstructor.config.constructor.config.Config
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

class TestClientTest {
    @Mock
    lateinit var config: Config
    lateinit var client: TestClient
    lateinit var context: TestContext
    lateinit var mockConstructor: MockedConstruction<TestContext>

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        mockConstructor =
            Mockito.mockConstruction(TestContext::class.java) { mocked, ctx ->
                context = mocked
            }
        client = TestClient(config)
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
