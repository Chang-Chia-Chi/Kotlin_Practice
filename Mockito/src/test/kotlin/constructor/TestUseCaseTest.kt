package constructor

import org.example.mockConstructor.config.constructor.TestService
import org.example.mockConstructor.config.constructor.TestUseCase
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq

class TestUseCaseTest {
    @Mock
    lateinit var svc: TestService
    lateinit var uc: TestUseCase

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        uc = TestUseCase(svc)
    }

    @Test
    fun `should send message when flag is true`() {
        val msg = "test_message"
        uc.run(msg)

        verify(svc, times(1)).sendMessage(eq(msg))
    }

    @Test
    fun `should not send message when flag is false`() {
        val msg = "test_message"
        uc.flag = false
        uc.run(msg)

        verify(svc, never()).sendMessage(anyString())
    }
}
