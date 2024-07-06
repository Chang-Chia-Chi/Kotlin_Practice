package constructor.usecase

import org.example.mockConstructor.config.constructor.service.MQService
import org.example.mockConstructor.config.constructor.usecase.SendUseCase
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq

class SendUseCaseTest {
    @Mock
    lateinit var svc: MQService
    lateinit var uc: SendUseCase

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        uc = SendUseCase(svc)
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
