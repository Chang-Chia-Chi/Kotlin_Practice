package usecase

import org.example.mockConstructor.config.service.MQService
import org.example.mockConstructor.config.usecase.SendUseCase
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations

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
        uc.run()

        verify(svc, times(1)).sendMessage(anyString())
    }

    @Test
    fun `should not send message when flag is false`() {
        uc.flag = false
        uc.run()

        verify(svc, never()).sendMessage(anyString())
    }
}
