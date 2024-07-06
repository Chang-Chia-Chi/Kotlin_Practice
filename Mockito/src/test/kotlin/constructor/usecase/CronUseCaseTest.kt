package constructor.usecase

import org.example.mockConstructor.config.constructor.service.SendMQService
import org.example.mockConstructor.config.constructor.usecase.CronUseCase
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.eq

class CronUseCaseTest {
    @Mock
    lateinit var svc: SendMQService
    lateinit var uc: CronUseCase

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
        uc = CronUseCase(svc)
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
