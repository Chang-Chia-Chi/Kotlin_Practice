package org.example.mockConstructor.config.constructor

class TestUseCase(
    val svc: TestService,
) {
    var flag: Boolean = true

    fun run(msg: String) =
        when (flag) {
            true -> svc.sendMessage((msg))
            false -> println("Do nothing...")
        }
}
