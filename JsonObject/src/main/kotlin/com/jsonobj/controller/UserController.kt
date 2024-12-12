package com.jsonobj.controller

import com.jsonobj.extension.serializeToMap
import com.jsonobj.model.Address
import com.jsonobj.model.User
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class UserController {
    @GetMapping("/user")
    fun getUser(): User {
        val user =
            User(
                id = 1,
                name = "John Doe",
                email = null,
                address = Address(street = null, city = "Metropolis", country = null).serializeToMap(),
                tags = listOf(),
            )
        return user
    }
}
