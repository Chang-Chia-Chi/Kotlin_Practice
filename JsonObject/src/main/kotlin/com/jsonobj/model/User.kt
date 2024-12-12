package com.jsonobj.model

data class Address(
    val street: String?,
    val city: String?,
    val country: String?,
)

data class User(
    val id: Int,
    val name: String,
    val email: String?,
    val address: Map<String, Any?>,
    val tags: List<String?>?,
)
