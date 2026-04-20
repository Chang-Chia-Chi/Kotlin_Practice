package com.workflow.infrastructure.http

import io.ktor.client.HttpClient
import io.ktor.client.engine.java.Java
import io.ktor.client.plugins.HttpTimeout
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Disposes
import jakarta.enterprise.inject.Produces

class HttpClientProducer {
    @Produces
    @ApplicationScoped
    fun httpClient(): HttpClient = HttpClient(Java) {
        install(HttpTimeout) {
            connectTimeoutMillis = 2_000
            requestTimeoutMillis = 2_000
        }
    }

    fun close(@Disposes client: HttpClient) = client.close()
}
