package com.workflow.infrastructure.http

import io.ktor.client.HttpClient
import io.ktor.client.engine.java.Java
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Disposes
import jakarta.enterprise.inject.Produces

class HttpClientProducer {
    @Produces
    @ApplicationScoped
    fun httpClient(): HttpClient = HttpClient(Java)

    fun close(@Disposes client: HttpClient) = client.close()
}
