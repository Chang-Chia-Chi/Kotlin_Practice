package com.workflow.worker

import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces

class WebClientProducer {
    @Produces
    @ApplicationScoped
    fun webClient(vertx: Vertx): WebClient = WebClient.create(vertx)
}
