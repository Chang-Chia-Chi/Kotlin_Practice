package org.example.mockConstructor.config.constructor.service

import org.example.mockConstructor.config.constructor.client.MQClient

class ReceiveMQService(
    override val client: MQClient,
) : MQService(client)
