package jms

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class JmsContextConfig(
    val queueName: String,
    val reconnectionTimeout: Duration = 30.seconds,
    val maxReconnectAttempts: Int = Int.MAX_VALUE,
    val connectionTimeout: Long = 5000,
    val enableAutoReconnect: Boolean = true,
)
