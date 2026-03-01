package jms

import jakarta.jms.JMSConsumer
import jakarta.jms.JMSContext

data class JmsListenerContext(
    val context: JMSContext,
    val consumer: JMSConsumer,
) {
    fun safeClose() {
        try {
            consumer.close()
        } catch (e: Exception) {
        }
        try {
            context.close()
        } catch (e: Exception) {
        }
    }
}
