package com.workflow.infrastructure.shutdown

import java.time.Duration

interface ShutdownParticipant {

    val shutdownOrder: Int

    val shutdownTimeout: Duration

    suspend fun shutdown()
}
