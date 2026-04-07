package com.workflow.dispatch.usecase.service.handler

import jakarta.enterprise.context.ApplicationScoped
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

private val BATCH_TOKEN_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

fun currentBatchToken(now: LocalDateTime = LocalDateTime.now()): String =
    now.truncatedTo(ChronoUnit.HOURS).format(BATCH_TOKEN_FORMAT)

fun interface BatchTokenClock {
    fun generate(): String
}

@ApplicationScoped
class SystemBatchTokenClock : BatchTokenClock {
    override fun generate(): String = currentBatchToken()
}
