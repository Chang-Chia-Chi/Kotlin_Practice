package com.mapreduce.extension

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jdbi.v3.core.HandleCallback
import org.jdbi.v3.core.HandleConsumer
import org.jdbi.v3.core.Jdbi

/**
 * Suspending extensions for [Jdbi] that dispatch blocking JDBC calls to [Dispatchers.IO].
 *
 * Usage in repositories:
 * ```kotlin
 * suspend fun findById(id: String): Entity? = jdbi.withHandleSuspend { h ->
 *     h.createQuery("SELECT * FROM entity WHERE id = :id")
 *         .bind("id", id)
 *         .mapTo(Entity::class.java)
 *         .findOne().orElse(null)
 * }
 * ```
 */
suspend fun <R, X : Exception> Jdbi.withHandleSuspend(callback: HandleCallback<R, X>): R =
    withContext(Dispatchers.IO) { withHandle(callback) }

suspend fun <R, X : Exception> Jdbi.inTransactionSuspend(callback: HandleCallback<R, X>): R =
    withContext(Dispatchers.IO) { inTransaction(callback) }

suspend fun <X : Exception> Jdbi.useHandleSuspend(consumer: HandleConsumer<X>) {
    withContext(Dispatchers.IO) { useHandle(consumer) }
}

suspend fun <X : Exception> Jdbi.useTransactionSuspend(consumer: HandleConsumer<X>) {
    withContext(Dispatchers.IO) { useTransaction(consumer) }
}
