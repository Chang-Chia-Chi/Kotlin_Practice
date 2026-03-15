package com.mapreduce.leader

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.statement.Update

/**
 * Base class for repositories that perform leader-only fenced writes.
 *
 * Provides [fencedUpdate] and [fencedBatch] helpers that:
 * 1. Read the fencing epoch from [FencingTokenHolder].
 * 2. Bind it into the SQL statement.
 * 3. Check the affected row count — 0 rows means a zombie was caught.
 *
 * Subclasses can also use [requireEpoch] directly for complex transactions
 * that need finer control over epoch propagation.
 */
abstract class FencedRepository(protected val jdbi: Jdbi) {

    /**
     * Read the current fencing epoch from [FencingTokenHolder].
     * Throws if not in a fenced context.
     */
    protected fun requireEpoch(): Long = FencingTokenHolder.require()

    /**
     * Read the current fencing epoch, or null if not in a fenced context.
     * Use this for methods that work both in leader and non-leader paths.
     */
    protected fun optionalEpoch(): Long? = FencingTokenHolder.get()

    /**
     * Assert that a fenced write affected at least one row.
     * Throws [StaleEpochException] on 0 rows (zombie caught by DB fence).
     */
    protected fun assertFenced(rowsAffected: Int, epoch: Long) {
        if (rowsAffected == 0) throw StaleEpochException(epoch)
    }

    /**
     * Execute a single fenced UPDATE statement.
     *
     * Reads the epoch from [FencingTokenHolder], passes it to the [bind] lambda,
     * and throws [StaleEpochException] if 0 rows were affected.
     *
     * The SQL should include `SET last_epoch = :epoch` and
     * `WHERE ... AND last_epoch <= :epoch` for the fence to work.
     *
     * @return number of rows affected (always >= 1).
     */
    protected fun fencedUpdate(
        sql: String,
        bind: (Update, Long) -> Unit,
    ): Int {
        val epoch = requireEpoch()
        val rows = jdbi.withHandle<Int, Exception> { h ->
            val update = h.createUpdate(sql)
            bind(update, epoch)
            update.execute()
        }
        if (rows == 0) throw StaleEpochException(epoch)
        return rows
    }

    /**
     * Execute a fenced UPDATE within an existing [Handle] (for multi-statement
     * transactions). Same semantics as [fencedUpdate] but uses the caller's handle.
     */
    protected fun fencedUpdate(
        handle: Handle,
        sql: String,
        bind: (Update, Long) -> Unit,
    ): Int {
        val epoch = requireEpoch()
        val update = handle.createUpdate(sql)
        bind(update, epoch)
        val rows = update.execute()
        if (rows == 0) throw StaleEpochException(epoch)
        return rows
    }

    /**
     * Execute a fenced batch operation. Iterates [items], executes a fenced
     * UPDATE for each, and fails fast on the first 0-row result.
     */
    protected fun <T> fencedBatch(
        items: List<T>,
        sql: String,
        bind: (Update, Long, T) -> Unit,
    ): Int {
        val epoch = requireEpoch()
        var totalRows = 0
        jdbi.useHandle<Exception> { h ->
            for (item in items) {
                val update = h.createUpdate(sql)
                bind(update, epoch, item)
                val rows = update.execute()
                if (rows == 0) throw StaleEpochException(epoch)
                totalRows += rows
            }
        }
        return totalRows
    }
}
