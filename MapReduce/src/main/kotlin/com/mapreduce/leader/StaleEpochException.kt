package com.mapreduce.leader

/**
 * Thrown when a fenced SQL write returns 0 rows affected, indicating that a
 * higher epoch has already written to the target row.
 *
 * This is the DB-level zombie detection mechanism. A pod carrying a stale
 * epoch (e.g., after a GC pause longer than the lease duration) will have
 * all its writes rejected by the `WHERE last_epoch <= :epoch` guard.
 */
class StaleEpochException(
    val epoch: Long,
    message: String = "Fenced write rejected — stale epoch $epoch (a higher epoch has already written to this row)",
) : RuntimeException(message)
