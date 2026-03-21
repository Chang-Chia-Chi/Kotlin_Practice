package com.mapreduce.shutdown

import java.time.Duration

/**
 * Component that participates in graceful shutdown.
 *
 * Discovered via CDI. The [ShutdownCoordinator] groups participants by
 * [shutdownOrder] (lower first), runs each group concurrently with
 * per-participant [shutdownTimeout], then proceeds to the next group.
 */
interface ShutdownParticipant {

    /** Shutdown ordering. Lower values execute first. Same-order participants run concurrently. */
    val shutdownOrder: Int

    /** Maximum time [shutdown] may take before the coordinator cancels it. */
    val shutdownTimeout: Duration

    /** Perform graceful teardown. Called exactly once by [ShutdownCoordinator]. */
    suspend fun shutdown()
}
