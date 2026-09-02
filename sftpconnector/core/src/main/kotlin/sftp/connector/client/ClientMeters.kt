package sftp.connector.client

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CancellationException
import sftp.connector.error.SftpException
import sftp.connector.error.WatchReaction

/**
 * What one client publishes about the work it does against its server.
 *
 * One timer, cut two ways: by which operation was asked for, and by how it ended. That second cut
 * is the point of it. A rising number of listings says the connector is busy; a rising number of
 * listings ending badly says the server is in trouble, and the two are the same number until the
 * outcome is a tag on it.
 *
 * How an operation ended is not decided here. Every failure the connector raises already answers
 * what is to be done about it, and reading that answer rather than sorting failures into buckets
 * a second time is what stops this file and the failure model from drifting apart.
 */
internal class ClientMeters(private val meters: MeterRegistry, private val endpoint: String) {

    /**
     * Runs [operation] and records how long it took and how it went. A failure is recorded and
     * then rethrown untouched, so nothing about the caller's error handling changes because the
     * call is being measured.
     */
    suspend fun <T> timing(operation: String, block: suspend () -> T): T {
        val started = Timer.start(meters)
        try {
            val result = block()
            started.stop(timer(operation, OK))
            return result
        } catch (failure: Throwable) {
            started.stop(timer(operation, outcomeOf(failure)))
            throw failure
        }
    }

    private fun timer(operation: String, result: String): Timer = Timer.builder(OP_SECONDS)
        .tags("endpoint", endpoint, "op", operation, "result", result)
        .register(meters)

    private companion object {
        private const val OP_SECONDS = "sftp_op_seconds"

        private const val OK = "ok"
        private const val RECOVERABLE = "recoverable"
        private const val FATAL = "fatal"
        private const val CANCELLED = "cancelled"

        /**
         * A failure nobody classified is counted as fatal rather than as something to retry. It is
         * a bug in the connector or in the code it was handed, and no amount of waiting has ever
         * cured one of those; counting it among the retryable failures would bury it in the noise
         * of a flaky network, which is the one place it must not be.
         */
        private fun outcomeOf(failure: Throwable): String = when {
            failure is CancellationException -> CANCELLED
            failure !is SftpException -> FATAL
            failure.disposition.watch == WatchReaction.STOP -> FATAL
            else -> RECOVERABLE
        }
    }
}
