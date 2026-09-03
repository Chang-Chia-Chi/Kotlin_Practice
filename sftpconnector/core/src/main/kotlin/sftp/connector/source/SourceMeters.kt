package sftp.connector.source

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import sftp.connector.client.ResultLabel
import sftp.connector.client.resultLabelOf

/**
 * What one source publishes about the directories it polls.
 *
 * The file counter is cut by what became of each listed entry, because the difference between
 * them is the whole diagnosis: seen and not emitted is a directory of files that never become
 * ready, which is an uploader problem; emitted and not acked is a consumer that has stopped,
 * which the in-flight gauge shows sitting at its ceiling. Every meter carries the endpoint and
 * never a file name or a tick number, which would make a series per file.
 */
internal class SourceMeters(
    private val meters: MeterRegistry,
    private val endpoint: String,
    inFlight: () -> Int,
) {

    init {
        Gauge.builder("sftp_inflight") { inFlight() }.tag("endpoint", endpoint).register(meters)
    }

    private val files: Map<String, Counter> = listOf("seen", "emitted", "notReady", "gone").associateWith { state ->
        Counter.builder("sftp_poll_files").tags("endpoint", endpoint, "state", state).register(meters)
    }

    private val settlements: Map<Settlement, Counter> = listOf(Settlement.ACK, Settlement.NACK, Settlement.CANCELLED)
        .associateWith { Counter.builder("sftp_ack_total").tags("endpoint", endpoint, "outcome", it.label).register(meters) }

    /** Runs one poll and records how long it took and how it ended. A failure is rethrown untouched. */
    suspend fun <T> timingPoll(block: suspend () -> T): T {
        val started = Timer.start(meters)
        try {
            return block().also { started.stop(pollTimer(ResultLabel.OK)) }
        } catch (failure: Throwable) {
            started.stop(pollTimer(resultLabelOf(failure)))
            throw failure
        }
    }

    fun listed(seen: Int, emitted: Int, notReady: Int) {
        files.getValue("seen").increment(seen.toDouble())
        files.getValue("emitted").increment(emitted.toDouble())
        files.getValue("notReady").increment(notReady.toDouble())
    }

    /**
     * A file left the set. A file that was gone is counted with the poll's files rather than with
     * the consumer's answers, because nobody answered: it is a fact about the directory.
     */
    fun settled(outcome: Settlement) {
        if (outcome == Settlement.GONE) files.getValue("gone").increment() else settlements.getValue(outcome).increment()
    }

    private fun pollTimer(result: String): Timer =
        Timer.builder("sftp_poll_seconds").tags("endpoint", endpoint, "result", result).register(meters)
}
