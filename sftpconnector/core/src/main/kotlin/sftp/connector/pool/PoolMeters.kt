package sftp.connector.pool

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

/**
 * What one pool publishes about itself.
 *
 * The gauges are sampled by whatever scrapes the registry, on a thread that cannot suspend and
 * must never be made to wait on the pool. So they read the count the registry last published
 * rather than asking it for a fresh one: taking the pool's lock to answer a scrape would make an
 * observation able to slow down the thing it observes, which is the one thing an observation must
 * not do.
 *
 * Every meter is tagged with the endpoint, because a service with two connectors has two pools and
 * an untagged gauge would show their sum, which is a number describing nothing.
 */
internal class PoolMeters(
    private val meters: MeterRegistry,
    private val endpoint: String,
    /** Answers without suspending. See [SessionRegistry.lastCount]. */
    reading: () -> PoolStats,
) {

    init {
        // Sessions the pool is holding that nobody else can borrow: lent out, or still being
        // opened. Counting the half-open ones here rather than leaving them out is what makes
        // this gauge and the idle one add up to everything the pool has, so a session cannot go
        // missing from the dashboard by being in a state no gauge was given.
        gauge(meters, "sftp_pool_active", endpoint) { with(reading()) { inUse + connecting } }
        gauge(meters, "sftp_pool_idle", endpoint) { reading().idle }
        gauge(meters, "sftp_pool_pending", endpoint) { reading().pending }
    }

    /**
     * How long callers wait for a session. Only the waits that ended in one are recorded: a wait
     * that timed out lasted exactly the acquire timeout, and mixing that constant into the
     * distribution would drag the percentiles toward a number the configuration already fixes.
     */
    private val acquireWait: Timer = Timer.builder("sftp_pool_acquire_seconds")
        .tag("endpoint", endpoint)
        .register(meters)

    /** Starts the clock on one caller's wait. The registry's clock, so a test can control it. */
    fun startWaiting(): Timer.Sample = Timer.start(meters)

    fun admitted(waiting: Timer.Sample) {
        waiting.stop(acquireWait)
    }

    private val acquireTimeouts: Counter = Counter.builder("sftp_pool_acquire_timeout_total")
        .tag("endpoint", endpoint)
        .register(meters)

    private val sessionsCreated: Counter = Counter.builder("sftp_pool_created_total")
        .tag("endpoint", endpoint)
        .register(meters)

    /** A caller gave up waiting. A pool sized for its load never reports one. */
    fun turnedAway() = acquireTimeouts.increment()

    /** A handshake was paid for. Rising on a pool that should be warm means sessions are dying. */
    fun sessionOpened() = sessionsCreated.increment()

    /**
     * A session was thrown away, and why. The reason is the whole value of this counter: sessions
     * retired on a schedule are the pool working, and sessions failing their check on the way out
     * to a caller are the network dropping them underneath it.
     *
     * The counter for a reason appears the first time that reason happens, which is Micrometer's
     * own way with a tagged counter and means a dashboard shows the reasons this deployment has
     * actually seen rather than every reason the connector can name.
     */
    fun evicted(reason: Retirement) {
        meters.counter("sftp_pool_evicted_total", "endpoint", endpoint, "reason", reason.label).increment()
    }

    /** A session has been out on lease too long. Any non-zero value is a caller to go and find. */
    fun leaked() {
        meters.counter("sftp_pool_leak_total", "endpoint", endpoint).increment()
    }

    private companion object {
        private fun gauge(meters: MeterRegistry, name: String, endpoint: String, value: () -> Int) {
            // The supplier form, not the weakly-held-object form: a gauge whose subject has been
            // collected reports NaN for the life of the process, and it does so silently.
            Gauge.builder(name) { value() }.tag("endpoint", endpoint).register(meters)
        }
    }
}
