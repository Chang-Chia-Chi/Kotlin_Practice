package sftp.connector.resilience

import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.kotlin.circuitbreaker.executeSuspendFunction
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import io.github.resilience4j.kotlin.timelimiter.executeSuspendFunction
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import io.github.resilience4j.timelimiter.TimeLimiter
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import sftp.connector.config.Backoff
import sftp.connector.config.ResilienceConfig
import sftp.connector.error.Attempt
import sftp.connector.error.CircuitOpen
import sftp.connector.error.CurrentAttempt
import sftp.connector.error.Disposition
import sftp.connector.error.NoSuchFile
import sftp.connector.error.OperationTimeout
import sftp.connector.error.SftpException
import sftp.connector.pool.SftpPool
import sftp.connector.transport.SftpSession
import java.time.Clock
import java.util.concurrent.TimeoutException
import kotlin.time.toJavaDuration

/**
 * What stands between a client operation and the pool: the retries, the breaker, the limit on
 * transfers and the clock on each try, in that order from the outside in.
 *
 * The order is the whole design. A retry has to see the breaker, so that once the breaker has
 * opened the retry loop stops on the spot instead of asking three more times; and the breaker
 * has to see the clock, so that a server which accepts requests and never answers them counts as
 * failing. Each try borrows a session of its own - one that failed under a try is evicted by the
 * pool on the way out - which is the whole of what "reconnect" means here: the retry is the
 * reconnect, and nothing ever swaps a session underneath a call.
 *
 * The limit on transfers is a coroutine semaphore and not the resilience library's bulkhead, on
 * purpose. The library's suspending bulkhead takes its permit on another dispatcher and only then
 * starts guarding it, so a caller cancelled at the switch back has taken a permit nothing will
 * ever give back - the same shape as the leak the pool review found in the handshake, and a
 * transfer slot lost until restart. A semaphore that suspends is cancelled cleanly, and a transfer
 * beyond the limit waits its turn rather than being turned away.
 *
 * Nothing here decides what a failure means. Every failure the connector raises already says
 * whether it is worth another go and whether the server is to blame for it, and this reads those
 * answers rather than keeping its own list of classes - with one reading it has to make. A failure
 * the wire produced - a session that died, a reply that never came, a connection that never
 * opened - is retried here, on a fresh session, because the reply was lost and the next try may
 * get one. A failure the server answered - a path that is not there, a request it refused - is
 * the server's decision about that request, and the same server asked the same thing a second
 * later gives the same answer; by the time such a failure arrives here the operation has already
 * said what it means to it (a rename has looked for its own landed file, a mkdir has found its
 * directory), and its retry is the next tick, which is the source's business. Retrying it here
 * would cost three tries and a backoff to learn nothing, and would rewrite the operation's own
 * account of what the server is left holding. A path that is not there is also not held against
 * the server: on a directory another system moves files out of it is ordinary, and a breaker that
 * counted it would open on a healthy server.
 */
internal class Resilience(
    settings: ResilienceConfig,
    private val pool: SftpPool,
    private val endpoint: String,
    private val meters: MeterRegistry,
    /** What the breaker's wait in open is measured on. Injected so a test can move it. */
    clock: Clock,
) {

    private val retries: RetryConfig = RetryConfig.custom<Any>()
        .maxAttempts(settings.retry.maxAttempts)
        .intervalFunction(settings.retry.backoff.asIntervals())
        .build()

    private val breaker: CircuitBreaker = CircuitBreaker.of(
        endpoint,
        CircuitBreakerConfig.custom()
            .failureRateThreshold(settings.circuitBreaker.failureRateThreshold.toFloat())
            .slidingWindowSize(settings.circuitBreaker.slidingWindow)
            // Judged as soon as the window is full, not after the library's own hundred calls.
            .minimumNumberOfCalls(settings.circuitBreaker.slidingWindow)
            .waitDurationInOpenState(settings.circuitBreaker.waitInOpen.toJavaDuration())
            .clock(clock)
            .permittedNumberOfCallsInHalfOpenState(1)
            // Ignored rather than merely not recorded: a failure that is not the server's fault
            // must not count as a success either, or a run of them would hide a failing server.
            .ignoreException { !it.countsAgainstTheBreaker() }
            .build(),
    )

    private val transfers = Semaphore(settings.maxConcurrentTransfers)

    private val roundTrips = TimeLimiter.of(TimeLimiterConfig.custom().timeoutDuration(settings.operationTimeout.toJavaDuration()).build())
    private val wholeFiles = TimeLimiter.of(TimeLimiterConfig.custom().timeoutDuration(settings.transferTimeout.toJavaDuration()).build())

    init {
        Gauge.builder("sftp_breaker_state") { breaker.state.reading() }.tag("endpoint", endpoint).register(meters)
    }

    /**
     * Runs [block] against a session, and again on a fresh one for as long as its failures are
     * worth another go and the budget allows.
     *
     * [block] is handed the try it is: what proves that an earlier try landed is the operation's
     * knowledge, and this is how it learns there was one. Everything underneath - the transport's
     * failures, a full pool - reports the same try, because it is put in the coroutine context.
     *
     * @param transfer whether this moves a whole file, which puts it under the limit on
     *   concurrent transfers and on the longer clock.
     * @param unhurried whether how long this takes is the other end's to decide - a file's size,
     *   a consumer's pace - which puts it on the longer clock without counting it as a transfer.
     * @param stillWorthRetrying asked before each retry; a listing that has already handed
     *   entries on answers no, because starting over would hand them on twice.
     */
    suspend fun <T> attempting(
        operation: String,
        path: String?,
        transfer: Boolean = false,
        unhurried: Boolean = transfer,
        stillWorthRetrying: () -> Boolean = { true },
        block: suspend (SftpSession, Attempt) -> T,
    ): T {
        var tries = 0
        val retry = Retry.of(
            endpoint,
            RetryConfig.from<Any>(retries).retryOnException { it.worthAnotherTry() && stillWorthRetrying() }.build(),
        )
        retry.eventPublisher.onRetry {
            LOG.warn("{} failed and is being tried again in {}: {}", operation, it.waitInterval, it.lastThrowable.message)
        }
        return retry.executeSuspendFunction {
            val attempt = Attempt(endpoint, operation, path, ++tries)
            if (attempt.number > 1) meters.counter("sftp_retry_total", "endpoint", endpoint, "op", operation).increment()
            withContext(CurrentAttempt(attempt)) {
                throughTheBreaker(attempt) {
                    throughTheTransferLimit(transfer) {
                        onTheClock(if (unhurried) wholeFiles else roundTrips, attempt) {
                            pool.withLease { lease -> block(lease.connection, attempt) }
                        }
                    }
                }
            }
        }
    }

    /**
     * Runs [block] once, on one session, behind the breaker and nothing else. For work whose
     * sequence only the caller understands: it cannot be tried again from here, and how long it
     * may take is its own business.
     */
    suspend fun <T> once(operation: String, block: suspend (SftpSession) -> T): T {
        val attempt = Attempt(endpoint, operation)
        return withContext(CurrentAttempt(attempt)) {
            throughTheBreaker(attempt) { pool.withLease { lease -> block(lease.connection) } }
        }
    }

    private suspend fun <T> throughTheBreaker(attempt: Attempt, block: suspend () -> T): T =
        try {
            breaker.executeSuspendFunction(block)
        } catch (open: CallNotPermittedException) {
            throw CircuitOpen(attempt)
        }

    /** A transfer past the limit waits for one ahead of it to finish; each of those is on the clock, so the wait is bounded by them. */
    private suspend fun <T> throughTheTransferLimit(transfer: Boolean, block: suspend () -> T): T =
        if (transfer) transfers.withPermit { block() } else block()

    /**
     * A try that runs out of time is cancelled, which is what sends a call blocked inside the SSH
     * library up the pool's ladder and gets the thread back; and then it is reported as what it
     * was, which is a request that may still land. A cancellation that arrives here from further
     * out looks the same and is not one, so the coroutine is asked before the timeout is believed.
     */
    private suspend fun <T> onTheClock(limit: TimeLimiter, attempt: Attempt, block: suspend () -> T): T =
        try {
            limit.executeSuspendFunction(block)
        } catch (ranOut: TimeoutException) {
            currentCoroutineContext().ensureActive()
            throw OperationTimeout(
                attempt,
                "no answer within ${limit.timeLimiterConfig.timeoutDuration}; the request may still land, so the session is not kept",
                ranOut,
            )
        }

    private companion object {
        private val LOG = LoggerFactory.getLogger(Resilience::class.java)

        private fun Backoff.asIntervals(): IntervalFunction =
            if (jitter) IntervalFunction.ofExponentialRandomBackoff(initial.toJavaDuration(), 2.0, 0.5, max.toJavaDuration())
            else IntervalFunction.ofExponentialBackoff(initial.toJavaDuration(), 2.0, max.toJavaDuration())

        private fun Throwable.worthAnotherTry(): Boolean =
            this is SftpException && disposition == Disposition.RETRY_ON_A_FRESH_SESSION

        private fun Throwable.countsAgainstTheBreaker(): Boolean =
            this is SftpException && disposition.countsAgainstTheBreaker && this !is NoSuchFile

        /** 0 closed, 1 half-open, 2 open; the states a breaker is forced or disabled into read as what they act like. */
        private fun CircuitBreaker.State.reading(): Int = when (this) {
            CircuitBreaker.State.CLOSED, CircuitBreaker.State.DISABLED, CircuitBreaker.State.METRICS_ONLY -> 0
            CircuitBreaker.State.HALF_OPEN -> 1
            CircuitBreaker.State.OPEN, CircuitBreaker.State.FORCED_OPEN -> 2
        }
    }
}
