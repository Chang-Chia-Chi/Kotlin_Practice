package infra.shuttle.testkit

import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * The wall clock the module reads (rows' `updated_at`, `next_attempt_at`, reconciliation's "older
 * than"). It is independent of `runTest`'s virtual time, which drives `delay` only: a test that
 * wants both moves them together, `clock.advance(d)` then `advanceTimeBy(d)`.
 */
class ClockFixture(start: Instant = Instant.parse("2026-01-01T00:00:00Z"), private val zone: ZoneId = ZoneOffset.UTC) : Clock() {
    @Volatile private var now: Instant = start

    fun advance(by: Duration) { now = now.plus(by.toJavaDuration()) }
    fun set(to: Instant) { now = to }

    override fun instant(): Instant = now
    override fun getZone(): ZoneId = zone
    override fun withZone(zone: ZoneId): Clock = ClockFixture(now, zone)
}
