package infra.shuttle.testkit

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.time.Duration.Companion.minutes

class ClockFixtureTest {
    @Test
    fun advances_and_sets_the_instant_the_module_reads() {
        val clock = ClockFixture(Instant.EPOCH)
        assertEquals(Instant.EPOCH, clock.instant())
        clock.advance(5.minutes)
        assertEquals(Instant.EPOCH.plusSeconds(300), clock.instant())
        clock.set(Instant.parse("2026-03-01T00:00:00Z"))
        assertEquals(Instant.parse("2026-03-01T00:00:00Z"), clock.instant())
    }
}
