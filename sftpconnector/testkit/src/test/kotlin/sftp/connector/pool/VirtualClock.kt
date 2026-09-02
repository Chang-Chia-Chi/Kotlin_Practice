package sftp.connector.pool

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

/**
 * A clock that reads the test scheduler, so advancing virtual time ages the pool's sessions by
 * exactly as much.
 *
 * Without it a test about a session that has lived half an hour would have to choose between
 * waiting half an hour and a pool that cannot tell time has passed. With it, `advanceTimeBy` moves
 * the housekeeper's next round and the age of everything it looks at together, which is what makes
 * a test about expiry deterministic rather than merely fast.
 */
@OptIn(ExperimentalCoroutinesApi::class)
fun TestScope.virtualClock(): Clock = object : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC
    override fun withZone(zone: ZoneId): Clock = this
    override fun millis(): Long = testScheduler.currentTime
    override fun instant(): Instant = Instant.ofEpochMilli(millis())
}
