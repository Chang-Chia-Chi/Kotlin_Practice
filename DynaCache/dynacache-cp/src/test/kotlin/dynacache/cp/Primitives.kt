package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Reply
import dynacache.engine.testkit.MutableClock
import java.time.Duration
import java.time.Instant

/**
 * The composite CP state machine driven on its own, with no Raft group behind it: a primitive is
 * a pure function of (command, log time), so its semantics need a stamp and an apply, not an
 * election. [apply] stamps a command the way a leader does (CP spec 5, `max(clock now, last
 * applied + 1)`) and answers with the reply the composite produced; [tick] appends the entries a
 * leader appends when the group is idle, the TTL tick and one `SESSION_CLOSED` per session whose
 * timeout has run out (CP spec 9.3), so a lease or a session expires here exactly as it does on a
 * real leader.
 *
 * Stamps climb by one millisecond per entry while the clock stands still, as they do in a group,
 * so a test may assert the lease remaining after a given number of entries.
 *
 * What genuinely needs a log stays on [CpTestKit]: leader failover, snapshot install, log time
 * across leader changes, and concurrent clients the log has to serialize.
 */
class Primitives {

    private val clock = MutableClock(EPOCH)

    /** The composite under this fixture, for a test that reads its state rather than its replies. */
    val stateMachine = CpStateMachine()
    private var index = 0L
    private var lastStamped = 0L

    /** Applies [command] as the entry stamped with the current log time, and answers its reply. */
    fun apply(command: Command.Cp): Reply = stateMachine.runOperation(++index, CpOp(stamp(), command)) as Reply

    /** Moves this leader's clock; no entry carries the new time until the next [apply] or [tick]. */
    fun advance(by: Duration) = clock.advance(by)

    /** [advance]s by [after], then appends the idle entries: the TTL tick, then every lapsed session. */
    fun tick(after: Duration = Duration.ZERO) {
        advance(after)
        stateMachine.runOperation(++index, TtlTick(stamp()))
        stateMachine.lapsedSessions().forEach { stateMachine.runOperation(++index, SessionClosed(stamp(), it)) }
    }

    private fun stamp(): Long {
        lastStamped = maxOf(clock.millis(), stateMachine.lastAppliedTs + 1, lastStamped + 1)
        return lastStamped
    }

    private companion object {
        /** The kit's epoch, so a stamp read in a failing assertion means the same in both worlds. */
        val EPOCH: Instant = Instant.parse("2026-09-06T00:00:00Z")
    }
}
