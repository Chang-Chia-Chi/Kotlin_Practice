package dynacache.cp

import dynacache.engine.Command

/**
 * A log entry the leader stamped with log time (CP spec 5): [ts] is wall-clock milliseconds, but
 * chosen as `max(leader clock, last committed ts + 1)`, so it only ever moves forward across
 * leader changes (C19). Every member evaluates expiry against the ts it last applied, never
 * against its own clock (C23).
 */
data class CpOp(val ts: Long, val command: Command.Cp)

/**
 * The entry the leader appends when the group is otherwise idle (CP spec 5, 9.4), so log time
 * keeps moving and a TTL expires without user traffic.
 */
data class TtlTick(val ts: Long)

/**
 * The no-op MicroRaft appends first in every leader's [term]. Applied, it proves the member has
 * applied every entry before it, which is what a fresh leader must have done before it may stamp.
 */
data class NewTerm(val term: Int)

/**
 * The entry the leader appends when a session's timeout has run out at log time (CP spec 4,
 * 9.3). Applying it forgets the [session] and releases every lock it held, in this one entry
 * (C18, I15). Stamped like every other entry, so log time never turns back (C19).
 */
data class SessionClosed(val ts: Long, val session: Long)
