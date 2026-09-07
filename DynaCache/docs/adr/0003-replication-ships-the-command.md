# Replication ships the command, not the value

A coordinator has to hand a write to its N-1 replicas (spec 5.1 step 5). It could ship the
resulting value with its DVV, or the command that produced it. The engine hands out replies,
not values: there is no hook to read a `Value` out of a partition or to install one, and adding
one would teach the engine about replication. So the coordinator ships the command's tokens,
exactly as a forward does (T19), with NX/XX already decided and the TTL turned into an absolute
instant, plus the version it created. A replica parses the tokens, applies them through its own
engine and stores the version in its side table.

Since T65 the command ships as the engine command codec's bytes: the entry the coordinator
logged, framed op code first, so a replica applies exactly what the coordinator's log holds.

Consequence: a command that depends on the replica's current value (`INCRBY`, `APPEND`, `LPOP`)
produces the same result only when the replica held the same value, which the quorum makes true
in the steady state and a missed write breaks until anti-entropy (T28) repairs it. Spec 5.3's
concurrent case is not a type merge here: the remote command is applied over the local value
under a version descending from both. A value codec (T31) plus an engine install hook would let
a replica receive values instead, and hinted handoff (T25) stores the `Replicate` envelope as
its hint either way.

Considered and rejected: shipping values now (needs the engine hook and the codec that T31
owns); replicating through `atomically` with a block (a block is code and does not cross a
wire).
