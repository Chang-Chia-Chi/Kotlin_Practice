# DynaCache

A Dynamo-style AP cache speaking the Redis wire protocol, with a Raft-backed CP subsystem for
linearizable primitives on the `cp:*` namespace. This glossary fixes the words the specs, the
plan, the tickets and the code share.

## Language

### Engine

**Command engine**:
A thing that executes a **command** and returns a **reply**. There are two: the **AP engine**
(Dynamo-style, owns its partition executors so a caller never serializes calls itself) and the
**CP engine** (Raft-backed, `cp:*` namespace). Both present the same shape.
_Avoid_: data engine, store, service

**Dispatcher**:
The router in front of both engines that sends a command to the AP or the CP engine by the
namespace rules of the CP spec. The one thing it does to a command is **re-target** it: a Redis
command of the compat set on a `cp:` key becomes the CP verb it means, so `INCR cp:counter:x` and
`CP.LONG.INCR cp:counter:x` are one command by the time an engine sees them. It never rewrites a
reply and never sends one command to both engines.
_Avoid_: gateway, front controller, translator

**Redis-compat set**:
The Redis commands the `cp:` namespace answers (`SET`, `GET`, the `INCR` family, `SETEX` and the
TTL commands), each re-targeted onto a CP verb. Anything else on a `cp:` key is `-NOTCP`, which
is what keeps the namespace the CP engine's alone (C16).
_Avoid_: aliases, compatibility layer

**Partition**:
The unit of single-writer execution on one node: a fixed-count local hash bucket that owns an
executor and its keys' data, and runs one command, batch or script at a time (C1). Placement
across nodes is a separate layer (see **vnode**).
_Avoid_: shard, slot, bucket

**Hash tag**:
The `{...}` part of a key name that, when present, is the only part hashed for both placement
and partition, so keys sharing a tag share a coordinator and a partition and may appear in one
batch or script (C12).
_Avoid_: key prefix, namespace

**Reply**:
What a command returns, in exactly the five RESP2 shapes Redis uses (simple string, error
with a typed kind, integer, bulk or nil, array). There is no separate domain result type.
_Avoid_: response, result

**Batch**:
A MULTI/EXEC sequence or one EVAL script: several commands that run on one partition with
nothing interleaved. Its keys are declared before it runs; a command inside the batch touching
an undeclared key gets an error reply, and the batch continues (Redis semantics, I11).
_Avoid_: transaction (implies rollback, which does not exist), unit of work

### Placement

**Vnode**:
A range of the hash ring owned by one node; the unit of placement that decides which nodes
hold a key and its replicas. Never the unit of execution.
_Avoid_: virtual node, token, range, partition

**Preference list**:
The N distinct nodes met walking the ring clockwise from a key's position; the first is the
**coordinator**.
_Avoid_: replica set, owners

**Coordinator**:
The first node of a key's preference list; the one node that executes a write or read for
that key and talks to the replicas.
_Avoid_: primary, master, leader (leader is a CP term)

**Partition executor**:
The single thread inside the command engine that runs everything for one partition. Owned by
the engine, never by a caller.
_Avoid_: dispatcher, actor, worker

### Cluster

**Transport**:
The seam through which nodes exchange cluster messages; the messages are the protobuf types
themselves. Two adapters exist: in-memory (tests, with partition, drop, delay, kill) and gRPC.
_Avoid_: channel, bus, network layer

**Membership**:
The gossip's current view of which nodes are alive, suspect or dead, and its change events.
Replication and hinted handoff read it; only SWIM writes it.
_Avoid_: cluster state, topology, peer list

**Network partition**:
A split in the transport where two sets of nodes cannot reach each other. Always say
"network partition" in full; a bare "partition" is the execution unit above.

**Contact node**:
The node a client happened to connect to. It is the **coordinator** of the keys it owns and
forwards the rest; the client never learns the difference.
_Avoid_: entry node, proxy, front node

**Router**:
The thing on each node that presents the command engine's shape and decides only whether this
node coordinates the key: it runs the command locally or forwards it and waits for the reply.
It is not the **dispatcher**, which chooses between the AP and the CP engine by namespace and
sits above it.
_Avoid_: proxy, forwarder, gateway

**Replica**:
Any node of a key's preference list; the coordinator is the first of them and a replica too.
A replica applies what the coordinator ships and answers its reads; it never decides.
_Avoid_: secondary, follower, slave

**Quorum**:
How many distinct replicas must answer before a request is answered: W acks for a write and R
answers for a read, the coordinator counting as one of each, with R + W > N (C4). A quorum
that does not form within the deadline is an error reply, never a hang.
_Avoid_: majority (that is Raft's word), consensus

**Version**:
The DVV a stored value carries; on a node it lives in the replication layer's side table next
to the engine, keyed by key, so the engine never learns of it. Replicas exchange values with
their versions, and a read answers with the version that dominates.
_Avoid_: timestamp, revision, vector clock

**Channel**:
One peer's envelopes to one node, in send order (the transport's promise); a node has one
incoming channel per peer. A Chandy-Lamport snapshot records what was on a channel between
this node's own state and the peer's marker.
_Avoid_: connection, stream, link

**Marker**:
The Chandy-Lamport envelope that carries a snapshot id and nothing else. The first one a node
sees for an id makes it record its state and send its own markers; every one closes the
channel it arrived on. A node's part is complete when every incoming channel is closed.
_Avoid_: barrier (that is the engine's word for a parked partition), token

**Snapshot set**:
Every node's part of one Chandy-Lamport snapshot: its state file plus one log per recorded
channel, under `<dir>/<id>/<node>/`. Consistent as a whole (C10); restored as a whole (I12);
deleted as a whole when a node's deadline passes with a channel still open.
_Avoid_: backup, dump (that is the single-node RDB file)
**Hint**:
A write held by a node that is not one of the key's replicas, because the replica it was meant
for was dead when the coordinator wrote (sloppy quorum). It is the whole write, unchanged: key,
tokens, version and TTL as an instant (C5). The holder's ack counts toward W like a replica's,
and when gossip sees the replica alive the holder replays the hint to it as an ordinary
replication write and forgets it on the ack (**handoff**, I9). A hint whose TTL has passed is
dropped instead.
_Avoid_: pending write, queued replica, backlog

**Read repair**:
What a coordinator does after a quorum read whose answers did not all carry the winning
version: the replica that holds the winner pushes its value and version to every replica the
winner dominates, after the client has its reply and never in its way. A replica whose version
is concurrent with the winner's is a **sibling** and is left alone for the merge. The value
crosses as bytes the engine encodes and decodes itself, so replication still ships commands
(ADR 0003) and only a repair ships a value.
_Avoid_: sync, anti-entropy (that is the background Merkle process), resend

### CP

**Log time**:
The time a CP state machine lives in: the stamp of the last entry it applied. The leader
stamps every entry with `max(its clock, last committed stamp + 1)` (C19), so log time only
moves forward across leader changes, and every member reads the same log time at the same
index (C23). A TTL, a lease or a session timeout is measured against it, never against a
member's own clock.
_Avoid_: wall time, current time, `clock.now()` in a state machine

**TTL tick**:
The entry a leader appends when nothing else has been appended for a tick interval, so log
time keeps moving while the group is idle.
_Avoid_: heartbeat (that is Raft's own, and carries no time)

**Fencing token**:
The number a FencedLock hands its holder, strictly greater than every token the same key ever
handed out, across releases, leader changes and snapshots (C17). A downstream system that
remembers the highest token it has seen can refuse a stale holder. It is state-machine state,
never a counter in a node's memory.
_Avoid_: lock id, version, epoch

**Lease**:
How long a FencedLock holder keeps the lock without renewing, measured in log time. It is the
lock's only TTL; `RENEW` by the holder extends it, `EXPIRE` on a lock key is rejected.
_Avoid_: timeout, expiry (the counter's word), TTL (say lease for a lock)

**Session**:
The identity a lock or permit is held by. The **session registry**, a primitive of the
composite state machine, hands out ids that climb from state-machine state and remembers each
session's last heartbeat in log time. A session lapses when its timeout has run out since that
heartbeat at a TTL tick; the leader then appends one `SESSION_CLOSED` entry for it, and applying
that entry (or a `CP.SESSION.CLOSE`) forgets the session and releases everything it holds in
that one entry (C18, I15). A command on behalf of a session that lapsed, closed or never
existed answers `-NOSESSION` before any primitive sees it.
_Avoid_: client, connection (a session may outlive one), lease (that is a lock's word)

**Permit**:
The unit a Semaphore hands out. A key's permits are either **available** or held, and every
held permit belongs to a session, so a session's death gives its permits back in the entry
that ends it (C18, I15). A session may only release what it holds; asking for more than is
available fails without blocking, and **draining** takes whatever is available at that entry.
_Avoid_: lock, slot, token (a token is a lock's fencing number)

**Latch**:
A CountDownLatch: a count that only ever falls, and stops at zero. It is armed only from
zero, so a latch parties are still counting down cannot be moved under them; a latch that
has run out may be armed again.
_Avoid_: barrier, gate, semaphore

**Reference**:
An AtomicReference: opaque bytes under a `cp:ref:*` key, swapped by a compare-and-set that
matches on byte content and nothing else (I21). Its TTL, like a counter's, runs on log time.
_Avoid_: value, object, string (the bytes are never decoded)

## Example dialogue

**Dev:** The Netty handler got a `SET`; do I need a lock before calling the engine?
**Expert:** No. You submit the command; the engine's partition executor for that key runs it.
Nothing outside the engine serializes anything.
**Dev:** And `MULTI/EXEC` with two keys?
**Expert:** Both keys must be on the same partition; the engine checks that before the batch
runs, and the batch runs on that partition's executor with nothing interleaved.
