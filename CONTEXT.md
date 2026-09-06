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
namespace rules of the CP spec. It routes; it never translates.
_Avoid_: gateway, front controller

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
The identity a lock or permit is held by; a session's death releases everything it holds
(C18). Until T41 a session is a number the caller supplies with the command and nobody
validates; the registry, heartbeats and `-NOSESSION` arrive with T41.
_Avoid_: client, connection (a session may outlive one)

## Example dialogue

**Dev:** The Netty handler got a `SET`; do I need a lock before calling the engine?
**Expert:** No. You submit the command; the engine's partition executor for that key runs it.
Nothing outside the engine serializes anything.
**Dev:** And `MULTI/EXEC` with two keys?
**Expert:** Both keys must be on the same partition; the engine checks that before the batch
runs, and the batch runs on that partition's executor with nothing interleaved.
