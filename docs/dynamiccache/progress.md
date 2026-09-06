# DynaCache - Progress Log

One entry per ticket, appended when the ticket is done. Later sessions read this to learn
what already exists and which deviations override the spec.

## Entry template

```
## T<nn>: <ticket title>

**Built:** what exists now that did not before.
**Concepts named:** the domain vocabulary this ticket introduced, and where the seams went.
**Acceptance:** each checkbox from the ticket, with the test that proves it.
**Deviations:** every place the code differs from the spec or plan, and why. "None" if none.
**For the next ticket:** seams left stubbed, gotchas, anything surprising.
```

A deviation recorded here overrides the spec for the code that already exists. A deviation
that is merely a shortcut is debt: say so, and say what would repay it. Decisions that change
a fixed contract are numbered `D<n>` starting at D1 and referenced from the spec.

---

## T01: Skeleton, frozen reply model, test tooling

**Built:** The four poms drop AssertJ and gain `org.mockito:mockito-core` 5.14.2 in test scope
next to JUnit 5.11 (managed in the parent, declared in all three modules). `dynacache-engine`
gains its first code, kotlin-stdlib only: `Reply` (`Simple`, `Error(kind, message)`, `Integer`,
`Bulk(bytes?)`, `Array`), `Key`, `PartitionId`, `Command` with `Ping`, and the `CommandEngine`
interface with its `ApEngine` stub. Sixteen tests: fourteen in the engine, one apiece in cluster
and server so every module's surefire runs.

**Concepts named:** `Reply` is the only result type, byte-content-equal down through nested
arrays, so a test writes the reply it expects as a literal. `Key` is binary-safe and equal by
bytes; it exposes `hashedBytes` (the hash tag when the key has one, the whole key otherwise) and
a non-negative `hash` over those bytes. That split is deliberate: two keys sharing a tag are
different keys that hash alike, which is what makes a batch on `{user1}.a` and `{user1}.b` legal
(C12). `hashedBytes` is what the ring will hash in T17, so the hash-tag rule lives in one place
for both layers. Seams: `CommandEngine` (`submit`, `atomically(keys) { ctx -> R }`, `close`) with
`PartitionContext.execute` as the batch's inner handle, exactly plan 2.3; the single adapter
`ApEngine` is a stub. `PartitionId` wraps an index; `Command` is a sealed root with `Ping` only.

**Acceptance:**
- `mvn package` green from `DynaCache/`, one passing test per module: engine 14, cluster 1,
  server 1.
- No pom mentions AssertJ (`grep -ri assertj` over poms and sources is empty); `mvn
  dependency:tree` shows `mockito-core:5.14.2:test` in all three modules.
- `Reply.Bulk(null)` and `Reply.Bulk(bytes)` compare by content, and so do bulks nested in an
  `Array`: `bulk replies compare by byte content, not by array identity`, `a nil bulk equals only
  another nil bulk`, `an array compares by its items, bulk bytes included`. `Key` equality is by
  bytes including non-UTF-8 ones: `keys compare by their bytes and are binary-safe`.
- Hash-tag rule: `a hash tag makes only the tag decide the hash`, `a key without a hash tag
  hashes whole`, `an empty or unclosed tag falls back to the whole key`, `the first tag wins and
  only its first closing brace ends it`.
- `CommandEngine.submit` and `atomically` exist with plan 2.3's shapes and throw
  `NotImplementedError`: `submit is a stub until the partition executors arrive`, `atomically is
  a stub until the partition executors arrive`.
- This entry.

**Deviations:** None against the ticket or plan 2.3. Three judgement calls worth recording:
`ApEngine.close()` is a no-op rather than a stub, because there is nothing to shut down yet and
a throwing `close()` would be a trap for T02's tests; `CommandEngine` does not extend
`AutoCloseable`, since nothing needs it yet; `Key.hash` is a 31-multiplier hash over
`hashedBytes` masked non-negative, not SHA-256, because it buckets keys into local partitions
only. Spec 3 puts SHA-256 on the ring, and T17 should hash `hashedBytes` with it there.

**For the next ticket:** T02 owns `ApEngine`'s constructor (partition count, injected `Clock`),
the executors and the store; `submit` and `atomically` are the only stubs, and `Command` gains a
variant per ticket. `Key` holds the array it is given rather than copying it, so nothing may
mutate a key's bytes after construction; `hashedBytes` copies on each read, which is fine at
today's call rate and worth caching if a profile ever says otherwise. Partition selection should
be `key.hash % partitionCount` directly, since `hash` is already non-negative.

## T17: Hash ring and preference lists

**Built:** `dynacache-cluster` gains its first code, `dynacache.cluster.Ring`. `NodeId` is a
value class over a node's name, comparable by that name. `Ring.of(nodes, vnodesPerNode = 128)`
builds the ring from a node set and a vnode count and nothing else: each node contributes
`vnodesPerNode` vnodes at 63 bits of SHA-256 over `"<node>#<vnode index>"`, sorted clockwise
with ties broken by owner then index. `positionOf(key)` is 63 bits of SHA-256 over
`Key.hashedBytes`. `preferenceList(key, n)` binary-searches the key's successor vnode and walks
clockwise collecting distinct owners; `vnodeOf(key)` returns that successor vnode. `Vnode` is
`(owner, index, position, rangeStart)` with `holds(position)` for the half-open range
`(rangeStart, position]`, wrapping at the top of the ring.

**Concepts named:** The ticket's words are the code's words. **Vnode**, **preference list** and
**coordinator** (the first entry of the list) come straight from CONTEXT.md, and no method or
field mentions a partition: the ring decides placement and the engine's partition executor is a
separate layer it never reaches (ADR 0001). The hash-tag rule lives once, in the engine:
the ring hashes `Key.hashedBytes`, so `{user1}.a` and `{user1}.b` share a position, a vnode and
a preference list without the cluster module knowing the brace rule at all. `Vnode` carries its
range because the range, not the position, is what T27's Merkle trees and T28's anti-entropy
compare; `Ring.vnodes` is public for the same reason. Ring positions are 63-bit non-negative
`Long`s, the same masking trick `Key.hash` uses, so ordering is plain signed comparison and no
unsigned arithmetic appears anywhere.

**Acceptance:**
- `C3_preference_list_has_n_distinct_nodes`: three nodes, a preference list of 3 has 3 entries,
  3 distinct entries, and is exactly the node set.
- `a preference list longer than the node set is refused`: `preferenceList(key, 4)` on a
  three-node ring throws `IllegalArgumentException`.
- `I5_same_inputs_same_ring`: a ring built from the node set and one built from the same set in
  reverse iteration order have the same node list and the same preference lists for 200 keys.
- `ring_determinism`: three independently built rings give identical preference lists for
  10,000 seeded keys.
- `ring_hash_tag_places_keys_together`: `{user1}.a` and `{user1}.b` share both a position and a
  preference list.
- `ring_load_is_even`: 100,000 seeded keys over three nodes, coordinator load max over min below
  1.25 at the default 128 vnodes per node, with no bump needed.
- `a key belongs to the vnode whose range holds its position` and `the vnode ranges tile the
  ring end to end`: 1,000 keys land in a vnode that holds them and whose owner is the
  coordinator; consecutive ranges abut and the first wraps onto the last.
- `mvn clean package` green: engine 14, cluster 9, server 1.
- This entry.

**Deviations:** None against the ticket, the plan entry or spec 2.4. Three judgement calls worth
recording. `Ring.of` takes a plain `Set<NodeId>` and sorts it itself rather than requiring a
`SortedSet`, so I5 holds no matter what order a caller's set iterates in, which the test asserts
directly. A position is the first 8 bytes of the SHA-256 digest masked to 63 bits rather than the
full 256-bit digest as a `BigInteger`; 384 points in a 2^63 space collide with negligible
probability, and the sort's owner-then-index tie-break keeps the ring deterministic even if two
ever did. `Ring.of` rejects a vnode count below 128 rather than silently raising it, because spec
2.4's floor is a contract and a caller asking for 16 has misread it.

**For the next ticket:** T18 wires one `Ring` per node in `InProcessCluster` and can share a
single instance across all three, since the ring is immutable and a pure function of its inputs.
`preferenceList` is the coordinator lookup T19's router needs: element 0 is the coordinator, and
the full list of N is what T22's quorum writes to. `vnodeOf(key)` and the public `Ring.vnodes`
are the ranges T27 and T28 build Merkle trees over; `Vnode.holds(position)` already handles the
wrapping range, so anti-entropy does not need to special-case the top of the ring. Membership
changes are out of scope here and stay so: a new node set means a new `Ring`, and dynamic
rebalancing is on the do-not-build list.

## T33: WAL writer and reader

**Built:** `dynacache.engine.persist`, the engine's only `java.nio.file` package, gains the
write-ahead log of spec 2.8: `WalWriter.append(op, payload)` appends one entry and returns the
sequence number it assigned, and `WalReader.readAll()` reads the file back as a `WalScan`. An
entry is the spec's layout verbatim, big-endian:
`[crc32:u32][length:u32][seq:u64][op:u8][payload]`, where `length` is the payload's byte count,
so the header is a fixed 17 bytes. Four tests, `@TempDir` only, no mocks: the filesystem is a
true boundary and a real temp file is cheaper than faking one.

**Concepts named:** A **WAL entry** is an opaque `payload` under a one-byte `op` code stamped
with a `seq`; the engine, not the log, knows what an op means, so the log stays a byte pipe and
T35 can add mutations without touching it. A **scan** is what one pass over a log file found:
the entries it trusts, plus where and why it stopped. That pairing is the deliberate interface
choice - recovery needs to know not only which entries survived but whether the file ended
cleanly, so `WalStop` has three values and `stoppedAt` carries the byte offset. `CLEAN_END`
means the last entry ended exactly at the end of the file. `TORN_TAIL` means the file ends
mid-entry, the shape a crash mid-append leaves. `CRC_MISMATCH` means an entry's bytes disagree
with its checksum. No seam was added: the writer and the reader are two concrete classes, and
the fsync-policy seam belongs to T34, which owns the sink the tests will count.

The checksum covers length, seq, op and payload, everything after itself. That is the choice
the ticket asked to be stated, and it buys one thing: a corrupted length field is caught by the
same check as a corrupted payload, instead of sending the reader off to a wrong offset. The
`wal_crc_detects_corruption` test asserts this directly by flipping a byte in a sequence number
as well as one in a payload.

**Acceptance:**
- `wal_write_read_roundtrip`: three entries written, including a zero-length payload, read back
  equal with sequence numbers 1, 2, 3, stop `CLEAN_END`, `stoppedAt` equal to the file size.
- `wal_crash_recovery`: three entries, then the file truncated inside the third entry's header
  and, on a second log, inside its payload. Both return entries 1 and 2, stop `TORN_TAIL`, and
  `stoppedAt` at the third entry's offset.
- `wal_crc_detects_corruption`: one bit flipped in the second entry's payload, and on a second
  log in its sequence number. Both return entry 1 only, stop `CRC_MISMATCH`, `stoppedAt` at the
  second entry's offset.
- `wal_seq_strictly_increasing`: a writer started at 100 returns 100 to 103; a second writer
  opened on the same file at 104 appends there; the reader sees 100 to 104, sorted and distinct.
- This entry.

**Deviations:** None against the ticket, the plan or spec 2.8. Three judgement calls worth
recording. First, a length field too large for the bytes that remain is reported as
`TORN_TAIL`, not as corruption: from the reader's position the two are indistinguishable, both
mean "no complete entry here", and a fourth enum value would buy nothing. Second, `append` is
`@Synchronized`. Concurrent appenders are T34's subject and are not tested here, but a writer
that interleaves half-records under two threads would be a trap to inherit, and the annotation
is one word. Third, the tests hardcode the 17-byte header as `4 + 4 + 8 + 1` rather than
importing a constant from the code, so the offsets they truncate and corrupt at come from the
spec rather than from the implementation they are checking.

**For the next ticket:** `WalReader.readAll()` throws `NoSuchFileException` on a missing file;
an empty file is a clean scan of nothing. T35 owns whichever of the two a first boot should
see, and the choice belongs there, not here. `WalWriter` takes its first sequence number rather
than deriving one - deliberately, since only recovery knows whether to resume after the last
entry or after a checkpoint, and `WalScan.entries.last().seq` gives it the number. The writer
opens the channel in `APPEND` mode, so T34's group commit must batch before the channel, not
seek within it, and T35's checkpoint truncation needs its own handle. `WalScan` carries no
`nextSeq` field because nothing needed one yet.

## T02: Engine walking skeleton and partition executors

**Built:** `ApEngine(partitionCount, clock)` owns a fixed list of `Partition`s. Each partition is
one JDK single-thread executor (daemon thread named `partition-<i>`) and a `HashMap<Key, Entry>`
that only that thread touches. `partitionOf(key)` is `key.hash % partitionCount`;
`submit(command)` picks the partition from the command's key (keyless `PING` runs on partition
0) and returns `CompletableFuture.supplyAsync(...)` on that executor, so the future completes on
the partition thread. `Command` gains `Get`, `Set(key, value, condition: Condition?, ttl:
Duration?)` with `enum Condition { NX, XX }`, single-key `Del`, `Exists`, `Type`. `SET` with a
TTL stores `now + ttl` as an absolute `Instant`; every access goes through one `live(key, now)`
helper that deletes an expired entry and reports it absent (spec 5.4 lazy check). The injected
`Clock` is read exactly once per command. `close()` calls `shutdown()` on every executor.
`dynacache-cluster` gains `suspend fun <T> CompletableFuture<T>.await()`, an alias of kotlinx's
own future bridge so callers import the cluster's name only. `atomically` is still
`TODO("T14: batches")`.

**Concepts named:** `Partition` (internal) is the CONTEXT.md partition made concrete: executor
plus store plus the command interpreter, one class. `Entry(value, expiresAt)` is the stored
cell; `expiresAt == null` means no TTL. `live` is the one place the lazy expiry rule lives, so
T09's wheel only has to remove keys earlier, never re-check. `Set.Condition` models NX/XX as one
nullable enum rather than two booleans, so both flags at once is unrepresentable and the parser
(T13) rejects that combination at the wire. `EX` and `PX` both arrive as a `Duration`; the
distinction is wire syntax, not engine meaning. Seams unchanged: `CommandEngine`,
`PartitionContext`, `Reply`, `Key`, `PartitionId` are exactly T01's.

**Acceptance:**
- `string_set_get_roundtrip`, `string_set_nx_rejects_existing` (value unchanged after the
  rejected SET), `string_set_xx_rejects_missing` (key still absent): green.
- `string_set_ex_expires`: SET EX 1, clock advanced 999 ms, value present; advanced 2 ms more,
  nil. A `MutableClock` in the test; nothing sleeps.
- `C1_one_command_at_a_time_per_partition`: every command reads the clock once on its partition
  thread, so the test injects a gate clock that records `Thread.currentThread()` and blocks in
  `instant()` until released. Sequence: submit GET on `{p}.1` (enters, blocks); submit GET on
  `{p}.2` (same partition); submit GET on a key found via `partitionOf` to be elsewhere; the
  semaphore shows a second entrant while the first is still blocked (partitions overlap) and
  the queued future is not done; release; the third recorded thread is the same object as the
  first (the queued command waited for the busy partition's one thread) and the second differs
  (the other partition ran concurrently). Mutation check: with the partition executor swapped
  to a two-thread pool the test fails on the thread-identity assertion. No test-only `Command`
  variant; the clock is the one allowed boundary mock.
- `keys_with_same_hash_tag_share_a_partition`: `{user1}.a` and `{user1}.b`, and `{user1}` and
  `x{user1}y`, share `partitionOf`.
- Redis shapes: `DEL replies 1 for a deleted key and 0 for a missing one`, `EXISTS replies 1
  for a live key and 0 for a missing or expired one`, `TYPE replies string for a String key and
  none for a missing one`, `PING replies PONG`. Cluster: `a coroutine awaits the engine's reply
  without blocking on the future` (under `runBlocking`; coroutines-test is not a cluster
  dependency and a real executor completes the future).
- This entry.

**Deviations:** None against plan 2.3, the frozen types or the ticket. Judgement calls: (1) the
expiry boundary is "readable through the deadline instant, gone after it"
(`now.isAfter(expiresAt)`), Redis's `now > when`; T09's C7 test should assume the same. (2)
`PING` runs on partition 0's executor rather than completing inline, so every command has one
path and the C1 gate can use any command. (3) `Command.Set` is a plain class, not a data class,
because `ByteArray` equality is by reference. (4) Lincheck was unavailable offline; the
gate-clock interleaving test above is the C1 proof, and it is deterministic in both directions
(it cannot pass with a multi-thread partition, since the busy thread cannot host the third
command). (5) The T01 server test that asserted the `submit` stub threw was rewritten to assert
PING through a real engine.

**For the next ticket:** `ApEngine.keyOf` is a `when` over every variant and
`Partition.execute` is another; T03 adds many variants and should consider whether a keyed
intermediate is worth the frozen-shape change (it was not for six commands). Multi-key
`DEL`/`EXISTS`/`MGET`/`MSET` fan-out (ADR 0002) belongs in `ApEngine.submit`, not in
`Partition`; a partition still only ever sees single-key work. `Entry.value` is a bare
`ByteArray`; T03/T04 will want a small sealed value type for Hash and List and `TYPE`, and C13's
type check should sit in front of `Partition.execute`'s branches. `live()` is the only expiry
hook: T09 schedules on the wheel in the `Set` branch and cancels in `Del`. The executors are
daemon threads; `close()` is `shutdown()`, not `shutdownNow()`, so queued commands still
complete. `atomically` (T14) should run `block` via the same executor with a `PartitionContext`
that calls `execute` directly.
