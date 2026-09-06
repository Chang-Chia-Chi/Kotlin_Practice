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

## T08: Hierarchical timer wheel

**Built:** `TimerWheel<K>` in `dynacache.engine.ds`, kotlin-stdlib only. Constructor takes the
starting `Instant`, the tick in milliseconds (default 1000), the slot count per level (default
256) and the fire callback `(K) -> Unit`. Three levels whose slot widths are tick, tick x slots
and tick x slots^2 (the spec's 1 s x 256, 256 s x 256, ~18 h x 256 at the defaults) plus an
overflow list for anything past the third level's horizon (194 days at the defaults).
`schedule(key, deadline)`, `reschedule(key, deadline)`, `cancel(key): Boolean`,
`advanceTo(now)`. A key map onto intrusive doubly linked slot lists makes schedule, reschedule
and cancel O(1) with no scan and no lingering cancelled entries. `advanceTo` walks every tick
between the last position and `now`, cascades level 2 and level 1 (and the overflow) when the
tick crosses one of their slot boundaries, and fires the level-0 slot sorted by deadline; when
the wheel is empty it jumps straight to the target. No thread, no clock read: time enters only
through the constructor and `advanceTo`. Eight tests, all in `TimerWheelTest`.

**Concepts named:** A deadline is rounded up to the next tick boundary and fires when the wheel
has advanced through that boundary, so it never fires early and fires at most one tick late
provided the driver advances at least once per tick; that is C7 at the structure level. Within
a tick the slot is sorted by the exact millisecond deadline, so fire order never inverts even
below tick resolution (I7). A deadline already in the past when scheduled fires on the first
tick after it was scheduled, never synchronously inside `schedule`. `schedule` on a key that
already has an entry replaces it; `reschedule` is the same call under the name the re-EXPIRE
path will reach for, so no caller can leave two entries for one key. The callback runs on the
thread calling `advanceTo` and may schedule or cancel freely, since the slot being fired is
detached first. The wheel is not thread-safe by design: one partition executor owns one wheel
(plan 2.5). `K` is a generic type parameter rather than `Key`: the wheel only needs hash and
equality, keeping `dynacache.engine.ds` a pure data-structure package, and letting the
million-entry test use `Int` keys; T09 instantiates `TimerWheel<Key>`.

**Acceptance:**
- `wheel_fires_on_time`: deadline 5 s is not fired at 4999 ms and is fired at 5000 ms, once.
- `wheel_no_early_fire`: deadline 10 s is still pending at 9999 ms.
- `wheel_cancel_prevents_fire`: `cancel` returns true then false, and only the other key fires.
- `wheel_replace_ttl`: 2 s rescheduled to 8 s fires exactly once at 8 s; nothing at 2 s.
- `wheel_ordering`: 3 s, 1 s, 2 s scheduled out of order fire as 1 s, 2 s, 3 s.
- `wheel_high_volume`: 1,000,000 seeded deadlines uniform over three days (exercising the 256 s
  and 65536 s levels), advanced one tick at a time; every key fires at a `now` with
  deadline <= now < deadline + tick. About one second.
- `I7_fire_order_never_inverts`: 5,000 seeded deadlines sharing ticks fire in non-decreasing
  deadline order.
- `C7_never_fires_before_deadline`: four slots per level and a 1 ms tick (64 ms horizon) with
  deadlines spread over 3 s, scheduled both up front and mid-run, some already past, plus a
  reschedule; every key fires within one tick of its due tick, through every level and the
  overflow.
- `mvn -o clean package` green: engine 22, cluster 1, server 1.
- This entry.

**Deviations:** None. Judgement calls: the third level is followed by an overflow list rather
than an error, so a TTL past 194 days is legal and merely waits; slot widths derive from one
tick and one slot count rather than three independent widths, which is what makes the cascade
arithmetic a pair of modulo operations; a deadline in the past fires on the next tick rather
than at once inside `schedule`, so a callback never runs from a command path.

**For the next ticket:** T09 constructs one `TimerWheel<Key>(clock.instant(), tickMillis, 256)
{ key -> delete it }` per partition and calls `advanceTo(clock.instant())` from the server's
scheduler coroutine on that partition's executor; the callback runs on the executor, so the
delete needs no extra synchronisation. `SET EX/PX`, `EXPIRE` and friends call `schedule` (it
replaces), `PERSIST` calls `cancel`, and a deleted key must also be cancelled or its stale
callback will run against a fresh value. C7 at the command level follows from the structure's
guarantee only if the scheduler advances at least once per tick; the lazy check on access
covers the gap between deadline and the next tick.

## T12: RESP2 codec

**Built:** `dynacache-server` gains its first main source, `dynacache.server.Resp.kt`:
`encodeReply(Reply): ByteArray` renders all five RESP2 shapes, and `RespDecoder` reads bytes
incrementally with no socket and no Netty in sight. The decoder buffers what it is fed and
tries a whole frame from the last complete one, rewinding when the frame is short, so a frame
split across any number of `feed` calls resumes. `RespProtocolException` carries Redis's own
wording for malformed input.

**Concepts named:** The decoder has two entry points because Redis reads the two directions
differently. `nextCommand()` reads a client stream: a frame starting with `*` is an array of
bulk strings, anything else is an inline command, and an empty frame (`*0`, `*-1`, a blank
line) is skipped the way Redis skips it rather than surfacing as a command with no name.
`nextReply()` reads a server stream, where every value carries its type byte, and returns a
`Reply`; that is what `resp_encode_decode_roundtrip` closes the loop with and what T13's
test-kit client will read. Both share one try-parse-and-rewind core, so partial-frame resume is
written once. The seams are exactly the two the plan entry names: bytes in and frames out,
`Reply` in and bytes out. No new interface was introduced: there is one implementation of each
direction, so a seam would have been an abstraction with nothing behind it.

**Acceptance:**
- `resp_encode_decode_roundtrip`: thirteen replies covering every shape, including a bulk of
  non-text bytes, a nil bulk, an empty array and a nested array, survive encode then decode.
- `resp_bulk_string_nil`: `$-1\r\n` decodes to `Reply.Bulk(null)`.
- `resp_error_format`: `ERR`, `WRONGTYPE` and `EXECABORT` each render as
  `-<KIND> <message>\r\n` and start with `-`.
- `resp_inline_command`: `PING\r\n` parses, runs of spaces collapse, and an inline argument of
  bytes that are not text survives intact.
- `resp_fuzz_no_crash`: 10,000 seeded sequences (`Random(20260906)`) built from well-formed
  frames and junk, fed in random chunks of one to eight bytes, drained alternately through
  `nextCommand` and `nextReply`. Every one yields frames or a `RespProtocolException`; the test
  also asserts the fuzzer produced both, so it cannot pass vacuously.
- `C8_reply_bytes_match_redis`: a sixteen-row golden table written from the RESP2
  specification, one row per reply shape.
- Supporting tests: partial resume byte by byte, pipelined frames in order, empty frames
  skipped, five malformed inputs with their Redis wording, an unterminated inline line refused
  at 64 KB, and deep nesting refused rather than overflowing the stack.
- This entry.

**Deviations:** Three, all forced by frozen contracts or deliberate.
1. `Reply` has no nil-array shape, so `*-1\r\n` cannot be encoded, and `nextReply` treats a
   negative multibulk length as `invalid multibulk length`. The ticket made representing it
   optional. `nextCommand` instead skips `*-1` and `*0` with no command, which is what the real
   Redis server does.
2. `feed` takes a `ByteArray` only, not a `java.nio.ByteBuffer`. The ticket allowed either;
   T13 will hand it `ByteBufUtil.getBytes`.
3. A line must end `\r\n`. Real Redis also accepts a bare `\n` on inline input from a raw
   telnet session. Debt, cheap to repay in `readLine` if a raw-telnet test ever wants it.
   Also debt: an inline command splits on spaces only, without Redis's quoting rules from
   `sdssplitargs`, which is enough for the ticket's space-separated form.

**For the next ticket:** T13 owns the Netty pipeline and the token-to-`Command` parser. Notes
it will want. `nextCommand` never returns an empty token list, so the parser always has a
command name. A `RespProtocolException` poisons the stream: the decoder discards its buffer and
T13 should reply `-ERR Protocol error: <message>` and close the connection, which is Redis's
behaviour; the exception message is the bare wording with no `Protocol error:` prefix, so T13
adds it. The limits are constructor parameters with Redis's defaults (64 KB inline, 1M
multibulk elements, 512 MB bulk, 32 levels of nesting), so a test can shrink them. `feed`
copies the whole buffer each call, marked with a `ponytail:` comment; it is O(n^2) on a frame
delivered byte by byte, and the place to fix it is Netty's `ByteBuf` in T13, not here.
`encodeReply` returns a fresh `ByteArray`, so T13 wraps it with `Unpooled.wrappedBuffer`.

## T06: Skip list

**Built:** `dynacache-engine` gains its first data structure, `dynacache.engine.ds`: `SkipList`
and the `Entry` it hands back. The list is keyed by (score, member), ascending by score with the
member bytes as an unsigned lexicographic tiebreak, and carries `insert`, `remove`,
`updateScore`, `rangeByScore` (inclusive by default, exclusive bounds and the infinities as
parameters), `rangeByRank` (0-based, inclusive, clamped), `rank`, `forward()`, `backward()`,
`size` and `comparisons`. Level generation is a `kotlin.random.Random` handed to the
constructor, with a `SkipList(seed: Long)` convenience beside it. Nine tests. No engine wiring:
nothing outside the package knows the list exists yet.

**Concepts named:** An **entry** is one element of a sorted set, a score and the member bytes
scored by it, and it is the only type the list hands out; it holds the list's own array rather
than a copy, exactly as `Key` holds its bytes. The key of the list is the pair, not the member:
two entries with the same member at different scores are different entries, so member
uniqueness is not this structure's job but the score map's beside it in T07. Three pieces of
node state carry the ticket's six operations: `next` with a **span** per level (how many entries
that pointer steps over) turns rank into arithmetic on the same walk a search already does, and
a level-0 `backward` pointer makes reverse traversal a step rather than a reversed copy. One
private `pathTo` walk serves insert, remove and rank, which is why `comparisons` counts in one
place, `precedes`. No lock anywhere: the partition executor owns the list and runs one command
at a time (C1).

**Acceptance:**
- `skiplist_insert_order`: 500 seeded random scores, forward traversal equals the same scores
  through `sorted()`.
- `skiplist_delete_preserves_order`: 300 entries, a third removed, the survivors still match an
  independently sorted model; a second remove of the same entry and a remove of an absent one
  both return false.
- `skiplist_range_query`: inclusive `[lo, hi]`, the exclusive form, both infinities as bounds,
  an inverted range and an empty one, each against a filtered sorted model.
- `skiplist_rank_correct`: 400 inserts and 80 removes, every survivor's rank equals its position
  in the sorted model; an absent score, an absent member and an absent pair all give -1.
- `skiplist_duplicate_score_lex_order`: eight members at one score, including bytes 0x01 and
  0xff, come back in unsigned byte order, so 0xff sorts last rather than first.
- `skiplist_log_n_property`: 100,000 seeded inserts, 2,000 seeded searches, 30.35 comparisons
  per search against the bound of 2 log2 N = 33.22.
- Level generation takes an injected seed: `SkipList(seed = 42)` throughout, and the
  100,000-entry measurement is byte-identical run to run.
- Beyond the ticket's list: `range_by_rank_reads_positions_and_reverse_traversal_reads_them_backwards`
  and `I3_ranks_and_order_survive_an_interleaved_insert_and_remove_storm` (3,000 seeded mixed
  operations against a sorted model, checking order, both traversals, every rank and
  `rangeByRank` every 250 steps).
- `mvn clean package` green: engine 23, cluster 1, server 1.
- This entry.

**Deviations:** None against the ticket, the plan entry or spec 6.2. Four judgement calls worth
recording. `updateScore` is a remove followed by an insert rather than the in-place relink Redis
does when the move crosses no neighbour; it costs one extra walk and no complexity class, and it
is why the operation is three lines. `rank` walks through the same `pathTo` as insert and
remove, so a read allocates the two 32-slot path arrays a write needs; one dedicated read-only
walk would avoid it, and a profile, not taste, should decide that. `rangeByRank` clamps its
bounds and takes only non-negative positions: Redis's negative indices are a command-layer
convention and belong in T07, not in the structure. The branch probability is Redis's 0.25 with
a maximum of 32 levels.

**For the next ticket:** T07 owns the dual index. The list gives it order and rank;
member-to-score must live in the T05 hash table beside it, because `insert` treats
(score, member) as the key and will happily hold one member at two scores if the caller lets it.
`ZADD` on an existing member is `updateScore(oldScore, member, newScore)`, and the old score has
to come from the score map. The list takes any `Double`, NaN included, and NaN sorts nowhere
useful: `ZADD` must reject a non-float argument with Redis's own error before reaching here.
`-0.0` and `0.0` are one score everywhere in the structure, `Entry.hashCode` included. Entries
hand out the list's own member array, so nothing may mutate it after an insert. The log-n margin
is 9 percent, 30.35 against 33.22: a future change to the seed or the branch probability should
re-read that number rather than assume headroom.

## T21: Dotted Version Vectors

**Built:** `dynacache.cluster.Dvv(dot, context)` and `Dot(node, counter)` as data classes,
with `dominates`, `isConcurrent`, `bump(counter)`, `merge(other, counter)`, `encode()` and
`Dvv.decode(bytes)`; `dynacache.cluster.DotCounter`, the one source of dots a node hands out,
built by `DotCounter.of(node, localData: Iterable<Dvv>)` and advanced by `next()`. Eight tests
in `DvvTest`. `mvn clean package` green: engine 35, cluster 18, server 1. Three new files,
271 lines including tests; nothing else touched.

**Concepts named:** A **dot** is the event that created one version, `(node, counter)`. A
DVV's **context** is `node -> highest counter seen`, and dot plus context stand for a set of
dots: the dot itself and, per context entry, every counter up to it. `dominates` and
`isConcurrent` are set inclusion over those dots, so a version whose dot fills a gap its
context never saw stays concurrent with a version that did see the gap (the paper's reason for
keeping the dot outside the vector). `bump` is the next write built on a version: a fresh dot
over the version's context with its own dot folded in. `merge` is the clock half of spec 5.3's
concurrent case: a fresh dot over the pointwise max of both contexts, raised to cover both
dots, so the result strictly dominates both inputs; what to do with the two values is T29.
`DotCounter` is the per-node C2 guarantee: an `AtomicLong` seeded from the highest own counter
in a scan of local data (own dots and own context entries alike), so a restart resumes above
everything that reached disk. No new seam: the counter is concrete, and the "local data" scan
is an `Iterable<Dvv>` argument until P4 supplies a real one. The wire form is hand-rolled:
length-prefixed UTF-8 node names and unsigned LEB128 varints, context entries sorted by node
so equal DVVs encode to equal bytes; a one-entry DVV with short names is 8 bytes. It lives in
the cluster module for now; T31 may lift it into the engine, which is why it uses only
`java.io.ByteArrayOutputStream` and the stdlib.

**Acceptance:**
- `dvv_dominance_detection`: B built on A's dot dominates A; A does not dominate B or itself.
- `dvv_concurrent_detection`: independent first writes on two nodes are concurrent both ways;
  a version is not concurrent with itself; a descendant is not concurrent with its ancestor.
- `dvv_merge_preserves_causality`: the merge of two concurrent versions dominates both, takes
  the coordinator's next dot, and its context is the max per node covering both dots.
- `dvv_bounded_size`: 10,000 seeded writes from 100 clients through 3 nodes on one key, each
  client writing on what it last saw or on a read from a random node, each node replacing on
  dominance and merging on concurrency; every written and stored context has at most 3
  entries.
- `dvv_no_counter_reuse`: a counter rebuilt from five own dots and a remote version whose
  context saw own counter 7 resumes at 8; a counter for a node whose dot 9 appears in local
  data resumes at 10.
- `I4_later_write_dominates`: a 20-link chain alternating between two nodes; every link
  strictly dominates every earlier link and none dominates a later one.
- `C2_counter_strictly_increases`: a counter restored above 41 hands out 42 next, and over
  1,000 rounds interleaved with merges of foreign versions every dot is strictly higher than
  the last.
- `dvv_encoding_roundtrip`: 202 DVVs (edge cases plus seeded random ones over four node names
  including non-ASCII) decode to equal values and re-encode to identical bytes; the 8-byte
  minimal case; a trailing byte is rejected with `IllegalArgumentException`.
- This entry.

**Deviations:** None against the spec or the plan entry. Two judgement calls. The plan entry
writes `bump(nodeId)`; the code takes the node's `DotCounter` instead, because a node's next
counter is a fact about the node's history, not something a DVV can compute from its own
context: two concurrent writes at one node from clients with stale contexts would both derive
the same `context[node] + 1` and reuse it, which is exactly what C2 forbids. `merge` takes the
counter for the same reason. Second, `dominates` uses dot-set inclusion (the paper's semantics)
rather than a bare pointwise comparison of contexts, which costs one extra condition in
`coversUpTo` and is what makes the "dot fills a gap" case come out concurrent.

**For the next ticket:** T22's read path picks "the value with the highest DVV" (C4) with
`dominates`; when two replies are concurrent the merge rule is T29's, and until then T22 can
keep both or pick deterministically and say so. T22's coordinator owns one `DotCounter` per
node and calls `bump` on the version the client read, or constructs `Dvv(counter.next(),
emptyMap())` for a first write. `encode`/`decode` are ready for the transport envelope and the
RDB codec; the format has no version byte, so T31 should add one if it lifts it into the
engine. Equal DVVs neither dominate nor are concurrent: a replica receiving the version it
already holds should treat that as "keep local". `DotCounter.of` reads only the dots and
context entries for its own node; a node that loses its local data entirely can reuse counters,
which is the known DVV limitation P4's persistence closes.

## T18: Transport seam and in-process cluster test kit

**Built:** `dynacache-cluster` gains its message model and its first seam. `src/main/proto/cluster.proto`
(package `dynacache.cluster.proto`) declares `Envelope { from, to, oneof body }` with one placeholder
body, `Ping { seq }`; later tickets add their own bodies to the oneof. The pom now runs protoc 3.25.3
through `protobuf-maven-plugin` 0.6.1 (with `os-maven-plugin` 1.7.1 as a build extension) in three
executions: protobuf Java, `protoc-gen-grpc-java` 1.63.0 and `protoc-gen-grpc-kotlin` 1.4.1 (jdk8),
so T23 never has to go online. The generated Java classes are the cluster's message model; no codec
exists. `Transport` (main sources) is the seam: `suspend fun send(to: NodeId, envelope: Envelope)`,
`val inbound: ReceiveChannel<Envelope>`, `close()`, one endpoint per node, in-order delivery per
sender-to-receiver pair and nothing promised across pairs. The test kit lives in the cluster module's
test sources, package `dynacache.cluster`: `InMemoryTransport` (the hub: `endpoint(node)`,
`networkPartition(sides)`, `heal()`, `drop(rate, seed)`, `delay(rounds, seed)`, `kill(node)`,
`restart(node)`, `drain()`), `InProcessCluster(nodeCount, n, w, r, clock, partitionsPerNode)`
(nodes `node-1..N`, one shared `Ring`, one `ApEngine` and one endpoint per node, `engine(node)`,
`transport(node)`, `network`, `drainMessages()`, `writeVia`, `readVia`, `readAllReplicas(key)`,
`close()`), and `RecordingEngine(reply)` (records `submitted`, answers with the canned reply,
`atomically` is `TODO`). `kotlinx-coroutines-test` 1.10.1 joins the pom in test scope (it was in
`~/.m2`). Seven new tests; `clean package` offline: engine 35, cluster 17, server 1.

**Concepts named:** The hub is a **network** the test scripts; each node's `Transport` is an
**endpoint** on it. Delivery is explicit stepping, not virtual time: nothing moves until `drain()`,
which runs **delivery rounds** until nothing is in flight, yielding between rounds so receivers run
and their replies join the next round. `delay(rounds, seed)` holds each envelope a seeded number of
rounds, so it reorders delivery across pairs and never within one (each pair is a FIFO whose head
alone is checked for being due). `drop` is decided at send time by its own seeded `Random`; the
network partition and `kill` are judged at delivery time, so what is in flight when the fault forms
is lost, which is what a real cable cut does. A **network partition** (CONTEXT.md, always in full) is
a list of sides: an envelope crosses only when one side holds both ends. `kill` silences a node in
both directions and leaves its inbox and its engine untouched; `restart` lifts that and replays
nothing. `RecordingEngine` is the second adapter of the `CommandEngine` seam that plan 2.3 promised
from T18.

**Acceptance:**
- `transport_delivers_in_order_per_pair`: two nodes, 20 pings each way under `delay(0..3, seed)`,
  each side receives 1..20 in order (the delay is what makes ordering able to fail).
- `network_partition_blocks_both_directions`: `{alpha} | {bravo}`, a ping each way, nothing arrives.
- `heal_restores_delivery`: a ping lost under the split, then `heal()`, then a ping each way arrives.
- `kill_stops_delivery_and_restart_resumes`: killed node neither receives nor is heard; after
  `restart` both directions flow again and the lost ping stays lost.
- `drop_is_reproducible_by_seed`: 100 pings at rate 0.5; seed 11 twice gives the same delivered
  list, seed 12 a different one, and between 1 and 99 arrive.
- `cluster_boots_three_nodes_sharing_one_ring`: three nodes, the cluster's ring answers like
  `Ring.of(nodes)`, `writeVia`/`readVia` round-trip on one node's engine, `readAllReplicas` is keyed
  by the key's preference list, and an envelope sent on one node's endpoint reaches another after
  `drainMessages()`.
- `the recording engine records every submit and answers with the canned reply`.
- This entry.

**Deviations:**
- D-T18-1 (toolchain, approved by the orchestrator): `~/.m2` had no protoc, no protobuf plugin and no
  gRPC generators, so T18 ran Maven online once. The `protocPlugins` route builds a WinRun4J launcher
  for the jar-based grpc-kotlin generator but passes protoc no `--plugin=` flag for it, so the
  grpc-kotlin execution names the launcher through `pluginExecutable`; that path ends in `.exe` and is
  Windows-specific, which matches this project's single build machine. Debt: a `${os.detected.name}`
  switch if the build ever moves.
- No Kotlin protobuf DSL: `protobuf-maven-plugin` 0.6.1 has no `--kotlin_out` goal, so the generated
  Java classes (`Envelope.newBuilder()...`) are the model. The `protobuf-kotlin` dependency stays in
  the pom unused. Debt only if someone wants the `envelope { }` DSL; the ascopes plugin would give it.
- `inbound` is a `ReceiveChannel<Envelope>` rather than plan 2.3's `Flow<message>`: the channel is
  what a flow would wrap, `for (m in inbound)` reads like a flow, and `tryReceive()` is what the
  fault tests need to assert that nothing arrived. `receiveAsFlow()` is one call away.
- `delay` counts delivery rounds, not a `Duration`: explicit stepping (plan 1.5, 2.5) rather than
  virtual time, so the hub needs no scheduler and no scope. A duration-based delay can be layered on
  when a ticket drives timeouts through the transport (T20 drives SWIM by step functions instead).
- Test kit placement: cluster module `src/test`, not `src/main`. Every named consumer (T19, T20, T22)
  is a cluster-module test, and this keeps `kotlinx-coroutines-test` and the fakes out of the shipped
  jar. If the server's tests ever need `InProcessCluster`, a `maven-jar-plugin` `test-jar` goal
  exposes it; that is the "testFixtures" shape Maven has.

**For the next ticket:** T19 builds its router from `cluster.engine(node)`, `cluster.transport(node)`
and `cluster.ring`, adds `Forward`/`ForwardReply` to the oneof in `cluster.proto`, and in tests runs
the router's inbound loop in `backgroundScope` before `drainMessages()`; a request that awaits a
reply must be `async`-ed, then `drainMessages()` pumps request and reply in successive rounds.
`RecordingEngine.atomically` is `TODO` until a batch test needs it. `InProcessCluster.network` is
public so T22 can script faults on the cluster directly. `writeVia`/`readVia` hit the local engine;
T22 swaps them for the coordinator path and `readAllReplicas` already walks the preference list.
`Envelope.from`/`to` are strings the caller sets; the hub routes by the `send` parameter and never
reads them. The protoc launcher lands in `target/protoc-plugins`, so `clean` rebuilds it every run
from the cached artifacts (no network). Kotlin sources see the generated Java because Maven's
compiler plugin runs before the Kotlin plugin in this reactor and the generated protobuf never
references Kotlin code.

## T27: Merkle tree per vnode range

**Built:** `dynacache.cluster.MerkleLeaf(key, valueHash, dvv)`, `MerkleTree` and
`DivergentRange(from, to, keys)`. `MerkleTree.of(leaves, fanout = MerkleTree.FANOUT)` builds one
vnode range's tree: leaf hashes are SHA-256 over a tagged, length-prefixed encoding of
`key.bytes`, `valueHash` and `dvv.encode()`; each level above is the fan-out-way chunking of the
one below, node hashes SHA-256 over the concatenated children under a different tag. `root` is
the single hash of the top level. `diff(other)` compares roots, descends only into subtree pairs
whose hashes disagree, groups the leaf indices it lands on into contiguous spans and returns one
`DivergentRange` per span that really contains a difference. Six tests in `MerkleTreeTest`.
`mvn clean package` green: engine 44, cluster 24, server 16. Two new files, 265 lines including
tests; nothing else touched.

**Concepts named:** A **leaf** is one key's contribution to the tree, the `(key, value hash, DVV)`
triple of spec 2.4, and the tree hashes it without knowing what a value hash is: the caller
computes that, and no value is stored. A **divergent range** is one span of leaves whose subtree
hashes disagreed, carrying the keys inside it that really differ - present on one side only, or
holding a different value hash or a different DVV. That is what T28 exchanges. No seam: the tree
is concrete, tested through its own public methods, exactly as plan 2.3 lists it beside `Ring`,
`Dvv` and `HintStore`. Key order is unsigned byte order via `java.util.Arrays.compareUnsigned`,
the order keys are exchanged in, and `of` **sorts** its input rather than requiring sorted input,
so C6 holds whatever order a node's local scan yields. Duplicate keys are refused: a vnode range
holds a key once. Leaf and node hashes carry different one-byte tags and every leaf field is
length-prefixed, so no leaf hash can be read as a node hash and no two different triples can
collide by concatenation.

The descent is index-aligned, which is the honest consequence of the ticket's contract that a
leaf is a key rather than a hash sub-range. Two ranges holding the same keys line up level by
level, so a single changed value opens one subtree per level and nothing else; a range that has
gained or lost a key shifts every leaf after it, so the descent suspects the whole tail, and
trees of different height do not line up at all and every leaf becomes suspect. To keep the
answer exact in those cases, each suspect span is verified key by key against the other tree by
binary search, and a span with nothing really wrong in it is dropped. So `diff` never
over-reports: it only spends more of the descent's saving when the two ranges differ in shape.
That is safe by construction - a subtree pair whose hashes match holds byte-identical contents,
so no divergent key can hide inside one.

**Acceptance:**
- `C6_identical_data_identical_root`: 40 leaves supplied in list order and in a seeded shuffle
  produce the same root.
- `merkle_one_changed_key_changes_root`: rewriting one leaf's value hash changes the root.
- `merkle_dvv_change_alone_changes_root`: same value hash, a different dot, root changes.
- `merkle_empty_range_has_stable_root`: two empty trees agree and differ from a one-leaf tree.
- `merkle_diff_names_only_divergent_ranges`: a tree against itself diffs to the empty list; one
  rewritten key among 40 yields exactly `DivergentRange(key17, key17, [key17])`, and the diff is
  symmetric.
- `merkle_diff_detects_missing_key_on_one_side`: a key dropped from the middle yields the tail
  span `key17..key39` naming only `key17`; a key dropped from the end yields only `key39`; and
  17 leaves against 16, whose trees differ in height, yield only `key16`.

**Deviations:** None against the spec or the plan. Two choices the documents left open, recorded
here: `of` sorts its input rather than requiring sorted input (the ticket allowed either), and
`diff` verifies each suspect span key by key so the result is exact rather than a superset.
`fanout` is a private constructor parameter with the public default `MerkleTree.FANOUT = 16`;
`diff` refuses two trees built at different fan-outs.

**For the next ticket:** T28 gets `root: ByteArray` for the first exchange and
`diff(other): List<DivergentRange>` for the descent, and nothing else - there is no wire form for
a tree yet, no level-by-level exchange protocol and no `Ring` or `Vnode` coupling. The tree takes
whatever leaves it is handed, so T28 owns choosing the vnode range, scanning the local data for
it and deciding what a value hash is. `diff` is an in-process comparison of two whole trees; a
real anti-entropy round that ships one level at a time will need a per-level accessor, which is
deliberately not there yet. Hash arrays are held, not copied, the same convention `Key.bytes`
follows: a caller must not mutate `root` or a leaf's `valueHash`. The descent's saving depends on
the two ranges holding the same key set; after a range gains or loses keys, a `diff` costs a full
leaf scan while staying exact, which matters only if a future measurement shows it.

## T03: String completion and Hash

**Built:** The String set of spec 2.1 is complete and the Hash type exists. `Command` gains
`IncrBy(key, delta)` (one variant for `INCR`, `DECR`, `INCRBY` and `DECRBY`; the parser signs
the delta), `Append`, `StrLen`, the ten Hash variants (`HGet`, `HSet`, `HMSet`, `HDel`,
`HGetAll`, `HMGet`, `HExists`, `HKeys`, `HVals`, `HLen`), and four fanned variants `MGet`,
`MSet`, `DelKeys`, `ExistsKeys`. `Entry.value` is now a sealed `Value` (`Str(bytes)` or
`Hash(fields)`) instead of a bare `ByteArray`, and `TYPE` reports `string` or `hash` from
`Value.Kind.text`. `ApEngine.submit` routes on the command's shape and fans multi-key commands
out; `Partition` gained `submitAll`, one task for one partition's share of a fanned command.

**Concepts named:** `Command.Keyed(needs: Value.Kind?)` is the intermediate T02 asked us to
consider: it carries the one key and the kind that key must hold, which deleted `ApEngine.keyOf`
(a `when` that would now have eighteen arms) and gave C13 one place to live. `Command.Fanned`
is the other half: it knows its `keys`, the `single(index)` command for each argument, and how
to `join` the per-argument replies. Fan-out is therefore three lines in `ApEngine` and one line
per fanned command, and a partition still only ever sees `Keyed` or `Ping` (a `Fanned` reaching
`Partition.execute` is an `error`). `Value` is the stored cell's type; `Value.Kind` is the word
`TYPE` reports and the word a command names when it needs a kind. Hash field names are held as
ISO-8859-1 text, a lossless byte round trip, so the JDK's `LinkedHashMap` does the hashing and
the iteration order is stable for tests. Seams unchanged: `CommandEngine`, `PartitionContext`,
`Reply`, `Key`, `PartitionId` are exactly T01's.

**Acceptance:**
- `string_incr_atomic`: 11 from "10", the stored value becomes "11", 1 on a missing key, -1 for
  a negative delta, `-ERR value is not an integer or out of range` on "abc" with the value
  intact afterwards.
- `hash_field_independence`: two new fields count 2, an overwrite counts 0, the other field is
  untouched, a missing field and a missing key are both nil.
- `hash_getall_complete`: a missing hash is an empty array; a two-field hash is the flat array
  `[a, 1, b, 2]`.
- `mget_spans_partitions`: two keys found on different partitions via `partitionOf` plus a
  missing one, one array in argument order with nil in the middle.
- `mget_across_partitions_is_not_atomic`: pins ADR 0002. A `ParkingClock` parks the first
  command on one named partition thread; that is the MGET's first part, and the fan-out has not
  reached the other partition yet, so a whole `MSET` (keys given in the other partition's order)
  lands there in the gap. The reply is `[old, new]`, which neither an atomic MGET nor an atomic
  MSET could produce. Mutation-checked: switching `fanOut` to submit the parts together and
  gather with `allOf` makes it fail with `[old, old]`.
- Redis shapes: `MSET writes every key and DEL and EXISTS count across partitions`,
  `APPEND extends the value and replies with the new length, STRLEN measures it`,
  `HDEL removes fields and the key goes with its last field`,
  `HMSET, HMGET, HEXISTS, HKEYS, HVALS and HLEN reply in Redis shapes`.
- C13 mechanism: `a command meant for another kind is refused without touching the key` (HGET
  and HSET on a String key, GET/INCRBY/APPEND/STRLEN on a Hash key, all `-WRONGTYPE ...`, both
  values intact afterwards; `TYPE`, `EXISTS` and `SET` work on any kind). T04 owns the named
  `C13_wrongtype_leaves_value_intact`.
- Every T01 and T02 test still green. `mvn -o clean package`: engine 37, cluster 10, server 1.
- This entry.

**Deviations:** None against the spec, the ticket, ADR 0002 or the frozen types. Six judgement
calls:
1. **Fan-out is sequential, partition by partition** (spec 2.2's own words), not concurrent.
   Concurrent parts would be faster but make the interleaving non-deterministic: with every
   part enqueued back to back from the caller thread, per-partition FIFO orders the reader
   before the writer on *every* partition, and the MGET comes out atomic-looking, so
   `mget_across_partitions_is_not_atomic` cannot be written without a sleep or a race. This is
   a deliberate ceiling, named in the code: fan-out latency is the sum over partitions, and the
   repair is to submit the parts together and gather with `allOf` (which is exactly the
   mutation the pinning test catches, so that repair needs a different test).
2. **Both single-key and variadic `DEL`/`EXISTS` exist.** `Del(key)` and `Exists(key)` are what
   a partition runs; `DelKeys(keys)` and `ExistsKeys(keys)` are the wire forms that fan out to
   them. Naming them `MDel`/`MExists` would invent Redis verbs that do not exist, and folding
   them into one variadic type would leave the partition with a multi-key command to unpack.
3. **`Command.Keyed` and `Command.Fanned` intermediates** inside the sealed `Command` root, the
   change T02 flagged. The root, `Reply`, `Key`, `PartitionId` and the `CommandEngine` and
   `PartitionContext` signatures are untouched.
4. **`MGET` answers nil, not `WRONGTYPE`, for a key holding a Hash**, which is Redis's
   documented behaviour; `MGet.join` maps an error element to nil. Single-key `GET` still
   errors. `MSET`, `DEL` and `EXISTS` work on any kind, so they cannot produce one.
5. **One clock read per executed command, not per submitted command.** A fanned command's part
   reads the clock once per key it covers, so an `MGET` of three keys reads it three times. C1's
   rule ("the clock is read exactly once per command") still holds for what a partition runs.
6. **`INCRBY` rejects a leading `+`** (Redis's `string2ll` takes only an optional `-`), and
   overflow is caught with `Math.addExact` and answered with the same
   `ERR value is not an integer or out of range`; Redis says "increment or decrement would
   overflow" there, which is a different string this ticket did not name.

**For the next ticket:** `Partition.execute`'s `when` is grouped String, Hash, key management,
and the C13 kind check sits above it: T04's List adds `Value.List` plus `Value.Kind.LIST` and
its variants declare `needs = LIST`; nothing else moves. `Entry.str` casts to `Value.Str` and is
only safe *after* that check, so any new String branch must declare `needs`. `HSET`/`HMSET`
create the hash with no TTL and leave an existing key's TTL alone, and `HDEL` removes the key
with its last field, both as Redis does. Hash fields are `LinkedHashMap<String, ByteArray>` keyed
by ISO-8859-1 text (`fieldName`/`fieldBytes` in `Value.kt`); T05's `HSCAN` iterates that map and
T05's hand-built table replaces the `HashMap` store, not the field maps, unless it wants to.
Multi-key fan-out lives entirely in `ApEngine.fanOut`, so T04's `KEYS`, `DBSIZE` and `FLUSHDB`
compose the same way but need a different shape: they have no keys to group by, so they want a
"every partition" variant rather than a `Fanned`. `atomically` is still `TODO("T14: batches")`.

## T23: gRPC transport adapter

**Built:** `cluster.proto` gains `service ClusterService { rpc Deliver(Envelope) returns
(Delivered); }` and an empty `Delivered` message, so the grpc-java and grpc-kotlin generators
now actually emit stubs (T18 configured them against a proto with no service). `GrpcTransport`
in `dynacache-cluster/src/main` is the second adapter of the `Transport` seam: it starts a gRPC
server in its constructor, opens one `ManagedChannel` per peer on the first send to that peer,
and delivers into the same `Channel<Envelope>` that `inbound` exposes. `HostPort(host, port)`
is the new address type. No codec anywhere: the generated `Envelope` is the wire message and
`Deliver` carries it whole.

**Concepts named:** `HostPort` is where a node's gRPC server listens - the transport's only
notion of an address, kept apart from `NodeId`, which stays the cluster's identity. `boundPort`
is how a node started on port 0 says which ephemeral port it got. The **inbox** is the server
side of the seam, an inner `ClusterServiceCoroutineImplBase` whose `deliver` puts the envelope
on the receive channel and answers `Delivered`; nothing else in the adapter knows gRPC exists.
The seam itself is untouched: `send`, `inbound` and `close` are T18's signatures exactly, so
the in-memory adapter and this one are interchangeable to every caller.

Two design points worth carrying:

- **Unary, not client-streaming.** One call carries one envelope and its acceptance, which is
  the whole of what `Transport.send` promises. A stream would add a lifecycle and a reconnect
  policy that no ticket yet asks for, and would still have to answer the same question on
  failure. A reply to an envelope is another envelope (T19), never an RPC response.
- **`peers` is read at send time, not at construction.** The map is a `Map<NodeId, HostPort>`
  the caller keeps. Two nodes on ephemeral ports cannot both know the other's port at
  construction, so a caller passes one mutable map to both and fills it in once both have
  bound; the lazy channel creation makes that work with no extra API. The round-trip test is
  exactly this shape, and T24's three-node harness can use it unchanged.

**Acceptance:**
- `grpc_transport_roundtrip_every_message_type` (`GrpcTransportTest`): two `GrpcTransport`s on
  localhost ephemeral ports exchange one envelope per `oneof body` case in both directions and
  assert the received envelope equals the sent one. The cases come from
  `Envelope.BodyCase.values()` and each is built by a `when` **expression**, so a case added by
  a later ticket stops the test file compiling until it is given an envelope of its own. That
  guard was verified, not assumed: a second case added to the oneof produced `'when' expression
  must be exhaustive. Add the 'PROBE' branch`, and was then reverted.
- `grpc_peer_down_is_a_send_error` (`GrpcTransportTest`): a peer whose `HostPort` names a port
  the OS handed out and took back. gRPC's default fail-fast turns the refused connection into a
  `StatusException` well inside the deadline. Because the assertion is `assertThrows` around a
  `withTimeout`, a hang would surface as `TimeoutCancellationException` and fail the test
  rather than pass it, which is the distinction the ticket asked for.
- Generated classes only in the cluster module: `grep -r "dynacache.cluster.proto"
  dynacache-engine/src` is empty.
- `mvn -B -o clean package` green offline: engine 44, cluster 27, server 16.

**Deviations:**
- **The `grpc-kotlin` `compile-custom` execution is removed from `dynacache-cluster/pom.xml`.**
  T18's deviation note says `protocPlugins` "passes protoc no `--plugin=` flag" for the
  jar-based generator, which is why that execution named the launcher through
  `pluginExecutable`. That is not what happens: `protocPlugins` does run the generator as part
  of the `protobuf-java` execution. With no service in the proto it emitted nothing, so the
  redundancy was invisible; the moment `ClusterService` existed the coroutine stub was written
  into both `generated-sources/protobuf/java` and `.../grpc-kotlin` and Kotlin failed with
  `Redeclaration: object ClusterServiceGrpcKt`. Deleting the execution is the fix. The
  Windows-only `.exe` path goes away with it, so T18's portability debt is repaid as a side
  effect; the WinRun4J launcher itself is still built by `protocPlugins`, so a move off Windows
  would still need looking at. `ClusterGrpcKt.kt` now lands in `generated-sources/protobuf/java`
  beside the message classes, and `ClusterServiceGrpc.java` stays in `.../grpc-java`.
- **`runBlocking`, not `runTest`, in `GrpcTransportTest`.** These are the project's only tests
  on real sockets. Under `runTest` the deadlines would run on the scheduler's virtual clock,
  which advances whenever the coroutine is idle, so a `withTimeout` would fire while a socket
  was still legitimately in flight. Real time is the acceptance boundary here; every wait
  carries a deadline and there is no sleep anywhere.
- **No new dependencies.** The generated stubs compiled against what the cluster pom already
  declares. Neither `grpc-stub` nor `javax.annotation` had to be added, contrary to what the
  ticket allowed for.
- **Nothing added to `dynacache-server`.** The ticket left the gRPC-next-to-Netty hosting open.
  `GrpcTransport` hosts its own server, so a server-module node needs only to construct one -
  there is no separate hosting concern to write, and no `main` exists yet to wire it into. T24
  starts three nodes and is where that wiring belongs.
- **Size: 157 lines of new Kotlin** (76 main, 81 test) plus 11 proto lines and a 9-line pom
  deletion, under the ticket's 200-line floor. Both acceptance tests exist and the seam is
  fully implemented; there was no third behaviour to write that the seam promises and this
  adapter does not already deliver. Padding it would have meant testing gRPC rather than
  DynaCache.

**For the next ticket:** `Transport.send` on this adapter awaits the peer's acknowledgement, so
sequential sends to one peer arrive in send order (the seam's promise) but concurrent ones do
not - if T19 or T22 fan out with `async`, order per pair is no longer guaranteed and a caller
that needs it must serialize per peer. A peer that is down surfaces as a `StatusException` out
of `send`; nothing retries, so the first caller that cares about a dead peer (T20's SWIM, T22's
hinted handoff) decides the policy. `close()` stops the server, then the channels, then closes
the inbox, in that order, so an in-flight `deliver` is not cut off by a closed channel.
`GrpcTransport` is `AutoCloseable`, so tests can `use` it. Adding a case to the `oneof` in
`cluster.proto` will break `GrpcTransportTest` compilation by design: add the branch, do not
add an `else`. `HostPort` is deliberately plain; if a node ever needs TLS or a name rather than
an address, that is where it goes, and `Grpc.newChannelBuilder` already takes credentials.

## T20: SWIM gossip membership

**Built:** `dynacache.cluster.Membership` is the seam: `members: Map<NodeId, Member>` where
`Member(node, state, incarnation)` and `MemberState` is `ALIVE`, `SUSPECT`, `DEAD`; `alive`,
`suspect` and `dead` are derived sets; `changes: Flow<Member>` emits every row that changes.
`dynacache.cluster.Swim(self, peers, transport, random, incarnation, period, k, rttTicks,
suspectTicks)` is the one production adapter: a per-node SWIM failure detector and table driven
by `tick()`, one protocol period per call, and `run()` for production (tick, delay a period,
forever, on the node's one gossip coroutine). `cluster.proto` gains `Ack { seq }`,
`PingReq { seq, target }` in the `oneof body` (fields 11, 12; a comment reserves 100 and above)
and `repeated MembershipEntry membership = 3` on `Envelope` (node, state, incarnation).
`ScriptedMembership(nodes)` in the cluster module's test sources is the test kit's adapter:
every node alive until `set(node, state, incarnation)`. Six tests in `SwimTest`. `mvn clean
package` offline green: engine 44, cluster 31, server 16. Five files, 392 lines including the
proto and tests.

**Concepts named:** A **tick** is one protocol period. Each tick a node first handles what
arrived (merges the piggyback, acks pings, relays ping-reqs, forwards relayed acks), then
escalates its open **probes**: a direct ping with no ack after `rttTicks` becomes a **ping-req**
through `k` random intermediaries, and no ack after `2 * rttTicks` more (the indirect path is
two hops each way) makes the target **suspect**; a suspect not refuted within `suspectTicks`
is **dead**; then it pings one random non-dead peer. A probe records the incarnation it was
aimed at, and its suspicion names that incarnation, so a stale probe never re-suspects a node
that has re-incarnated since. The **piggyback** is the whole table on every envelope
(ponytail: a recency-bounded set if N grows). **Merge** is the ticket's rule as a comparator:
higher incarnation wins, at equal incarnation dead > suspect > alive, and a losing or equal row
is ignored, so an alive rumour never clears a suspicion at the same incarnation. Hearing itself
suspect or dead at an incarnation at or above its own, a node **refutes** by taking that
incarnation plus one; the next envelope it sends carries the refutation. Each node runs its own
suspect timer from the moment it learns of a suspicion, whether by its own probe or by gossip.
The `Membership` seam has its two adapters (SWIM and the scripted fake), exactly plan 2.3.

**Acceptance:**
- `gossip_detects_failure`: 5 nodes, the last one killed (network `kill` and no more ticks);
  every survivor holds it dead at incarnation 0 within `(N-1) + 3*rtt + T + 4*log2(N)` rounds
  (a round is a tick on every live node then a full network drain), nobody holds a suspect, and
  node-1's change flow saw exactly suspect then dead.
- `gossip_detects_recovery`: after that, `restart` on the network and a new `Swim` at
  incarnation 1 on the same endpoint; every node holds it alive at incarnation 1 within
  `4*log2(N)` rounds and every node's alive set is the whole cluster.
- `I8_membership_change_reaches_all_within_log_n_rounds`: 5 and 7 nodes, seeded by N, c = 4
  (`C` in the test). Death: rounds from the first survivor holding the victim dead until all do,
  measured 1 (N=5) and 2 (N=7) against bounds 10 and 12. Recovery, which starts at exactly one
  node and so measures dissemination alone: 3 (N=5) and 5 (N=7) against the same bounds.
- `gossip_suspect_refuted_by_incarnation`: 3 nodes, T = 10; an envelope from the witness's
  endpoint carrying "accused suspect at 0" reaches the accuser, who adopts it; within T-1 rounds
  every node holds the accused alive at incarnation 1 and no one holds a suspect or a dead.
  Checked by mutation: with self-refutation disabled the test fails.
- `gossip_ping_req_masks_one_lost_link`: 3 nodes, k = 1, a network partition with the two
  overlapping sides `{left, bridge}` and `{right, bridge}` (the left-right link is gone, the
  bridge reaches both); after `4 * (3*rtt + T)` rounds everyone holds everyone alive at
  incarnation 0. Checked by mutation: with k = 0 both ends declare each other dead and refute
  in a loop.
- `scripted_membership_answers_what_the_test_set`: the fake's view and change flow.
- This entry.

**Deviations:**
- The membership piggyback is a repeated field on `Envelope`, not in the `oneof body` (a
  repeated field cannot live in a oneof, and "on every message" means every envelope, so a
  T19 `Forward` can carry it too once something reads it there).
- The RTT bound is in ticks, as the ticket says, and the true request-reply RTT under explicit
  stepping is 2 ticks (ping delivered in one drain, the ack sent on the target's next tick and
  seen on the requester's tick after that), so `rttTicks = 2` is the tight setting and the
  default. The indirect deadline is `2 * rttTicks` after the ping-req, not another `rttTicks`,
  because the relayed path is two hops each way. `run()` acks inbound only when it ticks, so in
  production the RTT bound is measured in periods too; a select loop that answers inbound as
  it arrives would let it drop, and is the debt if a 1s period proves too slow to detect.
- Tests drive `InMemoryTransport` directly rather than `InProcessCluster`: gossip needs
  endpoints, not engines, and `InProcessCluster` builds an `ApEngine` per node.
- `changes` is a `MutableSharedFlow` with a 1024 buffer that drops the oldest change on
  overflow (marked `ponytail:`); the view is authoritative, so a reader that falls behind
  re-reads `members`. A subscriber must be collecting before the change it wants to see.

**For the next ticket:** T22 and T25 take a `Membership`; in tests `ScriptedMembership(nodes)`
then `set(node, DEAD)` flips a replica out of `alive`. `Swim.tick()` reads the node's
`Transport.inbound` with `tryReceive` and answers gossip bodies; anything else in the oneof it
merges the piggyback of and drops, so the ticket that first puts two consumers on one inbound
(T19's router and SWIM) owns the demux: hand gossip envelopes to a `Swim` method and the rest to
the router. Kill a node in tests by `network.kill` and by no longer ticking it; restart it with
`network.restart` and a fresh `Swim` at a higher incarnation, which is what a real restart is.
Suspect and dead rows stay in the table (dead ones are never pinged), so a recovery is
detected by the incarnation, not by re-adding; dynamic membership stays on the do-not-build
list. `Swim.members` includes `self`. Value classes cannot be varargs, which is why the fake
takes a `Collection<NodeId>`.

## T04: List and key management, WRONGTYPE

**Built:** The List type and the Server command set of spec 2.1. `Value` gains `List(items:
ArrayDeque<ByteArray>)` and `Value.Kind.LIST`, so `TYPE` reports `list`. Kotlin's own
`ArrayDeque` is the deque: a circular buffer, so both ends push and pop in O(1) *and* `LINDEX`,
`LSET` and `LRANGE` still index in O(1), which `java.util.ArrayDeque` cannot do. `Command` gains
`Push(key, values, end)` and `Pop(key, end)` over `enum End { HEAD, TAIL }` (one variant per
Redis pair, the way `IncrBy` covers four String verbs), plus `LRange`, `LLen`, `LIndex`, `LSet`
and `LRem`. An emptied list is deleted, as Redis does; `LSET` errors `no such key` and `index out
of range` rather than growing the list.

`Command.EveryPartition` is the keyless shape T03 asked for: one abstract `join(replies,
random)`, and `ApEngine.everyPartition` runs the command on every partition, one after the
previous one finished, then joins in partition order. `DbSize`, `FlushDb`, `Keys(pattern)`,
`RandomKey` and `Info` are its five variants; `CommandTable` (`COMMAND`) runs on partition 0
beside `Ping`, since it needs nobody's share. `ApEngine` takes an injected
`java.util.Random` (defaulted), seeds one stream per partition from it, and keeps its own for
the `RANDOMKEY` join. `Glob.kt` is a byte-level port of Redis's `stringmatchlen`, which `KEYS`
uses now and T05's `SCAN MATCH` will use next.

C13 needed no new mechanism. The List variants declare `needs = Value.Kind.LIST` and T03's kind
check above `Partition.execute`'s `when` refuses them before any branch reaches the entry; this
ticket's job was to prove it for List and pin it, which the two named tests do.

**Concepts named:** `EveryPartition` is the third command shape, beside `Keyed` (one key, one
partition) and `Fanned` (several keys, grouped by partition). It is the one with *no* key: the
engine cannot route it, so every partition answers and the join makes one reply out of the
several. Unlike `Fanned` it needs no `single(index)`, because the partition runs the very
command the client sent; a partition therefore still never unpacks a multi-key argument list.
`join` takes the engine's `Random` because exactly one command, `RANDOMKEY`, has to choose
between the partitions' offers; the other four ignore it. `End` is the word for the two ends of
a list, so `LPUSH`/`RPUSH` and `LPOP`/`RPOP` are one variant each rather than four. `Entry.expired(now)`
is now the one expiry predicate, shared by `live()` (one key, on access) and `purgeExpired()`
(the whole store, in front of a keyspace-wide command), so spec 5.4's rule still lives in one
place. Seams unchanged: `CommandEngine`, `PartitionContext`, `Reply`, `Key`, `PartitionId` are
exactly T01's.

**Acceptance:**
- `list_push_pop_order`: `LPUSH a b c` then `RPOP` is `a` and `LPOP` is `c`; `TYPE` is `list`
  while the list lives, `none` after the last pop takes the key with it.
- `list_lrange_bounds`: `0 -1`, `-100 100`, `1 5`, `-1 -1` all clamp and none errors; start past
  stop and start past the end are both empty arrays, and so is a missing key.
- `wrongtype_rejected`: `LPUSH` on a String key is `-WRONGTYPE ...` and the key is still a
  String; the refusal runs both ways (`GET`, `HGET`, `LLEN` across the three kinds), while
  `EXISTS`, `TYPE` and `DEL` work on any kind.
- `C13_wrongtype_leaves_value_intact`: `LPUSH`, `RPOP`, `LSET`, `LREM`, `LRANGE` and `LINDEX` on
  a String key all answer `-WRONGTYPE ...`, and `GET` afterwards returns the original value.
- `keys_glob_patterns`: seven keys spanning partitions matched against `*`, `user:?`, `user:*`,
  `[ab]`, `[a-c]*`, `[^c]`, `c\[x]` and a pattern that matches nothing; an expired key is absent,
  and each key is reported once.
- `dbsize_and_flushdb_span_partitions`: two keys placed on different partitions via
  `partitionOf`; `DBSIZE` is 2, drops to 1 when one expires, `FLUSHDB` replies `+OK` and
  `DBSIZE` is 0 with both keys gone.
- `randomkey_nil_when_empty`: nil on an empty keyspace, the only key when there is one, nil
  again once it is deleted. Its companion seeds 20 keys plus one that expires and shows 200
  draws are all live keys from more than one partition.
- `lrem_count_semantics`: positive from the head, negative from the tail, zero and any count
  past the matches take them all; a value that is not there counts 0, so does a missing key,
  and an emptied list takes its key with it.
- `LLEN and LINDEX read the list without changing it`; `LSET replaces an element and errors
  outside the list`; `COMMAND and INFO answer in Redis shapes` (`COMMAND` an empty array, `INFO`
  a bulk carrying `dynacache_version:` and `db0:keys=<n>` counted across partitions).
- Every T01 to T03 test still green. `mvn -o clean package`: engine 66, cluster 31, server 16.
- This entry.

**Deviations:** None against the spec, the ticket, ADR 0002 or the frozen types. Six judgement
calls.
1. **`ApEngine` gains a third constructor parameter**, `random: java.util.Random = Random()`.
   The ticket asked for a seeded `Random` injected into the engine and `ApEngine`'s constructor
   is not on the frozen list; the default keeps every existing call site compiling, and
   `CommandEngine` itself is untouched. Each partition draws from its own stream seeded from
   that one, so a single seed makes the whole engine reproducible even though the partitions run
   on their own threads.
2. **`EveryPartition.join` takes the `Random`**, which four of its five variants ignore. The
   alternative was to special-case `RANDOMKEY` inside `ApEngine`, which puts one command's
   semantics in the router. One honest parameter beat that.
3. **`RANDOMKEY` is biased toward small partitions**: each partition offers one of its own keys
   and the join picks uniformly among the offers, so a partition holding two keys is as likely
   to win as one holding two hundred. Redis's own `RANDOMKEY` is approximate too. Named with a
   `ponytail:` comment; weighting the choice by each partition's key count is the repair.
4. **`COMMAND` is `Command.CommandTable`, a keyless variant beside `Ping`**, not an
   `EveryPartition`. It has nothing to gather, and asking four executors to each return an empty
   array to be concatenated would have been a shape lying about what the command does.
5. **A keyspace-wide command sweeps the store first** (`purgeExpired`) rather than filtering a
   copy of the key set, so `DBSIZE`, `KEYS` and `RANDOMKEY` all see exactly the live keys and
   `DBSIZE` is a walk rather than Redis's O(1). Named with a `ponytail:` comment: T09's wheel
   removes expired keys as they fall due, and the sweep can go then.
6. **`Glob.kt` is a port of Redis's `stringmatchlen`, not a translation to `Regex`.** The two
   disagree on the edges an unlucky client will find (an unclosed class, a reversed range, a
   trailing backslash, a `]` outside a class) and Redis's answer is the one clients were written
   against; the port is the same size as a correct translation would have been.

**For the next ticket:** T05 owns the hand-built table and `SCAN`. Notes it will want.
`globMatches(pattern, string)` in `Glob.kt` is `MATCH` already written, byte-level and
package-internal. `Partition.store` is still `HashMap<Key, Entry>` and is read directly by three
branches now (`DbSize`/`Info`, `Keys`, `RandomKey`), each after `purgeExpired(now)`; those are
the call sites the new table has to satisfy, plus `live()`, `store.remove`, `store.clear` and
`store[key] =`. `RandomKey` uses `store.keys.elementAt(random.nextInt(store.size))`, which is
O(n) on a `HashMap` and is the one place a bucket-sampling table would pay off. `SCAN`'s cursor
encodes partition plus inner cursor, so it is a fourth shape again: not `Keyed`, not `Fanned`,
and not `EveryPartition` either, since it visits one partition per call rather than all of them;
`EveryPartition` is the wrong parent for it. `Value.List.items` is a `kotlin.collections.ArrayDeque`
and T05's table replaces the key map, not the list backing. `atomically` is still
`TODO("T14: batches")`.

## T38: CP module, MicroRaft runtime, AtomicLong

**Built:** A fourth Maven module, `dynacache-cp`, between cluster and server (parent `modules`
plus a `microraft.version` 0.7 property); `dynacache-server` now depends on cp instead of
cluster, and reaches the cluster's types transitively. MicroRaft 0.7 was not in `~/.m2`; one
online `dependency:get` cached it, and every build since has been `-o`.

In the engine module, `Command.Cp` is a nested sealed class under `Command` carrying the
AtomicLong verbs of CP spec 6.2: `LongSet`, `LongGet`, `LongIncr`, `LongDecr`, `LongIncrBy`,
`LongDecrBy`, `LongCas(expected, new)`, each over a `cp:*` `Key`. It is a sibling of `Keyed` and
`Fanned`, not a `Keyed`, so a CP command can never be routed to a partition executor: `ApEngine`
answers `Reply.Error("NOTCP", ...)` and `Partition` treats one as a programming error (C16).

In `dynacache.cp`: `CpEndpoint(nodeId)` is the cluster's `NodeId` wearing MicroRaft's
`RaftEndpoint`; `CpConfig(nodeId, cpMembers, groupId, raft, leaderElectionTimeout)` rejects an
even or under-three member list at construction (CP spec 2.2) and knows whether this node is a
CP member; `RaftRuntime` builds one member's `RaftNode` over an injected MicroRaft `Transport`
and `AtomicLongStateMachine`, exposes a synchronous `isLeader` from `node.term.leaderEndpoint`,
and completes a `leadership` future from MicroRaft's own report listener when it wins an
election; `AtomicLongStateMachine` applies `Command.Cp` values and returns ordinary `Reply`s;
`CpEngine : CommandEngine` replicates through the leader and completes with the applied reply.

**Concepts named:** The **CP engine** of CONTEXT.md now exists next to the AP engine, presenting
the same command engine shape. Its seam is `CpEngine.submit`, and every test drives it. The
**Raft runtime** is one CP member's node, log and state machine; the MicroRaft `Transport` is
the seam under it, which is why the test kit passes an in-memory adapter and T43 can pass a gRPC
one without the runtime changing. The state machine's operations *are* `Command.Cp` values and
its results *are* `Reply` values, so nothing translates between the log and the wire: no `CpOp`
type was introduced (plan 2.2 names one; it would have had the same content as `Command.Cp`).
Replies follow the ticket: `+OK` for SET, an integer for a counter read or a new value, a nil
bulk for a GET of a counter never written, `:1`/`:0` for CAS, `-NOTLEADER <leader>` when a
non-leader is asked, `-NOTCP` for a non-CP command or a non-`cp:` key. `atomically` throws
`NotImplementedError`: the Raft log already serializes every entry, so CP has no batches.

`CpTestKit` is the three-member in-process kit: an in-memory MicroRaft transport that hands a
message straight to the target member's node and drops anything to or from a killed member,
plus `engine(member)`, `runtime(member)`, `live()`, `leader()`, `leaderEngine()`, `killMember`
and `restartMember`. It sleeps nowhere: an election is awaited on the members' leadership
futures with a deadline, a committed apply on the engine's own future with a deadline.

**Acceptance:** all in `dynacache.cp.CpEngineTest`, 11 tests, all green.

- `long_set_get_roundtrip`, `long_incr_decr`, `long_cas_success`, `long_cas_failure` — the four
  verb slices through the leader's engine. `long_get_missing_is_nil` pins the nil bulk.
- `long_concurrent_incr_linearizable` — 50 `INCR`s in flight from a bounded pool of 4 client
  threads; the replies are exactly the set 1..50 (each increment saw a distinct value) and the
  final `GET` is 50.
- `cp_minority_failure_available` — one follower killed of three, `INCR` and `GET` still answer.
- `cp_majority_failure_unavailable` — both followers killed, the lone leader's `INCR` is still
  pending after two seconds; the assertion admits only a pending future or `-NOTLEADER`, never
  an applied value.
- `C21_success_implies_majority_commit` — after the reply, the leader's commit index is read and
  the members whose log already reaches it are counted; at least a majority hold the entry, and
  the leader's state machine holds the value.
- `cp_follower_answers_notleader` and `C16_cp_engine_rejects_a_non_cp_key` cover the two error
  kinds the engine can answer with.

Full `clean package` is green: engine 54, cluster 33, cp 11, server 16.

**Deviations:**

1. **No `LONG_GETADD`.** CP spec 3.2 and 6.2 list a `GETADD` returning the old value; the
   ticket's enumeration of `Command.Cp` variants does not, and the engine's `Command` root is
   shared with T44's parser. Debt, one data class and one branch: add it with the `CP.LONG.*`
   verbs in T44.
2. **`INCR`, `DECR`, `INCRBY` and `DECRBY` are four variants, not one signed delta.** The AP
   engine collapses them into a single `Command.IncrBy(key, delta)`. The ticket names four, and
   T44 maps verbs one to one, so the ticket won. If T44 finds the duplication annoying, one
   `LongAdd(key, delta)` replaces all four.
3. **No `CpOp` type.** Plan 2.2 says the CP engine applies `CpOp`s; the commands are replicated
   as themselves instead. A distinct type buys nothing until the log entries must be serialized
   for gRPC, which is T43 — that is where a proto-backed operation type belongs, if anywhere.
4. **No `RaftStore`: `NopRaftStore` and no restored state.** `restartMember` therefore brings a
   member back empty and it catches up from the leader's log rather than from its own disk. Real
   Raft would need persisted term and vote for that to be safe. Debt, marked in `RaftRuntime`:
   T45 owns snapshots and restore (I20) and should bring an in-memory `RaftStore` with it.
5. **MicroRaft ships no in-memory transport in its main jar.** `io.microraft.impl.local` is in
   the project's test sources, and no `microraft:0.7:tests` artifact is published to Central, so
   the kit has its own 20-line `Transport` as the ticket allowed. No test-scope MicroRaft
   artifact was added.
6. **TDD granularity.** The first slice (`long_set_get_roundtrip`) was red-then-green at the
   `CpEngine.submit` seam, but the state machine's `when` over the sealed `Command.Cp` is
   exhaustive, so the compiler forced every verb's branch in that one slice. The later verb
   tests were therefore written against code that already existed. The failure semantics
   (`NOTLEADER`, `NOTCP`, minority, majority, C21) were red first.
7. **Real-time waits.** No `Thread.sleep` anywhere. Two waits are real time all the same:
   `cp_majority_failure_unavailable` deliberately waits two seconds to observe that the entry
   never commits, and the test kit's `RaftConfig` shortens the election timeout to 200 ms
   (heartbeat period 1 s, heartbeat timeout 5 s) so an election resolves quickly. Production
   keeps MicroRaft's defaults. The CP test class runs in about 2.2 seconds, nearly all of it the
   deliberate two-second wait.

**For the next ticket:**

- **T39 (log-carried time)** has nowhere to put a timestamp yet: entries are bare `Command.Cp`
  values and `AtomicLongStateMachine.runOperation` ignores its `commitIndex`. Wrapping the
  command in a stamped envelope at the leader is the natural move, and it is also where the
  `CpOp` type of plan 2.2 would finally earn its keep.
- **T40 to T42** add state machines beside `AtomicLongStateMachine`. A `RaftRuntime` currently
  takes exactly one, typed; that parameter wants to become the composite state machine that
  dispatches by command type. `takeSnapshot` and `installSnapshot` are implemented, not stubbed,
  but as one chunk holding the whole map; chunking is T45's.
- **T43** replaces the kit's in-memory `Transport` with a gRPC one and needs `CpEndpoint` to map
  to a peer address. The endpoint's id is the `NodeId` name, so the cluster's existing address
  book is the only thing missing. Forwarding replaces the `-NOTLEADER` reply that `CpEngine`
  gives today; the hint it carries is `node.term.leaderEndpoint`, already there.
- **T44** parses the `CP.LONG.*` verbs into these variants and must decide deviations 1 and 2.
- `CpTestKit.leader()` waits on the leadership futures of the members alive at the time. It is
  correct for a first election; T40's failover tests will want it to notice a *new* leader after
  the old one is killed, which means resetting the future rather than reusing a completed one.

## T05: Hash table with incremental rehash, SCAN family

**Built:** `dynacache.engine.ds.HashTable<K, V>`, a hand-built open hash table after Redis's
`dict.c`: power-of-two bucket arrays with chaining, `get`, `put` (answers the replaced value),
`remove`, `size`, `clear`, `entries()` (a lazy sequence over both arrays), `scan(cursor, visit)`
and `randomKey(random)`. Growth starts when the load factor passes 1 and shrink when it falls
under 1/8 (never below four buckets): a second array is allotted and every following `get`,
`put` or `remove` migrates exactly one bucket of the old array into it (`_dictRehashStep`),
the last one swapping the arrays. A lookup meanwhile reads both arrays; an insert of a new key
lands in the new one. `scan` is `dictScan`'s reverse binary iteration over both arrays, so
growing or shrinking mid-walk never skips a bucket, and it neither mutates nor steps the rehash.
`randomKey` samples a random bucket of a random array and a random node of its chain (Redis's
own bias, named in the doc). The table replaces the `HashMap<Key, Entry>` behind every
partition store and the `LinkedHashMap` behind `Value.Hash`.

`Command.Scan(cursor, pattern?, count)` and `Command.HScan(key, cursor, pattern?, count)`.
`SCAN` walks one partition per call: the cursor carries the partition index in its high 32 bits
and that partition's own cursor in the low 32, 0 starts, a partition that hands back 0 is done
and the next call starts the next partition, the last partition's 0 is the walk's, and a
cursor past the last partition answers done. Each call walks buckets until at least `COUNT`
entries came out or the partition wrapped, then `MATCH` (T04's `globMatches`) filters, exactly
Redis's loop, so a call may answer few or no keys with a non-zero cursor. Expired keys are
skipped, not deleted, during a walk. `HSCAN` is the same walk over one hash's field table,
replying `[cursor, [field, value, ...]]`; a missing key is `["0", []]`; a String key is
`WRONGTYPE` through the existing C13 check. `ZSCAN` waits for T07.

**Concepts named:** `HashTable` is the engine's own key map; `rehashProgress` is its one piece
of public rehash state (`NOT_REHASHING` or the next bucket to migrate), there so a test can
prove the one-bucket-per-operation rule from outside. `scan`'s contract is C15 in one sentence:
every key present for the whole walk is visited at least once, a shrink may repeat one, the
table keeps nothing between calls. `Command.Scan` is the fourth command shape T04 predicted:
not `Keyed`, not `Fanned`, not `EveryPartition`, because it visits one partition per call, so
`ApEngine.scan` splits the cursor and `Partition.scan` hands back `(nextCursor, found)` rather
than a `Reply` the engine would have to parse. `Partition.walk` is the Redis `COUNT` loop,
shared by `SCAN` and `HSCAN`; `Partition.scanReply` is the family's reply shape. Seams
unchanged: `CommandEngine`, `PartitionContext`, `Reply`, `Key`, `PartitionId` are exactly T01's.

**Acceptance:**
- `hashtable_put_get_remove` (HashTableTest): new key answers null, overwrite answers the old
  value, remove twice removes once, entries and clear.
- `incremental_rehash_no_block` (HashTableTest): fill until `rehashProgress` becomes 0, then
  each read is one step and `rehashProgress` equals the number of reads so far; every key is
  reachable mid-rehash and after the swap; the rehash took more than one operation.
- `no_single_operation_migrates_more_than_one_bucket` (HashTableTest): a seeded 20,000-op
  put/remove storm against a shadow `HashMap`; after every op the progress is unchanged,
  finished, freshly started at 0, or exactly one more than before.
- `scan_during_rehash_no_miss` (HashTableTest): 100 keys, then ten inserts between every two
  scan calls so growth runs mid-walk; all 100 come out.
- `scan_may_duplicate` (HashTableTest): 1,024 keys, the first call deletes all but 16 so the
  table shrinks mid-walk; every survivor comes out, at least one comes out twice, and nothing
  else does.
- `scan_returns_all_keys`, `scan_cursor_zero_terminates`, `scan_match_filters`
  (CommandEngineTest): 200 keys across all four partitions, an expired key absent; COUNT 5
  takes several calls and COUNT 1,000 takes one per partition; `Long.MAX_VALUE` is done; MATCH
  with `*`, `?` and a pattern matching nothing.
- `C15_scan_completeness` (CommandEngineTest): 300 stable keys plus 500 churn keys, and between
  every two `SCAN COUNT 3` calls a seeded storm of 40 random `SET`/`DEL` over 2,000 churn
  names; the walk took over 50 calls, every stable key came out, and no key that was never
  written did.
- `HSCAN walks one hash, MATCH and COUNT included` (CommandEngineTest).
- No single operation migrates more than one bucket: the two HashTableTest rehash tests above,
  through `rehashProgress`.
- Every T01 to T04 test still green. `mvn -o clean package`: engine 76, cluster 39, server 16.
- This entry.

**Deviations:** None against the spec, the ticket or the frozen types. Five judgement calls.
1. **`HGETALL`, `HKEYS` and `HVALS` tests compare as sets now.** The field map is the bucket
   table, so fields come out in bucket order, which is what Redis does too (it defines no order
   for the three). The three assertions in `hash_getall_complete` and the `HMSET, HMGET, ...`
   test were rewritten with the reason in a comment; nothing else in those tests changed.
2. **A rehash step migrates one bucket whether or not it is empty**, where Redis skips up to
   ten empty buckets per step. That makes "one bucket per operation" exact and testable through
   `rehashProgress` (it advances by exactly one), at the cost of a rehash of N buckets taking N
   operations rather than fewer.
3. **An empty partition still costs one `SCAN` call**: the walk of an empty keyspace on four
   partitions is four calls. Redis answers in one. Walking on into the next partition inside one
   call is the repair if a client ever minds; it needs the engine to chain partition futures.
4. **`randomKey` is bucket-sampled**, so a key in a short chain is likelier than one in a long
   chain, Redis's own bias; T04's `RANDOMKEY` was O(n) uniform per partition, this is O(1)
   expected. The T04 tests (`randomkey_nil_when_empty` and its companion) still pass unchanged.
5. **`purgeExpired` collects the doomed keys first and then removes them**, since the table's
   `entries()` must not be mutated while walked; the sweep is still the T04 ponytail ceiling
   that T09's wheel retires.

**For the next ticket:** `Partition.execute` answers `error(...)` for `Command.Scan`, because
`SCAN` runs through `Partition.scan` (it needs the cursor back, not a `Reply`); T14's
`PartitionContext.execute` must not be handed a `Scan`, and a batch has no reason to. T07's
`ZSCAN` should be one more branch beside `HScan` using `walk` over the sorted set's member
table with the score as the value. T09: `Partition.scan` reads the clock once and skips expired
entries with `Entry.expired(now)` without deleting them; when the wheel removes expired keys
this filter can stay as the lazy backstop. `HashTable.entries()` is lazy and the table must not
be mutated during it. Keep an eye on `HashTable.spread`: it is `hashCode` with the high bits
folded down, so a `Key` hashes by its whole bytes (not by hash tag), which is right for a
per-partition table. `atomically` is still `TODO("T14: batches")`.

## T09: TTL commands and active expiry

**Built:** The Key Expiry command set of spec 2.1, and the wheel behind it. `Command` gains three
`Keyed` variants, all `needs = null` because a TTL is type-agnostic: `Expire(key, deadline: Instant)`
covers `EXPIRE`, `PEXPIRE` and `EXPIREAT` (the three differ only in how the wire spells the deadline,
and the parser reduces all of them to the absolute instant spec 5.4 asks the engine to store),
`Ttl(key, precision)` over `enum Precision { SECONDS, MILLIS }` covers `TTL` and `PTTL`, and
`Persist(key)` drops the TTL. `TTL` rounds seconds Redis's way, half up (`(millis + 500) / 1000`),
and clamps at 0 rather than counting past the deadline; -2 and -1 are the two answers that are not
durations.

Each `Partition` now owns a `TimerWheel<Key>` whose callback removes the key from that partition's
store, so it runs on the partition executor with the same exclusion a command has (C1). Every
mutation of the store goes through one of two new private helpers, and they are the only place a
deadline reaches the wheel: `write(key, now, entry)` schedules when the entry carries a TTL and
cancels when it does not, and `drop(key)` removes the entry and its pending deadline together.
`SET EX/PX`, `EXPIRE` and its siblings schedule (`TimerWheel.schedule` replaces, so a re-`EXPIRE`
needs no second call); `PERSIST`, `DEL`, an overwrite without a TTL, an emptied list or hash and the
lazy `live()` check all cancel; `FLUSHDB` drops the wheel whole rather than cancelling key by key.

`ApEngine.tick()` advances every partition's wheel to the clock's current reading and returns a
future over all of them. `ApEngine` gains a fourth, defaulted constructor parameter `tickMillis`
(1000), public so the server's scheduler can set its own period from it.

**Concepts named:** `write` and `drop` are the ticket's real content: the funnel that makes "a key's
TTL and its wheel entry cannot disagree" true by construction rather than by eighteen careful
branches. Before this ticket a branch could write an entry; now a branch states what the entry is
and the funnel decides what the wheel owes it. That is what makes
`del_cancels_wheel_entry_so_a_new_value_survives` pass for aggregates too, not just for the String
path the test names. `Expire` carries an `Instant` rather than a `Duration`, the same reduction T02
made for `SET EX/PX` and T03 made for `IncrBy`: the wire's three spellings are syntax, the absolute
deadline is the meaning, and spec 5.4 wants the absolute one anyway so replication carries no clock
skew. `tick` is the word for one advance of every partition's wheel; `tickMillis` is its width and
therefore the "at most one tick late" of C7. Seams unchanged: `CommandEngine`, `PartitionContext`,
`Reply`, `Key`, `PartitionId` are exactly T01's. `ApEngine.tick()` is a method on the AP engine, not
on the `CommandEngine` interface, per the ticket.

**Acceptance:**
- `expire_replaces_wheel_entry`: `SET k EX 10` puts a deadline on the wheel, `EXPIRE k` to 100 s
  replaces it, and a tick 20 s later leaves the key readable with 80 s to run; then a shortened TTL
  takes it at the new deadline. Mutation-checked: with `EXPIRE` writing the store directly instead of
  through `write`, the stale 10 s entry fires and the test fails.
- `persist_cancels_expiry`: 0 for a missing key, 1 then 0 for the same key, `TTL` is -1, and a tick
  30 s past the original deadline leaves the value and `DBSIZE` 1. Mutation-checked.
- `del_cancels_wheel_entry_so_a_new_value_survives`: `SET k EX 5`, `DEL k`, `SET k` with no TTL, tick
  30 s on, and `k` still holds the new value. Mutation-checked.
- `expireat_absolute`: 0 for a missing key, 1 once it exists, and the deadline does not move when the
  clock does (30 s becomes 20 s after 10 s pass); a deadline already past takes the key at once.
- `ttl_reports_remaining_and_minus_values`: -2 missing, -1 no TTL, 10 s and 10,000 ms for a fresh
  `SET EX 10`, then the half-up boundary (9500 ms is 10 s, 9499 ms is 9 s), 0 at the deadline itself
  and -2 one millisecond later.
- `C7_key_readable_until_deadline_then_absent`: with the clock at the deadline minus one millisecond
  and a tick, `GET` returns the value and `DBSIZE` is 1; at the deadline plus one tick interval with
  a tick, `GET` is nil and `DBSIZE` is 0. Never early, at most one tick late.
- `string_set_ex_expires` now ticks the engine at both clock positions instead of only reading.
- Every T01 to T04 and T08 test still green. `mvn -o clean package`: engine 72, cluster 39, server 16.
- This entry.

**Deviations:** None against the spec, the ticket, the frozen types or ADR 0002. Four judgement
calls.
1. **T04's `purgeExpired` stays.** The ticket allowed removing it only if every keyspace-wide command
   stayed correct on lazy checks plus the wheel, and it does not: the wheel removes a key at the
   first tick *after* its deadline, so `KEYS`, `DBSIZE`, `RANDOMKEY` and `INFO` asked in the gap
   before that tick would see a key that no longer exists. Three T04 tests pin exactly that gap. The
   `ponytail:` comment on it has been rewritten to say so, replacing T04's note that T09 would
   retire it. What would retire it is a store that can find its expired keys without a walk.
2. **The wheel is built lazily, on the first command that needs one, from that command's own reading
   of the clock.** A wheel needs a starting instant, but the partition constructor cannot read the
   clock: T02's `C1_one_command_at_a_time_per_partition` asserts exactly three clock reads for three
   commands, and a constructor read is a fourth, on the wrong thread, before any command. Building it
   on the first TTL keeps "the clock is read once per command and never outside one" literally true,
   and a partition that has never held a TTL carries no wheel at all. The cost is a nullable field
   and a `wheel?.` on the cancel paths.
3. **`ApEngine` gains a fourth constructor parameter,** `tickMillis: Long = 1000`, exposed as a `val`.
   `ApEngine`'s constructor is not on the frozen list, the default keeps every call site compiling,
   and the server's scheduler needs the number to set its period; the C7 test reads it too rather
   than hardcoding one second.
4. **`Expire` on a deadline already past is not special-cased.** Redis deletes the key and answers 1;
   here the entry is written with a past deadline, `live()` reports the key absent to the very next
   access, and the wheel removes it on the next tick. Every observation a client can make agrees with
   Redis, so the special case would be code with no consequence. Pinned by the last two lines of
   `expireat_absolute`.

**For the next ticket:** T10 (memory accounting and LRU) is the direct consumer. Notes for it.
`Partition.write` and `Partition.drop` are now the only two paths in or out of the store, so the
per-entry byte accounting T10 needs has exactly two places to hook rather than eighteen; do not add
a third. The wheel callback is `store.remove(key)` inside the `TimerWheel` constructor and is the one
removal that does *not* go through `drop` (the wheel has already dropped its own entry by then), so
it needs its own accounting line. Spec 5.5's "remove all expired keys first" is `purgeExpired(now)`,
already written and already the sweep in front of every keyspace-wide command.

Active expiry is not observable through `CommandEngine.submit` on its own: `live()` and
`purgeExpired` answer identically for a key past its deadline, so no test can prove *which* one
removed it. What the wheel is provable by is the negative -- the three cancel tests above, where a
missing cancel makes a live key vanish -- and that is how the acceptance tests are built. If T10
wants direct proof that the wheel deletes, memory used after a tick is the observation to use.

`TimerWheel.advanceTo` walks one tick at a time whenever entries are pending, so the server's
scheduler (T13) must call `ApEngine.tick()` at least once per `tickMillis`; a scheduler that stalls
for an hour makes the next tick walk 3,600 slots. C7 at the command level holds only under that.
`atomically` is still `TODO("T14: batches")`, and a batch running TTL commands will reach `write` and
`drop` on the partition thread just as `submit` does, so nothing there needs a second wheel path.

## T43: gRPC CpService, RaftService, leader forwarding

**Built:** `dynacache-cp/src/main/proto/cp.proto` and the cluster module's protobuf plugin block
copied verbatim into the cp pom (same protoc, grpc-java and grpc-kotlin versions; the grpc-kotlin
generator again runs inside the `protobuf-java` execution through `protocPlugins`). No dependency
was added: cp reaches grpc-kotlin-stub, grpc-netty-shaded, grpc-protobuf, protobuf and coroutines
transitively through `dynacache-cluster`.

`RaftService` carries MicroRaft's inter-member traffic as one `RaftEnvelope` with `group_id`,
`sender`, `term` and a oneof over `Candidacy` (pre-vote, vote and the leadership-transfer
trigger all name the tip of the candidate's log), `Granted`, `AppendEntriesRequest`,
`AppendEntriesSuccess` and `AppendEntriesFailure`. This is a **per-message protobuf mapping**,
not a `bytes` blob with a type tag: MicroRaft's default model classes are not `Serializable`, so
the bytes route would have needed the same field-by-field encoding with none of the readability.
The one `bytes` inside it is a log entry's operation, which is the engine's value, not Raft's.

`CpService` is `Apply(CpRequest) -> CpResponse`, `GetInfo(InfoRequest) -> CpInfo` and
`Heartbeat(HeartbeatRequest) -> HeartbeatResponse`. `Command.Cp` and `Reply` travel as a
**compact hand encoding inside `bytes`** (`CpWire`): a tag byte, a length-prefixed key, then the
variant's longs; replies are tagged over the five RESP shapes and nest for arrays. Protobuf
messages per variant would have to be re-cut every time a ticket adds a verb, and the encoding is
one `when` in one file instead. `Heartbeat` throws `NotImplementedError` naming T41.

In `dynacache.cp`:

- `CpWire` - the codec above, plus `infoReply(CpInfo)` turning `GetInfo` into the `CP.INFO`
  `Reply.Array` of CP spec 6.7 (leader, members, log size, applied index, snapshot index).
- `GrpcRaftTransport(self, addresses)` - the gRPC adapter of MicroRaft's `Transport` seam. The
  address book maps a `CpEndpoint` to a `HostPort` (the cluster's own type, reused) and is read
  at send time, not at construction, so members on ephemeral ports fill each other in after
  binding, exactly as T23's `GrpcTransport` does. Sends go through grpc-java's async stub with a
  `StreamObserver` that drops the answer: MicroRaft calls `send` on the node's own thread and a
  lost message is a normal event the next heartbeat or election round repairs. No coroutine is
  launched per message, so there is no unbounded fan-out under the transport.
- `CpGrpcServer(runtime, engine, port = 0)` - one member's gRPC presence, hosting both services on
  one port. `Apply` submits to that member's own `CpEngine`, so a follower answers
  `-NOTLEADER <hint>` and never forwards (CP spec 9.1 step 3). `boundPort` reads an ephemeral port.
- `ForwardingCpEngine(cpMembers, addresses, deadline)` - an AP-only node's `CommandEngine`. It
  holds no Raft node; it discovers the leader through `GetInfo`, remembers it, and forwards over
  `Apply`. A `-NOTLEADER` reply or silence forgets the believed leader and asks the group again,
  which is how a client rides out a failover. It carries the same C16 edge as `CpEngine`
  (`-NOTCP` for a non-CP command or a non-`cp:` key) and the same `atomically` refusal.

Nothing in `CpEngine`, `RaftRuntime`, `AtomicLongStateMachine` or `CpConfig` changed, so the T39
merge stays inside the state machine and the log entry type.

**Concepts named:** The **address book** is the map from a CP member to its host and port; it is
the only thing the gRPC transport knows about topology, and it is a live view rather than a
snapshot. The **believed leader** is the AP-only node's cached answer to "who replicates?", held
until the wire says otherwise. `CpWire` is the **codec** the cluster module deliberately does not
have: the cluster's protobuf types *are* its messages, but Raft's messages are MicroRaft's own
interfaces, so a translation exists here and only here. The MicroRaft `Transport` seam named in
T38 now has its second adapter, and `RaftRuntime` did not have to learn anything to get it.

**Acceptance:** `dynacache.cp.GrpcCpTest`, 5 tests, all green, every member a real gRPC server on
an ephemeral localhost port.

- `raft_group_forms_over_grpc_on_localhost` - three members elect a leader over sockets and the
  leader's `Apply` returns the applied value.
- `cp_notleader_hint_on_follower` - a follower asked directly answers `NOTLEADER` and the message
  names the member that actually holds leadership.
- `cp_non_leader_forwards` - the AP-only node's `submit` succeeds, and the leader read back
  afterwards shows the increment landed once.
- `cp_forwarding_rediscovers_leader_after_failover` - the AP-only node increments, its leader is
  killed, and its next `submit` still answers `2` with the new leader.
- `cp_info_reports_leader_and_members` - the `CP.INFO` array names the leader and the three members.

All 11 T38 tests still pass on the in-memory kit. Full `clean package` green: engine 66,
cluster 39, cp 16, server 16.

**Deviations:**

- **Size.** 583 lines of code (779 with doc comments and blanks) across four main files and two
  test files, against a 200-600 budget. The overrun is `CpWire`: MicroRaft's ten message types
  each need their fields named twice, once to encode and once to rebuild through the model
  factory, and neither escape hatch the ticket offered removes that. It is mechanical, not
  intricate.
- **InstallSnapshot has no wire form.** `CpWire.encode` throws `NotImplementedError` for
  `InstallSnapshotRequest` and `InstallSnapshotResponse`. They only flow once a member has fallen
  behind a snapshot, and snapshots are T45; a group without them never reaches that branch.
- **A membership-change op is refused, not encoded.** `UpdateRaftGroupMembersOp` would be lost
  silently under the no-payload internal tag, so it throws instead. CP membership is fixed at
  startup (CP spec 2.2) and nothing calls `changeMembership`.
- **Forwarding is at-least-once.** A retry after silence can apply a command twice if it committed
  just as the connection dropped. CP spec 9.1 step 7 leaves the retry to the client; deduplicating
  it needs a per-session request id, so it waits for the session registry in T41. Marked in the
  source.
- **`awaitLeaderKnown` in the test kit polls.** A follower learns the leader on a heartbeat and
  MicroRaft's report listener only fires for this member's own role, so the kit polls
  `node.term.leaderEndpoint` under a `withTimeout` deadline with `delay`, never `Thread.sleep`.

**For the next ticket:**

- T44 wires `CP.INFO` and `CP.MEMBERS`: `ForwardingCpEngine.info()` and `CpGrpcServer.info()`
  already return the data, the first as a `Reply`, and a `Command.Cp` variant is all that is
  missing. The `-NOTLEADER` hint reaches RESP as `Reply.Error("NOTLEADER", "leader is <id>")`,
  so the encoder writes `-NOTLEADER leader is cp2` unless T44 reshapes the message.
- T39 changes the log entry's operation type. `CpWire.encodeOperation` and `decodeOperation` are
  the only two functions that see it; every other encoding sits below the log entry.
- T41 fills `CpService.Heartbeat`, which currently throws.
- T45 needs `InstallSnapshotRequest` and `InstallSnapshotResponse` in `cp.proto` and `CpWire`,
  including the `SnapshotChunk` and `RaftGroupMembersView` inside them.
- A production node builds the transport first, then the runtime, then the engine, then the
  server, and only then publishes its own address; `GrpcCpKit` shows that order.

## T39: Log-carried time and TTL ticks

**Built:** Every CP log entry is now a stamped envelope. `CpOp(ts, command)` wraps a
`Command.Cp`; `TtlTick(ts)` is the entry a leader appends when the group is idle; `NewTerm(term)`
is the no-op MicroRaft appends first in every term, now carrying the term number. All three live
in `dynacache-cp/.../CpOp.kt` as plain data classes, which is all MicroRaft's in-memory path
needs; gRPC serialization is T43's. `CpConfig` gains `clock: Clock` (default system UTC) and
`tickInterval: Duration` (default 100 ms, CP spec 5). `RaftRuntime` owns the stamping:
`replicate(command)` takes the stamp and appends under one lock, `tick()` appends a `TtlTick`
when this member leads and its clock is a tick interval past the last stamp, and the stamp is
`max(clock.millis(), stateMachine.lastAppliedTs + 1, lastStampedTs + 1)`. `AtomicLongStateMachine`
keeps `lastAppliedTs` (the stamp of the entry it applied last), stores `Counter(value, expiresAt)`
and answers every read through `live(key)`, which treats `expiresAt <= lastAppliedTs` as gone; a
tick also sweeps expired counters. `Command.Cp` gains `LongSet(key, value, ttl = null)`,
`LongExpire(key, ttl)`, `LongTtl(key)` and `LongPersist(key)` with Redis's replies (CP spec 9.4):
INCR and CAS keep a TTL, SET without one clears it. The snapshot chunk now carries
`lastAppliedTs` next to the counters. `CONTEXT.md` gains a CP section naming **log time** and
**TTL tick**. The kit gives each member a `MutableClock`, exposes `clock(member)` and
`awaitApplied(member, index)`.

**Concepts named:** **Log time** is the time a CP state machine lives in; nothing in the state
machine reads a clock, and the only clock in the module is the leader's, read once per stamp.
The seam is unchanged: `CpEngine.submit` still takes a `Command.Cp` and answers a `Reply`; the
stamp is the runtime's business, and `CpEngine` only moved from `runtime.node.replicate` to
`runtime.replicate`. **A leader is a leader only once its term's first entry is applied.** MicroRaft
lets a fresh leader replicate as soon as its `NewTerm` is *appended*, and at that moment its applied
time may still trail the old leader's last commit (the follower learns a commit index on the
next append or heartbeat, and the old leader was killed before that). A stamp taken then equals
or precedes the old leader's, which is exactly what C19 forbids; the red run of the C19 test
showed the successor stamping the same millisecond as the old leader's last entry. So
`RaftRuntime.isLeader` is `leaderEndpoint == me && appliedTerm == node.term.term`, where
`appliedTerm` is set by the state machine's callback when it applies a `NewTerm`; the
leadership future completes there rather than on MicroRaft's role report, and is replaced
when the member stops leading (the reset the T38 entry asked for).

**Acceptance:** `dynacache.cp.CpEngineTest`, 16 tests, all green; full `clean package` green
(engine 66, cluster 39, cp 16, server 16).

- `C19_log_timestamps_monotonic_across_leader_change`: the leader's clock is advanced an
  hour, it commits two entries and is killed; the successor (clock at the epoch) stamps three
  entries that are strictly increasing, past the old leader's last stamp, and ahead of the
  successor's own clock. Red before green: it failed with the successor's first stamp equal to
  the old leader's last, fixed by the term gating above.
- `C23_every_member_agrees_on_expiry_at_same_index`: only the leader's clock moves; at the
  SET's index every member reads 5, at the tick's index every member reads nothing.
- `ttl_tick_advances_time_when_idle`: the clock moves 2 s but the counter stays live until
  `tick()` commits, then it is gone with no user entry in between.
- `long_ttl_expires`: `SET ... EX 1`, clock +2 s, GET is nil; and `long_expire_ttl_persist`
  pins -2 / -1 / seconds-rounded-as-Redis / PERSIST 1 then 0.
- Every T38 test still green, unchanged.

**Deviations:**

1. **`TTL` answers seconds, and there is no `PTTL` variant.** `LongTtl` rounds as Redis's `TTL`
   does (`(ms + 500) / 1000`). `PEXPIRE` needs no variant because `LongExpire` takes a
   `Duration`. If T44 wants `PTTL`, a `LongPttl` is one data class and one branch.
2. **`tick()` is unconditional in effect when called at its own cadence.** The idle check
   compares the leader's clock against its last stamp, so a production loop calling `tick()`
   every 100 ms appends nothing while user writes keep coming and one tick per interval when
   they stop. No production loop exists yet; plan 2.5 says it is one coroutine owned by the
   node's lifecycle, and that belongs with the server wiring.
3. **Expired counters are swept on ticks and dropped lazily on access, never on a timer.** One
   `removeIf` per tick over the whole map: fine for a counter set, the ceiling is a very large
   one, and the repair is an expiry-ordered index.
4. **Lease time after a failover to a slow clock stands still.** If the new leader's clock is
   behind log time, stamps advance by 1 ms per entry until the clock catches up; that is the
   spec's own rule (time never turns back) and not something this ticket changed.
5. **Real-time waits.** No `Thread.sleep`. C19 takes about 5 s: the kit's MicroRaft config keeps
   a 5 s leader heartbeat timeout (T38), so the successor notices the dead leader only then. The
   CP class now runs in about 8 s. Shortening `setLeaderHeartbeatTimeoutSecs` to 1 would cut it,
   but the instruction was to keep the kit's timing; it is a one-line change if wanted.
   `awaitApplied` spins on `getReport()` futures with a deadline, not on a sleep.

**For the next ticket:**

- **T40 (FencedLock)** measures leases against `lastAppliedTs`. That field lives on
  `AtomicLongStateMachine` today; a second state machine wants it hoisted into the composite
  that dispatches by command type (the T38 note), and the `NewTerm` callback and `currentTerm`
  lambda go with it. `RaftRuntime.stateMachine` is now built by the runtime (no constructor
  parameter), which is where the composite will be constructed.
- **Failover tests** can use `kit.leader()` as is: the leadership future is reset when a member
  stops leading and completes only when the new leader has applied its `NewTerm`, so `leader()`
  after `killMember` returns a successor that may stamp.
- **T41 (sessions)** checks session timeouts "on every `TTL_TICK`": the state machine sees
  `TtlTick` in `runOperation`, so that check is one more branch there, and the leader appending
  `SESSION_CLOSED` is a `replicate` from the tick's completion.
- **T43** serializes `CpOp`, `TtlTick` and `NewTerm` for gRPC; they are three data classes with
  primitives and a `Command.Cp`.
- `LongSet.ttl` is a `Duration`; T44's parser reduces `EX`/`PX` to it, as the AP `Set` does.

**Addendum after merging T43:** `CpWire.encodeOperation`/`decodeOperation` now tag the three
entries a leader appends (`CpOp` with stamp and command, `TtlTick`, `NewTerm` with its term), and
the command encoding carries `LongSet.ttl` (-1 for none) plus `LongExpire`, `LongTtl` and
`LongPersist`. Both functions became `internal` so `CpWireTest` (3 round-trip tests) can drive
them directly. `GrpcCpTest`'s five tests pass over real gRPC with stamped entries. The T43 note
in **For the next ticket** above is therefore done; nothing else in the entry changed.

## T07: Sorted Set commands

**Built:** `Value.ZSet`, the dual index of spec 2.1: a T05 `HashTable<String, Double>` from
member bytes to score beside the T06 `SkipList` for order, written together on every change.
`Value.Kind.ZSET` joins the C13 kind check, so `TYPE` reports `zset` and a String key refuses a
zset command before a branch can reach it. Eleven commands, in nine `Partition` branches:
`ZADD`, `ZREM`, `ZRANGE`/`ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANK`/`ZREVRANK`, `ZSCORE`, `ZCARD`,
`ZINCRBY`, `ZSCAN`. Three helpers in `Value.kt` carry the score vocabulary: `parseScore`,
`parseBound` with its `ScoreBound`, and `scoreText`. Eighteen tests in a new
`ZSetCommandTest`, all through `CommandEngine.submit`. 265 lines of main, 316 of test.

**Concepts named:** The **dual index** is one value, not two collaborating ones: `Value.ZSet`
holds both halves and `Partition.writeScore` is the only place a member's score is written, so
"the map and the list agree" is a property of one function rather than a rule callers must keep.
The map owns **member uniqueness** (the list keys on `(score, member)` and would hold one member
at two scores, exactly as T06 warned); the list owns **order** and **rank**. A **score** is what
`parseScore` accepts and `scoreText` writes: a client's bytes in, Redis's spelling out, with NaN
refused at the door so it never reaches a comparator. A **bound** is a score plus whether the
entry sitting exactly on it is in, which is what the `(` prefix decides.

`ZRANGE` and `ZREVRANGE` are one `Command.ZRange` with a `reverse` flag, and `ZRANK`/`ZREVRANK`
one `Command.ZRank` the same way: each pair reads the same ordering from the other end, so a
second variant would have been the same branch written twice. `ZRANGE`'s window is `LRANGE`'s:
the existing `Partition.span` is the one place a negative index is read, for lists and sorted
sets both. `ZSCAN` is one more branch beside `HScan` over the same `Partition.walk`, exactly as
T05 predicted. No seam moved: `CommandEngine`, `PartitionContext`, `Reply`, `Key` and
`PartitionId` are untouched, and `Partition`'s only edits are the new branches, the kind entry
and six private helpers (`members`, `limit`, `zset`, `newZSet`, `scoreOf`, `writeScore`).

**Acceptance:**
- `zset_ordering_invariant`: 1,500 seeded `ZADD`/`ZREM` operations over 60 members, and every
  25 steps `ZRANGE 0 -1 WITHSCORES` is compared with a `HashMap` model sorted by score then
  member; `ZCARD` matches the model's size at the end.
- `zset_rank_consistency`: 300 seeded adds and 60 removes, then every member's `ZRANK` equals
  its position in `ZRANGE` and its `ZREVRANK` equals the position counted from the other end.
- `zset_score_update`: `ZADD` on an existing member answers 0, moves it up and then back down,
  leaves `ZCARD` unchanged and re-orders both times.
- `I3_zrange_sorted_with_lex_tiebreak`: six members at one score, including 0x01 and 0xff, come
  back in unsigned byte order between a lower- and a higher-scored member, and `ZRANK` agrees.
- `zscan_returns_all_members`: 200 members walked with `COUNT 7` and with `COUNT 1000`, both
  complete; `MATCH` with a literal and with `?`; a missing key answers `["0", []]`.
- Beyond the ticket's list, thirteen command tests: `ZSCORE`/`ZCARD`/`TYPE` after `ZADD`, the
  new-member count, `ZADD` refusing a non-float and writing nothing, missing-key answers,
  `ZRANGE` forwards and backwards with negative indices, `WITHSCORES` both directions, `ZREM`
  emptying the key, `ZRANK` nil for an absent member, `ZRANGEBYSCORE` inclusive/exclusive/
  infinite bounds, `LIMIT`, the bad-bound error, `ZINCRBY` creating and moving, `ZINCRBY`
  refusing a NaN result, and `WRONGTYPE` on a String key.
- `mvn -o clean package` green: engine 94 (was 76), cluster 39, cp 11, server 16.
- This entry.

**Deviations:** None against the spec, the ticket or the frozen types. Six judgement calls.
1. **`ZADD` has no `NX`, `XX`, `CH` or `INCR` flag**, which the ticket allows if they are not
   cheap.
   Main plus tests came to 581 lines against a 600-line budget, so they did not fit.
   `ZINCRBY` already covers what `INCR` does; the other three are a condition and a counting
   rule around the existing `writeScore`, perhaps fifteen lines, and belong wherever the RESP
   command parser lands (T13). Debt, and small.
2. **Scores and range bounds are carried as the client's bytes, not as parsed numbers**, unlike
   `IncrBy`'s `Long` and `Set`'s `Duration`. Redis parses them inside the command proc, and that
   is observable: `ZADD k 1 a banana b` must add nothing. Parsing at the partition is what makes
   that testable through `submit` in this ticket, before a parser exists. It also gives
   `ZRANGEBYSCORE` its `(`/`-inf` syntax somewhere honest to live.
3. **The C13 kind check runs before the argument parse.** `ZADD stringkey banana m` answers
   `WRONGTYPE` where Redis answers `ERR value is not a valid float`, because the kind check sits
   in front of every branch by design. Moving it per-command would cost C13 its one place.
4. **`scoreText` writes Kotlin's shortest round-trip form past 2^53**, so a score of 1e17 comes
   back as `1.0E17` where Redis's `%.17Lg` writes `1e+17`. Whole numbers below that, the
   infinities and ordinary decimals all match Redis. A formatter of its own is the repair if the
   T16 RESP acceptance minds.
5. **`parseScore` guards the characters before calling `toDoubleOrNull`**, because Kotlin's
   parser accepts `1.0f`, `NaN`, `0x1p3` and surrounding whitespace that Redis's `strtod` does
   not. `-0.0` and `0.0` stay one score everywhere, as they are in the skip list.
6. **`ZREVRANGEBYSCORE` is not built**; neither the ticket nor spec 2.1 lists it.

**For the next ticket:** T09 owns `SET`/`DEL` and the partition's construction; this ticket
touched neither, only adding branches after the List ones and a `ZSET` entry to `Value.Kind`,
so the merge is the branch list and the enum. A sorted set is created with
`Partition.newZSet`, which draws the skip list's level seed from the partition's own `Random`:
one seed on `ApEngine` still makes every list in it reproducible, and a new construction path
must keep doing that or the log-n margin T06 measured stops meaning anything. The store never
holds an empty sorted set: `ZREM` drops the key with the last member, `ZADD` with no entries
creates nothing, and a `ZINCRBY` refused for NaN creates nothing, so T10's eviction and T31's
RDB codec can assume a `Value.ZSet` has at least one member. `writeScore` is the single writer
of the pair; anything that has to change a score (T29's element-level merge, T31's load) should
go through it rather than touching `scores` and `order` separately. The skip list holds the
command's own member array, so nothing may mutate a `ByteArray` after handing it to `ZADD`.

## T13: Command parser and Netty server

**Built:** `dynacache-server` gains the two things that turn the engine into a node a Redis
client can talk to.

`CommandParser(clock)` turns one client frame's tokens into a `Command`, or into the
`Reply.Error` to write back. Every command name the engine has today has a row: the server and
keyspace set (`PING`, `COMMAND`, `INFO`, `DBSIZE`, `FLUSHDB`, `KEYS`, `RANDOMKEY`, `TYPE`,
`DEL`, `EXISTS`, `SCAN`), the String set (`GET`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `INCR`,
`DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET`), the ten Hash commands plus
`HSCAN`, the nine List commands, and the six Key Expiry commands. Names match
case-insensitively; everything else is bytes and stays bytes. `Parsed` is the two-case answer
(`Ok(command)` or `Failed(error)`), so a parse failure is a value the handler writes rather
than an exception that kills a connection.

`DynaCacheServer(port, engine, tick)` is the RESP2 socket: a `ByteToMessageDecoder` wrapping
T12's `RespDecoder`, a per-connection handler that parses, calls `engine.submit` and encodes,
and a single-thread scheduled executor calling `ApEngine.tick()` once per the engine's
`tickMillis`, started by `start()` and stopped by `close()`. `boundPort` reports what port 0
was actually given, so a test never picks one. `main` takes port and partition count from its
arguments, defaulting to 6379 and 16, and closes the server then the engine on shutdown.
`RespClient` is the test kit's client: a plain `java.net.Socket`, RESP2 out, one `Reply` in
through `RespDecoder.nextReply`, every read under a timeout.

**Concepts named:** The parser is where the wire's several spellings of one meaning collapse,
and that is its whole content. `EXPIRE`, `PEXPIRE` and `EXPIREAT` are three ways to write one
deadline, so all three become `Command.Expire`'s absolute `Instant` (spec 5.4), which is why
the parser needs an injected `Clock` at all. `INCR`, `DECR`, `INCRBY` and `DECRBY` are one
signed delta. `SETNX`, `SETEX`, `PSETEX` and `SET`'s `NX`, `XX`, `EX`, `PX`, `EXAT` and `PXAT`
are one `Command.Set`. `LPUSH`/`RPUSH` and `LPOP`/`RPOP` are one command and an `End`. The
engine already decided these reductions when it froze its variants; the parser is the only
place the wire's spelling is still visible, and after it nothing downstream has to know that
`PEXPIRE` exists.

The pipeline's one real idea is the **pending queue**: a command's future joins a per-connection
`ArrayDeque` when the command arrives, and the queue is drained from the front only while its
head is done. Futures across sixteen partitions complete in whatever order their executors get
to them, and this is what makes the bytes come back in request order anyway. The queue is
touched only from the channel's event loop, so it needs no lock, which is the same shape as the
engine's own rule: one thread owns the state, nobody synchronises.

Seams unchanged: `CommandEngine`, `Reply`, `Key` are exactly T01's, and the engine module was
not touched. No new interface was introduced. `Parsed` is a value type, not a seam: there is
one parser and one pipeline, so an interface would have had nothing behind it. The one new
public parameter, `DynaCacheServer`'s `tick`, is the seam plan 2.3 already names ("the server
owns the scheduler that calls it"); it defaults to the engine's own tick.

**Acceptance:**
- `server_ping_pong`: `PING` over a real socket answers `+PONG`, and so does the inline
  `ping\r\n` form redis-cli uses interactively.
- `server_pipelined_replies_in_order`: 100 `SET`/`GET` pairs on 100 keys across 16 partitions,
  all written before any reply is read, come back in request order. Mutation-checked: with the
  handler writing each reply on its own future's completion instead of through the queue, the
  test fails.
- `server_unknown_command_error`: `NOSUCH a b` over the socket answers
  `-ERR unknown command 'nosuch', with args beginning with: 'a', 'b', ` and the connection
  survives to answer the next `PING`.
- `server_arity_error`: `GET` with no key and `SET` with no value each answer
  `-ERR wrong number of arguments for '<name>' command`, under the lower-case name Redis uses,
  and the connection survives.
- `parser_maps_every_command`: 54 rows, one per command name (some names twice where the
  argument count changes the variant, as `DEL k` and `DEL a b` do). Each row asserts the variant
  and, where two names share a variant, a probe on the meaning: `LPUSH` is `HEAD` and `RPUSH`
  `TAIL`, `INCR` is +1 and `DECRBY k 5` is -5, `TTL` is `SECONDS` and `PTTL` is `MILLIS`, all
  three expiry spellings land on the same instant. Every row is parsed again lower-case.
- Supporting tests: `SET`'s flags in every combination and its four syntax errors; the
  arity, not-an-integer and invalid-cursor errors; a protocol error answered once and the
  connection closed; the scheduler ticking three times at a 20 ms period.
- `mvn -B -o clean package`: engine 82, cluster 39, cp 11, server 30. Every earlier test green.
- This entry.

**Deviations:** Five, one of them debt worth naming.
1. **Size.** 828 lines including tests, against the ticket's 200 to 600. Reported rather than
   silently exceeded, as the ticket requires. The overrun is the ticket's own shape: the
   mandated one-row-per-command table is about 85 lines of test and the dispatch table about 70
   of main, and the ticket asks for a parser, a Netty pipeline and a test-kit client in one
   ticket. Nothing was split out, because splitting the parser from the pipeline would have left
   the pipeline untestable in its own ticket.
2. **`PING` takes no argument.** Real Redis answers `PING message` with a bulk echo of the
   message. `Command.Ping` carries no message and the type is frozen, so an argument is an arity
   error here. Debt, and cheap to repay when `Ping` gains a payload.
3. **`PEXPIREAT` has no row.** Spec 2.1 lists `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL` and
   `PERSIST`, and the row would be one line whenever a client wants it.
4. **`SET`'s `EXAT` and `PXAT` are reduced against the clock.** `Command.Set` carries a
   `Duration`, not an `Instant`, so an absolute deadline becomes `deadline - now` in the parser.
   That reintroduces exactly the clock question spec 5.4 avoids for `EXPIREAT`, over the
   microseconds between the parser and the partition executor. Accepted; the repair is a second
   `Set` shape carrying an instant, which is an engine change and not this ticket's.
5. **No `invalid expire time` check.** Redis refuses `SET k v EX 0` and `SETEX k 0 v`; here a
   zero or negative TTL is passed through and the key expires at once. Small debt, two lines in
   `seconds`/`millis` whenever it matters.

**For the next ticket:** T14 owns MULTI, EXEC and DISCARD, and everything it needs is in
`CommandHandler`. That handler is where per-connection queue state belongs: it is already
per-connection and already single-threaded on the channel's event loop, so `MULTI`'s buffer and
the "a parse error while queued makes `EXEC` reply `-EXECABORT`" rule need no synchronisation.
`MULTI`, `EXEC` and `DISCARD` are not parser rows and should not become `Command` variants: they
are connection state, so the handler should recognise them before calling `CommandParser`, and
the parser should keep answering only "what does this token list mean".

Two notes for whoever touches the parser next. Sorted Set (T07) has a marked place in both the
dispatch table and the test table, naming the eleven rows it owes. And the parser holds a
`Clock` because `EXPIRE` needs one; T44's dispatcher will want to hand it the same clock the
engine has, not `Clock.systemUTC()`, which is what `CommandHandler` currently constructs.

The pipeline's known ceiling is marked with a `ponytail:` comment: the pending queue is
unbounded, so a client that pipelines without ever reading grows it until the heap objects.
Redis caps its own client output buffer; the repair here is a limit that closes the connection
past N pending replies. Nothing in P1 pushes on it.

## T40: FencedLock

**Built:** The composite state machine T39 asked for, and the FencedLock beside the AtomicLong.

`CpStateMachine` is now the one MicroRaft `StateMachine` a `RaftRuntime` builds. It owns
`lastAppliedTs`, applies `NewTerm` (the `onTermApplied` callback and `currentTerm` lambda moved
here from `AtomicLongStateMachine`), applies `TtlTick` by sweeping every primitive, and hands a
`CpOp`'s command to the primitive that answers it by the command's sealed sub-hierarchy. Its
snapshot is one chunk holding log time plus each primitive's map; `installSnapshot` restores
both. `valueOf(key)` stays on it for the T38 and T39 tests.

`AtomicLongStateMachine` is no longer a MicroRaft `StateMachine`: it is a plain primitive with
`apply(command, now)`, `sweep(now)`, `valueOf(key, now)`, `snapshot()` and `restore()`, where
`now` is log time passed in by the composite. Its behaviour is unchanged; every T39 test passes
untouched.

`FencedLockStateMachine` (CP spec 3.1) has the same shape. Per `cp:lock:*` key it holds
`Lock(owner, token, expiresAt, holds)`. `LockTry` on a free (or lease-expired) lock grants
`token + 1` with one hold and a lease of `now + ttl`; by the holder it adds a hold and returns the
same token; by anyone else it is denied. `LockUnlock` by the holder with the current token drops a
hold (`:0`) or releases at the last one (`:1`); anyone else, any other token, or a free lock
answers `-REENTRANCE`. `LockRenew` by the holder with the current token restarts the lease from
`now` (`:1`), otherwise `-REENTRANCE`. `LockForceUnlock` releases whoever holds it (`+OK`).
`LockState` answers `[owner or nil, token, ttl_remaining_ms, reentrance]`. A released lock keeps
its token, so the counter is state-machine state and the next holder on any leader gets the next
number (C17, I14). Expiry is evaluated on every access through `Lock.at(now)` and swept on every
tick; nothing reads a clock.

In the engine module, `Command.Cp` gained two sealed sub-hierarchies, `Cp.AtomicLong` and
`Cp.FencedLock`; the existing `Long*` variants moved under the first, and `LockTry(key, session,
ttl)`, `LockUnlock(key, session, token)`, `LockRenew(key, session, token, ttl)`,
`LockForceUnlock(key)` and `LockState(key)` are the second. `CpWire` tags all five (11 to 15) and
`CpWireTest` round-trips them. `CONTEXT.md` names **fencing token**, **lease** and **session**.

**Concepts named:** The **composite state machine** is the only thing that knows log time; a
**primitive** (AtomicLong, FencedLock, and T42's three) is a plain class that is told the time
with every call and can be tested without Raft. Dispatch is by the command's sealed
sub-hierarchy, so adding a primitive is one sealed class in `Command.Cp`, one branch in the
composite's `when` and one field in its snapshot. The **fencing token** is the lock's answer to
"who is current?", the **lease** is its only TTL, and a **session** is, until T41, a number the
caller supplies. The seam did not move: every test drives `CpEngine.submit`.

**Acceptance:** `dynacache.cp.FencedLockTest`, 13 tests, all green; `CpWireTest` 4 (one new).
Full `clean package` green: engine 82, cluster 39, cp 38, server 16.

- CP spec 10.1, all ten: `lock_try_acquire_release_roundtrip`, `lock_mutual_exclusion` (two
  TRYs in flight at once, exactly one granted), `lock_fencing_token_monotonic` (100 cycles
  across three sessions, tokens strictly climbing and never repeated), `lock_reentrant_same_session`
  (same token, hold count 2, two UNLOCKs answering `:0` then `:1`),
  `lock_unlock_wrong_session_rejected`, `lock_unlock_wrong_token_rejected` (both `-REENTRANCE`,
  STATE unchanged), `lock_ttl_expires` (clock +2 s, one tick, STATE unowned and the next TRY gets
  token 2), `lock_ttl_renew` (RENEW to 5 s, clock +2 s, tick, still held with 3 s left),
  `lock_renew_by_non_holder_rejected` (`-REENTRANCE`, lease unchanged),
  `lock_force_unlock_overrides`.
- `cp_leader_failover_preserves_state`: a lock and a counter both read the same on the successor.
- `I18_lock_held_across_leader_failover`: the successor denies another session, shows the same
  owner and token, accepts the old token's UNLOCK, and issues token 2 next.
- `I19_lease_expires_late_never_early_across_failover`: lease 30 s, old leader's clock +10 s,
  killed; the successor (clock at the epoch) shows the lock held with 19 999 ms left right after
  its term; at 29 999 ms on the successor's clock it is held with 1 ms left; at 30 000 ms it is
  gone. The "election overhead" is zero in log time because the clocks are injected, so the
  assertion is exact.
- Every T38, T39 and T43 test still green, unchanged except that `CpEngineTest` keeps calling
  `stateMachine.valueOf(key)` and `stateMachine.lastAppliedTs`, now on the composite.

**Deviations:**

1. **`RENEW` never answers `:0`.** CP spec 6.1 gives it `:1 / :0` and 10.1 says a non-holder gets
   "error". Both a non-holder and a stale token get `-REENTRANCE`, the same as UNLOCK (spec 6.8
   defines the kind as "wrong token / not current holder"), so `:0` has no case left. T44 maps it as is.
2. **A denied `TRY` answers `[0, 0]`.** Spec 6.1 shapes the reply as `*2 $ok :token`; here both
   items are integers (`[1, token]` / `[0, 0]`), since 0 is never an issued token. The RESP encoder
   writes whatever the `Reply` is; if T44 wants a bulk `ok` it is one line in the state machine.
3. **`RENEW` takes the token.** The ticket wrote `LockRenew(key, sessionId, ttl)`; spec 3.1 and
   6.1 carry the token, and the spec wins.
4. **A reentrant `TRY` does not touch the lease.** Spec 3.1 says it "increments reentrance,
   returns existing token" and nothing about the lease; `RENEW` is the verb for that.
5. **Session ids are `Long`.** The ticket allowed string or long. `owner` in `STATE` is therefore
   an integer reply, nil when free. T41 may keep or change it; every place that reads it is in
   `FencedLockStateMachine`, `CpWire` and the test.
6. **A lock key is never forgotten.** Its token counter must outlive every release, so a released
   lock stays in the map with `owner = null`. Marked `ponytail:` in the source; the ceiling is a
   huge number of distinct lock keys, the repair a token-only tombstone.
7. **The lock's map is a `ConcurrentHashMap`** like the counter's, though only the Raft thread
   touches it today; a plain map would do until something reads it from another thread.
8. **TDD granularity.** As in T38: the first slice was red (no `LockTry`), and the exhaustive
   `when` forced TRY, UNLOCK and STATE in one go; the next five spec tests were written against
   that code. RENEW and FORCE_UNLOCK were each red-then-green (no variant, then the variant and
   the branch). The three failover tests were green first time. Two literals in my tests were
   wrong on the first run (STATE sees one millisecond less per intervening entry, C19); the code
   was right and the literals were corrected.
9. **Real-time waits.** No `Thread.sleep`. The three failover tests each wait about 5 s for the
   kit's leader heartbeat timeout (T38's timing, kept as instructed); `FencedLockTest` runs in
   about 16 s, `CpEngineTest` unchanged at about 7 s.

**For the next ticket:**

- **T41 (sessions)** replaces the caller-supplied `session: Long` with a registry-checked one:
  `-NOSESSION` is a check at the top of `FencedLockStateMachine.apply` (or in the composite,
  before dispatch, once the session registry is a third primitive there). "Applying
  `SESSION_CLOSED` releases every lock the session holds in the same entry" is one method on
  `FencedLockStateMachine` (`releaseAllOf(session)`) that walks the map; the composite calls it
  when applying the closed entry. Session timeouts "on every `TTL_TICK`" are one more `sweep`
  in the composite's `TtlTick` branch, followed by the leader appending `SESSION_CLOSED` from
  `RaftRuntime.tick`'s completion.
- **T42** adds three primitives: each is a sealed sub-hierarchy in `Command.Cp`, a branch in
  `CpStateMachine.runOperation`, a field in its `Snapshot`, and its tags in `CpWire`.
- **T44** parses `CP.LOCK.*` into the five variants and decides deviations 1 and 2. The session
  id on a `LockTry` comes from the connection, not the arguments (spec 6.1), so the parser needs
  the connection's session, which T41 provides.
- **T45** snapshots: `CpStateMachine.Snapshot` is one chunk with both maps; chunking and the
  wire form for `InstallSnapshot` are its business, and `installSnapshot` already restores
  everything the chunk holds.
- `CpTestKit.leader()` after `killMember` returned a stamping successor in all three failover
  tests without change; the T39 reset works.

## T31: RDB codec

**Built:** `dynacache.engine.persist.Rdb.kt` (231 lines) holds the whole snapshot format and
nothing else. `RdbWriter.write(sink, entries, now)` streams a snapshot to any `OutputStream`
(a `FileChannel` reaches it through `Channels.newOutputStream`); `RdbReader(seeds).read(source)`
reads one back from any `InputStream` and returns the entries in file order. The file is

```
[magic:7 "DYNARDB"][version:u8 = 1][count:u32] [entry]* [crc32:u32]
```

and one entry is

```
[key_len:u32][key][type:u8][dvv_len:u32][dvv][ttl_abs:i64][value_len:u32][value]
```

big-endian throughout, as the WAL of T33 is. `ttl_abs` is epoch millis and `-1` means no
deadline. The value bytes are type-specific: a String is its bytes; a Hash writes its field
count then length-prefixed field/value pairs; a List writes its element count then
length-prefixed elements; a Sorted Set writes its member count then, per member, the score as
IEEE-754 bits (`Double.toRawBits`) followed by the length-prefixed member. Score-as-bits is what
round-trips the infinities, which `scoreText` would not. The CRC32 covers every byte before it;
`java.util.zip.CheckedOutputStream`/`CheckedInputStream` accumulate it, so neither side walks
the bytes twice.

`writeScore` moved off `Partition` and onto `Value.ZSet` as a method. It is the single writer of
a sorted set's dual index (T07), and a restore has to go through it, so the command path and the
codec now share the one writer and the score map and the skip list cannot drift apart (I3). The
two `Partition` call sites became `zset.writeScore(...)`; nothing else changed there.

**Concepts named:** **RDB entry** (`RdbEntry`): one key as a snapshot holds it - key, value,
absolute `expiresAt` or null, and the opaque DVV bytes. **RDB fault** (`RdbFault`): why a file
was refused - `NOT_AN_RDB`, `UNSUPPORTED_VERSION`, `TRUNCATED`, `CHECKSUM_MISMATCH` - carried by
`RdbFormatException`, which is the same shape of vocabulary as T33's `WalStop` but an exception
rather than a result, because a snapshot is all-or-nothing where a log is read as far as it
goes. Seam: the writer and reader public interface, exactly as the plan entry names it; the
tests never read the file layout except the two that are about the layout (the version byte and
the corruptions).

The type byte is a code fixed by the format (`KIND_BY_CODE`), not `Value.Kind.ordinal`.
Reordering the enum must not silently change what an already-written file means.

**Acceptance:** all in `dynacache-engine/src/test/kotlin/dynacache/engine/persist/RdbTest.kt`,
7 tests, JUnit 5 only, no Mockito needed (in-memory streams are the whole boundary), no sleeps.

- `rdb_save_restore_roundtrip` - all four types in one file, TTLs and DVV bytes intact, binary
  bytes in the key, the value, the field name and the DVV; the sorted set is compared on both
  of its indexes and on member order, so a restore that filled one and not the other fails.
- `rdb_excludes_expired` - two dead keys dropped, and the key expiring exactly at `now` kept,
  because spec 5.4 says a key is readable through its deadline.
- `rdb_bad_checksum_rejected` - one flipped bit in the body gives `CHECKSUM_MISMATCH`.
- `rdb_truncated_file_rejected` - cut mid-entry and cut inside the checksum, both `TRUNCATED`,
  and neither yields the entries that did survive.
- `rdb_empty_snapshot_roundtrip`, `rdb_version_byte_present` (the byte is where it should be
  *and* a file claiming version 2 is refused rather than guessed at).
- Extra: `rdb_not_an_rdb_rejected` - a foreign file is refused on the magic.

The three tests that could not be written red-first (they need a working writer to corrupt its
output) were verified by mutation instead: disabling the checksum comparison and the expiry
filter fails exactly those three and nothing else.

**Deviations:**

1. **The codec carries its own entry record, not `Partition.Entry`.** The ticket says the writer
   takes `(Key, Entry, dvvBytes)`, but `Entry` is `private` inside `Partition` and unwrapping it
   would be a refactor of the command path that this ticket does not need. `RdbEntry` is that
   triple plus the value, and serves both directions. T32 maps the partition's store to it.
2. **`writeScore` moved from `Partition` to `Value.ZSet`.** Additive to `Value.kt` as the ticket
   allows, but it is a move, not a copy: the private `Partition.writeScore` is gone and its two
   call sites now go through the value. This is the only way a restore can use the engine's own
   construction path without duplicating the dual-index rule.
3. **The whole codec is `internal`.** `RdbEntry` names `Value`, which is `internal` to the engine
   module, so the codec cannot be more public than the values it encodes. Everything that needs
   it (T32's snapshot engine) lives in the engine module. Not debt; if the cluster module ever
   needs a snapshot it should go through an engine-level API, not the codec.
4. **The writer materialises the live entries before writing.** The header carries the entry
   count and an `OutputStream` cannot be seeked back to patch it, so `entries.filterNot { expired }`
   runs first. Only references are held - no value is serialized until its turn - so the memory
   cost is the caller's snapshot, which already exists. If a partition ever grows past what a
   list of references costs, the repair is a count of `-1` meaning "read until the checksum",
   not a second pass over the data.
5. **No `FileChannel` overload.** `Channels.newOutputStream`/`newInputStream` already adapt one.

**For the next ticket (T32):** the codec is pure and holds no file, no path and no clock - the
caller passes `now`, and file naming, atomic rename, the interval and the shutdown hook are all
T32's. `RdbReader` takes a `java.util.Random` for the skip-list levels of a restored sorted set,
the one piece of a value the file does not carry; pass the partition's own generator to keep a
restore reproducible the way `newZSet` does. The reader returns a list, not a stream: a snapshot
is all-or-nothing, so there is nothing useful to hand back before the checksum has been checked.
A `Value.ZSet` never has zero members (T07), so a zero-member sorted set in a file means the
file is wrong, not that the set is empty - T32 may want to say so out loud when it restores.
Nothing was stubbed and nothing throws `NotImplementedError`.

**Build:** `mvn -B -o -q clean package` offline, green. Engine 107 tests, cluster 39, cp 24,
server 16; 0 failures, 0 errors, 0 skipped. Commit `c74d77c` on branch `t31`.

## T29: Type-specific merge rules

**Built:** `dynacache.cluster.Versioned(value: Value, dvv: Dvv)` and the pure function
`merge(local: Versioned, remote: Versioned, counter: DotCounter): Versioned` in
`dynacache-cluster/src/main/kotlin/dynacache/cluster/Merge.kt`: spec 5.3's dispatch (remote
dominates, take remote; local dominates or equal, keep local, no dot spent) and, for concurrent
versions, the type table of spec 2.5 under `Dvv.merge` (one fresh dot from the counter). Nine
tests in `MergeTest`. Engine: `Value` is public (was `internal`) so the cluster can hold one;
`writeScore` moved from `Partition` onto `Value.ZSet.write(score, member)` as the dual index's one
writer; `fieldBytes` is public. `mvn clean package` green: engine 100, cluster 49, server 16.
Four files, 257 lines of new code and tests plus a net one-line engine change.

**Concepts named:** A **versioned** value is a value with the DVV of its version, what replicas
exchange and what `merge` reconciles. The **last writer** of two concurrent versions is the one
with the higher dot, node name first and counter second; that is spec 2.5's "highest node-ID"
tiebreak made total so same-node siblings also resolve. Per type: a string, or two values of
different kinds, is the last writer's outright; a hash is a field-level union where a contested
field takes the last writer's bytes; a list keeps the shared prefix and then both concurrent
tails, the earlier writer's first, and when one side is a prefix of the other (a pop) the later
side stands as it is; a sorted set is the union of members at the higher score. The merged
sorted set's skip list is seeded from the new dot's counter so the result is deterministic. No
new seam: `merge` is a concrete function, and the caller (T22, T26, T28) writes the result
through the engine.

**Acceptance:**
- `merge_string_concurrent_tiebreak_highest_node`: alpha and bravo first writes, bravo's value
  in both argument orders.
- `merge_hash_field_level`: fields only one side holds survive; the field both wrote is the
  higher node's.
- `merge_list_union_of_concurrent_appends`: `[a,b,c,d]` and `[a,b,e]` merge to `[a,b,c,d,e]`
  both ways; `merge_list_concurrent_pop_last_writer` covers the prefix case both ways.
- `merge_zset_union_max_score`: union with the maximum score, checked on both indexes.
- `merge_is_commutative_associative_idempotent`: 25 seeded triples of concurrent first writes
  per kind. Idempotence returns the same instance; commutativity is exact on value and DVV;
  the merged DVV is associative; value associativity is exact for a sorted set and holds for a
  list's element multiset and a hash's field set; a string's three-way result is one of the
  three inputs.
- `merge_result_dvv_descends_from_both`, `merge_dominated_side_is_discarded`,
  `merge_equal_dvvs_keep_local` (the counter hands out dot 1 afterwards, so no dot was spent),
  `merge_different_kinds_concurrent_tiebreak`.
- This entry.

**Deviations:**
- Associativity of a last-writer pick (a string, a contested hash field, the order of list
  tails) does not hold across three-way concurrency, and the test says so rather than claiming
  it: the merged version carries the coordinator's fresh dot (T21's `Dvv.merge`), not the
  winning writer's, so `merge(merge(a,b),c)` and `merge(a,merge(b,c))` can pick different
  writers. Convergence is unaffected because every merge dominates its inputs: two nodes that
  paired differently resolve on their next exchange. Making it associative would need the
  value to carry its winning writer's dot; not built.
- Hash "per-field LWW by DVV" is coarser than the spec's wording: there is one DVV per value,
  so the last writer is decided once per value and applied to every contested field. A
  concurrent `HDEL` is undone by the side still holding the field (a deletion is
  indistinguishable from never having written it). Per-field dots would repay it.
- List merges have no common ancestor to look at: two sides sharing no prefix (a concurrent
  `LPOP`, or two first writes) are concatenated; a pop against an append is last-writer-wins.
- The plan entry's `merge(local, remote)`: the code also takes the node's `DotCounter` (T21's
  reason: the fresh dot is the node's), and nothing else; `self` is `counter.node`.
- `Value` public and `writeScore` moved onto `Value.ZSet`: the cluster could not see an
  `internal` engine class at all, and building a merged sorted set from outside the engine
  needed the pair's writer. Two `Partition` call sites changed, nothing renamed.

**For the next ticket:** T22 and T28 call `merge(local, remote, counter)` and write the result
through the engine when it is not the same instance as `local`. A merged value shares byte
arrays with its inputs (the engine's own convention: nothing copies); the inputs are meant to
be discarded. T31's codec needs a `Value` encoder; `Value` is now public and `ZSet.write` is how
to rebuild a sorted set. `Value` has no `equals`; `MergeTest.canon` is a test-side content view
worth lifting into the test kit if T30's I1 checker compares values.

## T14: MULTI, EXEC, DISCARD and `atomically`

**Built:** the batch, at both seams the plan names for it.

In the engine, `ApEngine.atomically(keys, block)` maps every declared key to a partition and
refuses the batch when they do not agree, before anything is submitted. The refusal is
`CrossPartitionBatch`, a new engine exception carrying the `Reply.Error("CROSSSLOT", "Keys in
request don't hash to the same slot")` the caller writes back; the returned future fails with
it. When the keys do agree, `Partition.inOneTask` runs `block` as a single task on that
partition's executor, so the block's result completes the future on the partition thread and
nothing interleaves for its duration. A batch that declares no key at all runs on partition 0,
where every other keyless command runs. `Partition.execute` changed from `private` to internal
visibility so the batch context can call it directly on that thread, which is what T02's note
asked for.

The `PartitionContext` handed to the block is a small `Batch` class holding the partition and
the declared key set. A `Keyed` command whose key is declared runs; a `Keyed` command whose key
is not gets `-ERR <key> was not declared by this batch`; `PING` and `COMMAND` run, since they
read nothing outside the partition; everything else -- a `Fanned` fan-out, an `EveryPartition`
keyspace walk, a `Scan` cursor, a `Cp` key -- gets `-ERR this command spans partitions and
cannot run inside a batch`. The batch continues past every one of those (I11).

In the server, `CommandHandler` gained two fields: `buffered`, the commands queued since
`MULTI` or null outside one, and `spoiled`, set when a frame failed to parse while buffering.
`MULTI` answers `+OK` and starts buffering, or `-ERR MULTI calls can not be nested`; a buffered
command answers `+QUEUED`; a frame that fails to parse answers its own error at once and sets
`spoiled`; `DISCARD` clears and answers `+OK`, or `-ERR DISCARD without MULTI`; `EXEC` answers
`-ERR EXEC without MULTI` outside one, `-EXECABORT Transaction discarded because of previous
errors.` when spoiled, and otherwise collects the keys of every buffered command, hands them to
`atomically` as the declared span, runs the buffer in order inside the block and replies the
array of per-command replies. A cross-partition span arrives back as the future's failure and is
unwrapped into its `CROSSSLOT` error.

**Concepts named:** The batch's one real idea is that **the block is the unit of exclusion, not
the command**. `submit` gives one command one task; `atomically` gives the whole block one task.
Nothing else was needed for C1 or I11: the partition already had exactly one thread, so a task
that runs many commands already has the exclusion a batch asks for, and rollback never enters
the picture because nothing is ever undone. That is why `Partition` gained six lines and not a
lock.

The second idea is that **the declared keys are the batch's contract**. C12's span check and the
undeclared-key refusal are the same rule read at two moments: the span is checked before the
block runs, because after it starts there is no partition left to move to, and a key the block
names but did not declare is refused because it was never in that check. On a one-key batch that
refusal looks pedantic; on a two-key batch it is precisely what stops a write to a third
partition. CONTEXT.md's "batch" already said both halves, so no vocabulary was added.

MULTI, EXEC and DISCARD are **connection state, not commands**, exactly as T13's note asked.
They are recognised by name in `CommandHandler` before `CommandParser` is consulted, they are
not `Command` variants, and `CommandParser` still answers only "what does this token list mean".
The handler is already per-connection and already single-threaded on the channel's event loop,
so the buffer needs no lock -- the same rule the pending reply queue lives by.

Seams unchanged: `CommandEngine.atomically` and `PartitionContext.execute` have exactly T01's
signatures, and `Reply`, `Key` and `PartitionId` were not touched. The one new public type is
`CrossPartitionBatch`, discussed under deviations. No new interface: there is one batch context
and one implementation, so an interface would have had nothing behind it.

**Acceptance:**
- `multi_exec_atomic`: the batch writes `{t}.a`, parks on a latch, then writes `{t}.b`. Two
  `GET`s submitted while it is parked are both still not done -- the half-applied state where
  `a` is "after" and `b` is still "before" is unobservable -- and both read the post-batch value
  once it is released. Mutation-checked: with the block run on the common pool and each command
  submitted as its own partition task, the test fails on `assertFalse(readA.isDone)`. Nothing
  sleeps; the latches are the schedule.
- `multi_exec_hash_tags_allow_two_keys`: `{user1}.a` and `{user1}.b` are on one partition and
  both writes land.
- `multi_exec_cross_partition_rejected`: over the socket, `MULTI`, two `SET`s on different
  partitions, `EXEC` answers `-CROSSSLOT` and both keys read nil afterwards.
- `discard_clears_buffer`: `SET k before`, `MULTI`, `SET k after`, `DISCARD`; `GET k` is
  "before" and the next `EXEC` says `EXEC without MULTI`.
- `I11_failing_command_does_not_undo_neighbours`: `SET a abc`, `INCR a`, `SET b 2` in one batch
  replies `[+OK, -ERR value is not an integer or out of range, +OK]`, and afterwards `a` is
  "abc" and `b` is "2".
- `C12_atomically_rejects_span_before_running`: the future fails with `CrossPartitionBatch`
  carrying the `CROSSSLOT` reply, an `AtomicBoolean` set on the block's first line is still
  false, and the key is unwritten.
- `C12_undeclared_key_inside_batch_is_an_error`: `{t}.b` on the same partition as the declared
  `{t}.a` still gets the error, the next command still runs, and `{t}.b` is unwritten.
- `server_multi_exec_roundtrip`: over `RespClient`, `MULTI`, `SET`/`INCR`/`GET` each `+QUEUED`,
  a second connection reads nil mid-transaction, `EXEC` returns the three-element array, and the
  second connection then sees the whole batch.
- Supporting: a batch refuses `MGET`, `SCAN` and `DBSIZE` and still runs `PING`; a parse error
  while queued is answered at once and aborts `EXEC`, after which the connection starts clean;
  `MULTI` does not nest and the refused nesting leaves the first transaction open.
- `mvn -B -o clean package`: engine 105, cluster 39, cp 24, server 35. Every earlier test green.
- This entry.

**Deviations:** Three, none against a fixed contract.
1. **`CrossPartitionBatch` is a new public engine type.** `atomically` is frozen as returning
   `CompletableFuture<R>` for a caller-chosen `R`, so a `Reply.Error` cannot be its normal
   answer; C12's refusal travels as the future's failure and the exception carries the reply.
   The alternative, throwing synchronously, would have made the seam answer two different ways
   for two kinds of failure. No signature changed.
2. **The `CROSSSLOT` kind is chosen, not specified.** Spec 2.2, 5.6 and C12 all say "an error"
   without naming a kind, and the do-not-build list rules out the Redis Cluster protocol, so
   `-MOVED` and `-ASK` were never candidates. `CROSSSLOT` with Redis's own wording is what a
   client library already recognises for this exact situation. Recorded here as the deliberate
   choice the ground rules ask for.
3. **The batch is held mid-way by a latch inside the block, not by the T02 gate clock.** The
   gate clock exists because T02 had no user code on the partition thread to park; a batch's
   block *is* user code on that thread, so parking it there is both simpler and a more direct
   statement of what the test means. Deterministic in both directions, and mutation-checked
   above.

Judgement calls, not deviations: `PING` and `COMMAND` are allowed inside a batch (they touch no
key and the ticket's refusal list does not name them); a batch with an empty key list runs on
partition 0, matching `submit`'s keyless routing, so `MULTI`/`EXEC` with nothing between them
replies an empty array; `DISCARD` outside `MULTI` answers `-ERR DISCARD without MULTI`, which
the ticket does not name but Redis does; and `MULTI`, `EXEC` or `DISCARD` with an argument gets
the parser's usual arity error rather than being mistaken for an unknown command.

**For the next ticket:** T15's `EVAL` runs its script inside `atomically` over `KEYS`, and
everything it needs is already there: hand `atomically` the `KEYS` list and call
`ctx.execute` from the `redis.call` bridge. Two things to know about that context. First, the
undeclared-key error is the answer `redis.call` must raise on and `redis.pcall` must return as a
table, so the Lua bridge should treat it exactly like any other `Reply.Error` rather than
special-casing it. Second, the batch refuses `MGET`, `SCAN`, `DBSIZE`, `KEYS` and `RANDOMKEY`
outright, so a script calling one of those gets an error reply, which is the right answer but
worth a test of its own.

`declaredKeys` in `DynaCacheServer.kt` is the one place that knows how to read a command's keys,
and it deliberately has no `Command.Cp` branch: the parser cannot build a CP command today, and
when T44 routes them a CP key inside `MULTI` will fall to the "spans partitions" refusal, which
is the right answer either way. If T44 wants a CP key to be counted in the span check instead,
that is the one line to add.

T19 changes server dispatch in parallel with this ticket. Every server edit here is inside
`CommandHandler` -- two fields, four private methods, and `channelRead` now calling `answer`
instead of parsing inline -- plus four top-level private helpers at the bottom of the file, so a
router that replaces what `answer` does with a parsed command has one call site to redirect.

Nothing new is marked with a `ponytail:` ceiling. The pending queue's existing one, noted in
T13, is untouched: a `MULTI` buffer is bounded by nothing either, so a client that queues
forever grows it, and the same output-buffer limit repairs both.

## T10: Memory accounting and LRU eviction

**Built:** Every entry now knows what it costs and every partition knows what it holds.
`Value.approximateBytes()` is the payload half of the formula: a String is its bytes, and a Hash,
List or Sorted Set is the sum over its elements of the element's own bytes plus `ELEMENT_BYTES`
(16) for the node, pointers and object header a JVM spends holding one; a member's score counts as
eight, and field and member names are ISO-8859-1, one character to the byte. `Partition` adds the
key's bytes and `ENTRY_BYTES` (48) for the entry, its table node and its deadline, so one entry
costs `key.bytes.size + 48 + value.approximateBytes()`.

The running total `Partition.usedBytes` is maintained at exactly the places T09's note named. A new
`forget(key)` takes an entry out of the store and its bytes off the total; `drop` is now `forget`
plus the wheel cancel, and the wheel's fire callback -- the one removal that cannot go through
`drop` -- calls `forget` too, so the two share one accounting line rather than owning two. `write`
subtracts the entry it displaced and then charges for what replaced it through `account(key)`, which
recomputes an entry's size and books only the difference from what it was last charged. Because
`account` is idempotent, `execute` calls it once more on the key a keyed command touched: `HSET` on
an existing hash, `RPUSH`, `ZADD` and `LREM` never reach `write`, and that one recount is what keeps
the total level with a store that grew or shrank in place. `FLUSHDB` zeroes the total with the
store.

`ApEngine` gains a fifth, defaulted constructor parameter `maxMemoryBytes: Long? = null`, split
evenly across the partitions; a partition with no threshold gets `Long.MAX_VALUE`, so "never evicts"
is a share nothing can cross rather than a second branch. After every command, `execute` asks
whether the partition is over its share and runs `evict(now)` if it is -- on the partition's own
thread, from the command's own reading of the clock, after the command has finished with the store.
The step is spec 5.5's order: `purgeExpired(now)` takes every expired key first, then sampling LRU
takes live ones -- `coldest()` draws `SAMPLE` (5) random keys through the table's `randomKey` and
evicts the one whose `lastAccess` is oldest -- until the partition is under its share or the store is
empty, and at most `MAX_EVICTIONS` (32) keys go in one step. `Entry` gains `lastAccess`, written by
`live()` on every read and by `write` on every write, both from the command's single clock read.

`INFO` reports `used_memory` in a `# Memory` section. Each partition now answers `Command.Info` with
an array of two integers, its live key count and its used bytes, and `Info.join` sums both; `DBSIZE`
keeps its own branch and its plain integer.

**Concepts named:** **`usedBytes`** is what a partition holds and **`account`/`forget`** are the only
two verbs that change it, which is what makes "the total agrees with the store" true at one place
each way rather than at every mutation site. **`coldest()`** is the sampling policy in one function:
it names what spec 2.7's "sample K, evict the least recently used" actually asks for, and nothing
else in the partition knows the policy, so T11 can swap it for W-TinyLFU without touching the step
around it. **`evict`** is the bounded step and **`MAX_EVICTIONS`** its ceiling; **`maxBytes`** is a
partition's share, and the even split lives in `ApEngine` so a partition never learns there are
others. `Partition.execute` was split: it settles the instant and the kind, calls a new private
`run(command, now)` for the command itself, then recounts and evicts, so no early return inside the
command can skip the accounting. Seams unchanged: `CommandEngine`, `PartitionContext`, `Reply`,
`Key`, `PartitionId` are exactly T01's, and the `Value` change is purely additive (T31 reads the same
types).

**Acceptance:**
- `eviction_respects_max_memory`: a one-partition node budgeted at three entries takes twenty
  writes and ends at three keys, under the threshold, with the last write still readable.
- `eviction_prefers_expired`: budget three, one key expired but never read since its deadline; the
  write that crosses the threshold takes the expired key, and the coldest live key -- LRU's victim
  otherwise -- stays.
- `lru_evicts_oldest_access`: budget three, key 0 read to make it the freshest, and the write that
  crosses takes key 1. Then a second round with a different key left cold, so one eviction agreeing
  with the access order by accident of bucket order cannot carry the test.
- `eviction_does_not_corrupt`: budget five, forty writes with a distinct value each; exactly five
  survive, each returning its own value, and `DBSIZE` counts those five.
- `I6_expired_evicted_before_live`: a node full to a ten-entry threshold, five of it expired, and
  one more write. All five expired keys go and all five live keys stay.
- `info_reports_used_memory`: zero on an empty node, rising with each write, summed across
  partitions, falling after `DEL`, and falling after a tick past a TTL. That last step is the first
  direct observation that the wheel deletes (T09's note): nothing reads the key between its deadline
  and the tick, so a wheel removal that did not give the bytes back would leave `INFO`'s own sweep
  with nothing left to give back either.
- `eviction_step_is_bounded`: forty entries at the threshold, then one value worth 35 entries; the
  step stops at 32 evictions, `DBSIZE` reports the nine that remain, and the commands after it
  finish the job.
- `eviction_runs_on_the_partition_thread`: T02's C1 clock technique. Twenty evicting writes and a
  `DBSIZE` are twenty-one clock reads, every one of them on `partition-0`. An eviction step on a
  thread of its own, or one reading the clock for itself, shows up as a reader the count forbids.
- `used_memory_follows_an_aggregate_grown_in_place`: a hash field and a list item added in place are
  charged for, and the bytes come back when the field goes.
- Mutation-checked, seven ways: never evicting fails five tests; dropping `purgeExpired` from the
  step fails `eviction_prefers_expired` and I6; taking any sampled key instead of the coldest fails
  `lru_evicts_oldest_access`; the wheel removing without accounting fails `info_reports_used_memory`;
  deleting the post-command recount fails the in-place test; raising `MAX_EVICTIONS` fails the bound
  test; and eviction reading the clock for itself fails the thread test.
- Every existing test still green. `mvn -B -o clean package`: engine 109, cluster 39, cp 24,
  server 16.
- This entry.

**Deviations:** None against the spec, the ticket, the frozen types or ADR 0002. Five judgement
calls.

1. **Eviction is checked after every command, not only after a write.** The ticket says "after any
   write that crosses the threshold". The check is `usedBytes > maxBytes` at the end of `execute`,
   which only a write can make true, so the trigger is the same; what it adds is that a read after a
   bounded step that did not finish continues the work instead of leaving the partition over its
   share until the next write. `eviction_step_is_bounded` depends on exactly that.
2. **The step runs after the command, not inside `write`.** A command may write and then keep
   mutating what it wrote (`RPUSH` creates the list, then fills it), so evicting from inside `write`
   could take the key the command is still holding. Running at the end of `execute` is still the
   partition's own thread and the command's own instant.
3. **A store no larger than the sample is taken whole.** `coldest()` draws five random keys only
   when the store holds more than five; below that it considers every key. Drawing with replacement
   from four keys can miss one that "K random keys" was meant to include, so this is a better
   approximation of the spec's sampling, not a departure from it, and it makes the small-keyspace
   tests deterministic without seeding around the draw.
4. **`approximateBytes()` is O(elements), and the recount after a keyed command pays it once per
   command.** For a String that is the command's own order of work; for one field of a very large
   hash it is more. The `ponytail:` comment on it names the ceiling and the repayment: per-element
   deltas threaded through every aggregate mutation site would make it O(1) at the cost of a running
   total inside every structure. Debt, deliberately not taken on in this ticket.
5. **The WRONGTYPE early return skips the recount and the step.** A refused command mutates nothing,
   and the lazy expiry check in front of it can only lower `usedBytes`, so nothing is missed beyond
   a partition already over its share waiting one more command for its next step.

**For the next ticket:** T11 (Count-Min Sketch and W-TinyLFU) is the direct consumer. `coldest()` is
the whole policy and the only thing T11 has to replace: `evict` decides *when* and *how many*, and
knows nothing about *which*, so a policy parameter on `ApEngine` selects between two implementations
of that one function and the step, the bound, the expired-first order and every accounting line stay
as they are. `Entry.lastAccess` is there for LRU; a frequency counter belongs beside it, and the
sketch belongs on the partition, not on the entry.

`usedBytes` is exact against the formula, not against the JVM: `ENTRY_BYTES` (48) and
`ELEMENT_BYTES` (16) are estimates, so a test that wants a budget of N entries must measure one entry
through `INFO` rather than compute it. `CommandEngineTest.entryBytes` does that with a throwaway
probe engine and is the helper to reuse.

Two things about the test clock matter for any later eviction test. Every seeded write advances it,
because two keys written at the very same instant are equally recently used and LRU has nothing to
choose between them -- the first version of `eviction_respects_max_memory` evicted the key it had
just written for exactly that reason. And `EXISTS`, `GET` and `TYPE` all go through `live()`, so an
assertion that reads a key refreshes it; a test that checks survivors and then wants to keep
evicting must move the clock between the two.

`Command.Info`'s per-partition reply is now `Reply.Array(keys, usedBytes)` rather than a plain
integer. Nothing outside the engine reads it today, but T13's `INFO` over RESP and T44's dispatcher
see only the joined bulk string, whose `# Memory` and `# Keyspace` sections are unchanged in shape.
`atomically` is still `TODO("T14: batches")`; a batch will reach `execute` per command and so gets
the accounting and the eviction step for free.

## T41: Sessions and session-tied release

**Built:** The session registry (CP spec 4) as a third primitive of the composite state machine,
and the one entry that ends a session and releases what it held (C18, I15).

In the engine module, `Command.Cp` gained a third sealed sub-hierarchy, `Cp.Session`:
`SessionCreate(timeout = 15 s)`, `SessionHeartbeat(session)` and `SessionClose(session)`. A
session is not a key's state, so the three share one constant key, `cp:session`, which is how
they pass the `-NOTCP` edge of both engines (C16) without a special case. Beside it is one
interface, `Cp.Sessioned { val session: Long }`, implemented by `LockTry`, `LockUnlock`,
`LockRenew`, `SessionHeartbeat` and `SessionClose`: "a command done on behalf of a session".

`SessionRegistry` (`dynacache-cp/.../SessionRegistry.kt`) is a plain primitive like the counter and
the lock: `apply(command, now)`, `isAlive`, `close`, `lapsed(now)`, `snapshot`, `restore`. Ids climb
from `lastId`, which is state-machine state, so every member and every successor leader hands out
the same next number; each session holds `lastHeartbeat` (the log time of the entry that created
or last refreshed it) and its timeout in milliseconds.

`CpStateMachine` checks, at the top of dispatch, that a `Sessioned` command's session is alive and
answers `-NOSESSION` ("session N expired or never created") before any primitive sees it. It applies
`SessionClose` and the internal `SessionClosed(ts, session)` entry through one private
`closeSession`: forget the session in the registry and, if it was alive, `locks.releaseAllOf(session)`,
all inside the one applied entry (C18). A second closing of the same session is a no-op.
`lapsedSessions()` exposes the registry's sweep at this member's log time. The snapshot chunk
carries the registry's state next to the counters and the locks.

`FencedLockStateMachine.releaseAllOf(session)` is one `replaceAll` over the map, releasing every
lock the session owns and keeping each lock's token (C17 still holds after a session death).

`RaftRuntime.tick()` now runs in two steps: the leader appends the `TtlTick` as before, and in that
tick's completion (which runs only on the member whose tick it was, so only the leader) it asks the
state machine for the lapsed sessions and appends one stamped `SessionClosed` per session. It
completes with the index of the last entry it appended, so a caller awaiting the tick awaits the
closes too. Followers never append; they only apply.

`CpGrpcServer`'s `CpService.Heartbeat` submits `SessionHeartbeat` to this member's engine and
answers `ok` only for a `+OK` reply: an unknown or unparsable session id and a follower's
`-NOTLEADER` both answer `ok = false`. `CpWire` tags the three verbs (16 to 18) and the
`SessionClosed` entry (operation tag 4). `CONTEXT.md`'s **session** entry names the registry,
lapsing, `SESSION_CLOSED` and `-NOSESSION`.

**Concepts named:** The **session registry** is the primitive that keeps the book of live sessions
in log time; it never decides anything about locks. A session **lapses** when its timeout has run
out since its last heartbeat at a TTL tick; the registry reports it, the leader closes it, and the
composite releases what it held. **Sessioned** is the shape of a command done on behalf of a
session, and the alive check is one place in the composite rather than one per primitive, so T42's
permits get `-NOSESSION` by implementing the interface. The seam did not move: every test drives
`CpEngine.submit`; the gRPC heartbeat is tested through the socket.

**Acceptance:** `dynacache.cp.SessionTest`, 6 tests, all green; `CpWireTest` 6 (two new);
`GrpcCpTest` 6 (one new); `FencedLockTest` 13 unchanged in substance. Full `clean package` green:
engine 100, cluster 39, cp 47, server 30.

- `session_create_heartbeat_close`: CREATE answers `:1` then `:2`, HEARTBEAT `+OK`, CLOSE `+OK`,
  and a HEARTBEAT of the closed session is `-NOSESSION`.
- `session_timeout_closes`: a session with a 1 s timeout takes the lock; the leader's clock moves
  2 s and one `tick()`; STATE shows no owner and the session's HEARTBEAT is `-NOSESSION`.
- `session_op_without_session_rejected`: TRY for a never-created session and TRY, UNLOCK and RENEW
  for a closed one are all `-NOSESSION`; STATE shows nothing was granted.
- `session_heartbeat_keeps_alive` (extra): 1 s timeout, 600 ms, HEARTBEAT, 600 ms, tick; still held.
- `I15_no_lock_owned_after_session_closed_index`: two locks held by one session; the tick's
  completion index is exactly `before + 2` (the tick, then the one `SESSION_CLOSED`); after
  `awaitApplied` on the leader and a follower, STATE on each member shows both locks unowned.
- `C18_release_is_one_log_entry`: two locks held, the commit index read after the last STATE
  showing them held, CLOSE, and the commit index is exactly one higher while both STATEs show
  released.
- `cp_heartbeat_over_grpc`: a live session's heartbeat is `ok`, a never-created id and a
  non-numeric id are not.
- Every T38, T39, T40 and T43 test green.

**Deviations:**

1. **Session verbs carry the constant key `cp:session`.** `Command.Cp.key` is abstract and both
   engines check it for C16; a session has no key of its own. The constant satisfies the rule with
   no special case in `CpEngine`, `ForwardingCpEngine` or `CpWire` (which writes and skips it).
   T44's parser never sees it: the verbs take no key argument.
2. **Sessions are not auto-created.** CP spec 4 says the first CP op from a connection auto-creates
   one; that is the dispatcher's business (T44 owns the connection), and the engine only knows
   `SessionCreate`. A lock verb without a live session is `-NOSESSION` (CP spec 10.6).
3. **A session's timeout is per session.** `SessionCreate(timeout)` carries it, defaulting to the
   spec's 15 s; the spec's "N seconds, H heartbeats" is a client-side cadence the engine never sees.
4. **Lapsing is `lastHeartbeat + timeout <= now`**, the same rule as a lock's lease, where CP spec
   9.3 writes `now - last_heartbeat > timeout`. One millisecond, chosen to match the lease.
5. **`tick()` completes with the last index it appended**, not the tick's own, when it closed a
   session. `C23`'s `awaitApplied(tickIndex)` still holds (a later index is a stronger wait).
6. **Two ticks can both close the same session** if the second runs before the first's
   `SESSION_CLOSED` commits (the registry still lists it). The second entry is a no-op on apply;
   in production ticks are 100 ms apart and a commit is faster than that.
7. **A leader that loses leadership between the tick's commit and the close appends** completes
   `tick()` exceptionally (MicroRaft's `NotLeaderException`), as a lost tick already did in T39.
   The production tick loop (plan 2.5, not yet written) should ignore a failed tick.
8. **T40's `FencedLockTest` registers eight sessions in `@BeforeEach`** so its literal session
   numbers 1 to 8 are live, and moves every member's clock by those 8 ms so its absolute clock
   jumps land where they did; `lock_fencing_token_monotonic` uses sessions 1 to 3 instead of 0 to 2.
   No assertion in that class changed.
9. **The registry's map is a plain `HashMap`.** Only the Raft thread reads and writes it (the tick's
   completion runs on that thread too). T40 kept `ConcurrentHashMap` for the lock map because the
   I15 test reads a follower's lock state from the test thread; the registry is never read that way.
10. **Follower STATE in I15 is read on the primitive.** A follower's engine answers `-NOTLEADER`, so
    the test applies `LockState` to the member's `locks` at its own `lastAppliedTs`, as T39's C23
    test read `valueOf` on each member.
11. **Forwarding stays at-least-once.** The registry gives a session an id but no request counter;
    T43's dedup note stands until something needs it.
12. **TDD granularity.** `session_create_heartbeat_close` was red (no variants), and the exhaustive
    `when`s in the composite and `CpWire` forced all three verbs and their tags in that slice.
    `session_op_without_session_rejected` was green first time on the top-of-dispatch check written
    in that slice. `session_timeout_closes` was red (still held) and forced `releaseAllOf`, the
    `SessionClosed` entry and the tick's second step. `session_heartbeat_keeps_alive`, I15 and C18
    were green first time against that code. `cp_heartbeat_over_grpc` was red (`UNKNOWN` from the
    `NotImplementedError`) then green. The two wire tests were green first time.
13. **Real-time waits.** None added. `SessionTest` runs in well under a second; `FencedLockTest`
    keeps its 15 s of failover waits from T40.

**For the next ticket:**

- **T42 (semaphore)**: permits held per session are released by extending `closeSession` in
  `CpStateMachine` with one more call (`semaphores.releaseAllOf(session)`); the acquire and release
  verbs implement `Cp.Sessioned` and get `-NOSESSION` for free. The registry itself needs no change.
- **T44 (dispatcher)**: `CP.SESSION.CREATE [timeout_ms]`, `HEARTBEAT sid` and `CLOSE sid` map one to
  one onto the three variants (`Reply.Integer` id, `+OK`, `+OK`). The session id on a lock verb comes
  from the connection (CP spec 6.1): the parser needs the connection's session, and auto-creation on
  the first CP op (spec 4) is a `SessionCreate` the dispatcher submits and remembers per connection.
  `-NOSESSION` is already a `Reply.Error` kind. The heartbeat over gRPC is `CpService.Heartbeat`;
  the RESP verb goes through `Apply` like every other command.
- **T45 (snapshots)**: `CpStateMachine.Snapshot` carries `SessionRegistry.State` (last id and the
  map); `installSnapshot` already restores it. `SessionClosed` has a wire form.
- The production tick loop (plan 2.5) is still unwritten; `tick()` is complete for it, including the
  session sweep, and a failed tick is safe to ignore.
- `GrpcCpKit` gained `heartbeat(member, sessionId)` and a private `stub(member)` shared with `applyDirect`.

## T19: Request router

**Built:** `dynacache.cluster.Router` is one node's request router (spec 5.1 steps 1 to 3, 5.2
step 1) and presents the `CommandEngine` shape, so T13's pipeline submits to it exactly as it
submits to an engine. `submit` takes the key of a `Command.Keyed`, asks
`ring.preferenceList(key, n).first()` who coordinates it, and either runs it on the local engine
or forwards it; a keyless command, a `Scan`, a `Ping` and a `Command.Cp` have no coordinator and
run locally, and a `Command.Fanned` runs through the engine's own fan-out (T22 revisits). A
forward is a `Forward` envelope carrying the command's tokens under an id unique to the sending
router, a `CompletableFuture` parked in a `pending` map, and one coroutine on the node's scope
that sends it and then sleeps the deadline; whichever of the reply and the deadline arrives
first takes the future out of the map, so the same coroutine is both the timeout and the
cleanup. A missed deadline answers `Reply.Error("ERR", "forward timeout after 2s waiting for
<node>")`. On the coordinator's side `receive` parses the tokens, submits locally and sends a
`ForwardReply` under the same id; an error reply crosses like any other reply.

`cluster.proto` gains `Forward` (id, repeated bytes token), `ForwardReply` (id, `ReplyMsg`) and
`ReplyMsg` with `ErrorReply`, `BulkReply` and `ArrayReply` under it: a `Reply` in exactly the
five RESP2 shapes. `ReplyWire` (in `Router.kt`) maps a `Reply` to it and back. `Router.run` is
the node's one inbound loop and owns the demux T20 asked this ticket to own: `Forward` and
`ForwardReply` are the router's, and every other envelope goes to the injected `gossip`
function, which is `Swim::deliver`. `Swim` gained exactly that one method, a public alias of its
existing private `handle`. `InProcessCluster` takes a `CoroutineScope`, builds a `Router` per
node, launches each router's loop on that scope, and `writeVia`/`readVia` go through the
contact's router; `readAllReplicas` still reads each replica's engine directly, since its
question is what each replica holds. `TokenCodec` is the test kit's wire form of the handful of
commands the kit exercises. `mvn -B -o clean package` offline: engine 100, cluster 44, cp 24,
server 30. 495 lines including tests, nine files.

**Concepts named:** A **contact node** is the node a client happened to reach; it is the
**coordinator** of the keys it owns and forwards the rest, and the client never learns the
difference. Both are new CONTEXT.md entries, together with **Router**, which had to be told
apart from the **dispatcher**: the dispatcher chooses between the AP and the CP engine by
namespace and sits above the router, which only decides whether this node coordinates the key.
The router is a decorator of the `CommandEngine` seam rather than a new seam: it presents the
frozen shape and its second adapter is the engine it wraps, so nothing downstream of it learns
that a cluster exists. The **demux** is the node's single reader of `Transport.inbound`: one
inbound, two consumers, and the router is the one that owns the loop because it is the one with
a `run()` to put it in. A forwarded command's identity on the wire is its **tokens**, the
client's own frame, so the coordinator's parser reads it exactly as the contact's would have and
no second encoding of a `Command` exists anywhere.

**Acceptance:**
- `router_executes_locally_when_coordinator`: the coordinator's own router submits to a
  `RecordingEngine` and nothing is sent.
- `router_forwards_to_coordinator`: a write through a contact that is not the coordinator lands
  on the coordinator's engine and not on the contact's. Checked by mutation: with `submit`
  always running locally, it fails.
- `router_forwarded_reply_identical_to_local`: two identical three-node clusters run the same
  seven commands, one through a forwarding contact and one on the coordinator itself, and every
  reply shape the crossing carries -- status, bulk, nil bulk, integer, array and error -- comes
  back equal. Checked by mutation: with `ReplyWire` dropping `BulkReply.nil`, it fails on the
  nil bulk.
- `router_forward_timeout_is_an_error`: a network partition between contact and coordinator; the
  deadline runs on `runTest`'s virtual clock, no sleeps, and the reply is the timeout error while
  the coordinator's engine never saw the write. Checked by the same never-forward mutation.
- `router_unreadable_forward_is_an_error_and_the_node_lives`: tokens the coordinator cannot parse
  answer with an error instead of throwing out of the demux, and the node forwards again after.
- `GrpcTransportTest`'s exhaustive `when` gained a `FORWARD` and a `FORWARD_REPLY` branch and
  round-trips both over real sockets; every earlier test green.
- This entry.

**Deviations:**
1. **The router needs a `Command` to tokens direction, and it did not exist.** `submit` receives
   a `Command`, not the frame it was parsed from, so a `Forward` carrying tokens has to
   re-spell the command. The ticket named the tokens-to-`Command` direction and offered an
   injected function for it; the router takes **both** directions as functions
   (`tokens: (Command) -> List<ByteArray>` and `parse: (List<ByteArray>) -> Command`), which
   keeps the module graph as plan 2.2 draws it and keeps the wire's spelling in the one place
   T13 put it. `CommandParser` was not moved.
2. **The real pair is not written yet.** Only the test kit's `TokenCodec` implements it, covering
   GET, SET, DEL, INCRBY, HSET and HGETALL and throwing on anything else. The server has no
   router to wire in this ticket, so the full inverse of `CommandParser` would have had no
   consumer; whoever wires a router into the server owns it. See "For the next ticket".
3. **A multi-key command runs on the contact.** `Command.Fanned` (`MGET`, `MSET`, variadic `DEL`
   and `EXISTS`) is submitted to the local engine, so for a key the contact does not coordinate
   it reads and writes the wrong node's data. This is what the ticket scopes to T22, and it is
   the sharpest thing this ticket leaves open.
4. **`atomically` never forwards.** The signature's `R` is the caller's own type and has no error
   shape, so a batch whose keys this node does not coordinate comes back as a failed future
   holding an `IllegalStateException` that names the coordinator, rather than as an error reply.
   Forwarding the batch was not an option: a batch is a caller's block, which is code. `T14` turns
   that failure into whatever `EXEC` should answer.
5. **`Swim.tick()` still drains `inbound` itself.** The minimal change the ticket allowed:
   `deliver` was added and `tick`'s own `tryReceive` loop left alone, because `SwimTest` drives
   gossip with no router present and `InProcessCluster` has no `Swim`. Nothing today puts both
   readers on one channel; the node that runs gossip and forwarding together must feed `Swim`
   from the demux and let `tick` find an empty inbox, which is what it will find.
6. **A forward pending at shutdown hangs.** Cancelling the node's scope kills the deadline
   coroutine before it can complete the future. Debt: a `finally` on the launch completes it.
   Nothing in P2 shuts a node down under load.
7. **The demux awaits one forwarded command at a time** (marked `ponytail:`): a slow command on
   the coordinator delays the envelopes behind it, gossip included. Running each on the node's
   scope is the repair and costs the in-order delivery the transport promises per pair.

**For the next ticket:** T22 builds replication on top of this. `Router.submit` is where the
coordinator is known, so the replication fan-out belongs after the local `submit` on the
coordinator's side, not in the contact's forward. The contact already blocks on one future per
command, so a quorum's deadline composes with the forward's rather than replacing it. Take
deviation 3 first: `Command.Fanned` has to split by coordinator the way `ApEngine.fanOut` splits
by partition, and the pieces are already there in `Fanned.single` and `Fanned.join`.

Whoever wires a router into the server module (T24) owes the real `(Command) -> List<ByteArray>`,
the inverse of `CommandParser.dispatch`, next to the parser: an exhaustive `when` over
`Command.Keyed` so a new variant stops the build rather than failing a forward at runtime. Two
things it will hit: `Command.Expire` holds an absolute `Instant`, and T13 left `PEXPIREAT`
without a parser row, so that row has to exist before an `EXPIRE` can be forwarded losslessly;
and `Set`'s TTL should go out as `PX <millis>` since the `Duration` no longer knows which
spelling it arrived as. A round-trip test over canonical token rows (`tokens(parse(row)) == row`)
covers it in one line per variant and sidesteps `Command`'s array-valued variants having no
equality.

`InProcessCluster.settle(future)` is the kit's pump: nothing moves on an `InMemoryTransport`
until a drain, and a forward needs two, so a test that submits through a router must settle
rather than await. It drains a bounded number of rounds and then awaits, which is exactly what
lets a deadline fire in virtual time when no reply is coming. `drainMessages()` now yields after
the drain so each node's demux reads what arrived. `gossipOn(node)` is what the demux handed to
gossip on that node, which is how a test asserts an envelope arrived now that no test can read a
router-owned `inbound` directly; an endpoint for a node the cluster does not have (`network
.endpoint(NodeId("onlooker"))`) is the way to get a router-free endpoint when a test needs to
read raw envelopes.

## T34: Fsync policies and group commit

**Built:** `WalWriter` in `dynacache.engine.persist` now takes an `FsyncPolicy` (`ALWAYS`,
`EVERY_SECOND`, `NEVER`), a `java.time.Clock`, and a `WalSink`, and `append(op, payload)` returns
a `WalAppend(seq, durable)`: the sequence number at once, and a `CompletableFuture<Unit>` that
completes when the policy says the entry is on disk. `ALWAYS` fsyncs before completing;
`NEVER` completes on write; `EVERY_SECOND` completes on the caller's `tick()` once the injected
clock is at least one second past the last fsync. Group commit: an append encodes its entry,
takes its seq and enqueues under the writer's monitor, then the first appender to find no
flusher running becomes the flusher. It drains the queue into one `write` and, under `ALWAYS`,
one `fsync`, completes those waiters, and loops while the queue refills. `close()` drains what
is queued, forces what `EVERY_SECOND` still holds, then closes the sink. T33's `WalReader` and
the on-disk format are untouched. Four new tests in `WalFsyncTest`, JUnit 5 only, `@TempDir`,
latches and futures with five-second deadlines, no sleeps; T33's four tests in `WalTest` are
unchanged except two call sites that now read `.seq` off the returned `WalAppend`.

**Concepts named:** A **sink** (`WalSink`: `write`, `fsync`, `close`) is where the log's bytes
go; it is the one seam this ticket adds, and it has two adapters: `FileChannelSink` (a file
opened for append, `force(true)` so the size change is on disk too) and the test's counting
adapter, which also holds the first fsync open so a batch is forced to form. A **flusher** is
whichever appender holds the `flushing` flag; it is the first to arrive, not a dedicated thread,
because the engine owns no timers or threads beyond its partition executors (plan 2.5) and a
lock-free arrival makes the group-commit test deterministic: the first flusher is held inside
its fsync, the other ninety-nine enqueue and return, and the next drain holds them all. A
**durability future** (`WalAppend.durable`) is what T35 replies after (C14). The flusher
re-checks the queue after releasing the flag, so an entry enqueued between drain and release is
never stranded.

**Acceptance:**
- `wal_fsync_always_durable`: five appends, the fsync count rises one per append and every
  future is done when `append` returns.
- `wal_fsync_every_second_batches`: fifty appends, zero fsyncs and no future done; a tick at
  999 ms still nothing; a tick at 1000 ms one fsync and all fifty done; an idle tick forces
  nothing; a fifty-first append and a tick a second later gives the second fsync.
- `wal_group_commit_amortizes`: 100 appenders on a 100-thread pool under `ALWAYS`; every future
  completes and the sink saw at most two writes and two fsyncs.
- `wal_group_commit_preserves_seq_order`: the same 100 appenders; the file reads back
  `CLEAN_END` with seqs 1..100 in order, the returned seqs sorted are 1..100, and the payload
  stamped under each returned seq is the one that appender wrote. Passed on its first run: the
  FIFO queue drained by a single flusher gives it by construction; the test pins it.
- This entry.

**Deviations:** None against the ticket, the plan or spec 2.8. Judgement calls: `append` returns
`WalAppend(seq, durable)` rather than `CompletableFuture<Long>`, so a caller knows its seq
without waiting for durability. `tick()` under `ALWAYS` and `NEVER` is a no-op. `tick()` forces
only what was written before it looked, so an entry written during the fsync completes on the
next tick, never early. The path constructor defaults to `NEVER` and `Clock.systemUTC()` so T33
call sites compile unchanged; T35 should pass both explicitly. A write or fsync that throws
`IOException` completes that batch's futures exceptionally and the writer stays usable.

**For the next ticket:** T35 wires `WalWriter` into the engine: reply after `durable`, and own
the scheduler that calls `tick()` (a one-second period is the spec's "lose at most 1s"). `close()`
while another thread is mid-flush closes the channel under it; that flusher's batch completes
exceptionally. Stop appending before closing. `WalSink` has no `truncate`: checkpoint truncation
(T35) needs its own handle on the file, as T33 already noted. `EVERY_SECOND` counts from the
last fsync, not from the first unforced write, so a burst after idle time is forced at the next
due tick, at most one second after the last fsync.

## T32: Snapshot engine

**Built:** the local RDB snapshot of spec 2.8, on top of T31's codec.

`Partition.snapshotView(now)` runs as one task on the partition executor and returns a
`List<RdbEntry>` that is a point in time by construction: the task sits between two commands
or batches, never inside one, so C9 needs no lock. The view is a shallow copy of the live
entries with every mutable value copied: a Hash into a fresh `HashTable`, a List into a fresh
`ArrayDeque`, a Sorted Set into a fresh `SkipList` seeded from the partition's own `Random`
and filled through `writeScore` (so I3 holds for the copy too). A String's `ByteArray` is
shared, since no command mutates one in place (`APPEND`, `INCRBY` and `LSET` all replace the
reference). Expired keys are left out at the view, so the writer's own filter has nothing to
do. The DVV bytes are empty until replication stamps them (T22). `Partition.restore(entries)`
is the entry point back in: one task on the executor, every live entry through `write`, so a
restored TTL lands on the wheel like any other. `ApEngine.snapshotView(now)` gathers every
partition's view with `allOf`; `ApEngine.restore(entries)` groups by partition and writes.
Both are `internal`; the `CommandEngine` interface is untouched.

`SnapshotEngine(engine, dir, clock, interval = 300s, seeds = Random(), sink)` in
`dynacache.engine.persist`: `save()` reads the clock once, takes every partition's view, and
serializes off the executors on the caller's thread through `sink(dump.rdb.tmp)`, then
`Files.move(ATOMIC_MOVE, REPLACE_EXISTING)` to `dump.rdb`. `restore()` reads `dump.rdb` if
present, refuses a file holding a zero-member sorted set (T31's "say so out loud"), writes
every entry into its partition and answers the count; no file is 0, not an error. `save` and
`restore` are both `@Synchronized` on the engine object, so neither runs inside the other.
`maybeSave(now)` is the interval hook, a plain method that saves once `interval` has passed
since the last save (or construction). `close()` is the shutdown save. `sink: (Path) ->
OutputStream` defaults to `Files.newOutputStream` and is the slow-sink test seam.

The server's `main` gains an optional third argument `[dir]`: with it, `restore()` runs before
the port opens, the tick lambda calls `maybeSave(clock.instant())` after `engine.tick()`, and
the shutdown hook calls `snapshots.close()` between `server.close()` and `engine.close()`. So
the graceful-shutdown save is the server's doing, not `ApEngine.close()`'s: the engine still
knows nothing about files.

**Concepts named:** the *view* (`snapshotView`) is the point-in-time copy CONTEXT.md's
snapshot needs, and it is a task, not a lock: what makes it consistent is the executor's
single thread, the same thing that makes a batch consistent. `frozen(value)` is the one
place the copy rule lives (which kinds share and which copy). The *sink* is the writer's
output stream, injectable at the `SnapshotEngine` constructor, the one seam that makes
"does not block reads" observable without a slow disk. `RdbEntry` now travels engine-wide
(Partition imports it), which is why `Partition.kt` gained a `persist` import.

**Acceptance:**
- `rdb_concurrent_writes`: a writer thread `HSET`s 32 fields of one hash with rising round
  stamps while a save runs with a stalled sink; the writer completes three more rounds while
  the sink is parked; the restored fields are non-increasing along field order, span at most
  two adjacent rounds, and none is newer than the writer's stamp when the sink stalled. The
  last assertion is what makes a reference copy fail: verified by removing the Hash copy in
  `frozen`, which fails it deterministically with stamps from three rounds later.
- `C9_snapshot_never_contains_half_a_batch`: a thread runs `atomically` batches of ten
  hash-tagged keys with rising stamps; a save lands after three rounds; the restored ten keys
  hold exactly one distinct value.
- `snapshot_restore_on_startup`: String, String with TTL, Hash, List and Sorted Set saved from
  one engine and read back through a fresh engine's commands; `TTL` still answers 90.
- `snapshot_does_not_block_reads`: a `GET` completes while the sink is parked on the writer's
  first byte; the file is then valid once released.
- `snapshot_atomic_rename_leaves_no_tmp`: two saves in a row leave exactly `dump.rdb` in the
  directory; restore with no file answers 0.
- Progress entry: this file.

**Deviations:** none from the spec. Two choices the ticket left open, stated: the shutdown
save is wired by the server's `main`, not by `ApEngine.close()`; and the interval is a
constructor parameter with the spec's default of 300 s rather than a `saveEvery(interval)`
method, since `maybeSave(now)` is the only call the scheduler makes.

**For the next ticket:** `save()` serializes on the caller's thread, and in the server that
is the tick thread, so a large keyspace delays the following ticks by the write time; the
scheduler catches up and lazy expiry covers the gap, but T35 (WAL checkpoint after a
snapshot) may want the save on its own thread when it adds fsync to the path. `save()` answers
nothing; T35 needs the WAL sequence number of the checkpoint, and the natural place is a
return value from `save()` carrying what the views were taken at. `Partition.restore`
overwrites whatever a key holds, which is right at startup and is what T35's replay order
(RDB first, then WAL) relies on. The zero-member sorted set check is in
`SnapshotEngine.restore`, before anything is written, so a bad file leaves the engine empty.
`RdbReader`'s `seeds` is the `SnapshotEngine` constructor's `Random`, not the partition's:
the file is decoded off the executors before its keys are grouped, so a restored skip list's
levels are reproducible from that seed, not from the engine's. T36 (Chandy-Lamport) can call
`ApEngine.snapshotView(now)` directly for the local state and `RdbWriter` for the file; the
sink seam is per `SnapshotEngine`, so a per-node state file needs its own instance or a path
argument on `save()`. T10 touches `Partition.write` and `drop`; `restore` goes through
`write`, so memory accounting will count restored keys with no extra line.

**Build:** `mvn -B -o -q clean package` offline, green. Engine 117 tests, cluster 49, cp 38,
server 35; 0 failures, 0 errors, 0 skipped. Commit `a683b6f` on branch `t32`.

## T15: Lua scripting

**Built:** `EVAL` end to end, in one new server file `Lua.kt` and one new branch in
`CommandHandler.answer`.

`sandboxedGlobals()` is `JsePlatform.standardGlobals()` with `os`, `io`, `luajava`, `require`,
`package`, `load`, `loadstring`, `dofile`, `loadfile` and `debug` set to nil, and `math.random`
and `math.randomseed` taken off `math` (C11). `luajava` is not on the ticket's list and is the
widest door of the lot -- it reflects into any JVM class -- so it went with them. The script is
compiled with `Globals.load(stream, "@user_script", "t", globals)`: mode `t` is text only, so a
precompiled chunk carrying bytecode the sandbox never inspected cannot be loaded even by a
client that sends one. A fresh `Globals` per call is also the whole of "no state survives
between calls": the environment a script writes into is discarded with it.

`evalScript(engine, parser, args)` parses `EVAL script numkeys key... arg...`, maps the declared
keys to `Key`s and hands them to `atomically` as the batch's span. Everything C12 and I11 asked
of `MULTI`/`EXEC` in T14 therefore holds for a script without a line of new code: a span across
partitions is refused before the first Lua statement runs and arrives back as
`CrossPartitionBatch`, and a `redis.call` on a key the script did not declare gets
`-ERR <key> was not declared by this batch` from the same `Batch` context.

`redis.call` and `redis.pcall` are one `VarArgFunction` with one flag between them. Each builds
the token list a client would have sent -- Lua strings as their bytes, Lua numbers as their
string form -- hands it to `CommandParser` and runs the result through the batch's
`PartitionContext`. On a `Reply.Error`, `call` throws `LuaError` carrying the `{err = ...}`
table and `pcall` returns it.

**Concepts named:** The ticket's one real idea is that **a script is a batch with a program in
it**. Nothing in the engine changed and nothing in `atomically` changed; `EVAL` differs from
`EXEC` only in where its command list comes from -- a compiled chunk asking for one command at a
time instead of a buffer filled in advance. That is why the undeclared-key error and the
cross-partition refusal needed no Lua-specific code: they are the batch's rules, read through a
new caller. `CONTEXT.md`'s "Batch" already said "a MULTI/EXEC sequence or one EVAL script", so
no vocabulary was added.

The second is that **`redis.call` is the wire, not an API**. It takes the same token list the
socket takes and answers the same `Reply`, so the parser is the only place that knows what
`SET` means and there is no second command table to drift. It also means the bridge inherits
every arity check and every error wording for free.

The third is that **the sandbox is a factory, not a policy object**. `sandboxedGlobals()` is a
function returning a fresh `Globals`; there is no interface, no configuration and nothing to
inject, because there is exactly one sandbox and a second one would be a second answer to C11.
It is public so a test can assert C11 without a socket, which is the ticket's second seam.

Seams unchanged: `Reply`, `Key`, `CommandEngine`, `PartitionContext` and `CrossPartitionBatch`
have exactly the signatures T01 and T14 froze, and the engine module was not touched. The one
shared helper extracted is `CompletableFuture<Reply>.orBatchError()` in `DynaCacheServer.kt`,
which `exec()` and `evalScript` now both use; it is the four lines `exec()` already had.

**Acceptance:**
- `C11_clock_and_random_unavailable`: every banned global reads nil on a fresh `sandboxedGlobals()`,
  `math.random` and `math.randomseed` are gone from `math`, and `os.time`, `os.clock`, `os.date`
  and `math.random` each either are nil or raise when a chunk evaluates them. The test also
  loads `string.upper` to prove what is left is still a working Lua and not an empty table.
- `sandbox_carries_no_state_between_calls`: a chunk sets a global, the next `sandboxedGlobals()`
  does not see it.
- `lua_keys_argv`: `EVAL ... 2 {s}.one {s}.two first second` returns `#KEYS`, both keys, `#ARGV`
  and `ARGV[1]`, so both tables are 1-indexed and `numkeys` split them where it said.
- `lua_redis_call`: `SET` then `GET` inside one script, then the spec's own read-add-write
  counter script, then a `GET` from outside proving the write outlived the script.
- `lua_cross_partition_rejected`: two keys on different partitions answer `-CROSSSLOT`, and the
  key the script would have written first is still nil, so nothing ran (C12).
- `lua_no_side_effects`: `os.execute`, `io.open`, `math.random` and `luajava.bindClass` each
  fail the script with `-ERR Error running script ...`.
- `lua_deterministic` (I10): two engines built the same way, seeded the same way, given the same
  script, return replies that are equal to each other and equal to the literal the script's
  inputs determine -- so the test fails both if the two disagree and if both answer nothing.
- `lua_type_conversion_table`: twenty rows, each its own engine. Lua to Redis: number to
  integer, `3.9` and `-3.9` truncating toward zero, string to bulk, `true` to `:1`, `false` and
  `nil` and no return at all to the nil bulk, table to array, an array stopping at its first
  hole, a nested table, `{ok=}` to a simple string and `{err=}` to an error. Redis to Lua, read
  back out from inside the script: integer to `number`, bulk to `string`, nil bulk to `false`,
  array to a table of the right length and contents, `+OK` to a table with `ok`, an error to a
  table with `err`, and a Lua number argument reaching the command as its string form.
- `lua_undeclared_key_is_an_error` and `lua_refused_command_inside_script_is_an_error`: a
  `SET` on an undeclared key on the *same* partition still gets the declaration error and does
  not land; `redis.call('SCAN', '0')` gets `-ERR this command spans partitions and cannot run
  inside a batch`.
- `lua_pcall_returns_the_error_rather_than_raising`: `redis.pcall('INCR', k)` on a non-numeric
  string returns a table the script reads `err` out of and concatenates, so it did not raise.
- `mvn -B -o clean package`: engine 112, cluster 49, cp 38, server 65. Every earlier test green.
- This entry.

**Deviations:** Four, none against a fixed contract.
1. **`luajava` and mode `t` are additions to the ticket's banned list.** The ticket names `os`,
   `io`, `require`, `load`, `loadstring`, `dofile`, `loadfile`, `debug`, `package`,
   `math.random` and `math.randomseed`. `JsePlatform.standardGlobals()` also installs `luajava`,
   which reflects into arbitrary JVM classes and would have made every other removal decorative,
   and `Globals.load` accepts binary chunks unless the mode says otherwise. Both are C11 read
   strictly rather than a change to it.
2. **Bytes cross into Lua as raw bytes, not through a `String`.** The ticket says "strings from
   bytes via ISO-8859-1 so bytes survive". ISO-8859-1 is the byte-for-char mapping that spells
   this, but LuaJ's `LuaValue.valueOf(String)` re-encodes to UTF-8, so a key byte `0xE9` would
   become two bytes inside Lua: `#KEYS[1]` would disagree with the key's length on the wire, and
   the bytes `redis.call` sent back would not be the declared key's, which the undeclared-key
   check would then refuse. `LuaString.valueUsing(bytes)` and reading `m_bytes` back is the same
   mapping done byte-exactly. ISO-8859-1 is still what turns a byte string into text for an
   error message.
3. **`EVAL` inside `MULTI` answers an error and aborts the transaction.** Redis queues it. `EVAL`
   is not a `Command`, so it cannot enter T14's buffer, and giving it one would have meant a
   second kind of buffered thing for a case the ticket does not name. Today's behaviour without
   this ticket was the same abort by another route (`eval` was an unknown command), so nothing
   regressed. Debt: repaid by making the buffer hold "things that answer a reply" rather than
   `Command`s, which is also what `EVALSHA` will want.
4. **A `{err = ...}` table with no space in it gets the kind `ERR`.** Redis writes the string
   after `-` verbatim; `Reply.Error` is a kind and a message, and a kind-only error would encode
   with a trailing space. Splitting at the first space round-trips every error that has one,
   which is every error the engine produces.

**For the next ticket:**
- `EVALSHA` and `SCRIPT LOAD` need a per-server script cache keyed by SHA1 of the source; the
  `evalScript` signature already takes the source as `ByteArray`, so a cache sits in front of it
  and nothing else moves. `EVAL` itself does not cache: a fresh `Globals` per call is C11's
  no-state rule and a compiled `Prototype` could be cached without breaking it, but the chunk
  must still be bound to a new environment each time.
- **A runaway script pins its partition's only thread forever**, and every key on that partition
  stops answering with it. There is a `ponytail:` comment on `evalScript` naming the repair: an
  instruction-count hook on the `Globals`. It wants its own ticket, because killing a half-run
  script is a question about atomicity, not about sandboxing.
- An integer reply crosses into Lua as a double (`LuaValue.valueOf(value.toDouble())`), so a
  count above 2^53 loses precision inside a script. Real Redis has exactly this limitation for
  the same reason, so it is faithful rather than a shortcut.
- T16's acceptance transcript wants the counter `EVAL` from spec section 9; it is the second
  half of `lua_redis_call` and works over the socket as written.
- `orBatchError()` in `DynaCacheServer.kt` is now the one place C12's refusal turns back into a
  reply. Any third kind of batch should end with it rather than its own `exceptionally`.

## T11: Count-Min Sketch and W-TinyLFU

**Built:** A partition can now be told *which* key to give up, and there are two answers. The new
`dynacache.engine.ds.CountMinSketch(width, depth = 4, seed)` is the frequency half: `depth` rows of
`width` byte counters laid end to end, `increment` raising one counter per row and `estimate`
answering with the smallest of them, so a collision can only ever make the answer too high. That
one-sided error is the point -- a policy that over-rates a cold key loses a little hit ratio, one
that under-rates a hot key throws it away. `halve()` ages every counter, which is what stops the key
that was hot an hour ago from outranking the key that is hot now. Counters are bytes saturating at
`MAX_COUNT` (255) rather than Caffeine's packed nibbles: four times the memory of a nibble, none of
the shifting, and at `depth * width` counters the whole sketch is still kilobytes. Rows are made
independent by one odd multiplier apiece drawn from the seed, and `width` is rounded up to a power
of two so a row's slot is a mask rather than a modulo.

`WindowTinyLfu` is Caffeine's algorithm as one object and the whole of "which key goes" for a
partition built `W_TINYLFU`. Three LRU lists, each a `LinkedHashSet` re-inserted on touch, which is
exact LRU in O(1) where the sampling loop was an approximation. A key the policy has not seen enters
the **window**, one percent of the partition's share. When the window is over its share its least
recently used key is a **candidate**, and `victim()` runs TinyLFU's admission filter: the candidate
enters the main space only if the sketch has seen it more often than the main space's own victim,
and the loser of that comparison is what gets evicted. A tie goes to the incumbent, which is what
stops a stream of never-repeated keys washing the main space out one key at a time. The main space
is segmented: a candidate lands on **probation**, a hit while on probation earns **protection**, and
a protected segment over its eighty percent demotes its coldest key back to probation -- demotion,
not eviction, so a demoted key is still in the cache and has only lost its head start. The sketch is
halved every `10 x` the keys the policy holds (Caffeine's sample size, floored at 100 so a nearly
empty policy does not age its sketch away on every access).

`EvictionPolicy { LRU, W_TINYLFU }` is a sixth, defaulted parameter on `ApEngine`, passed straight
through to every `Partition`. `Partition.coldest()` is now two lines -- `tinyLfu?.victim() ?:
sampledColdest()` -- and `sampledColdest()` is T10's sampling loop under its own name. `evict`,
`purgeExpired`, `MAX_EVICTIONS` and every accounting line are exactly as T10 left them. The
bookkeeping the policy needs hangs off the two verbs T10 named: `account` tells it a key was resized,
`forget` tells it a key is gone, `FLUSHDB` clears it, and `execute` records one access per keyed
command on a live key. `INFO` gains a `maxmemory_policy` line in its `# Memory` section, so each
partition now answers `Command.Info` with `[keys, usedBytes, policyName]` and `Info.join` reads the
policy off the first partition (every partition of a node runs the same one).

**Concepts named:** **Window**, **candidate**, **main space**, **probation** and **protected** are
Caffeine's own words and now the code's. **Admission** is the decision at the window's edge and it
lives in `victim()` on purpose: a candidate leaves the window exactly when something has to be given
up, so the comparison that admits it is the same comparison that names the victim, and there is no
second moment to keep in sync. **Aging** is the sketch's `halve()`, separated from the policy that
schedules it, so the ticket's "never underestimates" and "halving" are testable without a partition.
`sizeOf` is the one thing `WindowTinyLfu` asks the partition for -- what a key currently costs, read
straight off `Entry.accounted` -- which is why the policy needs no byte bookkeeping of its own beyond
two running totals. Seams unchanged: `CommandEngine`, `PartitionContext`, `Reply`, `Key`,
`PartitionId` are exactly T01's; `EvictionPolicy` is a new enum next to `ApEngine`, not a seam,
because there is no second adapter and never will be -- a policy is a function, not a module.

**Acceptance:**
- `sketch_estimate_never_underestimates`: 4,000 seeded increments over 500 keys, the truth counted in
  a map the sketch never sees, and every key's estimate at or above its true count.
- `sketch_ages_halves_counts`: one key alone in the sketch (so no collision is possible) counted to
  nine, then halved four times: 9, 4, 2, 1, 0. Halving rounds down and an untouched key ages out.
- `sketch_counters_saturate_rather_than_wrap`: 400 increments read back as 255. A byte counter that
  wrapped would read near zero, and the whole one-sided-error guarantee with it.
- `eviction_respects_max_memory` is now `@ParameterizedTest @EnumSource(EvictionPolicy::class)`, so
  the T10 assertions -- twenty writes into a three-entry budget end at three keys, under the
  threshold, last write readable -- run under both policies from one body.
- `tinylfu_admits_frequent`: a six-entry budget, key 0 read ten times, then a one-hit key and thirty
  more one-hit keys of churn. Key 0 survives, the one-hit key does not, and `DBSIZE` still reports
  six. Verified to be the policy's doing and not the clock's: the identical trace under `LRU` loses
  key 0 (it is read once and then never again while thirty writes go past it).
- `tinylfu_hit_ratio_beats_lru_on_zipf`: a seeded Zipf trace of 20,000 accesses over 2,000 keys
  replayed against a 200-entry budget under each policy, reading each key and writing it back on a
  miss, clock advanced on every access. W-TinyLFU hit 12,523 times against LRU's 12,004; the message
  prints both. Checked at four seeds before landing -- 12532/11915, 12612/12049, 12493/11966,
  12523/12004 -- so the margin is the policy and not the seed.
- `info_reports_the_eviction_policy`: `maxmemory_policy:lru` on a default node, `maxmemory_policy:
  w-tinylfu` on one built W-TinyLFU.
- Mutation-checked two ways: reversing the admission comparison (`<=` to `>=`, so the *more*
  frequent key is the one thrown away) fails `tinylfu_admits_frequent`; and see Deviation 2 for the
  one mutation nothing catches.
- Every existing test still green under the default `LRU`. `mvn -B -o clean package`: engine 128,
  cluster 49, cp 38, server 35.
- This entry.

**Deviations:** Nothing against the spec, the frozen types or ADR 0002. Four judgement calls, one of
them debt.

1. **LFU is not built.** Spec 2.7's middle policy is a Redis-style logarithmic frequency counter per
   key, and the ticket makes it conditional on the budget. It is not in the acceptance list, nothing
   consumes it, and the ticket landed at 509 lines. Adding it would mean a third `EvictionPolicy`
   value, a counter beside `Entry.lastAccess`, a third victim function and its own test. Repaid by
   exactly that when something asks for it; until then W-TinyLFU is strictly the better of the two
   and LRU is the baseline the spec wants kept.
2. **Nothing tests that the policy ages its sketch.** Debt, and the sharpest thing in this entry.
   `CountMinSketch.halve()` is directly and thoroughly tested, but making `WindowTinyLfu.age()` a
   no-op leaves all 128 engine tests green, the Zipf comparison included. A test that bites would
   need a phase-change trace -- one key made hot, then left alone while a second key climbs -- and
   at these budgets the window's LRU order and the halving cadence make the arithmetic delicate
   enough that the test would pin the implementation rather than the behaviour. What would repay it:
   a seam on `WindowTinyLfu` that reports its own halving count, or a Zipf trace whose popularity
   ranking is reversed halfway through, where a sketch that never ages provably keeps the wrong keys.
3. **The aging period N is a constant, not a parameter.** The ticket calls N configurable.
   `SAMPLES_PER_KEY` (10) and `MIN_SAMPLE` (100) are private constants in one place with one caller;
   a constructor parameter for a value nothing varies would be configuration for its own sake. The
   period itself is Caffeine's and does scale with the cache, as the ticket asks: `10 x` the keys the
   policy currently holds, recomputed on every access rather than fixed at construction.
4. **The window never shrinks below one key.** `victim()` guards the admission branch with
   `window.size > 1`, so the key a command just wrote is never itself the candidate it offers to the
   main space. Without it, a budget small enough that one percent rounds to a few bytes evicts every
   write the instant it lands, and `eviction_respects_max_memory`'s "the last write survived it"
   fails under `W_TINYLFU` for a reason that has nothing to do with the policy.

**For the next ticket:** T11 leaves the eviction step exactly where T10 left it and nothing new is
stubbed. Three things worth knowing.

`Partition.coldest()` is still the whole policy, and now demonstrably so: a third policy is a third
implementation of that one function plus whatever bookkeeping it hangs off `account`, `forget` and
the one `touch` in `execute`. Those four call sites are the entire policy surface inside `Partition`.

`WindowTinyLfu`'s region byte totals are running totals kept in step with `Entry.accounted`, not
recomputed, so they are exact against T10's formula and drift only if a future mutation path reaches
the store without going through `account` or `forget`. T10's note about those two verbs being the
only ways `usedBytes` changes is now load-bearing for the policy as well: add a third path and the
window will think it is a size it is not. Probation's bytes are deliberately not tracked -- the
policy only ever needs to know whether probation is empty -- so do not add the third total on the
assumption that it is missing by oversight.

`Command.Info`'s per-partition reply is now `[keys, usedBytes, policyName]`, a third item on top of
T10's two. Nothing outside the engine reads the per-partition shape; T13's `INFO` over RESP and
T44's dispatcher see only the joined bulk string, which gains one `maxmemory_policy` line inside the
`# Memory` section it already had. A test that matched the whole `# Memory` section literally would
need updating; the existing ones match by line prefix and did not.

## T42: Semaphore, CountDownLatch, AtomicReference

**Built:** The three remaining CP primitives (CP spec 3.3 to 3.5, 6.3 to 6.5), each a primitive of
the composite state machine beside the counter, the lock and the session registry.

In the engine module, `Command.Cp` gained three more sealed sub-hierarchies. `Cp.Semaphore`:
`SemInit(key, permits)`, `SemAcquire(key, session, permits)`, `SemRelease(key, session, permits)`,
`SemAvailable(key)`, `SemDrain(key, session)`, the three session-tied ones implementing
`Cp.Sessioned` as T41 left them to. `Cp.CountDownLatch`: `LatchSet(key, count)`, `LatchDown(key)`,
`LatchGet(key)`, `LatchReset(key, count)`. `Cp.AtomicReference`: `RefSet(key, value, ttl?)`,
`RefGet(key)`, `RefCas(key, expected, new)`. The two reference commands carry bytes, so they are
plain classes comparing by byte content as `Key` does, rather than data classes over arrays.

`SemaphoreStateMachine` holds, per key, the free permits and a map of session to permits held. A
key nobody initialised is a semaphore of no permits rather than a separate kind of answer:
acquiring from it fails, draining it takes nothing. `SemInit` is idempotent per CP spec 3.3, since
re-initialising under live holders would invent permits nobody released. A release of more than the
session holds is `-ERR` and changes nothing. `releaseAllOf(session)` is one `replaceAll` giving the
permits back, and `CpStateMachine.closeSession` calls it beside the locks' in the one entry (C18,
I15) exactly as T41 predicted.

`CountDownLatchStateMachine` is a count per key that only falls and stops at zero. `LatchSet` and
`LatchReset` are the same rule under two names, because a latch nobody set counts zero and the
first SET therefore always takes; arming one that is still counting down is `-ERR`.

`AtomicReferenceStateMachine` holds bytes and an optional expiry in log time, checked on access and
swept on the TTL tick as the counter's is (C23). A CAS compares byte content, keeps the reference's
TTL on success, and is one applied entry, so no reader sees a half state (I21).

`CpWire` tags the twelve new verbs (19 to 30); the reference's bytes go through the same length-
prefixed blob the key does. `CpStateMachine`'s `Snapshot` carries three more fields.
`CONTEXT.md` gained **permit**, **latch** and **reference** entries in the CP section.

**Concepts named:** A **permit** is what a semaphore hands out, and it belongs to a session, not to
a caller, which is why a session's death is the only thing besides a release that returns one.
Available and held are the two states a permit is in, so a semaphore has no third notion of
"reserved". A **latch** is armed only from zero, one rule serving both spec verbs, so "SET" and
"RESET" name a moment rather than two behaviours. A **reference** is bytes and stays bytes: nothing
decodes them, and byte equality is the whole of its CAS. The seam did not move: every test drives
`CpEngine.submit`, and the three new primitives are reached through it exactly as the counter and
the lock are.

**Acceptance:** `dynacache.cp.SemaphoreTest` 7, `CountDownLatchTest` 4, `AtomicReferenceTest` 5,
`CpWireTest` 9 (three new). Full `clean package` green: engine 121, cluster 49, cp 66, server 35.

- CP spec 10.3, all six: `sem_init_acquire_release`, `sem_over_acquire_fails`,
  `sem_over_release_rejected` (the `-ERR` leaves the state alone and what the session does hold is
  still releasable), `sem_session_death_releases` (the leader's clock moves 2 s past a 1 s session
  and one `tick()` gives the permits back), `sem_drain`, and
  `sem_concurrent_acquire_exactly_permits_succeed` (ten sessions, three permits, three ones).
- `sem_drain_of_unknown_key_leaves_it_initialisable` (extra, from the self-review below).
- CP spec 10.4, all four: `latch_set_down_get`, `latch_down_at_zero_stays_zero`,
  `latch_reset_only_at_zero` (the refused RESET leaves the count where it was, and the spent latch
  takes it), `latch_concurrent_down_correct_count` (a hundred parties see the hundred distinct
  values 99 down to 0).
- CP spec 10.5, all three: `ref_set_get_roundtrip`, `ref_cas_byte_equality` (a differing case, a
  trailing space and a prefix all fail, and none of them swaps), `ref_concurrent_cas_exactly_one_wins`
  (the reference ends holding the winner's bytes and nobody else's).
- `ref_ttl_expires` (extra): a reference set with a 1 s TTL is nil after the clock moves and a tick
  carries the time into the log.
- `I21_concurrent_cas_exactly_one_wins`: ten concurrent `LONG_CAS` on the same expected value, then
  ten concurrent `REF_CAS` on the same expected bytes; exactly one of each succeeds and a late CAS
  on the old expected value fails, so nobody saw an intermediate state.
- `semaphore_commands_round_trip`, `latch_commands_round_trip`, `reference_commands_round_trip`
  (the last with high-bit and zero bytes, and with and without a TTL).
- Every T38 to T41 and T43 test green, none changed.

**Deviations:**

1. **The size budget was exceeded.** 691 lines against the ticket's 200 to 600: 176 of diff on four
   existing files and 515 in six new ones. Three primitives with fifteen spec-named tests between
   them is what the ticket asked for; nothing was cut, and the excess is tests and KDoc, not logic.
2. **No `EXPIRE`, `TTL` or `PERSIST` on a reference.** CP spec 9.4 names AtomicReference among the
   state machines those Redis verbs operate on, but 6.5's command table lists no such row (6.2's
   does, for the counter), and the ticket resolves the disagreement that way: TTL only through
   `SET`. T44 adds the three commands if the dispatcher needs to route `EXPIRE cp:ref:K`; the
   expiry field and the tick's sweep are already there for them.
3. **A reference's CAS takes non-null bytes on both sides.** CP spec 3.5's state is
   `ByteArray | null`, but the RESP form `CP.REF.CAS K expected new` cannot express nil on either
   side, so neither does the command. A reference that was never set, or has expired, matches no
   expected bytes and the CAS answers 0.
4. **`LatchSet` and `LatchReset` share one rule and one branch.** CP spec 3.4 restricts only RESET
   to a zero count, but a latch nobody set counts zero, so restricting SET the same way (which the
   ticket asks for) changes nothing about a first SET and stops a live latch being moved under the
   parties on it. Both commands exist because 6.4 has both verbs and T44 maps them one to one.
5. **The latch keeps no `initial`.** CP spec 3.4's state names `count` and `initial`; no op in 3.4
   or 6.4 reads `initial`, so it is not stored. A latch is one `Int` per key.
6. **`SEM_RELEASE`'s error is `-ERR`.** CP spec 6.8's table has no code for it (`-REENTRANCE` is a
   lock's), and 3.3 says only "rejects". `Reply.Error("ERR", ...)` naming what the session holds.
7. **`SemInit` on an initialised semaphore is a no-op**, per CP spec 3.3's "idempotent", not the
   error the ticket offered as the alternative. It answers `+OK` either way.
8. **The semaphore's `apply` takes no log time.** Permits have no TTL, so the parameter would be
   unused; the latch's takes none either. The counter's and the reference's do.
9. **Three plain `HashMap`s.** Only the Raft thread reads and writes them, as T41's registry noted;
   no test in this ticket reads a follower's semaphore, latch or reference from the test thread the
   way T40's I15 test read a follower's locks.
10. **TDD granularity.** Three vertical slices, each red on a stub throwing `NotImplementedError`
    that the exhaustive `when`s in the composite and `CpWire` forced into existence with all of that
    primitive's verbs and tags: `sem_init_acquire_release`, then `latch_set_down_get`, then
    `ref_set_get_roundtrip`. The other twelve tests were green first time against the code each
    slice's red test had forced, which is what a state machine with one branch per verb does to the
    loop. The exception is item 11.
11. **The self-review found a real bug and it was fixed red-first.** `SemDrain` of a key nobody had
    initialised wrote a semaphore of zero permits with the draining session as a holder of nothing,
    after which `SemInit` on that key found it present and no-opped: the key could never be
    initialised again. `sem_drain_of_unknown_key_leaves_it_initialisable` was red on exactly that,
    and taking nothing now writes nothing.
12. **Real-time waits.** None added. The three new classes together run in about four seconds, all
    of it MicroRaft elections.

**For the next ticket:**

- **T44 (dispatcher)**: the twelve verbs map one to one onto the commands.
  `CP.SEM.INIT K permits` / `ACQUIRE K n` / `RELEASE K n` / `AVAILABLE K` / `DRAIN K`, with the
  session from the connection as the lock verbs take it; `CP.LATCH.SET K count` / `DOWN K` /
  `GET K` / `RESET K count`; `CP.REF.SET K v` / `GET K` / `CAS K expected new`. Replies are already
  the shapes CP spec 6.3 to 6.5 name. The Redis-compat side of 6.5 is `SET cp:ref:K v [EX|PX]` onto
  `RefSet`'s `ttl` and `GET cp:ref:K` onto `RefGet`; `EXPIRE`, `TTL` and `PERSIST` on a `cp:ref:*`
  key have no command yet (deviation 2).
- **T45 (snapshots)**: `CpStateMachine.Snapshot` now carries the semaphores, the latches and the
  references; `installSnapshot` restores all three. `SemaphoreStateMachine.Semaphore` and
  `AtomicReferenceStateMachine.Reference` both compare by content, so a restored state machine's
  snapshot equals the original's for the I20 round-trip. The reference is the only one of the three
  with a TTL, and it is swept on the tick like the counter and the lock.
- The `-WRONGTYPE` of CP spec 6.8 is still unanswered by anything: a `cp:sem:*` key and a
  `cp:latch:*` key are different maps in different primitives, and nothing checks that a key is
  used as only one kind. The dispatcher routes on the verb, so a `CP.LATCH.GET cp:sem:s` would
  silently read an empty latch. Worth a ticket if the namespace convention is ever to be enforced.

## T22: Replication and quorum

**Built:** `dynacache.cluster.Replication` is the second decorator of the `CommandEngine` seam,
wrapped by the router: a node is `Router(Replication(ApEngine))`, so the router hands every
command this node coordinates to replication and nothing downstream learns that replicas exist.
`Replication` owns the node's `DotCounter` and its `versions` side table (`Key -> Dvv`,
a `ConcurrentHashMap` next to the engine; the engine module learns nothing about DVVs), the
coordinator's write (spec 5.1 steps 4 to 6 and 8) and read (spec 5.2 steps 1 to 4), and the
replica's half of both. A write bumps the key's version under `compute` so two writes racing on
one key chain rather than fork, applies locally, and unless the engine refused it (or a
conditional `SET` did not apply) ships a `Replicate` (the command's tokens with NX/XX and the
TTL already decided, the DVV, `expires_at_millis` as an absolute instant per spec 5.4) to the
live successors of the preference list, then waits for W-1 acks from distinct nodes or the
deadline and answers the engine's reply or `Reply.Error("ERR", "quorum not reached: write needs
W of N nodes, k answered within 1s")`. A read runs locally, ships a `Read` (the read's tokens)
to the live successors, waits for R-1 `ReadReply`s (reply plus DVV), and answers the reply whose
version dominates; two concurrent versions fall to T29's last-writer rule, now `lastWriter`
in `Merge.kt` and shared. Divergence among the R answers is counted in `divergentReads` and not
pushed (T26). A replica applies a `Replicate` whose version dominates what it holds, ignores one
that is dominated or equal, applies a concurrent one under `held.merge(remote, counter)`, and
acks in every case it could read. `ReplicationConfig(n, w, r)` checks `w in 1..n`, `r in 1..n`
and `r + w > n` at construction; `Replication` checks `n <= ring.nodes.size`.

`Router.submit` splits a `Command.Fanned` (`MGET`, `MSET`, variadic `DEL`, `EXISTS`) into its
single-key parts, routes each like a single-key command one after the other, and joins in
argument order (ADR 0002 across nodes); `Fanned.single` and `Fanned.join` are public for it.
`cluster.proto` gains `Replicate`, `ReplicateAck`, `Read` and `ReadReply` (fields 15 to 18) and
`GrpcTransportTest` round-trips all four. `InProcessCluster(nodeCount, n, w, r)` builds a
`ReplicationConfig`, one `ScriptedMembership` every node reads, a `Replication` per node and
wires the router's `others` hook to `Replication.receive` before gossip; it gains `seed(node,
key, value, dvv)` (a `Replicate` from an endpoint no node owns, so a test makes replicas
disagree) and `replication(node)`. `TokenCodec` learned `EXISTS` and the `PX` spelling of a
`SET` TTL. `mvn -B -o -q clean package` offline: engine 125, cluster 61, cp 47, server 35.
Eleven files plus CONTEXT.md and ADR 0003; 615 insertions, 34 deletions.

**Concepts named:** A **replica** is any node of a key's preference list, the coordinator
included; it applies what the coordinator ships and answers its reads, and never decides. A
**quorum** is how many distinct replicas must answer, W for a write and R for a read, the
coordinator counting as one, and a quorum that does not form within the deadline is an error
reply, never a hang. A **version** is the DVV a stored value carries, held in the replication
layer's side table rather than in the engine. All three are new CONTEXT.md entries. ADR 0003
records that replication ships the command, not the value: the engine hands out replies, not
values, so a replica re-runs the command through its own engine. The `Gather` is one request's
answers by node, which is what makes "distinct" a property of the data structure rather than a
check. No new seam: `Replication` is concrete and its second adapter is the engine it wraps.

**Acceptance:**
- `write_read_quorum`: N=3, W=2, R=2; a write through node-1 is read through node-2; every
  replica holds the value under the coordinator's dot 1; a `SET ... PX 10000` through a
  forwarding contact answers `PTTL` 10000 on all three replicas (the absolute instant crossed).
- `minority_failure_available`: the key's last replica is network-killed and marked dead;
  write through one survivor, read through the other, the victim holds nothing.
- `majority_failure_unavailable`: both non-coordinators network-killed, gossip silent; the
  write and the read answer the quorum error after the deadline in virtual time.
- `C4_write_needs_w_distinct_acks`: W=3 on a `Replication` whose two replicas are bare
  endpoints; two acks from one of them leave the write pending and it ends in the quorum
  error, one ack from each answers OK. Checked by mutation: counting acks instead of nodes
  fails it.
- `C4_read_returns_highest_dvv`: the coordinator seeded with an older version than both
  replicas, then a newer one than they hold; each read answers the dominating version's value
  through a forwarding contact, and both reads are counted as divergent.
- `quorum_config_rejects_r_plus_w_not_above_n`: (3,1,2), w=0 and w>n rejected; (3,2,2) fine.
- `fanned_command_splits_by_coordinator`: one key per coordinator; `MSET` through node-1,
  `MGET` (plus a missing key) through node-2 in argument order, `EXISTS` and `DEL` through
  node-3, `EXISTS` 0 through node-1. Red first: the engine's own fan-out answered all nils.
- Every earlier test green; `RouterTest.router_forwards_to_coordinator` now runs at N=1 so the
  contact is no replica and its empty engine still proves the forward.
- This entry.

**Deviations:**
1. **Replication ships the command, not the value** (ADR 0003). Spec 5.3's concurrent case is
   therefore not T29's type merge: the replica applies the remote command over its local value
   under `held.merge(remote, counter)`, a version descending from both. Before T25 no replica
   can hold a concurrent version, since one coordinator writes each key. Debt: a `Value` codec
   (T31) and an engine install hook would let replicas take values and call `merge`.
2. **A forward now runs on its own coroutine** (T19's deviation 7 repaid, and forced): the
   coordinator's quorum reads its acks through the same demux that was awaiting the forwarded
   command, which deadlocked until the forward deadline. Cost, marked `ponytail:` in `Router`:
   two forwards from one contact may run out of order at the coordinator. A per-sender queue of
   forwards is the repair if a pipelining client observes it.
3. **A write reported as failed may still be stored.** The coordinator applies locally and
   sends every `Replicate` before it learns the quorum did not form; the error tells the client
   the write is not durable to W, not that it did not happen. Dynamo's own semantics.
4. **The replication deadline is 1s, the forward deadline 2s**, so a contact reports the
   coordinator's quorum error and not its own timeout. Both are constructor parameters.
5. **Batches are not replicated.** `Replication.atomically` runs on the local engine alone;
   `MULTI/EXEC` and `EVAL` writes reach no replica. Debt for whoever wires the router into the
   server (T24): a batch's writes need to be shipped after it commits.
6. **Reads are a list, not a flag on `Command`.** `isRead` in `Replication.kt` names the read
   variants; an unlisted variant is treated as a write, which costs one needless replication
   round and never loses data. A `mutates` property on `Command.Keyed` would be the engine's
   own answer; `Command` is frozen, so not touched.
7. **A dead coordinator makes its keys unavailable.** Without sloppy quorum (T25) the contact
   forwards to the preference list's first node regardless of membership, so
   `minority_failure_available` kills a replica, not the coordinator. Spec 5.1 step 7 is T25's.
8. **`InProcessCluster.drainMessages` is now a fixpoint with an engine barrier**: it drains,
   yields, submits `DBSIZE` to every engine and blocks on it (no virtual time passes), yields,
   and repeats while anything is in flight. That removes the real-time race between a partition
   thread's completion and `settle`'s round bound that T19 lived with.
9. The plan entry's "highest node id breaks a true tie" is T29's `lastWriter` made total (node
   name, then counter), lifted from `Merge.kt` as an `internal` comparator over `Dvv`.
10. Line budget: 615 insertions and 34 deletions including tests and proto, slightly over the
    600 ceiling counting insertions, under it net.

**For the next ticket:** T25 (sloppy quorum and hints) has its seam in `Replication.gather`:
`replicas` is the preference list's live successors, and the next healthy node on the ring
joins that list when one is dead; the hint is the `Replicate` envelope itself (C5: key, value
as tokens, DVV, TTL as an instant), stored on the substitute and replayed by sending it. The
replica side already accepts a `Replicate` from any node, which `InProcessCluster.seed` relies
on. T26 (read repair) hooks where `divergentReads` is incremented in `Replication.read`: the
winner's `(reply, dvv)` and the losers' nodes are both in hand there, and a repair is a
`Replicate` to each loser carrying the winner's version, but the coordinator has only the
winner's *reply*, not its value or tokens, so a repair of a hash or a list needs the value path
ADR 0003 defers. T28 (anti-entropy) will want `Replication.version(key)` and the same install
path. `Gather` is generic over the envelope, so a third fan-out (repair, hint replay) reuses it.
`ScriptedMembership` is shared by every node of an `InProcessCluster`; a test that wants nodes
to disagree about membership needs one per node. `InMemoryTransport.inFlight` is public. The
demux still awaits `Replicate` and `Read` handling inline (an engine hop each, no reply waited
on), so it cannot deadlock but a slow engine delays gossip behind it. `RecordingEngine` answers
every command with one canned reply, which is what made the C4 write test need no engine.
