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
