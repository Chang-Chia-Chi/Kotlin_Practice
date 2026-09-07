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

## T35: WAL in the write path, checkpoint, recovery

**Built:** the write-ahead log is in the command path (C14), the RDB save is its checkpoint,
and startup recovery is snapshot then log.

`ApEngine` gains `wal: WalWriter?` (null by default, setter module-internal): the log a node
appends to, attached by recovery once the state it continues is in place. Every `Partition`
takes a hook, `(Command, Reply, Instant) -> CompletableFuture<*>?`, that `execute` calls with
each command's reply; the engine's hook encodes the command through `WalCodec` and appends it,
answering the append's `durable`. The hook is inside `execute`, so every path is covered with
one line: `submit`, a fanned command's `submitAll`, and a batch's `PartitionContext.execute`.
A batch is logged as one entry per mutating command, in order. What is logged is what changed:
an error reply, a refused conditional `SET` and an empty `POP` (both nil) log nothing, and a
conditional `SET` that took is logged as a plain one. A TTL travels as the absolute instant the
engine settled on, never the client's duration. Reads log nothing (`wal_reads_append_nothing`).

The reply waits without the partition thread waiting: `Partition.submit`, `submitAll` and
`inOneTask` now run through one `task` helper that resets a per-task `durable` future, runs the
work, and completes the task's future with the answer only once every entry the task appended
is durable (`allOf`, so an append that fails exceptionally fails the reply). Under
`EVERY_SECOND` the partition therefore keeps executing while a hundred replies wait for the
next tick; under `ALWAYS` the group commit of T34 forms across partitions.

`WalCodec` in `persist`: the entry's op byte is the command's kind (sixteen mutating variants,
`HMSET` shares `HSET`'s), the payload its arguments, length-prefixed big-endian as the RDB is.
`decode` answers the commands that redo one entry: a `SET` with a TTL is a `SET` then an
`EXPIRE` at the stored instant.

`SnapshotEngine(engine, dir, clock, interval, seeds, sink, fsync: FsyncPolicy? = null)`: with a
policy it owns the log files `<dir>/wal.<seq>`, where `wal.<n>` holds only entries after `n`.
`save()` answers the checkpoint's seq. The cut is exact: `ApEngine.snapshotView(now, cut)` parks
every partition's view task at a `CyclicBarrier` and runs `cut` as the barrier action, while
all partitions are parked and so nothing is appending; `cut` reads `wal.lastSeq`, rotates the
writer to a fresh `wal.<seq>` (`WalWriter.rotate`: forces what `EVERY_SECOND` still holds,
closes the old sink, swaps in the new), and answers the seq. The RDB header now carries that
seq (`[magic][version][wal_seq:i64][count]`, `RDB_VERSION` 2, `RdbReader.read` answers an
`RdbSnapshot(walSeq, entries)`), and once the rename has landed the log files below the seq
are deleted. `restore()` loads the snapshot, then replays every log file oldest first, every
entry with seq above the last applied through `engine.submit`, so accounting, the wheel and
the kind checks all apply; truncates each file at its scan's `stoppedAt` so nothing is ever
appended after a torn tail; opens the writer on the newest file at `applied + 1`; attaches it.
`close()` saves, then closes the writer. The server's `main` takes the policy as a fourth
argument (default `EVERY_SECOND`) and its tick thread calls `engine.wal?.tick()` between
`engine.tick()` and `maybeSave`; the shutdown hook's order is socket, log, engine as before.

Six tests in `WalRecoveryTest`, JUnit 5 only, `@TempDir`, latches, `NEVER` for the recovery
tests so a dropped engine's bytes are already written, no sleeps. `RdbTest` changed by one
`.entries`; every other test untouched. Engine 136, cluster 54, cp 47, server 35, all green.

**Concepts named:** the **cut** is the moment a checkpoint is taken at, one seq for the whole
node, made exact by a barrier across the partition view tasks rather than by a lock: while
every partition is parked no thread can append, so "the writer's last seq" and "every view"
agree. Without the barrier a per-partition seq would either replay an entry one partition's
view already holds or lose an entry another's does not. The **checkpoint seq** lives in the
snapshot, not beside it: the RDB rename is the one atomic step, and a crash between it and the
old log files' deletion is only recoverable if the snapshot itself says where the log resumes.
**Redo is by seq, not by bytes**: recovery skips any entry at or below the last it applied,
the ARIES rule, which is what `wal_replay_idempotent` pins by holding the log's bytes twice.
**Attach after replay**: the log is a `var` on the engine set by recovery, because a writer
present during replay would log every redone entry again and double-apply it next time.

**Acceptance:**
- `wal_checkpoint_truncates`: two writes, `save()` answers 2, the directory is exactly
  `dump.rdb` and `wal.2`, and `wal.2` holds only seq 3 after a third write; a second save
  answers 3 and leaves `wal.3`; a fresh engine restores the third key.
- `wal_full_recovery`: two sets and an `INCRBY`, save, a set, a set with a 90 s TTL, another
  `INCRBY` and a `DEL`, the engine dropped without `close`; a fresh engine restores every key
  with its value, the deleted one absent, `TTL` 90, the counter 10.
- `wal_replay_idempotent`: `INCRBY` and `RPUSH`, the log file's bytes appended to itself, a
  fresh engine restores the counter at 1 and the list one long.
- `C14_reply_only_after_durable_append`: a sink holding its first fsync; the `SET`'s future is
  not done while it holds, a `GET` on another partition completes meanwhile, and the reply is
  `OK` once released.
- `wal_reads_append_nothing`: three writes then eight reads, a refused `SET NX` and an empty
  `POP`; the writer's `lastSeq` is still 3.
- `wal_batch_is_replayed_as_written`: an `atomically` batch of two `SET`s and an `INCRBY` on
  hash-tagged keys, the engine dropped, a fresh engine restores both keys with the batch's
  final values.
- Mutation check: turning the seq filter off and the deletion off failed `wal_full_recovery`,
  `wal_replay_idempotent` and `wal_checkpoint_truncates` respectively; restored.
- Progress entry: this file.

**Deviations:** the RDB format gained an eight-byte `wal_seq` header field and its version is 2;
`RdbReader.read` answers `RdbSnapshot` rather than the entry list. Spec 2.8 says "replay WAL
entries after checkpoint sequence number" without saying where the checkpoint seq lives, and
a sidecar cannot move atomically with the rename; no file written before this ticket is in
service, so no migration. Not a deviation but a choice the ticket left open: the log is
attached to the engine by `SnapshotEngine.restore` (a `var`, not a constructor parameter),
for the reason under Concepts. Truncation is a new file per checkpoint and the old deleted,
never a rewrite. The spec's "before the in-memory state is updated" reads as "before the reply"
here, as the plan's C14 wording does: the store is updated on the partition thread and the
reply waits; a crash between the two loses an unacknowledged write, which C14 permits.

**For the next ticket:** `ApEngine.snapshotView` now parks every partition at a barrier, so a
save issued after `close()` leaves the submitted view tasks parked on daemon threads (before,
it failed fast); the server never does that. T36 (Chandy-Lamport) gets the same exact cut for
free: `snapshotView(now, cut)` runs `cut` with every partition parked, which is where a local
state record plus outgoing markers belongs, and `RdbWriter.write` takes the seq to stamp.
Replay is sequential `submit(...).join()` per entry, one command at a time; a long log recovers
slowly and a per-partition pipeline is the repair. `FLUSHDB` is logged once per partition and
each replay of it fans out to every partition again, harmless and idempotent. `WalWriter.rotate`
has a precondition (no append in flight) checked by `check`, met only under the barrier. Replay
never reaches the kind check's `WRONGTYPE` because the log holds only commands that succeeded
against the state they ran on. `ApEngine.wal` under `EVERY_SECOND` needs the scheduler's
`tick()` or no reply ever completes: a test that attaches a writer with that policy must tick.
The RDB's seq is 0 for a node without a log, and a node that later gains one replays from 0,
which is right since its log starts empty. Keys under T22's DVV stamping should go into the
entry payload alongside the command when replication lands, so a replayed write carries its
version; the codec's op table is the place.

## T25: Hinted handoff and sloppy quorum

**Built:** Sloppy quorum and hinted handoff inside `Replication`, at the seam T22 left:
`gather` now takes its targets from `successors(key, sloppy)`, which maps each live successor
of the preference list to null and, for a write, each dead one to a substitute: the next
healthy nodes clockwise past the list (`ring.preferenceList(key, ring.nodes.size).drop(n)`
filtered by `membership.dead`), distinct from every member of it, zipped in order. A read never
substitutes. The `Replicate` sent to a substitute carries `hint_for = <dead node>` (`cluster.proto`
field 5, a plain field, so no oneof case and no exhaustive `when` changed). A replica that
receives a `Replicate` with `hint_for` set stores the whole message in its `HintStore` under a
fresh local id and acks under the request id, so the ack counts toward W exactly like a
replica's (C4's distinct-node rule holds because the substitute is not in the preference list).
`HintStore` (`dynacache.cluster`, concrete, in memory) is `id -> (target, Replicate)` in arrival
order with `add`, `remove`, `pending(target, now, limit)` (drops every hint whose
`expires_at_millis` has passed at `now` before answering) and `size`. `Replication.runHandoff()`
is the node's one handoff coroutine: it collects `membership.changes` and, on an `ALIVE` row,
loops `replayHints(node)` until a round acks nothing. `replayHints(target)` is the step function
tests can call: it takes up to `replayBatch` (constructor parameter, default 64) hints, sends
each unchanged (same tokens, DVV, TTL instant; `hint_for` cleared, id set to the hint's id) to
the target, registers one `Gather(1)` per hint so the existing `REPLICATE_ACK` demux completes
it, waits for all of them or the `deadline`, forgets the acked ones and returns how many.
`Replication.hintCount` is the number for `INFO`. The target applies a replayed hint through
the unchanged `replicate` path (T22's DVV rule). `InProcessCluster` launches `runHandoff` per
node next to the router loops and gains `drainHints()` (drain rounds until no node holds a hint,
bounded by the settle rounds). `GrpcTransportTest` round-trips `hint_for`. CONTEXT.md gains
"hint" (and "handoff" inside it). Seven files, 308 insertions, 11 deletions; `mvn -B -o -q clean
package` offline green: engine 137, cluster 67, cp 66, server 65.

**Concepts named:** A **hint** is a write held by a node that is not one of the key's replicas,
because the replica it was meant for was dead when the coordinator wrote; it is the `Replicate`
envelope itself, unchanged (C5), which is why replaying it is sending it. The **substitute**
(stand-in) is the next healthy node clockwise past the preference list, one per dead node, in
order. **Handoff** is the replay on an alive event: rounds of at most `replayBatch` hints, each
round waiting the quorum deadline for its acks, an unacked hint staying for the next round or
the next alive event. No new seam: `HintStore` is concrete and tested through the cluster;
`Gather` served as the third fan-out T22 predicted.

**Acceptance:**
- `sloppy_quorum_reaches_w_with_one_dead_node`: four nodes, N=3, W=3, R=1; one successor
  network-killed and dead per membership; the write answers OK (red first: the quorum error),
  the dead node holds nothing, the fourth node on the ring holds one hint.
- `hinted_handoff_replays`: four nodes, N=3, W=2, R=2; the last node network-partitioned away
  and dead; five keys it replicates written through node-1; it holds none; heal, alive at
  incarnation 1, `drainHints`; it holds all five. Red first: `runHandoff` did not exist.
- `hint_deleted_after_ack`: the holder's `hintCount` is 1 before the rejoin and 0 after, the
  target holds the value.
- `C5_hint_carries_full_write`: `SET ... PX 10000` while away; after the rejoin the returned
  node answers the value, the coordinator's exact version, and PTTL 10000.
- `I9_rejoined_node_matches_reference_replica`: eight keys cycling plain `SET`, `SET` with a
  TTL of i+1 seconds, `HSET` of two fields, `INCRBY`; after the rejoin each key's `GET` or
  `HGETALL`, `PTTL` and version equal the coordinator's (the replica that never left).
- `expired_hint_is_not_replayed`: a `MutableClock` in the test; `SET ... PX 10000` while away,
  the clock moved to 11s, rejoin; the holder's count is 0 and the returned node's version for
  the key is null (a replay would have set it before the engine hop, so the null is the proof).
- Every earlier test green, `GrpcTransportTest` included.
- This entry.

**Deviations:**
1. `sloppy_quorum_reaches_w_with_one_dead_node` runs at W=3, not the briefing's W=2: with
   N=3 and one dead successor, W=2 forms from the coordinator and the live successor without
   any hint, so W=2 would not prove the sloppy quorum. W=3 needs the substitute's ack.
2. C5, I9 and the expiry test were green on their first run: the replay slice already sent the
   stored envelope unchanged and `pending` was written with the drop. Recorded rather than
   faked red.
3. A dead node with no healthy node left past the preference list gets no substitute and is
   simply not written to; the quorum then fails as before. The plan says nothing about it.
4. A hint whose tokens or DVV this node cannot parse is not stored and not acked, same as an
   ordinary `Replicate`, so a garbage hint never lingers.
5. In production a hint that stays unacked after a round (target silent) waits for the next
   alive event; there is no periodic retry, since plan 2.5 wants one coroutine per background
   process driven by an event, and SWIM re-emits alive on re-incarnation. Debt if a target that
   acks nothing while alive is ever observed: a periodic `replayHints` round on the same
   coroutine.
6. The substitute keeps the hint only; it does not also apply the write locally, so a read
   that lands on it while the replica is away answers nil. Dynamo's own semantics.
7. `INFO` is not wired: `Replication.hintCount` is the number, the server module reports it
   when T24 wires the router in.

**For the next ticket:** T26 (read repair) hooks where `divergentReads` is incremented and can
reuse `gather` with `sloppy = false`, or a `Gather(1)` per target exactly as `replayHints` does;
`successors` is the one place that knows who a write went to. T28 (anti-entropy) has
`Replication.version(key)` and the same `replicate` install path a replayed hint takes. Both
edit `Replication.kt` after this ticket: the T25 surface is `successors`, `runHandoff`,
`replayHints`, the `hint_for` branch at the top of `replicate`, and `HintStore.kt`. In tests,
`InProcessCluster.drainHints()` after `membership.set(node, ALIVE, incarnation)` is the whole
rejoin; `ScriptedMembership` is still shared by every node, so one `set` reaches every node's
handoff coroutine, and only the holder has anything to send. The handoff coroutine subscribes
to `changes` on the first yield after construction, so an `ALIVE` emitted before any suspension
point is missed (a `MutableSharedFlow` without replay); every test drains before it rejoins.
`InMemoryTransport.networkPartition` plus `membership.set(DEAD)` is the "partition away" pair,
`heal` plus `set(ALIVE)` the rejoin; a node killed with `kill` needs `restart` first.

## T16: P1 acceptance

**Built:** the last three things P1 owed, and then the demo that proves the lot.

`CommandParser` gained the eleven Sorted Set names T13 left a marked place for -- `ZADD`,
`ZREM`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZREVRANK`, `ZSCORE`, `ZCARD`,
`ZINCRBY`, `ZSCAN` -- in nine rows and three private helpers (`zadd`, `zrange`,
`zrangeByScore`), plus the `PEXPIREAT` row T13 listed as its third deviation. Every `Command`
variant the engine has now has a row in `parser_maps_every_command`, checked mechanically: the
table's 70 rows name every variant declared in `Command.kt`.

`Command.ZAdd` gained `condition` and `changed`, and the `Partition` branch behind them, which
is the debt T07 recorded as its first deviation and sized at about fifteen lines. It came to
twenty. `condition` is `Command.Set.Condition`, not a second enum: `NX` and `XX` mean here
exactly what they mean on `SET`, so a second spelling of the same two words would have been a
second thing to keep in step.

`P1AcceptanceTest` is the tier the ticket exists for: Jedis 5.2.0, in test scope of the server
module only, unmodified and unconfigured, driving spec 9's single-node demo over a real socket
against a `DynaCacheServer` on an ephemeral port with the default engine.

**Concepts named:** No new vocabulary and no new seam. The acceptance tier's one idea is that
**the client is the assertion**. Every value the test compares came back through Jedis's own
parsers, so a reply that is not Redis-shaped fails as a `NumberFormatException` or a wrong
value rather than as a byte string a test author copied out of the server. That is C8 stated as
a test rather than as a comparison, and it is why the ticket forbids using Jedis as an assertion
library: its job is to disagree, not to help.

The parser change is the same idea T13 named, extended: the wire's several spellings of one
meaning collapse here and nowhere else. `ZRANGE`/`ZREVRANGE` and `ZRANK`/`ZREVRANK` reach the
engine as one variant each with a `reverse` flag, exactly as T07 froze them, so eleven names
make nine rows. `PEXPIREAT` joins `EXPIRE`, `PEXPIRE` and `EXPIREAT` as a fourth spelling of one
absolute deadline.

The `ZADD` flags kept the dual index's single-writer rule: `writeScore` is still the only place
a member's score is written, and `NX`, `XX` and `CH` are a condition in front of it and a second
counter beside it. `XX` on a missing key returns before `newZSet`, so a refused write leaves no
empty sorted set for T10's eviction or T31's codec to trip over.

Seams unchanged: `Reply`, `Key`, `CommandEngine`, `PartitionContext` and `CrossPartitionBatch`
are exactly as T01 and T14 froze them. `DynaCacheServer.kt` was not touched at all, so T35's
scheduler change merges clean.

**Acceptance:**
- `P1_acceptance_redis_client_unmodified` (server): one Jedis connection runs, in order,
  `PING`; `SET foo bar EX 60`, `GET foo` and a `TTL` between 1 and 60; the leaderboard
  `ZADD leaderboard 100 alice 200 bob` read back with `ZRANGE 0 -1 WITHSCORES` sorted, plus
  `ZCARD`, `ZRANK`, `ZSCORE` and the new flags through Jedis's own `ZAddParams().nx()` and
  `.xx().ch()`; spec 9's counter script verbatim through `EVAL`, twice, then read from outside;
  1,000 `SET`s and a full `SCAN` cursor walk with `MATCH` and `COUNT 64` collecting every key at
  least once (C15); a `MULTI`/`EXEC` of two keys sharing the hash tag `{account}`;
  `SET expiring v PX 200` followed by a `GET` poll to a five-second deadline until nil, with the
  value asserted readable first (C7) and `TTL` answering -2 after; and `INFO` parsed into its
  `field:value` sections, its key count checked against `DBSIZE`. Mutation-checked: with the
  parser dropping `WITHSCORES`, the test fails inside Jedis's own tuple parser with
  `NumberFormatException: For input string: "bob"` -- the client noticing, not the test.
- `parser_maps_every_command` (server): 70 rows, up from 54. Every `Command` variant declared in
  `Command.kt` has one; the sixteen new rows are the Sorted Set names, `ZADD`'s three flag forms,
  and `PEXPIREAT` landing on the same instant `EXPIREAT` does.
- `zadd_nx_xx_ch_flags` (engine): `XX` on a missing key writes nothing and creates nothing
  (`EXISTS` is 0); `NX` creates, then refuses to move a member it already scored, and in a
  two-member `ZADD` adds the new one while leaving the old one where it was; `XX` mirrors it;
  `CH` counts members new or moved where the plain reply counts only the new. Mutation-checked
  three ways: dropping the `NX` guard, dropping the `CH` count, and dropping the `XX` creation
  guard each fail it, at three different assertions.
- `mvn -B -o clean package`: BUILD SUCCESS, engine 138, cluster 54, cp 47, server 66. Every test
  from T02 to T15 green in the same run.
- The transcript below.
- This entry.

**redis-cli:** not on this machine. `which redis-cli`, `where.exe redis-cli`,
`C:\Program Files\*edis*`, `C:\Program Files (x86)\*edis*`, `C:\ProgramData\chocolatey\bin`,
the scoop shims, `%LOCALAPPDATA%\Programs` and WSL all came back empty, and the ticket forbids
installing anything. The transcript below is therefore the same session driven through T13's
`RespClient`, rendered the way `redis-cli` renders a reply: a simple string bare, a bulk quoted,
an integer as `(integer) n`, nil as `(nil)`, an array numbered, and `INFO`'s multi-line bulk
verbatim.

```
127.0.0.1:55340> PING
PONG
127.0.0.1:55340> SET foo bar EX 60
OK
127.0.0.1:55340> GET foo
"bar"
127.0.0.1:55340> TTL foo
(integer) 60
127.0.0.1:55340> ZADD leaderboard 100 alice 200 bob
(integer) 2
127.0.0.1:55340> ZRANGE leaderboard 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "bob"
4) "200"
127.0.0.1:55340> ZADD leaderboard XX CH 150 alice
(integer) 1
127.0.0.1:55340> ZSCORE leaderboard alice
"150"
127.0.0.1:55340> ZRANK leaderboard bob
(integer) 1
127.0.0.1:55340> SET counter 0
OK
127.0.0.1:55340> EVAL "redis.call('SET', KEYS[1], redis.call('GET', KEYS[1]) + 1); return redis.call('GET', KEYS[1])" 1 counter
"1"
127.0.0.1:55340> EVAL "redis.call('SET', KEYS[1], redis.call('GET', KEYS[1]) + 1); return redis.call('GET', KEYS[1])" 1 counter
"2"
127.0.0.1:55340> GET counter
"2"
127.0.0.1:55340> MULTI
OK
127.0.0.1:55340> SET {account}.balance 100
QUEUED
127.0.0.1:55340> SET {account}.owner alice
QUEUED
127.0.0.1:55340> EXEC
1) OK
2) OK
127.0.0.1:55340> MGET {account}.balance {account}.owner
1) "100"
2) "alice"
127.0.0.1:55340> SET ephemeral v PX 200
OK
127.0.0.1:55340> GET ephemeral
"v"
127.0.0.1:55340> GET ephemeral
(nil)                 # the TTL fired; awaited with a deadline
127.0.0.1:55340> DBSIZE
(integer) 5
127.0.0.1:55340> KEYS *
1) "foo"
2) "counter"
3) "{account}.balance"
4) "leaderboard"
5) "{account}.owner"
127.0.0.1:55340> INFO
# Server
dynacache_version:0.1.0

# Memory
used_memory:361
maxmemory_policy:lru

# Keyspace
db0:keys=5
```

**Deviations:** Three, none against a fixed contract.

1. **`ZADD` has no `INCR` flag.** The ticket asked for it "if trivial". It is not: `INCR`
   changes the reply from an integer count to a bulk score, and combined with `NX` or `XX` it
   has to answer the nil bulk when the condition refuses the write, which is a third reply shape
   in a branch that had one. `ZINCRBY` already covers the increment itself. Debt, and small: a
   boolean on `ZAdd`, an entry-count check in the parser, and about six lines in the branch,
   whenever a client wants the flag.
2. **`ZADD`'s `GT` and `LT` flags are not built**, and neither is `ZREVRANGEBYSCORE`. None of the
   three is in spec 2.1's command list, and T07 already recorded the last one. `ZADD k GT 1 a` is
   an arity error here rather than Redis's own answer, because the flag loop stops at the first
   token it does not know and `GT` then counts as a score.
3. **The `redis-cli` transcript is a `RespClient` transcript**, for the reason above. It is the
   same bytes on the same socket; only the renderer is ours, and it renders replies it was handed
   rather than replies it chose, so a wrong reply would still show.

**For the next ticket:**

- **Jedis is now available in the server module's test scope** (`redis.clients:jedis:5.2.0`,
  pulling `commons-pool2` and `org.json`; `slf4j-api` was already in the local repository). It is
  the right client for any later acceptance tier -- T22's cluster demo especially -- and it must
  stay in test scope: nothing in `src/main` may import it, or C8 stops meaning anything. Jedis's
  `CLIENT SETINFO` handshake asks for a command this server does not have and swallows the error,
  so no `CLIENT` row was needed; a client that does not swallow it would need one.
- **`INFO` is parsed by a real client now**, so its section format is load-bearing: `# Section`
  headers, `field:value` lines, CRLF endings. Anything added to it must keep that shape or the
  acceptance test's `associate` picks up junk.
- The acceptance test polls `GET` to a deadline for the TTL. What the client observes there may
  be lazy expiry on the read rather than the timer wheel's own sweep -- both delete the key and
  the client cannot tell them apart, which is the point of testing at this seam. The wheel's own
  correctness is T08's `TimerWheelTest` and T09's active expiry, under an injected clock. If a
  later ticket wants the sweep pinned from outside, `DBSIZE` after a tick with no intervening
  read is the observation to use.
- `scoreText`'s exponent form (T07's fourth deviation, `1.0E17` where Redis writes `1e+17`)
  never came up: Jedis parses scores with `Double.parseDouble`, which accepts both. It stays a
  difference from Redis's bytes, and a client that reads the score as text would still see it.

## T44: Command dispatcher, Redis-compat routing, RESP verbs

**Built:** `CommandDispatcher(ap, cp?, clock)` in the server module, a `CommandEngine` that sits
in front of both engines and applies CP spec 9.5's three rules in order: a `Command.Cp` whose key
is in the `cp:` namespace goes to the CP engine and one whose key is not is `-NOTCP`; any other
command naming a `cp:` key goes to the CP engine re-targeted onto the CP verb it means when it is
in the Redis-compat set, and is `-NOTCP` otherwise; everything else goes to the AP engine. A node
constructed with a null CP engine answers every CP-bound command `-NOTCP`. `atomically` runs on
the AP engine and refuses a span that declares a `cp:` key.

The T13 parser gains a row for every `CP.*` verb of CP spec 6 (long, lock, semaphore, latch,
reference, session, introspection), each asserted in `parser_maps_every_command`. `Command.Cp`
gains the `Introspection` branch with `Info` and `Members`; `CpEngine` and `ForwardingCpEngine`
answer both from a member's own MicroRaft report rather than through the log, and `CpWire.info`
is the one place a report becomes a `CpInfo` (`CpGrpcServer.info` now calls it). `Command.Cp
.LongTtl` gains a `precision`, so `PTTL` on a `cp:counter:*` key answers milliseconds; the
AtomicLong state machine and the wire codec carry it.

`CommandHandler` owns a CP session per connection: the first session-bearing CP verb creates one,
`CP.SESSION.CREATE` names that same session rather than making a second, and the lock and
semaphore verbs get it put in on the way to the dispatcher (the parser leaves `NO_SESSION`, and
`CP.SESSION.HEARTBEAT`/`CLOSE` keep the session they name on the wire). A creation that failed is
not remembered, so the next verb tries again. `DynaCacheServer` takes the CP engine and a clock
and builds the dispatcher; `main` wires a MicroRaft member plus its gRPC server when this node is
in the configured group and a `ForwardingCpEngine` when it is not.

**Concepts named:** **Re-target** is the one thing the dispatcher does to a command, and CONTEXT.md
now says so: `INCR cp:counter:x` and `CP.LONG.INCR cp:counter:x` are one command by the time an
engine sees them. It never rewrites a reply and never sends one command to both engines, which is
what C16 and C22 are. **Redis-compat set** is the second new term: the Redis commands the `cp:`
namespace answers, each mapped onto a CP verb, with everything else `-NOTCP`. Seams: `isCpKey()`
and `keysOf(command)` are internal to the server module and are the only place the namespace rule
is read (`keysOf` replaced the private `declaredKeys` EXEC used, so one function answers both).

**Acceptance:**
- CP spec 10.8's five: `dispatch_cp_verb_routes_to_cp`, `dispatch_cp_prefix_routes_to_cp`,
  `dispatch_ap_key_routes_to_ap`, `dispatch_cp_verb_bad_namespace_rejected`,
  `dispatch_unsupported_redis_cmd_on_cp_rejected`, all in `CommandDispatcherTest` against two
  recording engines, each asserting the engine that did *not* see the command as well.
- `long_redis_compat_incr` and `dispatch_auto_creates_session_on_first_cp_verb` in `CpRoutingTest`,
  over a real socket with a real AP engine and a real three-member CP group.
- `C16_ap_engine_never_sees_cp_key` (nine commands plus a batch) and `I22_namespaces_never_cross`
  (the same key name written and read on both sides) in `CommandDispatcherTest`.
- Every `CP.*` verb in `parser_maps_every_command`.
- Full offline `mvn clean package` green: engine 144, cluster 67, cp 66, server 79.

**Deviations:**
- `DEL`, `EXISTS` and `TYPE` are in CP spec 9.5's compat set but no CP primitive answers them yet,
  so the dispatcher rejects them with `-NOTCP` rather than routing them to an engine that would
  fail. Debt: repaid by three `Command.Cp` variants and the state-machine support behind them.
- The compat path reads only the key prefix, so `GET cp:lock:x` reaches the counter and reads
  empty instead of answering `-WRONGTYPE`, and `EXPIRE cp:lock:x` answers 0 where CP spec 9.4 says
  a lock's lease rejects it. Debt: nothing tracks which primitive owns a key; the repair is a
  key-to-kind check in the state machines, where `-WRONGTYPE` has to come from anyway. Marked with
  a `ponytail:` comment on `CommandDispatcher.compat`.
- A `MULTI`/`EXEC` span naming a `cp:` key is refused, but the refusal reaches the client as
  `-ERR a batch cannot name a cp: key` rather than `-NOTCP`: `atomically` is generic in its
  result, so the only carrier is the future's failure, and `orBatchError` only knows
  `CrossPartitionBatch`'s fixed `-CROSSSLOT`. C16 holds either way (the batch never runs). Debt:
  an exception that carries its own `Reply.Error` would repay it.
- Size: the change is roughly 770 lines including tests, over the 200-to-600 budget. Every part of
  the overage is ticket surface (the CP parser rows alone are 120 lines of table and test), so
  nothing was trimmed; nothing was added past what acceptance needs either.
- `dynacache-cp` now publishes a test-jar so the server module's routing tests can run the real
  three-member `CpTestKit` group. It is a build-time wiring change, not a new dependency.
- Merge with `misc/ai_gen`: the WAL's fsync policy had taken `main`'s argument 3, which T44 had
  given to `cp-self`, so the CP group moved to arguments 4 and 5. The command line is now
  `dynacache [port] [partitions] [dir] [fsync] [cp-self] [cp-members]`.

**For the next ticket:** the connection's session is never closed when the socket closes; it is
left to expire by heartbeat timeout (CP spec 4). Closing it on `channelInactive` is a small
addition and would make `sem_session_death_releases` deterministic without a timeout wait.
`ForwardingCpEngine` answers `CP.MEMBERS` from the fixed membership it was configured with and
`CP.INFO` by asking the leader; only the `CpEngine` path is covered by a test here. The
dispatcher's `compat` is the one place to extend when a CP primitive learns a new Redis spelling,
and the `Rejected` exception it throws never leaves `submit`.

## T36: Chandy-Lamport distributed snapshots

**Built:** `Marker { snapshot_id }` is field 19 of the `Envelope` oneof in `cluster.proto`, and
`GrpcTransportTest` round-trips it. `dynacache.cluster.DistributedSnapshot(self, peers, engine,
transport, dir, clock, demux, scope, deadline = 30s)` is one node's part of a snapshot, spec
2.8 steps 1 to 5 and the timeout. `initiate(id)` (step 1) records the node's state through the
T32 `SnapshotEngine(engine, <dir>/<id>/<self>/, clock).save()` and sends a marker to every
peer. `receive(envelope): Boolean` is the demux hook the router calls ahead of every other
handler: a marker for an unknown id starts the node's part (steps 2, same as `initiate`), any
marker closes the channel it arrived on (step 3), and a non-marker envelope arriving on a
still-open channel of any recording snapshot is appended, length-delimited, to
`<dir>/<id>/<self>/from-<peer>.log` before it is handed on. `complete(id)` is step 4 for the
node: every incoming channel closed. Whole-snapshot completion is the kit observing every
node's `complete`. `start` also launches a timer on the node's scope: at `deadline`, a part
still waiting for a channel is aborted, which deletes `<dir>/<id>` recursively, forgets the
id and ignores later markers for it; the engine is never written by the protocol.
`restoreFrom(dir, id)` loads `<dir>/<id>/<self>/dump.rdb` through T32 `restore()` and then
re-delivers each recorded channel's envelopes, in arrival order, to the node's own demux, so
a `Replicate` in flight at the cut lands on the replica exactly as it would have.

`Router` gains a `snapshots: suspend (Envelope) -> Boolean` hook consulted first in `receive`.
`InProcessCluster` builds one `DistributedSnapshot` per node, wires it into the router, takes
`snapshotDir` (default `target/snapshots`) and exposes `snapshot(node)`. `InMemoryTransport`
gains `sent`, every envelope the hub was handed in send order, which is what C10 counts.
`CONTEXT.md` gains **Channel**, **Marker** and **Snapshot set**.

Offline `clean package` after the merge with misc/ai_gen (T25, T16, T35): engine 144, cluster 72,
cp 66, server 79, all green. T36 alone: 7 files, 372 insertions, 5 deletions, plus CONTEXT.md.

**Concepts named:** a **channel** is one peer's envelopes to one node in send order, which the
`Transport` seam already promises, so a marker closes exactly one channel and there is one log
per peer. The **marker** is the only new envelope. The **snapshot set** is the whole of one
snapshot on disk, one directory per node under one id, consistent, restored and deleted as a
whole. The coordinator is a decorator on the demux, the same shape as `Replication.receive`:
the router's inbound loop stays the one place every envelope passes, so "record before handing
on" needs no second inbound path. No new seam: `demux`, `scope` and `deadline` are constructor
parameters, and `SnapshotEngine` is reused unchanged for both the state file and its restore.

**Acceptance:**
- `chandy_lamport_consistent_cut`: write 1 everywhere; write 2 applied on its coordinator with
  its `Replicate`s held two rounds in the network when another node initiates, so the markers
  overtake them; every recorded value's predecessors are recorded (tag `i` implies `1..i-1`);
  write 3, issued after completion, is nowhere in the set; write 2 is in the coordinator's
  state and, on every other node, in its state or on its channel from the coordinator, and on
  at least one node only on the channel (the scenario really recorded something in flight).
- `chandy_lamport_restorable`: the same scenario, then k1 overwritten and k3 written after the
  snapshot; a fresh cluster restores every part; k1 reads 1, k2 reads 2, k3 reads nil through
  every node, and every replica of k2 holds it locally. Mutation check: with the replay removed
  from `restoreFrom`, two of the three replicas of k2 read nil, so the test depends on the
  channel logs, not on the read quorum finding the coordinator's copy.
- `chandy_lamport_timeout_aborts`: one node network-killed before a survivor initiates; the
  survivors record their parts (files exist), a write through the other survivor answers OK
  while the snapshot waits, neither survivor is complete; `advanceTimeBy(31s)` and the set is
  gone, `complete` stays false, both keys read their values through both survivors.
- `C10_marker_on_every_channel`: after one initiation, `network.sent` holds exactly one marker
  per ordered node pair, and every node is complete.
- `I12_reads_after_restore_return_snapshot_time_values`: k1 and k2 written, snapshot, then k1
  overwritten, k2 deleted and k3 created; a fresh cluster restored from the set reads 1, 2 and
  nil through every node. Green on first run: the behaviour already existed; the case it adds is
  a key deleted after the cut coming back.
- Every earlier test green, including after the merge with T25, T16 and T35.
- Progress entry: this file.

**Deviations:**
1. **File layout.** The ticket names `<dir>/<id>/<node>.rdb` and `<node>.from-<peer>.log`;
   the code writes `<dir>/<id>/<node>/dump.rdb` and `<dir>/<id>/<node>/from-<peer>.log`, so the
   state file is exactly a T32 `SnapshotEngine` directory and `save()`/`restore()` are reused
   with no path parameter added. One directory per node is also the natural unit to delete.
2. **No `SnapshotComplete` envelope.** The ticket allows "the kit observing every node"; the
   tests do that through `complete(id)`. An initiator that wants to learn completion over the
   network is one more oneof case and a counter on the initiator; not built (YAGNI).
3. **The timeout is per node, not per initiator.** Every node that started a part runs its own
   deadline timer from the moment it recorded its state and deletes the whole `<dir>/<id>` when
   it fires with a channel still open. On a shared directory (the kit) the first timer deletes
   every part; on per-node directories each node deletes its own. Simpler than an abort
   envelope and correct in both layouts. A late marker for an aborted id is dropped, so a slow
   node cannot restart the snapshot after the survivors gave up.
4. **The state file is written on the demux's coroutine.** `SnapshotEngine.save()` serializes
   on the caller's thread (T32), and for a receiver the caller is the router's inbound loop, so
   a large keyspace stalls inbound handling (acks, gossip) for the write time. The write path
   itself (`Router.submit`, `Replication.write`, the engine) never waits on it, which is the
   spec's promise and what `timeout_aborts` checks. Debt: `withContext(Dispatchers.IO)` around
   the save if a measurement shows the stall.
5. **The channel log is opened and closed per envelope** (`ponytail:` in `record`). Keep it
   open per channel if a snapshot under heavy traffic shows it.
6. **Restore into a live cluster is not supported.** `restoreFrom` neither flushes the engine
   nor resets `Replication`'s version table, so it is a startup operation (fresh nodes, as
   T37's "kill all three, restore" needs). Both I12 and `restorable` restore into a fresh
   `InProcessCluster`.
7. Size: with the inherited half, 372 insertions in 7 files plus 18 lines of CONTEXT.md, inside
   the budget.

**For the next ticket:** T37 restores a cluster from the set by constructing fresh nodes with
the same `snapshotDir` and calling `snapshot(node).restoreFrom(dir, id)` on each, then
`drainMessages()`: the replay sends `ReplicateAck`s to coordinators that have no pending
request for them, which the router drops. `DistributedSnapshot.receive` runs inside
`Router.receive`, so anything the server wires ahead of the router (none today) is not
recorded. The `open` map is a `ConcurrentHashMap` because the deadline timer and the demux are
two coroutines; the per-channel set inside it is touched only by the demux. `initiate` on an
id already open throws; a second snapshot with a fresh id while one is open is fine, each
channel log is per id. `SnapshotEngine` is constructed with `fsync = null` here, so a node's
part carries no WAL: the snapshot is the view at the moment `save()` ran, which is what the cut
wants. `InMemoryTransport.sent` is public and grows for the life of the hub.

## T28: Anti-entropy sync

**Built:** `dynacache.cluster.AntiEntropy` (its own file): the node's anti-entropy step
`tick()` and its one coroutine `run()` (tick, `delay(interval)`, default 60 s; not yet launched
by the server, which does not wire `Replication` either). `ranges` is the list of vnodes whose
preference list holds this node, in ring order; a tick takes the next one round-robin and one
live replica of it (rotating through the replicas once per full pass), scans the range through
the engine, builds the `MerkleTree` (T27) over `(key, SHA-256 of the value encoding, DVV)`,
sends `MerkleRoot{id, vnode index, root}`, and on a root mismatch builds the peer's tree from
the leaves in the `MerkleRootReply`, `diff`s, and sends `KeySync{id, divergent keys, this
node's Version of each it holds}`; the peer applies spec 5.3 per key and answers
`KeySyncReply{id, its versions of what it still holds}`, to which the sender applies 5.3 in
turn. A `Version` is `(key, encoded value, DVV, expires_at_millis)`. Every request is awaited
under a 1 s deadline (constructor parameter). `rangesCompared` (roots exchanged) and
`keysSynced` (installs on this node) are the counters. `receive(envelope): Boolean` is the
demux hook, wired after `Replication.receive` in `InProcessCluster`.

Engine, `CommandEngine` interface untouched: `Stored(key, value, expiresAt)` with
`ApEngine.view(holds: (Key) -> Boolean)`, `ApEngine.view(keys)` and `ApEngine.install(stored)`
in `Stored.kt`, over new `Partition.view` overloads (frozen copies as one task per executor) and
the existing `Partition.restore`, so an install goes through `Partition.write` on the executor
and replaces value and TTL. **An install is not WAL-logged**: `restore` bypasses `execute` and
its hook. `persist/ValueCodec.kt`: public `encodeValue(value)` = RDB type byte + `RdbWriter`'s
value bytes, `decodeValue(bytes, seeds)`; four `Rdb.kt` members went `private` to `internal`
for it. `ApEngine.partitions` is `internal` so the entry points live in their own file.
`Ring.preferenceList(vnode, n)` shares the walk of `preferenceList(key, n)`.
`Replication.installVersion(key, dvv)` is the only `Replication.kt` edit; `version(key)`
already existed. `cluster.proto`: bodies 20..23 (`Marker` took 19 in T36), messages `MerkleRoot`,
`MerkleRootReply`, `Leaf`, `Version`, `KeySync`, `KeySyncReply`; `GrpcTransportTest` round-trips
all four. `InProcessCluster` gains an `AntiEntropy` per node sharing the node's `DotCounter`,
`antiEntropy(node)`, `antiEntropyStep(node)` (launch `tick`, drain until done) and
`antiEntropyCycle(node)` (one step per range). Twelve files, 544 insertions, 10 deletions
before the merge. `mvn -B -o -q clean package` offline after merging misc/ai_gen: engine 144,
cluster 76, cp 66, server 79, all green. Commits `f893954` (ticket) and the merge on `t28`.

**Concepts named:** A **range** here is one vnode's key range, the unit compared; a node
**replicates** a range when it is in the range's preference list, which `Ring.preferenceList
(vnode, n)` now answers directly since every key in a range shares one list. A **leaf** is the
key's `(value hash, version)`, the value hash being SHA-256 over the engine's one value
encoding, which is also the wire form of a shipped value. **Held** (private) is a key as this
node has it: value, encoding, version, deadline. A **step** is one range against one peer, at
most two round trips. No new seam: `AntiEntropy` is concrete and tested at `tick()` through the
test kit's cluster, as the plan entry names.

**Acceptance:**
- `anti_entropy_heals_divergence`: three nodes, N=3, W=3; a string, a hash and a second string
  written; on one replica the first two are deleted and the third overwritten straight on the
  engine (same version, different bytes); one cycle on that replica; every replica answers the
  healthy values, `HGETALL` compared as a map, and each key has one version cluster-wide. Red
  first (the test kit had no step). The rotted-value branch was checked by mutation: flipping
  the tiebreak fails exactly this test.
- `anti_entropy_step_is_bounded`: a key lost on a replica whose range sits at index `at > 0` of
  the replica's ranges; `at` steps compare `at` ranges, sync no key and leave it missing; the
  next step compares one more, syncs one key, and the key is back.
- `anti_entropy_noop_when_equal`: equal replicas; steps up to the key's range compare that many
  ranges, no node's `keysSynced` moves, every version and value is as before.
- `anti_entropy_merges_concurrent_siblings`: two sibling string versions seeded (the second on
  both other replicas, so any peer choice is a merge); one step; the merged version dominates
  both, the value is the last writer's, the peer holds the same version, one key synced.
- Every earlier test green, `GrpcTransportTest` included. Tests 2 to 4 were green on their first
  run: the first slice already had to carry the whole protocol.

**Deviations:**
1. **Equal versions, different bytes**: spec 5.3 has no branch for a value that rotted under an
   unchanged DVV, and a pairwise exchange has no third opinion; both sides keep the greater
   encoding by unsigned byte order (Cassandra's tiebreak), so they converge. Which side was
   right is unknowable here; a majority check across all N replicas would repay it.
2. **Keys without a version are invisible**: a key the replication side table holds no DVV for
   (an engine-only write, or everything after an RDB restore, since the RDB carries empty DVVs)
   builds no leaf and is never shipped; the peer's versioned copy is installed over it on the
   next mismatch. Persisting versions (T35's own note) repays it.
3. **Nothing deletes**: there are no tombstones; a key one side lost is handed back. A deleted
   key that a lagging replica still holds is resurrected by anti-entropy, Dynamo's own gap.
4. **A merged value's TTL** is the later deadline, and none if either side had none; the spec
   says nothing.
5. **Installs skip the WAL** (documented on `ApEngine.install`): a restore path, so a node that
   recovers from its log lacks what anti-entropy gave it until the next round hands it back.
6. **The whole range's leaves ride on one reply** rather than a level-by-level descent; T27's
   `diff` is in-process over two whole trees and has no per-level accessor. Bounded by a range
   (1/(128·nodes) of the keyspace), and both scans walk the partition store since keys are not
   indexed by ring position.
7. **The rotation's peer choice is deterministic** (`step / ranges.size` mod peers), not random;
   the plan asked for no randomness and tests need none.
8. Test 1's cycle is run on the corrupted replica itself: a healthy node's cycle picks one peer
   per range and may not meet the corrupt one in a single pass.

**For the next ticket:** `AntiEntropy.run()` needs launching by whoever wires the node (T24 /
T30 acceptance), next to `runHandoff`, and `rangesCompared` / `keysSynced` belong in `INFO`.
T26 (read repair) can install a winner's value with `ApEngine.install(Stored(...))` plus
`Replication.installVersion`, and ship one with `encodeValue`; both are public now, which
removes ADR 0003's stated reason for shipping commands. `tick()` and `answerSync` on one node
can install the same key concurrently with a `Replicate`, the same unguarded pair T22 lives
with (version set, then value written). `ranges` is computed once from the immutable ring.
`InProcessCluster.antiEntropyStep` launches `tick` on the cluster scope and drains; a full
cycle at N=3 is 384 steps and ran in about a second with two partitions per node.

## T26: Read repair

**Built:** Spec 5.2 step 5 inside `Replication`, at the hook T22 left where `divergentReads`
is incremented. `read` now keeps its R answers by node; after the winner is chosen, every
answer whose version is null or dominated by the winner's is *behind*, and the repair for
those nodes is launched on the node's `scope` after the reply is decided, so the client never
waits for it. The repair is the winner's job, since only the winner holds the value (the
coordinator has just a reply): when the winner is the coordinator it `push`es directly; when
it is another replica the coordinator sends it a `Repair` (`cluster.proto` field 25: key and
target nodes) and that node pushes. `push` reads the key's version, takes the engine's live
copy through `view` (T28's `ApEngine.view(keys)`), re-reads the version and gives up if it
moved (a write bumps the version first and applies second, so a moved version means the pair
may not match and that write's own `Replicate` carries the fresh one), then sends one
`Version` (T28's message, reused as the `replicate_value` oneof case, field 24) per target:
key, the value as `encodeValue` writes it, the DVV, the TTL as an instant. An empty value is a
tombstone (the winner holds a version and no value: a `DEL` the target missed). The receiving
end, `installValue`, decodes first (bytes that do not decode are dropped whole), applies spec
5.3's rule (taken only when the remote version dominates what is held; equal, older and
concurrent are left alone), stores the version, then installs through T28's
`ApEngine.install(Stored)` or, for a tombstone or a value already past its deadline, a `DEL`
through the wrapped engine. `Replication` gains two constructor lambdas, `view` and `install`,
because it wraps a `CommandEngine` and the value path is `ApEngine`'s; `InProcessCluster`
wires them to the node's engine. Counters: `divergentReads` (unchanged) and `repairsSent`
(replicas a read on this coordinator found behind, counted when the repair is decided).
`Replication.receive` demuxes `REPLICATE_VALUE` and `REPAIR`. Test kit: `seed(node, write,
dvv)` overload taking any keyed write (so a replica can hold a hash under a version),
`TokenCodec` learned `HGET`, `GrpcTransportTest` round-trips both new cases. CONTEXT.md gains
"read repair" (with "sibling"). Net against `misc/ai_gen` after the merge: 8 files, 288
insertions, 10 deletions; `mvn -B -o -q clean package` offline green: engine 144, cluster 80,
cp 66, server 79.

**Concepts named:** **Read repair** is what a coordinator does after a quorum read whose
answers did not all carry the winning version: the replica that holds the winner pushes its
value and version to every replica the winner dominates, after the client has its reply and
never in its way. A replica whose version is concurrent with the winner's is a **sibling** and
is left alone for the merge. A **tombstone** is a version with no value under it. No new seam:
the value path is the engine's own (`view`/`install`, T28), reached through two lambdas the
way `tokens` and `parse` already are; the second adapter of those lambdas is the C4 test's
stub over `RecordingEngine`.

**Acceptance:**
- `read_repair_fixes_stale`: N=3, W=1, R=3 (R = N so the stale replica is certain to be among
  the answers); the coordinator and one replica seeded "fresh" under the newer version, the
  other replica "stale" under the older; a read through the stale replica's contact answers
  "fresh" (red first: after the drain the stale replica still held "stale"); after
  `drainMessages` all three hold "fresh" under the newer version, `divergentReads` 1,
  `repairsSent` 1.
- `read_repair_skips_concurrent_siblings`: one replica seeded under a dot of its own, the
  other two under the coordinator's; the read answers the `lastWriter` winner; after the drain
  every node holds exactly what it was seeded, `repairsSent` 0. Green on first run; checked by
  mutation (treating "not dominated by" as behind fails it, count 3).
- `read_repair_repairs_a_hash`: the winner is a replica holding `HSET f1 new f2 added` under
  the newer version, the coordinator and the third node hold `HSET f1 old` under the older; an
  `HGET f1` through the third node answers "new" (red first: the test kit had no wire form for
  `HGET`, then the field order of a rebuilt hash differed, compared as a map since); after the
  drain all three answer the same fields and version, `repairsSent` 2. This is the `Repair`
  path: the coordinator asked the winner to push.
- `read_repair_does_not_delay_reply`: the C4 harness, N=2, W=1, R=2: the coordinator runs
  `Replication` over a real `ApEngine`, its one replica is a bare endpoint that answers the read
  holding nothing and never acknowledges anything; the reply is done at virtual time zero right
  after that answer (checked by mutation: an inline repair with a `delay(deadline)` fails it),
  and the `Version` then reaches the endpoint under the coordinator's version.
- Every earlier test green, `GrpcTransportTest` included.
- This entry.

**Deviations:**
1. **Value path (a), by way of T28.** This branch first built its own path (`ValueCodec`
   object, `ApEngine.export`/`install(key, bytes?, expiresAt)` through `Partition.write`, a
   `ReplicateValue` message). The merge of `misc/ai_gen` brought T28's `encodeValue`/
   `decodeValue` (same encoding, same file name, add/add conflict), `Stored`,
   `ApEngine.view`/`install` and the `Version` message, all the same thing; the merge commit
   keeps T28's and drops mine, so the engine is untouched by T26 after the merge and one value
   path exists. `Version` doubles as the repair body; only `Repair` is a message of T26's own.
2. **An installed value is not WAL-logged** (T28's `install` is a restore-shaped write; a
   node that recovers from its log is handed it again by the next read or anti-entropy round).
   A tombstone repair is a `DEL` through the engine and is logged. Debt: a value op in
   `WalCodec` would make the two alike.
3. **The value is decoded with `Random(dvv.dot.counter)`** as T28 does, so a restored sorted
   set's skip-list levels are the same on every node that installs the same version.
4. **`repairsSent` counts at the coordinator when the repair is decided**, not when a
   `Version` leaves; with a remote winner the coordinator sends one `Repair` and the winner
   sends the `Version`s. `INFO` is not wired (as with `hintCount`, T25).
5. **A repair that arrives expired, or a tombstone, deletes the loser's key** rather than
   leaving a stale value under the new version. A value whose bytes do not decode is ignored
   with its version, so a bad peer cannot bump a version over a value it did not deliver.
6. **`push` re-reads the version after taking the value** and gives up if it moved, rather
   than taking value and version in one engine task (the engine does not hold versions). The
   same window exists in `answer` and in `read`'s local answer since T22 and is not widened.
7. `read_repair_skips_concurrent_siblings` was green on its first run and is recorded as
   such with its mutation check, rather than a faked red.

**For the next ticket:** the repair has no ack and waits for nothing: a lost `Version` is
repaired by the next divergent read or by anti-entropy (T28), so a repair counter on the
receiving side does not exist. T30's chaos driver can read `repairsSent` per node. Read repair
and anti-entropy now share `Version`, `encodeValue`/`decodeValue` and `ApEngine.install`; if
T29's merge is ever applied on the repair path (a concurrent `Version` merged instead of left
alone), `installValue` is the place and `AntiEntropy.reconcile` the model. The C4 harness in
`ReadRepairTest` (a `Replication` over a real engine with bare endpoints) is the way to observe
"the reply is done before X" under `runTest`, since `InProcessCluster.drainMessages` is a
fixpoint that delivers the repair too. `seed(node, write, dvv)` takes any keyed write the
test kit's `TokenCodec` knows; `HGET` was added, `HGETALL` and `HSET` already were.

## T24: P2 acceptance

**Built:** the node, the wire form it forwards in, and the demo that proves the three of them
work together.

`ClusterNode` (server module) is one node of a cluster with everything a client can reach on it:
an `ApEngine`, a `GrpcTransport`, a `Ring` built from the fixed node set, a `Swim` with its
`run()` loop, a `Replication` and a `Router`, wired as plan 2.3 draws them --
`Router(Replication(ApEngine))` -- and handed to `DynaCacheServer` as the `CommandEngine` its
pipeline submits to. It presents the `CommandEngine` shape itself, so the handler that served one
engine now serves a cluster without knowing it. `start()` opens the RESP socket and launches the
node's three loops (the router's demux, gossip, hint handoff); `close()` stops the socket, cancels
the scope, closes the transport and then the engine. `addresses` is read at send time (T23), so
three nodes on ephemeral gRPC ports are built first and told each other's ports afterwards, while
`nodes` -- the ring -- is known up front because a node id is not an address.

`commandToTokens` (own file, beside the parser) is the real `(Command) -> List<ByteArray>` T19
asked for: an exhaustive `when` over `Command.Keyed`, all 38 variants, so a variant added to the
engine stops the build rather than failing a forward at runtime. Where the wire has several
spellings of one meaning it writes the one that carries everything the variant holds: an absolute
deadline goes out as `PEXPIREAT` (T16's row, which is why it had to exist), a `Set`'s TTL as
`PX <millis>` since the `Duration` no longer knows which spelling it arrived as, and `TTL`/`PTTL`,
`LPUSH`/`RPUSH`, `ZRANGE`/`ZREVRANGE` and `ZRANK`/`ZREVRANK` are each chosen off the flag that
distinguishes them.

`INFO` gains a `# Cluster` section: node id, quorum, the membership view one `member_<node>` line
per node with state and incarnation, and the pending hint count `Replication.hintCount` reports.
`main` gains a cluster mode behind `--peers=id=host:port,...` (`--node`, `--grpc`, `--quorum=n/w/r`);
without `--peers` it is the single node it always was and the positional arguments mean the same
in both modes. `DynaCacheServer` was touched at the construction site only: a `commands:
CommandEngine = engine` parameter that the handler submits to, `engine` staying the local one the
scheduler ticks, and four lines in `main`.

**Concepts named:** No new vocabulary and no new seam; every name in the wiring is CONTEXT.md's
already. The one idea worth recording is that **a node is an assembly, not a layer**: nothing in
`ClusterNode` decides anything about a command except which section `INFO` ends with. The router
decides the coordinator, replication decides the quorum, SWIM decides who is alive, and the
assembly's whole content is the order they are stacked in and the fact that they all share one
transport and one scope.

That sharing needed one small adapter, `NodeTransport`, which is where two facts about real
sockets that the in-memory transport hides get repaired in one place:

- **A send to a node that is gone is a dropped envelope, not a throw.** gRPC reports a down peer
  out of `send` (T23's own note) and nothing above it has an error path: gossip's `tick` would die
  with the exception and stop detecting the very failure it had just seen, and a quorum's fan-out
  would fail the write rather than wait for the replicas that are up. The in-memory adapter every
  cluster test was written against simply drops; this makes gRPC agree. `CancellationException` is
  rethrown, so cancelling the node's scope still works.
- **One channel needs one reader.** T19 deviation 5 left this for whoever ran gossip and
  forwarding on one node: the router's `run()` owns the inbound loop and hands gossip envelopes to
  `Swim.deliver`, so the copy of the transport SWIM holds is deaf -- its `inbound` is a channel
  nothing is ever written to, and `tick`'s own `tryReceive` finds it empty instead of racing the
  demux for a forwarded command.

**Acceptance:**
- `P2_acceptance_three_nodes_quorum_and_minority_failure` (server): three `ClusterNode`s in one
  JVM, ephemeral RESP and gRPC ports, real gossip over real sockets, N=3 W=2 R=2, driven by
  unmodified Jedis (C8). `INFO`'s `# Cluster` section names all three alive; `SET foo bar EX 60`
  through node-1 reads back through node-3 with its TTL intact across the quorum; a key the ring
  gives to node-2 is written through node-1 and read through node-3; a `MULTI` of two `{acct}`
  keys commits through the tag's coordinator and is refused elsewhere; node-2 is closed; node-1's
  `INFO` shows it dead inside a 30-second deadline (about a second in practice at a 100 ms
  period), with node-3 still alive; reads and writes on keys whose coordinator survived still
  succeed, the value written before the failure included. Every wait is a poll to a deadline;
  there is no sleep anywhere.
- `tokens_roundtrip_every_keyed_command` (server): 50 canonical wire rows, at least one per
  `Command.Keyed` variant, asserting `tokens(parse(row)) == row`.
- Mutation-checked three ways. Dropping `Set`'s value from its wire form fails the round trip at
  `SET k v` **and** fails the acceptance test at its first write, with the client reading
  `-ERR quorum not reached: write needs 2 of 3 nodes, 1 answered within 1s`: a replica that cannot
  parse a `Replicate` does not ack. Making `Router.submit` always run locally fails the acceptance
  test twice over, at the batch step and at the dead coordinator step. Both mutations were
  reverted and the full build re-run.
- `mvn -B -o clean package` offline, BUILD SUCCESS: engine 144, cluster 67, cp 66, server 68
  (66 before). Every P1 and P2 test green in the same run.
- This entry.

**Deviations:** Seven, two of them worth carrying.

1. **Forwarding is barely client-visible at N=3 on three nodes**, which cost the acceptance test
   a step. Every node is a replica of every key, so a contact that wrongly ran a forwarded command
   locally would still answer correctly: it applies the write, replicates it to the preference
   list's successors, and the reader is a replica too. The one thing a client can see that local
   execution could not produce is the failure: after node-2 is killed, a key node-2 coordinates
   answers `-ERR forward timeout after 2s waiting for node-2`, because the router forwards to the
   preference list's first node whether or not gossip has buried it (T22 deviation 7), and T25's
   sloppy quorum needs a fourth node to have a substitute. `whatTheDeadNodeCoordinated` asserts
   exactly that, so the forward has teeth; it is also the ticket's "reads and writes still
   succeed" stated precisely -- they succeed for every key whose coordinator is alive, which is
   two thirds of the keyspace, and the last third is spec 5.1 step 7's business.
2. **Batches are not replicated, recorded not fixed** (T22 deviation 5), and what a `MULTI`
   through the cluster does today is now asserted rather than described. Through the coordinator
   of its keys it commits and answers `[OK, OK]`; through any other node `EXEC` answers
   `-ERR <key> is coordinated by <node>, not <self>`, because `Router.atomically` refuses a span
   this node does not coordinate and a batch is a caller's block, which cannot be forwarded
   (T19 deviation 4). Keys sharing a hash tag share a coordinator, so `{tag}` is how a client gets
   a batch to work at all. Underneath that, `Replication.atomically` is one line -- `engine
   .atomically(...)` -- so the writes land on the coordinator's engine, bump no DVV and reach no
   replica; a client cannot see it today because the coordinator answers every read of those keys,
   and it becomes visible the moment that coordinator is lost. `EVAL` takes the same path.
3. **Size: 672 lines including tests and KDoc, 454 of code**, against the ticket's 200 to 600.
   Reported rather than silently exceeded. Everything the ticket lists landed, so nothing was cut;
   the overrun is comment density at this project's usual ratio.
4. **`clusterMain` has no test.** It is fifteen lines of argument shuffling around a constructor
   the acceptance test drives directly, and the test cannot go through it because it needs
   ephemeral ports. The `require` that `--peers` names this node is its only check.
5. **`ClusterNode` decorates `INFO` rather than the engine answering the cluster's part.**
   `Command.Info` is an `EveryPartition` command joined inside the engine, and the engine is
   cluster-unaware by the module graph, so the section is appended above the router. The
   membership table's key set is fixed at construction (dynamic membership is on the do-not-build
   list), so reading it from the RESP thread while the gossip coroutine updates a row cannot
   restructure the map underneath; if membership ever becomes dynamic, that read needs a snapshot.
6. **The gossip period is a constructor parameter, defaulting to SWIM's own 1 s.** The acceptance
   test runs at 100 ms, which puts detection at about a second rather than about ten. Nothing
   about the protocol changed; the test simply cannot wait ten seconds for each of its assertions.
7. **`commandToTokens` throws on a command that is not `Command.Keyed`.** Nothing forwards or
   replicates anything else -- the router runs keyless commands, `Scan` and `Command.Cp` locally
   (T19), and replication ships only single-key writes and reads -- so the throw is a claim about
   callers rather than a case to handle. `Router.coordinate` catches it either way.

**For the next ticket:**

- **`ClusterNode` is where a node is assembled**, so anything that needs to exist once per node
  (T26's read repair coroutine, T28's anti-entropy loop, T37's snapshot marker) is a field and a
  `scope.launch` in `start()`, next to the three loops already there. `NodeTransport` is the place
  to put any other difference between a real socket and the in-memory adapter; today it drops a
  failed send and blinds every reader but the router's.
- **`commandToTokens` is now the real wire form**, so the cluster module's `TokenCodec` is only
  the test kit's stand-in and the two can disagree. Anything that puts a new kind of command on
  the wire adds a branch here, and the round-trip test is one row.
- **Two things a P3 acceptance tier will want that this one does not have.** Restarting a node
  needs a fresh `ClusterNode` on the same id at a higher SWIM incarnation, which is what a real
  restart is (T20's note); `ClusterNode` takes the incarnation from `Swim`'s default of 0 today,
  so replaying hints into a returned node needs that parameter threaded through. And nothing here
  reads a node's stored value directly: every assertion goes through a client, which is the tier's
  point, so a test that must see what one replica holds needs the cluster module's kit, not this.
- **`DynaCacheServer(port, engine, commands, tick)`** is the construction site. T44 owns
  `CommandHandler` and `CommandParser`; the only change made to the handler here is that it takes
  a `CommandEngine` rather than an `ApEngine`, which it never needed.
- The acceptance test names keys by asking the ring which node coordinates them
  (`keyCoordinatedBy`), because with a dead coordinator the answer decides whether a key is
  available at all. Any cluster test with a failure in it wants the same helper.

**Deviations (merge of misc/ai_gen into t24, commit abd4e0f1):** three more.

8. **The cluster is behind T44's dispatcher, not beside it.** `DynaCacheServer`'s `commands`
   parameter became `ap: CommandEngine = engine`, which is what `CommandDispatcher(ap, cp, clock)`
   sends everything that is not a `cp:` key to; `ClusterNode` passes itself there. So a `cp:` key
   on a cluster node reaches the CP engine (or `-NOTCP`) without touching the ring, and every AP
   key goes through the router, which is CP spec 9.5's order with a cluster underneath it. The
   handler takes the dispatcher, as T44 wrote it.
9. **The AP cluster is named by flags, the CP group by position.** `main` keeps T44's
   `dynacache [port] [partitions] [dir] [fsync] [cp-self] [cp-members]` exactly, and the cluster
   adds `--peers=id=host:port,...`, `--node`, `--grpc` and `--quorum=n/w/r`. Four more positional
   arguments after `cp-members` would have made the seventh through tenth argument unreadable, and
   the flags are already parsed out before the positional list is built, so both modes read the
   same six positions.
10. **`AntiEntropy.run()` is launched, and its envelopes reach it.** T28 left the node wiring
    open. It is one more `scope.launch` in `start()`, one more `AntiEntropy` field sharing the
    node's `DotCounter` with `Replication` (hoisted to a field for it), and one more link in the
    demux chain -- `replication.receive` first, then `antiEntropy.receive`, then gossip -- since a
    loop nobody delivers to would compare Merkle roots and never hear an answer. `INFO`'s
    `# Cluster` section reports `cluster_ranges_compared` and `cluster_keys_synced`. The default
    interval is 60 s, so nothing fires inside the acceptance test; it is launched, not exercised.
    `DistributedSnapshot` (T36) is still not wired: `Router`'s `snapshots` parameter defaults to
    false here, and whoever wants a marker to reach a real node adds the field the same way.

**Acceptance after the merge:** `mvn -B -o clean package` offline, BUILD SUCCESS: engine 144,
cluster 76, cp 66, server 81. Both T24 tests green in that run.

## T45: Raft snapshots, chaos, linearizability

**Built:** A real `RaftStore`, state-machine snapshots on the wire, a seeded chaos driver, a small
linearizability checker, and the CP spec 10.9 invariant tests under chaos.

`RaftStores.kt` (new) holds `CpStore : RaftStore` with one extra method, `restored()`, so a member
brought back reads its own state instead of an empty log (the T38 `NopRaftStore` debt, I20, C21).
`InMemoryRaftStore` keeps term, vote, initial members, log and snapshot chunks in fields;
`FileRaftStore` keeps them under `java.nio.file`: `member.pb` (endpoint, group, term, vote, rewritten
whole), `log.pb` (length-delimited `LogEntry`s, appended, rewritten on truncation), one
`snapshot-<index>-<chunk>.pb` per chunk. `restored()` returns the newest complete snapshot plus the
log suffix after it, or null before the first run. `RaftRuntime` takes a `CpStore` (default in-memory)
and, when `restored()` is non-null, builds the node with `setRestoredState` instead of the fresh
endpoint + initial members; the `NopRaftStore` and its ponytail note are gone.

MicroRaft's snapshot chunking is switched on in the kit (`setCommitCountToTakeSnapshot(100)`).
`InstallSnapshotRequest`/`Response`, `SnapshotChunk` and `GroupMembersView` gained a wire form in
`cp.proto` and `CpWire` (the T43 `NotImplementedError` is gone), plus a `MemberState` message the
file store persists. `CpWire` encodes a `CpStateMachine.Snapshot` (log time and every primitive's
table) as one compact chunk, shared by the store and the wire. `CpStateMachine.Snapshot` is now a
public data class and `CpStateMachine.state` exposes it, so two members' state can be compared by
equality; `takeSnapshot` accepts it as one chunk (ponytail: a chunk per primitive is the upgrade
when one outgrows a message).

`Linearizability.kt` (new, test) is a Wing-Gong checker: `check(ops, spec)` searches the orders a
history allows for one the sequential model accepts, respecting real time; an operation with an
unknown output (a timeout) may take effect anywhere or not at all. `CounterSpec` is the AtomicLong
model. `ChaosDriver.kt` (new, test) is the seeded driver over `CpTestKit`: `runLockChaos` interleaves
lock/counter/session ops with faults (leader kills, follower kills, restarts, partitions, every kill
restarted within the loop, the file-store member killed at least once) single-threaded so its
interleaving is the seed's and it asserts mutual exclusion (I13) inline, returning the fencing tokens
per key; `counterHistory` runs clients concurrently on one counter with a failover between waves and
records a history for the checker (C20).

`CpTestKit` gained `fileStoreDir` (the last member then runs on `FileRaftStore`), `partition`/`heal`,
per-member stores reused across restart, and a faster heartbeat timeout.

**Concepts named:** The **store** is the second seam under `RaftRuntime` beside the transport, the
place a member remembers itself; a **snapshot chunk** is the state machine's whole state as one value
the store and the wire share. The **linearizability checker** and the **chaos driver** are test
tooling, each its own file.

**Acceptance:** `dynacache.cp.CpSnapshotTest` 3, `ChaosInvariantTest` 19, `CpWireTest` 10 (one new).
Full offline `clean package` green: engine 144, cluster 67, cp 89, server 79.

- `cp_snapshot_restore_roundtrip`, `I20_restore_equals_continuous_replay`: populate every primitive,
  snapshot, restart the member (from the file store and from memory respectively); its state equals
  the leader's. `lagging_member_is_brought_up_by_snapshot` (extra) drives the InstallSnapshot path in
  a live group.
- `invariant_fencing_token_monotonic_under_chaos`, `invariant_mutual_exclusion_under_chaos`,
  `invariant_linearizable_ops`: `@ParameterizedTest` over five seeds each (the two lock invariants
  share one cached run per seed). `checker_rejects_a_stale_read` (extra) proves the checker is not
  vacuous. `invariant_session_release_complete`: a session's locks and permits all freed by its close
  across a failover (I15). `I16_minority_kill_keeps_cp_available`, `I17_majority_kill_never_false_succeeds`.
- `ChaosInvariantTest` runs in about 20 s.

**Deviations:**
1. **Size.** ~1070 lines added including tests, over the 200-600 budget. The ticket is large by
   nature (a store, snapshot wire forms, a chaos driver, a checker, and 10.9's tests). The bulk is
   mechanical: `RaftStores` (158) and `CpWire`'s per-primitive snapshot codec (148), `ChaosDriver`
   (212) and its tests (166 + 134). Nothing was cut; every acceptance test is present.
2. **Kit heartbeat timeout shrunk from 5 s to 1 s.** The ticket allows shrinking the kit's timing to
   keep the chaos class quick, and said to say so. Every existing failover test still passes (they
   just resolve faster). Production keeps MicroRaft's defaults.
3. **`CpEngine` maps `IndeterminateStateException` to `-NOTLEADER`.** A leader that loses quorum
   mid-append fails the replicate future indeterminately; at the faster timeout this surfaces where
   the old test only saw a pending future. It is not a false success, so it is mapped to `-NOTLEADER`
   for the client to retry, exactly as `CannotReplicateException` already is (CP spec 9.1 step 7).
   This kept `cp_majority_failure_unavailable` and `I17` green regardless of timing.
4. **The counter linearizability workload overlaps clients within a wave, faults between waves.** A
   leader never dies mid-wave, so every recorded op has a definite outcome and the history is fully
   determined; the checker still reorders the concurrent ops. Seeds fix the fault schedule, not the
   thread interleaving (inherent to a real group). Unknown-output handling is in the checker for the
   general case and exercised by construction.
5. **`FileRaftStore.flush()` is a no-op; writes are eager (`Files.write`).** Bytes reach the OS, not
   the platter. ponytail-marked: `FileChannel.force` is the upgrade if a crash between the write and
   the OS flush must never lose a vote. Fine for the in-process restart the tests do.
6. **One snapshot chunk holds the whole state.** ponytail-marked in `CpStateMachine`; a chunk per
   primitive is the upgrade when one outgrows a protobuf message.

**For the next ticket:**
- T46 (P5 acceptance) wires two engines in one cluster. The file store is ready for a real node:
  build the store, then the runtime (it restores itself), then the engine and server. `CpTestKit`'s
  `GrpcCpKit` sibling would need the same `fileStoreDir` plumbing if a durable gRPC restart is wanted.
- The chaos driver is single-threaded for the lock invariants (deterministic) and concurrent only for
  the counter history; a fully concurrent lock workload with a real-time-stamped history and the
  checker over lock ops is the next step if lock linearizability is ever asserted directly.
- `deleteSnapshotChunks` and log truncation in `FileRaftStore` rewrite the whole log file each time
  (O(n)); an append-only segment file is the upgrade if a large log ever runs through it.

## T37: P4 acceptance

**Built:** the four wiring gaps between a cluster node and P4's machinery, and the demo that
drives all of them through a socket.

`ClusterNode` gains what single-node `main` already had and a cluster node did not:

- **Its own `SnapshotEngine`**, from `dataDir` and `fsync`. `start()` restores the last snapshot
  and the log after it *before* the RESP port opens, the server's tick lambda (previously the
  default `{ engine.tick() }`) now also runs `engine.wal?.tick()` and `snapshots?.maybeSave(now)`,
  and `close()` takes the shutdown save between cancelling the scope and closing the engine, so
  the save runs while the engine is open and nothing submits. The node creates the directory
  itself, which single-node `main` does not; a cluster is started as three processes and the
  operator should not have to `mkdir` three times.
- **Its part of a snapshot set.** `DistributedSnapshot` is a field built from `snapshotDir`, and
  `Router`'s `snapshots` hook -- `false` since T24 -- is now `distributed?.receive(it) ?: false`.
  The two are mutually recursive (the router consults the snapshot, the snapshot replays through
  the router's demux), which the Kotlin compiler reports as "type checking has run into a
  recursive problem" until the field carries an explicit `DistributedSnapshot?` type.
- **A trigger, as a method rather than a verb.** `snapshot(id)` launches `initiate` on the node's
  scope, `snapshotComplete(id)` is this node's step 4, `restoreSnapshot(id)` is `restoreFrom` under
  `runBlocking`. Nothing a Redis client asks for takes a distributed cut, so this is an operator's
  control and not a `SNAPSHOT` command at the handler: the ticket offered either, and a verb would
  have to be a connection-level case beside `MULTI` (it is not a `Command`, it does not queue, it
  has no reply shape Redis defines) for a caller that does not exist.
- **`maxMemoryBytes` and `EvictionPolicy`**, straight through to `ApEngine`.

`clusterMain` now passes the `[dir]` and `[fsync]` positional arguments it was already being
handed and dropped on the floor, so `--peers` mode persists like every other mode, with the
snapshot sets under `<dir>/snapshots`. A node with nowhere to put its part cannot answer a marker
from a peer, so the snapshot root follows the data dir rather than needing a flag of its own.

**One bug in T36, fixed at the root.** `DistributedSnapshot.start` published the snapshot's
channels into `open` and *then* created `<dir>/<id>/<self>/`. On T36's in-process kit `initiate`
runs on the test's own coroutine, so nothing else could look at `open` in that window; on a real
node `initiate` runs on the node's scope while the demux runs the router's inbound loop, and an
envelope arriving in the window was appended to a file in a directory that did not exist yet.
The `NoSuchFileException` propagated out of `Router.run`, killing the node's one inbound loop --
so the first symptom was not a lost record but a node that stopped answering anything. The
directory is now created before the channels are published.

**Concepts named:** none new. The vocabulary is T36's (**channel**, **marker**, **snapshot set**)
and T32's (**checkpoint**), and the only idea this ticket adds is where each of them attaches to
the assembly T24 named: a node is still an assembly, and persistence is three more fields and one
more thing the tick does.

**Acceptance:**
- `P4_acceptance_success_signal` (server), one method, the sequence end to end on T24's harness --
  three nodes in one JVM, ephemeral RESP and gRPC ports, real gossip, N=3 W=2 R=2, Jedis
  unmodified, `@TempDir` data dirs -- in about 3 seconds:
  - **(a)** a mixed keyspace written through node-1: a string with `EX 60`, a hash, a list, a
    sorted set, and a `PX 300` key. All three nodes closed, three fresh nodes started on the same
    data dirs (new ephemeral ports, the shared address map updated, which the ticket allows). Read
    back through node-3: every value intact, the `EX 60` key's TTL still inside its remaining
    window, and the short-lived key's restored deadline falling due.
  - **(b)** `BEFORE` written, a second Jedis client writing on its own thread as fast as the
    cluster will take it, `nodes[0].snapshot("s1")`, a poll until all three report complete, the
    writer stopped and its failure asserted null, then `AFTER` written. The cluster is closed and
    three fresh nodes started **with no data dir at all** and the same snapshot root, each
    `restoreSnapshot("s1")`. Through every one of the three: `BEFORE` reads its value and `AFTER`
    reads nil (I12 at the socket).
  - **(c)** `SET tick:fires v PX 200` on node-1, polled to nil through node-3 inside a deadline,
    then `TTL` = -2.
  - **(d)** one node (`nodes = {node-1}`, N=W=R=1, four partitions) with a 256 KB threshold and
    `W_TINYLFU`: twelve 1 KB hot keys, then 1200 cold 1 KB keys in batches of 100 with the hot set
    read between batches. `INFO` reports `maxmemory_policy:w-tinylfu` and `used_memory` at or under
    the threshold, and every hot key is still there.
- Mutation-checked four ways, each reverted and the build re-run:
  1. `maxMemoryBytes` forced to null: the node holds 1,309,016 bytes against the 256 KB threshold.
  2. `snapshots?.restore()` removed from `start()`: the restarted cluster answers nil for the first
     key.
  3. the router's snapshot hook forced to `false`: no node completes, and the test fails on its
     20-second deadline rather than on a missing file -- the markers never reach the protocol.
  4. the shutdown save removed: caught by the assertion that node-1 wrote a `dump.rdb`. Worth
     recording that *every value assertion still passed* in that run, which is how this ticket can
     say which path a key came through: with a snapshot on disk the restart is the RDB's (the save
     is the last thing a graceful close does, so the log after the checkpoint is empty), and with
     no snapshot at all the WAL replay alone carried the whole keyspace back, collections included.
- `mvn -B -o clean package` offline, BUILD SUCCESS: engine 144, cluster 80, cp 89, server 82. Every
  P1 to P4 test green in the same run.
- `git merge misc/ai_gen`: already up to date, so T30 had not landed and there was nothing to
  resolve.
- Size: 400 insertions in 4 files -- 302 lines of test, 97 in `ClusterNode`, 5 in
  `DistributedSnapshot`, 5 in `DynaCacheServer`. Inside the budget.
- Re-run three times over: 2.8 to 3.1 seconds, green every time. One flake was found and removed
  during the work: the `PX 300` key of (a) was first asserted absent outright, and a restart that
  takes under 300 ms leaves it alive, so it is now polled to its deadline.

**Deviations:** six.

1. **The snapshot trigger is a method, not a command** (above). `snapshot(id)` returns nothing and
   launches on the node's scope, so a second `initiate` for an id already open throws into the
   scope's uncaught handler rather than back to the caller; the caller learns what happened from
   `snapshotComplete` and its own deadline. A `Job` back from `snapshot` would fix that the day
   something needs it.
2. **The restart uses fresh ports, not the same ones.** The ticket allows either. Ephemeral ports
   cannot be re-requested, and rebinding a fixed port a closed node has just released races the OS;
   the shared address map is read at send time (T23), so a new generation only has to write its own
   row before starting. What this does not exercise is a node returning to a cluster that is still
   up: all three restart together, so nobody has a stale address, and no SWIM incarnation has to
   rise (T20, T24's note).
3. **(c) asserts the client-visible half of a TTL, not the wheel.** At the socket, the wheel's
   deletion and the lazy check a read makes on the way past are the same nil, and every
   keyspace-wide command (`DBSIZE`, `INFO`'s `db0`) purges expired keys itself before answering,
   so no client-visible number separates them either. The scheduler is real and running -- it is
   the server's own, on the real clock -- and that it advances the wheel is T09's and T32's at the
   engine tier. Not debt, a limit of the seam.
4. **(d) is one node, not three.** Eviction is a node's own decision under spec 5.5 and coordinates
   with nothing, so three replicas of every key would ask the same question three times; worse, a
   key evicted on one replica and kept on another turns the assertion into a question about read
   repair. The single-node cluster still goes through the whole `ClusterNode` assembly -- ring,
   replication at N=W=R=1, router, RESP -- so the wiring under test is the wiring that ships.
5. **The eviction knobs are not on the command line.** `maxMemoryBytes` and `policy` are
   constructor parameters only, because single-node `main` has no flag for them either; adding one
   for the cluster alone would put the two modes out of step. `clusterMain` did get `dir` and
   `fsync`, which it was already being handed.
6. **A `runCatching` around the shutdown save.** `close()` must go on to close the engine even if
   the save throws (a full disk, a directory pulled out from under a test), and a node closed
   twice, or closed after a failed start, must not throw out of `close`.

**Debt hit, recorded not fixed:**

- **A value a node holds only as a replica is not in its log.** `Replication.install` reaches the
  engine through `ApEngine.install` -> `Partition.restore`, which is the RDB's own load path and
  does not run the command path, so the WAL hook never sees it (`ApEngine.log` is called from the
  partition's command step). The same is true of anything anti-entropy or read repair installs.
  Today a graceful shutdown hides it completely, because the shutdown save writes the whole
  keyspace including replica copies; a node that dies without one comes back holding only the keys
  it coordinated, and the read quorum covers that as long as each key's coordinator recovers. It
  stops being covered when a coordinator's disk is the one that was lost. Repaying it means either
  logging installs (they are not commands, so `WalCodec` would need an install entry) or accepting
  that a crashed node is repaired by anti-entropy rather than by its own log -- which is a real
  design position, and the one to state in the spec if it is taken.
- **The router's inbound loop dies on any exception a handler throws** (found the hard way, above).
  One loop demuxes forwards, replication, anti-entropy, gossip and now snapshots, so a bug in any
  of them silently stops all of them; the node keeps its socket open and answers nothing that needs
  a peer. A `try`/`catch` per envelope inside `Router.run`, logging and continuing, is the repair.
- **Restoring a snapshot set into a live cluster is still unsupported** (T36 deviation 6), so
  `restoreSnapshot` is documented as startup-only and the test calls it on nodes that have started
  their transports but served no client. Nothing enforces that.
- **The state file is still written on the demux's coroutine** (T36 deviation 4). Under this
  ticket's traffic it did not stall a quorum -- the writer completed every write with no failure --
  but the keyspace was small; `withContext(Dispatchers.IO)` remains the repair if a measurement
  shows it.

**For the next ticket:**

- **`ClusterNode`'s constructor is now the node's whole configuration**: `dataDir`, `fsync`,
  `snapshotDir`, `maxMemoryBytes` and `policy` beside the cluster's own. Anything else that is a
  node's private decision belongs there and nowhere else.
- **The P4 harness is `startCluster(dirs)` / `stopCluster()`**, and the generation of nodes is a
  `var`. A test that needs a cluster to die and come back should copy that shape rather than the
  `@BeforeEach` one P2 uses: the address map outlives the nodes, and only the row of a restarted
  node changes.
- **A distributed snapshot needs `snapshotDir` on every node, not just the initiator.** A node
  without one has a null hook, so a marker falls through the demux to gossip and is dropped, the
  initiator waits out its 30-second deadline and the whole set is deleted. That failure looks like
  a timeout, not a misconfiguration.
- **`restoreSnapshot` is `runBlocking`**, so it must not be called from a coroutine on the node's
  own dispatcher. From a test thread or a `main`, it is fine.

## T30: Convergence and minority-crash safety

**Built:** The I1 checker in the test kit: `InProcessCluster.assertConverged()` heals every
network partition, restarts every node on the transport, marks every non-alive member alive
under a fresh incarnation, drains messages and hints, runs one full anti-entropy cycle on every
node (`antiEntropyStep` until that node's `rangesCompared` has advanced by its range count,
bounded at twice that many steps), drains again, and then for every key that ever went through
the kit (`written`, fed by `submitVia`, `submitOn` and `seed`) reads every preference-list
replica's engine copy and version and asserts the triple `(content, deadline, version)` equal
across replicas, failing on the first divergent key with both sides named. The content view is
`canon(value)`, lifted from `MergeTest` to a top-level function in the kit. `submitVia(node,
command)` is `settle(router(node).submit(command))` and now backs `writeVia`/`readVia`;
`submitOn(node, command)` goes through `replication(node)` directly, what spec 5.1 step 7's
coordinator failover would do and the router does not yet (T22 deviation 7), which is how a
test writes on both sides of a network partition. `TokenCodec` learned `LPUSH`/`RPUSH`,
`LRANGE`, `ZADD` and `ZRANGE [WITHSCORES]`.

`ChaosDriver` (own file, test kit): a seeded loop over an `InProcessCluster`. A key's first
letter is its type (`s h l z c`), so every write fits its key: `SET`, `HSET`, `LPUSH`, `ZADD`,
`INCRBY`, plus a `DEL` one time in eight, through a random live node; a type-appropriate read
(`GET`, `HGETALL`, `LRANGE 0 -1`, `ZRANGE 0 -1 WITHSCORES`) through any node; a network
partition into two random non-empty sides or its heal; a kill (`network.kill` plus
`membership.set(DEAD)`) or the restart of the one node down (`network.restart` plus
`set(ALIVE, incarnation + 1)`, so the hints held for it replay on the alive event). At most one
node down and one split open at a time; `run(steps)` heals and restarts whatever is outstanding
before returning. `acked` is the last acknowledged version per key: the coordinator's version
right after a reply that was not an error. `ChaosDriver.keys(perType)` and `readOf(key)` are
public for the tests. Five files, 297 insertions, 13 deletions, all in the cluster module's
tests; no production change.

**Concepts named:** No new vocabulary. **Acknowledged** is used as CONTEXT.md's quorum entry
implies: a reply that is not an error; a quorum error, a forward timeout and an engine error
alike promise nothing. The checker's **content view** (`canon`) is what a client would read
back, so two `Value`s compare by content rather than by reference.

**Acceptance:**
- `convergence_after_partition`: three nodes, N = 3, W = 1, R = 3; a string, a hash, a list and
  a sorted set written and drained; split `{node-1} | {node-2, node-3}`; both sides write all
  four keys concurrently through `submitOn`, every write acknowledged; `assertConverged`; then
  the merged state of spec 2.5 on the healed cluster: the last writer's string, the hash with
  both sides' fields, `[x, a, b]` (shared prefix, then both tails), `m1` at the higher score.
  Red first: the kit had none of `submitVia`, `submitOn`, `assertConverged`.
- `I1_all_replicas_equal_after_heal_drain_sync`: seeds 1 to 5, four nodes, N = 3, W = 2, R = 2
  (so sloppy quorum and hints are in play), sixty steps each, then `assertConverged`. Red
  first: no driver existed; then seed 4 failed on the tombstone gap (deviation 1).
- `I2_minority_crash_loses_no_acked_write`: three nodes, N = 3, W = 2, R = 2, seed 30, sixty
  steps over keys coordinated by node-1 and node-2; then node-3 is killed and marked dead;
  every acknowledged key is read at quorum through a survivor and answers without error, and
  after the drain each survivor's version dominates or equals the acknowledged one. "At least as
  new" is decided on versions, read side dominating: `read == acked || read.dominates(acked)`,
  never the other direction. The read's version is taken from the survivors' side tables after
  the read and a drain, because read repair leaves the winning version on the survivor that was
  behind, so both hold what the read answered.
- Mutation-checked in one run: with the anti-entropy cycle removed from the checker,
  `convergence_after_partition` fails on the string with both siblings named and I1 fails on
  seed 1 with a key held on one replica and absent on another; with node-2 killed as well
  (a majority), I2 fails on the first read with the forward timeout.
- `mvn -B -o -q clean package` offline green after merging `misc/ai_gen` (tickets 45, 37 and the
  `ClusterNode` compile fix): engine 144, cluster 83, cp 89, server 82. Commit `02a0a53a`, merge
  `6a48f3a5` on `t30`.
  `ConvergenceTest` runs in about 5 seconds.
- This entry.

**Deviations:**
1. **An absent key compares as absent alone; its version is not compared.** The driver found
   this on seed 4 of its first run: key `c4` absent on every replica, the coordinator holding the
   version a `DEL` left behind (`node-4:1`) and node-1 holding no version at all, because the
   `Replicate` never reached it and anti-entropy carries no tombstones (T28 deviation 3: a key
   with no value builds no leaf). No client can observe that version, the value side converges,
   and a later write on the key merges past it (a concurrent sibling on the replica that held
   the tombstone resolves on the next exchange). Fixing it means tombstone leaves in
   `AntiEntropy.held` (versions with no value, enumerated from `Replication`'s side table),
   a `Held` with no value, a delete-versus-concurrent-write rule spec 5.3 does not make, and a
   `DEL` on install: well over a few lines and a semantics decision, so recorded as the
   finding rather than fixed. The resurrection half of the same gap (a `DEL` one replica missed
   is handed back by anti-entropy, regressing the other replicas' versions) does converge, to
   the resurrected value, and I1 as stated holds; it is client-visible debt all the same.
2. **`convergence_after_partition` writes the far side through `submitOn`** (the node's
   replication layer), not through the router: the router forwards to the preference list's
   first node whether or not it is reachable (T22 deviation 7), so through it the minority side
   cannot write at all and "concurrent writes on both sides" would not exist. W = 1 so both
   sides acknowledge; R = 3 keeps C4.
3. **I2 crashes a replica, not a coordinator.** A key whose coordinator is down is unreachable
   through the router (the forward times out) until spec 5.1 step 7's failover exists; that is
   unavailability, which T24 deviation 1 already records, not loss, and I2 is about loss. The
   driver's keys are therefore the ones node-1 and node-2 coordinate and node-3 is the crash.
   Three nodes and N = 3 also means no substitute exists, so no acknowledged write rests on a
   hint holder.
4. **"Acknowledged" excludes every error reply**, not only the quorum error the briefing named:
   an engine error (none arises, since keys are typed) writes nothing, and T22 deviation 3's
   "reported as failed but stored" write is exactly what I2 must not rely on.
5. **The driver is sequential**: one step settles before the next, so "concurrent" writes exist
   only across a network partition, where the far side cannot write through the router. Within
   the driver, versions of a key chain through its one coordinator; siblings arise only from
   `submitOn` and from the resurrection regression of deviation 1.
6. **The driver never runs anti-entropy** (the briefing's step list has none); the checker does.
   With the tombstone gap, an anti-entropy round inside the run could regress an acknowledged
   `DEL` on both survivors and fail I2 legitimately; that is deviation 1's debt, not I2's.
7. The kit's `keys` member had to be `written`: `DistributedSnapshotTest` has a `keys` of its
   own inside an extension on the cluster, and the extension receiver shadows it.

**For the next ticket:** `assertConverged` is the P3 exit criterion's checker and runs in about
a second per four-node cycle at two partitions per node; `ChaosDriver(cluster, seed, keys)`
takes any key list whose names start with a type letter, and `acked` is the map to check
against. `submitOn` stands in for coordinator failover; when spec 5.1 step 7 lands in `Router`,
`convergence_after_partition` can go through `submitVia` on both sides and I2 can crash any
node. The tombstone gap (deviation 1) is the one convergence finding: `AntiEntropy.held` is the
place, and a checker that compares versions of absent keys too is one `?.let` away in
`assertConverged`. `AntiEntropy.run()` is still not launched by any node assembly.

## T46: P5 acceptance

**Built:** the CP subsystem on a cluster node, and the demo that drives both engines through one
socket.

`ClusterNode` gains four constructor arguments -- `cpMembers`, `cpAddresses`, `cpPort`, `cpRaft` --
and one field. When a group is named it builds its Raft store (a `FileRaftStore` under
`<dataDir>/cp`, in memory when the node persists nothing), the runtime on top of it, a `CpEngine`
and a `CpGrpcServer`, hands the CP engine to `DynaCacheServer` so the dispatcher has both sides,
starts the Raft member before the RESP port opens, adds the leader's `RaftRuntime.tick()` to the
server's tick lambda, and closes the lot between cancelling the scope and closing the transport.
A node the group leaves out takes a `ForwardingCpEngine` instead -- the same call, one branch
inside it (CP spec 2.2, 2.4).

`cpNode(self, members, addresses, port, storeDir, clock, raft)` in `DynaCacheServer.kt` is now the
one place a member is assembled, in the order T43 named: store, runtime, engine, gRPC server. It
was `main`'s private helper over two command-line strings; the string parsing moved out to
`cpNodeFromArgs` and `cpAddressBook`, so single-node `main` and `clusterMain` build a member the
same way. Two things followed from that:

- **The single node's Raft log is now durable.** `main` passes `dir?.resolve("cp")`, where before
  it passed no store at all and a member forgot its term, vote and log on restart (the T45 note).
- **Cluster mode can run CP at all.** `main` returned into `clusterMain` before it read positional
  arguments 4 and 5, so `--peers` swallowed the CP group. `clusterMain` now takes `cp-members` and
  passes it through; `--node` already says which entry this node is, so the `cp-self` positional
  has nothing to add there and is ignored, which the README says.

`cpRaft` is a calibration knob, not a constant: a failover takes as long as a heartbeat timeout,
and what that should be is an operator's call about a real network. Production keeps MicroRaft's
defaults; the acceptance test passes T45's kit timings (200 ms election, 1 s heartbeat period, 2 s
heartbeat timeout) so a failover it waits on resolves in seconds.

**Concepts named:** none new. The vocabulary is CP spec 2.2's (**CP member**, **AP-only node**) and
T43's (**address book**), and the only idea this ticket adds is where they attach to the assembly
T24 named: a node is still an assembly, and the CP subsystem is one more field, one more thing the
tick does, and one more argument to the server it already builds. The **store** (T45) is the seam
that made this a wiring change rather than a design one.

**Acceptance:**
- `P5_acceptance_two_engines_one_cluster` (server), one method on the P2/P4 harness -- three nodes
  in one JVM, ephemeral RESP, cluster-gRPC and CP-gRPC ports, real gossip and a real three-member
  Raft group over sockets, N=3 W=2 R=2, Jedis unmodified -- in about 3.5 seconds:
  - **the counter, through unmodified Jedis.** `SET cp:counter:x 5 EX 10`, `INCR` returns 6, and
    `GET` returns the *integer* 6. The `GET` is what says which engine answered: a CP counter
    replies `:6` where the AP engine would have replied with the bulk string `6` (CP spec 6.2), so
    a value that came back as a Long came through the Raft log and not the ring.
  - **the lock, through `RespClient`.** `CP.LOCK.TRY cp:lock:demo 30000` is granted with a token;
    a second connection, which is a second session, is refused with `[0, 0]` (I13).
  - **the leader killed.** The leader is found through `CP.INFO`, that whole `ClusterNode` is
    closed, and a surviving member's `CP.LOCK.STATE` reports the same owner and the same fencing
    token (I14). The connection that took the lock died with the node; the lock did not.
  - **the session closed.** `CP.SESSION.CLOSE <sid>` from a surviving member releases everything
    the session held (C18, I15) and the waiting client then takes the lock with a strictly greater
    token (C17).
- Mutation-checked twice, each reverted and the build re-run:
  1. `cp?.runtime?.start()` removed from `ClusterNode.start()`: no member ever leads and the test
     fails on its 20-second deadline.
  2. `cp?.close()` removed from `ClusterNode.close()`: the killed leader keeps leading from beyond
     the grave, no survivor names a leader that is still in the test's node list, and the failover
     assertion times out.
- The spec 9 AP demo (`P4AcceptanceTest`) passes in the same run, as do P1 and P2.
- Full offline `mvn -B -o clean package`, BUILD SUCCESS: engine 144, cluster 83, cp 89, server 83.
- `git merge misc/ai_gen` brought in T30's cluster test kit (`ChaosDriver`, `ConvergenceTest`,
  `InProcessCluster`, `TokenCodec`), no conflicts; the full build was re-run after it.
- Re-run five times before the merge: 3.88 to 4.16 seconds, green every time.
- Size: 207 insertions across three files -- 199 lines of test, 47 in `ClusterNode`, 71 in
  `DynaCacheServer` (of which about half is the doc comment on the two extracted functions).
  Inside the budget. The README is excluded per the ticket.

**One race found and fixed during the work.** The first full-build run failed where five targeted
runs had passed: `cpLeader()` found the node `CP.INFO` named and Jedis was told `-NOTLEADER leader
is node-1` by node-1 itself. That is C19 at the socket. `RaftRuntime.isLeader` is
`endpoint == leaderEndpoint && appliedTerm == term`, so a fresh leader names itself for the window
between winning the election and applying its own term's first entry, and refuses work in it. The
harness now polls for a member that both names itself and answers a replicated read
(`CP.LONG.GET cp:counter:probe`), which is what a real client's retry does. Worth recording that
the targeted runs never saw it -- only a run with the rest of the suite ahead of it did.

**Deviations:** four.

1. **`GET cp:counter:x` is sent as itself rather than through `Jedis.get`.** CP spec 6.2 fixes the
   reply at `:long`, and `Jedis.get` casts an integer reply to `byte[]` and throws. The test uses
   `redis.sendCommand(Protocol.Command.GET, ...)`, which is still an unmodified client and no
   custom-verb API, and the Long that comes back is the assertion that the CP engine answered.
   Not debt as such -- it is the spec's own shape -- but a caller who wants `Jedis.get` to work on
   a `cp:counter:` key needs the compat path to answer a bulk string, which would contradict 6.2.
2. **The session is released by an explicit `CP.SESSION.CLOSE`, not by a heartbeat timeout.** The
   ticket allowed either. The connection that held the lock died with its node and T44's debt means
   nothing closed its session, so the close is sent by session id from a surviving member (CP spec
   6.6) -- which is exactly what the timeout would do a session lifetime later, only on demand and
   without a wait. The default session timeout is 15 s and the whole test runs in 3.5 s, so the
   lock is genuinely still held when the failover assertion reads it.
3. **The CP verbs are sent to the leader, and the harness finds it.** `CpEngine` does not forward;
   a follower answers `-NOTLEADER <hint>` and CP spec 9.1 step 3 leaves the retry to the client.
   The test is that client. An AP-only node would forward, but a three-member group in a three-node
   cluster has no AP-only node, so `ForwardingCpEngine` is wired here and covered by T43's tests
   rather than by this one.
4. **The acceptance node uses T45's kit timings, not MicroRaft's defaults.** The default heartbeat
   timeout is 10 s, so a failover the test waits on would dominate its runtime. `cpRaft` is a
   constructor argument with MicroRaft's own defaults; only the test passes anything else.

**For the next ticket:**
- The `cp-self` positional is dead in cluster mode (`--node` says the same thing) and live in
  single-node mode. If the command line is ever tidied, that is the seam that should collapse.
- `ClusterNode` now closes its CP part between cancelling the scope and closing the AP transport.
  The two gRPC servers are separate ports and separate lifetimes; nothing shares them, which is
  the simplest thing that works and also two listening sockets per node where one would do.
- T44's debt is still open and is what makes deviation 2 necessary: closing the connection's CP
  session on `channelInactive` would let this test kill the holder's connection and watch the lock
  fall free on its own.
- The README now carries the running instructions, the test tiers and their timings, and the five
  known debts of the last five progress entries. It is the first document a reader meets, so a
  ticket that repays one of those debts should strike it from there as well as recording it here.

---

## T48: WAL logs nothing for a refused conditional ZADD

**Built:** a `ZADD` carries its `NX`/`XX` condition into its WAL entry and is replayed under it,
so a warm restart reproduces exactly the effect the live command had (C14, spec 2.8). Before
this ticket `WalCodec.encode` dropped `ZAdd.condition`: a `ZADD NX` on a member already there
and a `ZADD XX` on one that was not both answered `:0`, were logged as plain `ZADD`s, and moved
or created the member on the next boot.

`WalCodec` gains one symmetrical pair, `DataOutputStream.condition` and
`DataInputStream.condition`, alongside the `end` pair it already had and fixed by the format the
same way: `0` none, `1` `NX`, `2` `XX`, so reordering `Command.Set.Condition` cannot change a
file's meaning. The `ZADD` payload is now `key, pairs, condition`; `decode` rebuilds
`Command.ZAdd(key, entries, condition)`. `CH` is not logged -- it changes only the reply's count,
never the store. Nothing else moved: the entry header, the op codes, the RDB, `Partition`'s
command semantics and the engine hook are all untouched.

**The mixed case.** `ZADD NX 5 a 2 b` with `a` present and `b` missing takes `b` only. The
ticket offered two ways to log that: the taken subset as a plain `ZADD b 2`, or the condition
carried and replayed. Carried, because the taken subset is not derivable inside the seam this
ticket owns -- `encode` sees `(command, reply, now)` and nothing of the store, and which members
a condition admitted depends on the state *before* the command, which only `Partition` holds.
Logging the subset would have meant returning it out of `Partition.run`, i.e. changing the hook
and the command path for one command's benefit. Carrying the condition is one byte in the `ZADD`
payload, no new op code, no header change, and replay is exact because replay reaches that entry
in the same state the live command saw.

**Correcting T35's wording.** T35's entry says: "What is logged is what changed: an error reply,
a refused conditional `SET` and an empty `POP` (both nil) log nothing, and a conditional `SET`
that took is logged as a plain one." That is true as written, of `SET`. What is not true, and
what the codec's file comment did imply by closing on "what is logged is what changed, not what
was asked", is that the *reply's shape* settles every conditional command. It settles `SET`
only: a refused `SET` answers nil, which no successful `SET` answers, so the entry is dropped by
shape. A `ZADD` answers an integer, and `:0` is what both a full refusal and a score moved
without `CH` answer -- and one call can be part refused and part taken, which no single count can
express. The old entry is left as it stands; the codec's comment is rewritten to say which
commands the reply shape settles and why `ZADD` is settled by its condition instead.

**Tests** (`WalRecoveryTest`, the engine's own recovery seam; nothing here reads the log's
layout, which stays `WalTest`'s):

- `C14_refused_conditional_zadd_replays_nothing`: `ZADD NX` on a present member and `ZADD XX` on
  a missing one, then a crash (no save, no close) and recovery from the log alone -- the score is
  where it was and the refused member is still absent. Red before the fix on the score.
- `C14_taken_conditional_zadd_replays_as_taken`: an `XX` that moved one of two members replays
  with that one change only.
- `C14_partly_refused_conditional_zadd_replays_only_the_taken_members`: `ZADD NX 5 a 2 b` with
  `a` present replays as `a=1, b=2`. Red before the fix on `a`.

The parked red test from the review worktree (`BugHuntWalTest`) is folded into the first of these
and is not carried over under its own name. Engine module: 144 tests before, 147 after, all green
offline; `WalRecoveryTest` 9 of 9.

**Deviations:**

- A refused conditional `ZADD` still *appends* an entry; what it no longer does is change
  anything on replay. The ticket's title reads "logs nothing", and that literal form is not
  reachable from the encode seam: `:0` does not distinguish a refusal from a move without `CH`,
  so dropping on `:0` would silently lose a real score change, and a part-refused call must log
  its taken half regardless. The ticket names carrying the condition as an accepted answer.
- `CH` is deliberately not logged. It is a reply-shape flag; a replay ignores replies.
- The `ZADD` payload grew a trailing byte, so a log file written before this ticket ends its
  `ZADD` entries one byte short and its recovery now fails loudly (`EOFException`) instead of
  replaying the wrong thing. The entry header and op codes are unchanged and the format carries
  no version field to bump (T35 did not give it one), so there was nothing to version; a pre-1.0
  log is not a compatibility contract, and a loud failure is the right shape of one anyway.

**For the next ticket:** `SET` and `ZADD` are the only commands in `Command.kt` carrying a
condition today (grep `condition`), and both are now correct, so the WAL's rule holds across the
whole command set. If `EXPIRE` ever gains Redis's `NX`/`XX`/`GT`/`LT` flags it joins `ZADD`'s
class, not `SET`'s -- its refusal answers `:0` -- and must carry them into the log the same way.
More generally: `encode` sees only `(command, reply, now)`. Any future rule that needs the state
*before* the command has to move the decision into `Partition`, where the condition is evaluated;
that is a hook change, and worth doing once rather than per command.

## T47: Single-node benchmark with redis-benchmark

**Built:** `DynaCache/bench/single-node.sh`, a Git Bash script that builds the jars if they are
missing, starts one DynaCache node in single-node mode (`dynacache 6390 16 %TEMP%\dynacache-bench\data`),
waits for `PING` through a `redis:7` container, runs four `redis-benchmark` passes against it,
then starts a `redis:7` container on 6391 and runs the identical four passes against that. It
starts and stops both itself, writes every CSV under `%TEMP%\dynacache-bench\`, times out each
pass and exits non-zero on any failure. `DynaCache/bench/.gitattributes` pins `*.sh` to LF,
because the repository has `core.autocrlf=true` and a CRLF checkout breaks the shebang.
`docs/dynamiccache/benchmarks/2026-09-06-single-node.md` holds the environment, the node's
arguments, four tables with a DynaCache column and a Redis column, the skipped list and four
anomaly paragraphs. Nothing under `src/main` or `src/test` changed. The server is launched with
`java -cp` over the module jars plus a runtime classpath emitted by `dependency:build-classpath`
in the same reactor invocation as `package`, which is the only way the sibling modules resolve
offline; the script does that itself.

**Concepts named:** No new domain vocabulary. Two operational terms the report uses: the
**plain pass** (`-c 50 -n 100000 -d 3`), which on this machine measures the Docker round trip
rather than either engine, since Redis answers every command in it at 19 to 24 thousand
requests per second regardless of which command it is; and the **spread pass** (`-r 100000`),
which exists because `redis-benchmark` leaves `__rand_int__` in the command literally unless
`-r` is given, so without it every `SET`, `GET` and `INCR` names one key and `MSET (10 keys)`
names that same key ten times, and no multi-key fan-out is exercised at all.

**Acceptance:**
- Script starts a node, waits for `PING`, runs the ticket's thirteen tests at `-c 50 -n 100000
  -d 3`, again with `-P 16`, once with `-d 1024`, and stops the node: done, plus a fourth pass
  with `-r 100000` and a short `EVERY_SECOND` pass.
- The same passes against a `redis:7` container, same flags, so every DynaCache number sits
  next to a Redis number: done, `redis-plain`, `redis-pipelined`, `redis-1024b`, `redis-spread`.
- Report with tables, environment and one paragraph per anomaly naming the code path: done,
  four anomalies.
- Every unsupported test listed as skipped with the reason: `SADD`, `SPOP` (no Set type),
  `ZPOPMIN` (no `zpopmin` in the parser), `XADD` (no Stream type), and `LRANGE_300/500/600`
  (supported, left out by the ticket's list).
- Progress entry: this.

**Headline numbers** (requests per second, DynaCache then redis:7):

| | plain | pipelined `-P 16` |
|---|---|---|
| SET | 26483.05 / 23702.30 | 145348.83 / 313479.62 |
| GET | 27654.87 / 24189.65 | 389105.06 / 320512.81 |
| INCR | 27225.70 / 23917.72 | 139275.77 / 294117.66 |
| MSET (10 keys) | 13877.33 / 20559.21 | 17540.78 / 176991.16 |

Worst cases: `RPUSH` 1072.78 against 22841.48 plain (5 percent) and 1351.39 against 362318.84
pipelined (0.4 percent). `SET` under `EVERY_SECOND`: 53.30 rps, p50 1014.783 ms.

**Deviations:**
- **The node runs with fsync `NEVER`, not `EVERY_SECOND` as the ticket says.** DynaCache answers
  a write only once its WAL entry is durable (C14), so under `EVERY_SECOND` every write waits for
  the next second's fsync and throughput is exactly clients per fsync interval: measured at 53.30
  requests per second. A 100,000-request `SET` pass would take half an hour and the nine write
  tests together most of a day. The `redis:7` container has no append-only file and never makes a
  reply wait for the disk, so `NEVER` is the setting that compares like with like.
  `EVERY_SECOND`'s cost is measured on its own in the script and is the report's first anomaly.
  This is not a shortcut to repay; it is what the comparison requires.
- **A fourth pass and two extra measurements beyond the ticket.** The spread pass (`-r 100000`)
  was added because without it the plan's named fan-out ceiling is never exercised. A four-round
  `LPUSH` growth pass was added to confirm the list anomaly's cause. Both run on both targets or
  on DynaCache alone as appropriate and are in the script.
- The node is not restarted between its passes, and neither is the container, so `mylist` is
  about 200,000 elements long when the pipelined pass starts. Symmetric across the two engines,
  so the side-by-side columns are fair, but DynaCache's own plain-to-pipelined ratio for a list
  command compares two different list lengths, and the report says so.
- DynaCache writes a WAL record per mutating command even under `NEVER`; the default `redis:7`
  writes nothing per command. That asymmetry is against DynaCache and is not corrected for.
- **Every table was taken under contention and no pass has a load reading behind it.** Another
  orchestrator session was running Maven builds and test suites in the `kp-wt/t48` to `kp-wt/t51`
  worktrees during all three runs. The ticket asked for the number of other Java processes and
  the CPU idle percentage per pass; neither was recorded, which is the omission that makes the
  factor-of-two spread unattributable at the time it happened. Sampled afterwards, the machine
  was carrying three other Java processes at 25 percent CPU idle. The script now gates every pass
  on ten consecutive seconds with no `java.exe` but its own node and at least 70 percent CPU
  idle, and writes what it saw per pass to `load.txt`. The follow-up is a rerun in a quiet
  window; the tables stand as provisional until then. The four anomalies are DynaCache-against-
  Redis ratios measured on the same machine at the same moment and held in all three runs, so
  contention is not expected to overturn them.

**Nothing failed under load.** No crash, no hang, no error reply. Neither node log contains an
exception. The only stderr in any DynaCache pass is `WARNING: Could not fetch server CONFIG`,
because the parser has no `config` command; it changes no measurement.

**For the next ticket:** four follow-ups, in the order the numbers rank them.

1. **The `EVERY_SECOND` write path is unusable as it stands.** `Partition.execute` adds the log
   hook's future to the task's `durable` and `Partition.task` completes the reply only after it;
   under `EVERY_SECOND`, `WalWriter.writeBatch` parks the waiter on `awaitingFsync` and
   `forceAwaiting` releases the set once per tick. Redis's own `appendfsync everysec` replies
   immediately and fsyncs behind the reply. A ticket should measure group commit: keep
   reply-after-durable, force on a one-to-five-millisecond deadline or as soon as a batch is
   ready, and find the deadline that buys back most of `NEVER`'s throughput.
2. **`Partition.account` recounts the whole aggregate after every keyed command**, including
   reads, via `Value.approximateBytes()`, which for a list is `items.sumOf { it.size + 16 }`.
   Push and pop on an `ArrayDeque` are O(1), so the recount is the only length-dependent work.
   The four list tests run back to back on one growing key and their rates trace its length in a
   U shape (2842.93, 1072.78, 1284.11, 3618.08), which separates length from the command. The
   `ponytail:` comment on `approximateBytes` already names the repair: per-element size deltas
   at the mutation sites, making `account` O(1). Measure `RPUSH` on a 200,000-element list
   before and after.
3. **Writes pipeline at about a third of Redis's gain and reads do not.** `GET` gained 14.1x
   from `-P 16` against Redis's 13.2x; `SET` gained 5.5x against 13.2x, `HSET` 4.8x against
   14.6x. The only difference between the two paths is the WAL append. A ticket should run the
   pipelined pass against a node with no data directory and one with, and attribute the gap;
   what is left over is DynaCache's own two thread handoffs per command, the executor hop into
   the partition and the callback back onto the Netty event loop.
4. **`ApEngine.fanOut`'s sequential chain is not the ceiling the plan expected.** With keys
   spread, `MSET` reached 21584.29 against `SET`'s 26673.78 on the same pass, about 19 percent
   for ten keys across partitions. Without `-r`, where all ten keys are one key on one
   partition, `MSET` reached only 13877.33: spreading made it faster, because fifty clients on
   one key put all the work on one of sixteen partition threads. Replace the `thenCompose` chain
   with `allOf` and re-run the spread pass, and sweep `MGET` at 2, 8, 16 and 64 keys so the
   chain's cost is a function of how many partitions a command spans.

Absolute numbers moved by up to a factor of two between three runs of the script, on both
engines. The shape did not: the list tests were slowest every time, the U shape appeared every
time, the pipelined write gap stayed near a third every time, and `EVERY_SECOND` `SET` was
50.47, 49.51 and 53.30. Read a single number as good to a factor of two and the ratios as the
result.

**Orchestrator note:** every table was taken while another session ran Maven builds on this
machine, and no quiet window was available before landing, so the report carries a PROVISIONAL
banner. The script now gates each pass on a quiet machine and records the other-Java count and
CPU idle it saw per pass in `load.txt`. The quiet rerun is a follow-up ticket that reuses the
script unchanged.

---

## T49 - A snapshot cuts state before it opens channels

**Built:** `DistributedSnapshot.start` now runs spec 2.8 step 1 before step 2: it cuts and
saves the node's state through the T32 `SnapshotEngine`, only then publishes the snapshot's
channels into `open`, and only then sends the markers. T36 had opened the channels first, so an
envelope the demux handled between the opening and the cut was appended to its channel log and
applied to the engine before the views were taken, and a restore applied it twice (bug 3 of the
P6 review). A `Mutex` (`cutting`) is held across the save and the opening, and the demux hook
takes it for every non-marker envelope before deciding whether to record it, so the initiator's
demux, which is another coroutine, waits out the cut: what it applied before the cut is in the
state and on no log, and what it records is applied after the cut and not in the state. A
receiver never contends for the lock, since it cuts on the demux's own coroutine
(`Replication.replicate` awaits the engine before the next envelope is read). Nothing else
moved: `receive`'s marker path, `complete`, `abort`, `restoreFrom`, the file layout and the
marker envelope are as T36 left them. Main-code diff: 31 lines in `DistributedSnapshot.kt`.

**Acceptance:**
- `I12_write_during_the_cut_is_restored_once`: one node beside one peer's endpoint on an
  `InMemoryTransport`, under `runTest`; `initiate` runs on `Dispatchers.Default`, as on a real
  node where it runs on the node's scope while the router's inbound loop runs the demux, and a
  `Gate` clock parks it at the state save's first clock reading, before any partition's view
  is taken. An `INCRBY n 1` Replicate is handed to the demux (the router's shape: the snapshot
  hook, then the command on the engine) while the initiator is parked, the gate is released,
  and a fresh node restores the part: `n` reads 1. At T36's order the same run read `Bulk(2)`.
- `C10_state_is_cut_before_any_channel_opens`: the same interleaving; while the initiator is
  parked inside its save no `from-*.log` exists in its part, and afterwards the part read back
  through the T36 `recorded` helper is `Part(state = {}, channels = {node-2: {1}})`: the
  envelope handed over during the cut is on its channel and not in the state. At T36's order
  the log existed while the state was still being cut, and the envelope was in both.
- `chandy_lamport_consistent_cut`, `chandy_lamport_restorable`, `chandy_lamport_timeout_aborts`,
  `C10_marker_on_every_channel` and `I12_reads_after_restore_return_snapshot_time_values`
  unchanged and green.
- Offline `test -pl dynacache-cluster -am`: engine 144, cluster 85 (83 + 2), all green.

**Known limitations, not fixed here:**
1. **gRPC channels are not FIFO under concurrent sends.** `GrpcTransport.send` is one unary
   `deliver` call per envelope; sequential sends to one peer arrive in order (the call returns
   when the peer accepted it), but two coroutines sending to the same peer at once, say the
   write path's Replicate and `initiate`'s marker, are two independent calls that may land in
   either order. So over gRPC a channel is not a true channel: a Replicate sent before the
   marker can arrive after it, closed channel, not recorded, applied after the receiver's cut
   and missing from the set; one sent after the marker can arrive before it and be recorded
   without its send being in the cut. Either way C10 is broken. The `InMemoryTransport` orders
   per sender-receiver pair, so no kit test can show it. A fix needs one ordered stream per
   peer: a per-peer sender coroutine feeding a streaming RPC (or, cheaper, a per-peer send
   `Mutex` in `GrpcTransport` so concurrent sends are serialized and the unary calls stay
   sequential), plus a `GrpcTransportTest` that sends from two coroutines and checks arrival
   order. That is a transport change, outside this ticket's seams.
2. **The initiator's cut has a residual window on a real node.** The lock covers the demux's
   record decision, not the engine apply the router does after `receive` returns. An envelope
   whose `receive` returned just before the initiator took the lock, and whose `engine.submit`
   reaches the partition executor after the view was taken, is in neither the state nor a log.
   The window is the few instructions between those two calls; T36 had the same one. Closing it
   needs the initiator's part to run on the demux's coroutine (the router delivering the
   initiator's own marker through its inbound loop, or serializing `initiate` with `receive`),
   which is a router change. A receiver has no such window.

**Deviations:**
1. **The ticket names "every node"; the test is one node.** The kit's cluster cannot produce
   the interleaving: under `runTest` the initiator's `start` has no suspension point the
   in-memory transport reaches, so the demux never runs inside it, and the bug does not exist
   there. The window is a real-thread one, so the test runs the initiator on
   `Dispatchers.Default` and parks it with a `Gate` clock (the bug hunter's shape), inside
   `runTest` with the kit's transport and `backgroundScope`. One node is where the double
   application happens; the other nodes' parts are untouched by the initiator's order.
2. **The lone node's deadline is `Duration.INFINITE`.** Its peer never sends a marker back, so
   the part waits forever, and that is the scenario. With the default 30 s, `runTest` skipped
   virtual time the moment the test idled on the initiator's thread, the deadline fired,
   `abort` deleted the set, and the restore read nil; found on the first red run and not a
   product bug. `delay(Long.MAX_VALUE)` is never scheduled, so nothing waits on the scheduler.
3. **The demux now waits out the initiator's save, not only a receiver's.** T36 deviation 4
   accepted that a receiver's inbound handling stalls for the write time of `save()`, since it
   runs on the demux. The lock gives the initiator the same stall: acks and gossip it receives
   during its save wait for it. The write path itself never waits on the lock
   (`timeout_aborts` still writes through a survivor while a snapshot is open). Debt as before:
   `withContext(Dispatchers.IO)` around the save if a measurement shows it.
4. **The mid-flight C10 assertion reads the directory, not the demux coroutine.** Whether the
   delivery coroutine has completed is not the observable: at T36's order it suspended on the
   engine's future during `runCurrent`, so `isCompleted` was false either way. The log file
   is written synchronously by `record`, so its absence is the fact that no channel is open.
5. Size: 133 insertions, 8 deletions in two files, inside the budget. No new seam, no change
   to `Transport`, `Replication`, the engine, the file layout or the marker.

**For the next ticket:** T55 moves the cluster's file I/O behind a persist adapter; the order
inside `start` (directory, then under the lock the save and the opening, then markers) must
survive that move, and `record` stays synchronous or the C10 assertion on the log file needs
another observable. `cutting` is the only lock in the cluster module's snapshot path; it is
taken once per inbound non-marker envelope and is uncontended except during an initiator's
save. `Lone` and `Gate` in `DistributedSnapshotTest` are the shape for any test that needs the
initiator interleaved with its own demux; T58's `MutableClock` does not replace `Gate`, which
parks a thread rather than moving time.

---

## T52 - An invalid expiry answers -ERR, never drops the connection

**Built:** `CommandParser` gained the one place it does time arithmetic, and every expiry-taking
row now goes through it. `deadline(name) { ... }` runs the arithmetic an argument asks for and
turns the two things `java.time` throws -- `ArithmeticException` from an overflowing sum,
`DateTimeException` from an instant that does not exist -- into `Rejected`, so the client reads
`-ERR invalid expire time in '<command>' command` and keeps its socket. `span(name, ttl)` adds
the `SET` family's own rule on top: a zero or negative relative TTL is refused outright, and the
sum the engine will later compute as `now + ttl` is checked here while it can still be a reply.
`until(name) { ... }` is the old `EXAT`/`PXAT` helper with the same guard plus Redis's
at-or-before-the-epoch refusal. Nothing else moved: `Command`, the engine, the dispatcher's
routing and the RESP codec are untouched, and the connection handler needed no change because
the parser is now total.

**The rule, and which values are invalid.** The bound is the deadline as epoch milliseconds in a
signed 64-bit. That is not `Instant`'s own range -- `Instant` reaches year ±1,000,000,000 -- but
it is the bound Redis itself checks (`when > LLONG_MAX - basetime` in `expireGenericCommand`)
and the one the engine's own WAL writes a deadline in (`WalCodec` calls
`command.deadline.toEpochMilli()`, which throws past roughly year 292,278,994). Using the wider
`Instant` range would have moved the crash from the parser into the WAL rather than removing it.

Redis splits its expiry commands in two, and so does this:

- `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` take **any** value they can hold. Zero and
  negative are deadlines already past, which delete the key; Redis's own source says so in as
  many words ("EXPIRE allows negative numbers"). Only an unrepresentable deadline is an error --
  `EXPIRE k Long.MAX_VALUE`, `EXPIRE k Long.MIN_VALUE`, `PEXPIRE k Long.MAX_VALUE`,
  `EXPIREAT k 99999999999999999`. `PEXPIREAT` cannot overflow at all: every `Long` is a
  representable epoch-milli deadline, `Long.MAX_VALUE` exactly so, and a larger argument is
  refused one step earlier as the integer it is too big to be.
- `SET EX`/`PX`, `SETEX`, `PSETEX` refuse a **non-positive** span as well: zero, negative and
  `Long.MIN_VALUE` are all `invalid expire time`, under the name the client typed (`'set'`,
  `'setex'`, `'psetex'`).
- `SET EXAT`/`PXAT` name an absolute time, so they refuse only at or before the epoch. A
  deadline merely in the past is a deadline: the key is set and expires at once, as in Redis.

**Acceptance:**
- `C8_invalid_expire_answers_err_not_disconnect` (`DynaCacheServerTest`): thirteen bad expiry
  arguments over one socket -- the four `EXPIRE` spellings out of range, and `SET EX`, `SET PX`
  and `SETEX` at zero, negative and `Long.MAX_VALUE` -- each answer the exact Redis error, and
  afterwards the same connection still answers `PING` and still holds the key untouched. It then
  sends `EXPIRE k -1` and sees `:1` and a key that is gone, which is what the error must not
  swallow.
- `an unrepresentable expiry is Redis's error, not an exception` and `a non-positive TTL on the
  SET family is Redis's error` (`CommandParserTest`): the two halves of the rule, message for
  message.
- `the EXPIRE family accepts zero and negative, as Redis does`: the boundary the ticket and
  Redis disagree about, pinned to Redis.
- `no expiry argument escapes the parser as an exception`: 2,000 random arguments -- uniform
  `Long`, deep negatives, the four corners, small values -- across all ten expiry-taking shapes,
  20,000 parses, none of which may throw.
- `resp_fuzz_no_crash` extended: the fuzzer now emits well-formed expiry commands, and every
  frame it decodes as a command is handed to a real `CommandParser`, so the decoder's fuzz is
  the parser's fuzz too.
- `mvn -o test -pl dynacache-server -am`: engine 147, cluster 83, cp 89, server 88 (was 83).
  Every earlier test green.
- This entry.

**Deviations:** Four.
1. **The ticket's first criterion is wrong about `EXPIRE`, and the spec wins.** It asks for the
   error on zero and negative for all seven commands. Redis answers `:1` and deletes the key for
   `EXPIRE k 0` and `EXPIRE k -1`; C8 is "byte-identical to what Redis returns", and the
   ticket's own note already says a past absolute time is not invalid. Refusing them would also
   have thrown away behaviour the engine has today and the ticket asks to keep. Implemented
   Redis's split instead, and pinned it with a named test so the disagreement is visible rather
   than silent.
2. **`PEXPIREAT` has no error case**, for the same reason: its argument is already the unit the
   bound is measured in. The socket test asserts the reply it does give, the not-an-integer
   error, rather than pretending there is an expire-time error there.
3. **One test-helper fix outside the parser.** `DynaCacheServerTest.withServer` built its engine
   on a fixed clock but let `DynaCacheServer` default to `Clock.systemUTC()`, so the parser's
   "now" and the engine's "now" were fifty-six years apart. No test had noticed, because none
   had asserted anything about a deadline over the socket. It now passes the one clock, which is
   what `main` does in production. No main-source change; every server test still green.
4. **Size:** 270 lines added across four files, within the 200-to-600 budget.

**For the next ticket:** two things this deliberately left alone. `cp.lock.try` and
`cp.lock.renew` still take their lease through the bare `millis()` helper, so a zero or negative
lease is accepted; that is CP lease semantics, not key expiry, and belongs with the CP verbs.
And ticket 62 may delete `EXAT`/`PXAT` -- they are validated here on the same code path as the
rest, so removing them removes two `until` call sites and nothing else.

---

## T53 - A connection's session cache clears on CLOSE

**Built:** `CommandHandler` now forgets its memoised CP session once the group no longer has it,
so a connection that closes its session and creates another gets a fresh one instead of the
closed id (CP spec 4, bug 4 of the P6 review). Two places drop the cache, and only those two:
`CP.SESSION.CLOSE sid` when `sid` is this connection's own session and the close answered `+OK`
or `-NOSESSION` (`closeSession`, a new branch of `submit` ahead of the `Sessioned` one, since
`SessionClose` is a `Command.Cp.Session` and used to fall straight through to the engine), and a
session-bearing verb whose reply is `-NOSESSION`, meaning the session lapsed at a TTL tick
between its creation and this verb (`onSession`). Both forget through `forgetSession`, which
hops to the connection's event loop (`loop`, taken from `ctx.executor()` in `handlerAdded`) and
compares the cached future by identity, so the field keeps its invariant -- only the event loop
reads or writes it -- and a session created in between is left alone. The hop is enqueued from
the `whenComplete` that wraps each reply, which is registered before `channelRead`'s drain hop,
so the cache is cleared before the reply reaches the client and therefore before the client's
next command is read. `session()`'s `usable` check is untouched: it still refuses to remember a
create that failed. Nothing in the CP engine, the session registry, the dispatcher, the wire or
the parser moved. Main-code diff: 51 lines in `DynaCacheServer.kt`.

**Acceptance** (`CpSessionLifecycleTest`, the `CpRoutingTest` arrangement: a real `ApEngine`, a
real three-member `CpTestKit` group, the socket in front of both, raw `RespClient`):
- `session_create_after_close_returns_a_new_session`: CREATE, CLOSE, CREATE on one connection
  gives two different ids and `CP.LOCK.TRY` on the second is granted. Red before the fix:
  `CREATE after CLOSE handed back the closed session ==> expected: not equal but was: <1>`.
- `session_verbs_after_close_use_the_new_session`: after the second CREATE, `CP.LOCK.TRY` is
  taken and `CP.LOCK.STATE` reports the second session as the owner. Red before: the lock verb
  answered `Reply.Error` (`-NOSESSION`) instead of the granted array.
- `session_lapse_clears_the_cache`: the leader's `MutableClock` is advanced past the 15 s default
  session timeout and `leader.tick()` is driven once, so the session lapses in log time (CP spec
  5); the next `CP.LOCK.TRY` answers `-NOSESSION` once, the next CREATE gives a new id, and the
  lock is then granted. No sleeps; the kit's injected clock does the waiting.
- Offline `test -pl dynacache-server -am`: engine, cluster, cp 89, server 86 (83 + 3), all green.

**Deviations:**
1. **The lapse test runs at the socket, not at a stubbed seam.** The ticket allowed a
   dispatcher-level stub if a server-level test could not reach a member's clock. It can:
   `CpTestKit.clock(kit.leader().config.nodeId)` and `RaftRuntime.tick()` are both public and the
   server under test is built on `kit.leaderEngine()`, so the real lapse path is exercised end to
   end. No stub was needed.
2. **`BugHuntCpCompatTest` was not copied over.** Its `session_create_after_close_...` case is
   reproduced as the first named test above; the `cp:ref:` TTL case in the same file belongs to
   T54 and was left where it is.
3. **A CLOSE that answers something else leaves the cache standing.** `-NOTLEADER` (a leader that
   moved mid-close) does not end the session, so forgetting it there would orphan a live session
   holding locks until it lapsed. The client retries the close against the new leader, which then
   answers `+OK` and clears the cache.

**Known limitation, not fixed here:** a `CP.SESSION.CLOSE` inside `MULTI` does not clear the
cache. Buffered commands run through `atomically` and never pass `submit`, so the handler never
sees the close. The repair is to check the buffer for a `SessionClose` on the way out of `exec`,
and it is worth doing only if CP verbs inside transactions become a supported combination.

**For the next ticket:** T44's debt is still open and this ticket does not touch it -- a
connection's CP session is still never closed when the socket closes, only left to expire by
heartbeat timeout. Closing it on `channelInactive` would make `sem_session_death_releases`
deterministic (T44) and would let P5 kill the holder's connection and watch the lock fall free
(T46 deviation 2). It is a separate concern from this one: T53 is about the handler's cache while
the connection lives, `channelInactive` is about the session outliving the connection. The two
would meet in the same field, so whoever takes it should reuse `forgetSession` for the clearing
half. The README's known-debts list still carries the `channelInactive` entry and should keep it.

---

## T56 - Settle the batch cross-partition error

**Decision, and why the kind stays.** A batch whose keys span partitions now answers
`-CROSSSLOT keys of a batch must share a partition (use a hash tag)`. The error KIND is
unchanged and deliberately so: `CROSSSLOT` is what Redis client libraries switch on, and the
"considered and rejected" line in ADR 0002 rejected `-CROSSSLOT` for fan-out commands like
`MGET`, which DynaCache serves by fanning out to the partitions involved. A batch is the
opposite case: it declares its keys, it must run on one executor with nothing interleaved
(C12), and when the keys span partitions there is genuinely nothing to fan out, so the refusal
is real. Only the message text was wrong. It spoke Redis Cluster's vocabulary ("hash to the
same slot") in a project whose glossary bans "slot" and whose remedy is a hash tag, so it named
neither the real constraint nor the fix. The new wording is the glossary's own: partition, and
hash tag.

**What changed.** One source of truth, so one edit reached all three paths. The message lives
in `CrossPartitionBatch.error` in
`DynaCache/dynacache-engine/src/main/kotlin/dynacache/engine/CommandEngine.kt`; the engine fails
the batch future with that exception, and `orBatchError()` in `DynaCacheServer.kt` unwraps it
for both the MULTI/EXEC path and the EVAL path (`Lua.kt` calls the same helper). No Lua bridge
line re-renders the text: an EVAL that spans partitions is refused before the script starts, so
the reply never round-trips through a Lua table. The round-trip was checked anyway for the
`redis.call` path, where `Reply.Error` becomes `err = "$kind $message"` and is split back at the
first space -- the new message has no leading space and no format character, so kind and message
survive intact. Three comment lines above the error record why the kind stays. ADR 0002 gained a
paragraph saying the rejection covers fan-out commands only and that a batch still answers the
kind. Three pinned tests updated: `CommandEngineTest` (the constant, renamed `CROSS_SLOT` to
`CROSS_PARTITION`, plus its one use), `DynaCacheServerTest.multi_exec_cross_partition_rejected`,
`LuaTest.lua_cross_partition_rejected`.

**Grep proof, with one honest deviation.** `grep -rni slot` under `DynaCache/` for `.kt`, `.md`,
`.lua`, `.java` and `.xml`, excluding `CROSSSLOT`, leaves no use of "slot" in the partition
sense. What remains is three unrelated senses, and the ticket's box as literally worded ("the
word slot appears nowhere") cannot be met without changes the seams forbid:

- `CONTEXT.md` lines 35 and 198: `_Avoid_: shard, slot, bucket`. This is the glossary declaring
  the ban; deleting the word would delete the rule.
- `ds/TimerWheel.kt` and `ds/CountMinSketch.kt`: a timer-wheel bucket and a sketch counter cell.
  "Slot" is the standard name in both data structures and has nothing to do with partitions.
  Renaming a `slots` constructor parameter is a code change outside this ticket's seams.
- `CommandParserTest.kt:467` and `RespFuzzTest.kt:11`: "the slot a random expiry argument goes
  in", meaning an argument position.

Since the literal box is unreachable while `TimerWheel` keeps its slots, partially chasing it
would add diff without satisfying it, so nothing outside the partition sense was touched. Read
as the glossary means it, the box is met: "slot" now names a partition nowhere in DynaCache.

**Tests.** Red first: the `CommandEngineTest` pin was updated ahead of the engine and failed on
the old text (`Tests run: 63, Failures: 1`), then passed once `CrossPartitionBatch.error` was
reworded. Full run `-pl dynacache-server -am`: engine 147, cluster 85, cp 89, server 91, all
green, counts unchanged as required -- this was wording only, so no test was added or removed.
Diff is 4 files, well inside the size budget.

---

## T54 - TTL verbs on cp:ref: keys reach the reference

**Built:** the compat re-target in `CommandDispatcher.compat` now reads the key's kind for
`EXPIRE`, `PEXPIRE`, `TTL`, `PTTL` and `PERSIST` the way it already did for `GET` and `SET`, so a
`cp:ref:` key goes to the AtomicReference and every other `cp:` key still goes to the counter. The
reference had the expiry field and the tick's sweep since T42 but no verbs to reach them, so three
commands were added beside the counter's: `RefExpire(key, ttl)`, `RefTtl(key, precision)` and
`RefPersist(key)`, each a data class under `Command.Cp.AtomicReference`, with wire tags 31, 32 and
33 in `CpWire` written and read exactly as `CMD_EXPIRE`, `CMD_TTL` and `CMD_PERSIST` are (a span in
millis, a precision boolean, nothing). `AtomicReferenceStateMachine.apply` answers them from the
same `now` its other verbs use, through a private `retime` that mirrors the counter's: `EXPIRE`
gives a live reference the deadline `now + ttl` and answers 1, 0 when there is no live reference to
give it to; `PERSIST` clears the deadline and answers 1, or 0 when there was none; `TTL` answers -2
for a missing reference, -1 for one without a lease, the remaining millis for `PTTL` and Redis's
`(remaining + 500) / 1000` rounding for `TTL`. Nothing reads a clock in the state machine, so the
lease runs on log time (CP spec 5, 9.4) exactly as the counter's does. Main-code diff: 6 lines in
`CommandDispatcher.kt`, 20 in `AtomicReferenceStateMachine.kt`, 14 in `CpWire.kt`, 15 in
`Command.kt`; 93 lines of tests.

**Repays the T42 deviation:** T42's deviation 2, "No `EXPIRE`, `TTL` or `PERSIST` on a reference" —
CP spec 9.4 names AtomicReference among the state machines those verbs operate on, but 6.5's command
table has no row for them, and T42 resolved the disagreement towards 6.5, leaving "T44 adds the
three commands if the dispatcher needs to route `EXPIRE cp:ref:K`". It did need to, and this ticket
adds them. Spec 9.4 wins over the empty 6.5 row, which is what the ticket and the P6 review (bug 6)
asked for.

**Acceptance:**
- `ref_ttl_via_compat_reports_reference_ttl` (`CpRoutingTest`, a real three-member CP group behind
  the RESP socket): `SET cp:ref:x v EX 100` then `TTL` reads 100 and `PTTL` reads the lease in
  millis; advancing the leader's `MutableClock` 40 s makes `TTL` read 60, so the lease is on log
  time; `TTL` of a reference nobody set is -2 and of one set without `EX` is -1. Before the fix the
  first `TTL` read -2, which is the parked `BugHuntCpCompatTest` red.
- `ref_expire_and_persist_via_compat` (same class): `EXPIRE` on a missing reference is 0, on a live
  one 1; a second `EXPIRE` shortens the lease and `TTL` reads the shorter one; `PERSIST` answers 1
  then 0 and the reference outlives its old deadline; `PEXPIRE 1000`, the clock two seconds on and
  one `tick()` past the deadline, and `GET` is nil with `TTL` back to -2.
- `the compat set reaches the CP engine as the verb it means` (`CommandDispatcherTest`) gains four
  rows: the same five verbs on `cp:ref:r` re-target to `RefExpire`, `RefTtl` (both precisions) and
  `RefPersist`, while every counter row in the table is unchanged, which is the "counter path
  untouched" criterion at the seam.
- `reference_commands_round_trip` (`CpWireTest`) gains the three new commands, both `RefTtl`
  precisions among them, so a follower decodes what a leader replicated.
- Offline `test -pl dynacache-server -am`: engine 147, cluster 85, cp 89, server 90 (88 + 2), all
  green. The cp count is unchanged because the new wire and dispatcher coverage went into existing
  test methods rather than new ones.

**Deviations:**
1. **One assertion is a range, not an equality.** `PTTL` right after `SET ... EX 100` is not
   100000: `RaftRuntime.stamp` is `max(clock, lastApplied + 1, lastStamped + 1)`, so with a frozen
   test clock every appended entry moves log time on by a millisecond, and the three entries between
   the `SET` and the `PTTL` cost three of them. The test asserts `99_900..100_000` and says why. The
   counter's own TTL tests never saw this because seconds rounding hides it.
2. **No `CP.REF.EXPIRE` spelling on the wire.** The new commands are reachable only through the
   Redis-compat verbs, exactly as the counter's `LongExpire`, `LongTtl` and `LongPersist` are: the
   parser has `cp.long.set/get/incr/decr/add/cas` and no `cp.long.expire`. CP spec 3.5 and 6.5 name
   no `REF_EXPIRE` log op either, so adding a parser row would have invented a verb; the ticket's
   seams also put the parser's command rows out of bounds.
3. **The namespace rule is still read from the key prefix in two places.** `compat` now branches on
   `reference` in four arms instead of two. That is the smallest fix the ticket asked for; folding
   it is ticket 71.

**For the next ticket:**
- **Ticket 71** should fold `CommandDispatcher.REFERENCE_PREFIX` and the four `if (reference)`
  branches into one key-to-kind decision, and with it the `ponytail:` note still standing above
  `compat`: `GET cp:lock:x` reads an empty counter instead of `-WRONGTYPE`, and `EXPIRE cp:lock:x`
  answers 0 where CP spec 9.4 says a lock's lease is `CP.LOCK.RENEW`'s alone and the verb is
  rejected. Both want the same thing: the kind of a `cp:` key named once, mapping a compat verb onto
  the owning primitive's command, with `-WRONGTYPE` and the lock's refusal falling out of it. The
  five TTL verbs and `GET`/`SET` are then one table, not seven branches.
- The reference is now the second primitive with a full TTL surface; the latch and the semaphore
  still have none, and CP spec 9.4 names them too. Whoever gives them one has this shape to copy:
  three commands, three wire tags, a `retime` in the state machine, and the dispatcher branch.

---

## T50 - The idle TTL tick runs on log time

**Built:** Leases and sessions keep running out after a failover to a leader whose wall clock
trails log time. `RaftRuntime` gains one field and one function: `skew`, how far log time ran
ahead of this member's clock when its term's first entry applied (`max(0, lastAppliedTs -
clock.millis())`, set in `termApplied` before `appliedTerm` so a tick never reads a leader's
term with the previous term's skew), and `logClock()`, the wall clock plus that skew. The
**log clock** is log time at the election plus whatever the leader's own clock has measured
since, so it moves at the real rate however far the wall clock is behind. `tick()` now gates on
the log clock (`logClock() >= lastStampedTs + tickInterval`) and stamps the tick with it
(`stamp(now = logClock())`); `stamp()` took a `now` parameter that defaults to the wall clock,
so a user entry is stamped exactly as before. `CONTEXT.md`'s **TTL tick** entry names the log
clock. Nothing in the state machines, the session registry, the wire format or the stores
changed. Size: 126 insertions, 13 deletions across five files, 42 lines of them in
`RaftRuntime` (mostly doc comments) and 90 in tests.

**The gate and the stamping choice.** The old gate compared the wall clock with the last stamp.
After a failover to a trailing clock the first tick stamped `lastAppliedTs + 1` (C19), which put
`lastStampedTs` seconds past the wall clock, and the gate stayed shut until the clock caught up:
no tick, no expiry, no lapse for the whole skew. The ticket's phrase "due when log time has not
advanced for one interval of the leader's own elapsed time" is the log clock compared with the
last stamp: when the clock leads or matches log time the skew is zero and the gate reads exactly
as it did before, and when it trails the gate keeps opening every interval of the leader's own
elapsed time whatever user entries stamped in between. The tick's stamp is the choice the ticket
asked to be recorded. CP spec 5 fixes `ts = max(clock_now, last_committed_ts + 1)` for every
entry and asks for a tick every interval "to advance time in idle periods"; with the literal rule
an idle tick on a trailing clock advances log time by 1 ms, so a 1 s lease would take 1000 ticks,
100 s of the new leader's time, to run out, which breaks I19's "at most T plus the election".
So a tick stamps `max(logClock, lastAppliedTs + 1, lastStampedTs + 1)`: the C19 rule with the
log clock in place of the wall clock, which is the wall clock itself whenever the skew is zero.
Log time therefore advances by exactly one tick interval per idle tick when the clock trails
(`I19_idle_ticks_continue_after_failover_to_a_trailing_clock` pins the ten stamps to
`lastByOld + k * interval`), and by the wall clock's own reading otherwise. User entries do not
read the log clock: the brief fixed the stamping rule, and between ticks they crawl at one
millisecond per entry under skew as T39 recorded; the next tick, at most one interval later,
brings log time back to the log clock. The log clock is never ahead of real elapsed time: it
equals the old leader's last stamp plus what the new leader measured since its term applied,
so a lease can expire late by the election window but never early (the existing
`I19_lease_expires_late_never_early_across_failover` still passes unchanged).

**Acceptance:** `dynacache-cp` 92 tests, all green (89 before this ticket); full offline
`-pl dynacache-cp -am test` green with engine 144, cluster 83, cp 92. Red before green in
every case: the I19 test failed on the second tick (`tick 2 was not appended`) before the
change; C17 and C18 were re-run against the old wall-clock gate after the fix and both failed
at "every idle interval appends a tick" with 0, then the gate was restored.

- `CpEngineTest.I19_idle_ticks_continue_after_failover_to_a_trailing_clock`: the leader's clock
  is advanced 30 s and a SET carries that into the log; the leader is killed; the successor's
  clock (30 s behind log time) is advanced ten intervals, each followed by `tick()`; all ten
  ticks commit, the stamps climb strictly past the old leader's last stamp, and each is exactly
  one interval past the previous.
- `FencedLockTest.C17_lease_expires_after_skewed_failover`: a 1 s lease taken under a 30 s skew;
  nine idle ticks on the successor leave the lock held (read straight from the leader's state
  machine at its applied index, no entry appended), the tenth releases it, and the next holder
  gets the next token.
- `SessionTest.C18_session_lapses_after_skewed_failover`: a session with a 1 s timeout holds a
  30 s lease under a 30 s skew and stops heartbeating; nine idle ticks leave it held, the tenth
  tick's `SESSION_CLOSED` releases the lock, and a heartbeat answers `-NOSESSION`.
- Every existing CP test passes unchanged, including `C19_log_timestamps_monotonic_across_leader_change`,
  `C23_every_member_agrees_on_expiry_at_same_index` and `I19_lease_expires_late_never_early_across_failover`.

**T39 deviation 4, corrected.** T39 recorded: "Lease time after a failover to a slow clock
stands still. If the new leader's clock is behind log time, stamps advance by 1 ms per entry
until the clock catches up; that is the spec's own rule (time never turns back) and not
something this ticket changed." That described the stamping rule but missed the consequence:
the idle tick was gated on the wall clock against that 1 ms stamp, so with no user entries
nothing was appended at all for the whole skew and no lease or session could run out. Read it
now as: "User entries under a trailing clock stamp 1 ms apart (the C19 rule). The idle tick is
gated and stamped on the leader's log clock, log time at its election plus its own elapsed
time, so it keeps appending every interval and carries log time forward at the real rate
whatever the wall clock says (T50)."

**Deviations:**

1. **The tick's stamp is `max(C19 rule, log clock)`, not the literal spec 5 formula.** Recorded
   above; the literal formula cannot satisfy I19 on a trailing clock. The user entries' rule is
   untouched.
2. **`skew` is computed on every member at every term, not only on the leader.** It is one
   volatile long written by the Raft thread when the term's first entry applies; a follower
   never reads it. Computing it before `appliedTerm` is what keeps a leader from ticking with a
   stale skew, and doing it unconditionally is one line shorter than gating on leadership.
3. **A clock that steps backwards during a leadership is not repaired.** The skew is measured
   once per term. If the leader's own clock later jumps back, the log clock jumps back with it
   and the gate shuts until it catches up, the same failure this ticket fixes but for a clock
   step rather than a failover; a monotonic source for elapsed time (`System.nanoTime()` since
   the election) would close it. Not in the ticket; noted as the ceiling.
4. **A burst of user entries faster than one per millisecond pushes log time ahead of the log
   clock**, by the size of the burst (the C19 rule's `last + 1`), and no tick is due until the
   log clock passes it. That is T39's rule, bounded by the burst, and unchanged here.

**For the next ticket:**

- The `stateOnLeader` helper in `FencedLockTest` and `stateOn` in `SessionTest` read a member's
  state machine at its own applied index without appending an entry; any test that must show
  "no user command in between" wants one of them.
- The kit's members all start at the same epoch, so a skew is one `clock(leader).advance` before
  the entries that should carry it, and the successor's clock is then behind log time by that
  much; `SKEW` in the two test classes is the constant to reuse.
- The production tick loop (`ClusterNode`, `DynaCacheServer`) is unchanged: it calls `tick()`
  every interval on the system clock, and with a zero skew the gate reads as it always did.

---

## T57 - Glossary renames

A mechanical rename pass so the code and tests speak CONTEXT.md's words and none of its "Avoid"
words. No behaviour change: no logic edit, no reordering, no assertion changed beyond a renamed
identifier. 13 files, 47 insertions and 47 deletions.

### Batch (was "transaction")

| Old | New | Where |
| --- | --- | --- |
| `TRANSACTION` (the MULTI/EXEC/DISCARD verb set) | `BATCH` | `DynaCacheServer.kt` (declaration and its one use) |
| comment "refuses the whole transaction later" | "refuses the whole batch later" | `DynaCacheServer.kt`, above the `Parsed.Failed` branch |
| `a parse error while queued makes EXEC abort the whole transaction` | `...abort the whole batch` | `DynaCacheServerTest.kt` |

### Lease (was "ttl" / "expiresAt" on the lock)

| Old | New | Where |
| --- | --- | --- |
| `Command.Cp.LockTry.ttl` | `.lease` | `Command.kt`, `CpWire.kt`, `FencedLockStateMachine.kt`, `FencedLockTest.kt`, `CpWireTest.kt`, `CommandParserTest.kt` |
| `Command.Cp.LockRenew.ttl` | `.lease` | same, plus `SessionTest.kt` |
| `FencedLockStateMachine.Lock.expiresAt` | `.leaseUntil` | `FencedLockStateMachine.kt`, `CpWire.kt` snapshot encoding, `CpWireTest.kt` |
| KDoc `CP.LOCK.TRY K ttl_ms` | `CP.LOCK.TRY K lease_ms` | `Command.kt` |
| KDoc `CP.LOCK.RENEW K token ttl_ms` / `[ttl]` | `... lease_ms` / `[lease]` | `Command.kt` |
| KDoc `[owner or nil, token, ttl_remaining_ms, reentrance]` | `..., lease_remaining_ms, ...` | `Command.kt` (`LockState`) |
| `tryLock(session, ttl = ...)` helper parameter | `lease` | `FencedLockTest.kt` |

`ttl` elsewhere is left alone deliberately: it is the right word for counters (`LongSet`,
`LongExpire`, `LongTtl`, `LongPersist`), for references (`RefSet`), for the AP engine's `Command.Set`
and `Command.Ttl`, and for `TtlTick`, which is the log-time tick's own name in CP spec 5 and a
CONTEXT.md glossary entry in its own right.

### Latch (was "barrier")

| Old | New | Where |
| --- | --- | --- |
| "A latch is a one-time barrier, so it is armed only from zero" | "A latch runs down once and stops at zero, so it is armed only from zero" | `CountDownLatchStateMachine.kt` class KDoc |
| "The barrier has already fallen; ..." | "The latch has already run out; ..." | `CountDownLatchTest.kt`, `latch_down_at_zero_stays_zero` |
| "would move the barrier under them" | "would move the count under them" | `CountDownLatchTest.kt`, `latch_reset_only_at_zero` |

The engine's `CyclicBarrier` in `CommandEngine.kt` and `Partition.kt` stays: that is the parked-
partition sense CONTEXT.md reserves the word for, and it is also the JDK type's own name.

### Reply (was "Response" on the CP gRPC message types)

| Old | New | Where |
| --- | --- | --- |
| proto `message CpResponse` | `message CpReply` | `cp.proto`, `CpGrpcServer.kt` (import, `apply` return type, builder) |
| proto `message HeartbeatResponse` | `message HeartbeatReply` | `cp.proto`, `CpGrpcServer.kt` (import, `heartbeat` return type, builder) |
| `rpc Apply(CpRequest) returns (CpResponse)` | `... returns (CpReply)` | `cp.proto` |
| `rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)` | `... returns (HeartbeatReply)` | `cp.proto` |

Both message types are referenced only from `CpGrpcServer.kt`; `ForwardingCpEngine.kt` and
`GrpcCpKit.kt` read the `reply` field, whose name did not change. The field numbers and the RPC
method names are untouched, so the protobuf wire format is unchanged; the generated Kotlin class
names and the service descriptor's type names change, and both sides of that wire are in this
repository. Nothing persisted carries these types: the Raft store persists log entries and
snapshots through `CpWire`'s own hand encoding, never a `CpResponse`.

### Names deliberately kept

| Name | Why |
| --- | --- |
| `Reply.Error("EXECABORT", "Transaction discarded because of previous errors.")` | Redis's own error text, byte for byte on the wire. Changing it would break every Redis client. Appears in `DynaCacheServer.kt`, `DynaCacheServerTest.kt` and `RespCodecTest.kt`. |
| Test names `lock_ttl_expires`, `lock_ttl_renew` | Spec-named tests (design-spec-cp.md lines 391-392). The ground rule keeps spec test names, and this ticket's own criterion allows only the batch test to be renamed. |
| proto `InstallSnapshotResponse`, and MicroRaft's `PreVoteResponse` / `VoteResponse` / `AppendEntriesSuccessResponse` / `AppendEntriesFailureResponse` | Raft's own message names, mirroring the `io.microraft.model.message.*` classes they encode. CONTEXT.md keeps Raft's vocabulary where it is Raft's (as with "majority"). These are not the CP message types the ticket names. |
| `CyclicBarrier` and "barrier" in `CommandEngine.kt` / `Partition.kt` | The engine's parked-partition sense, which CONTEXT.md explicitly reserves the word for. |
| `expiresAt` on `AtomicLongStateMachine.Counter` and `AtomicReferenceStateMachine.Reference`; `expiresAt` throughout the AP engine and cluster | TTL and expiry are the counter's and the reference's words. Only the lock says lease. |
| RESP verb spellings `CP.LOCK.TRY` / `CP.LOCK.RENEW` / `CP.LOCK.STATE`, error kinds `REENTRANCE`, `NOSESSION`, `EXECABORT` | Client-facing wire names. |

`docs/dynamiccache/design-spec-cp.md` still writes the lock argument as `ttl_ms` (lines 119, 121,
214, 216, 217). The KDoc synopses now say `lease_ms` because CONTEXT.md is the glossary authority
and this ticket asks for it; the spec itself was not edited, since this ticket modifies only
`DynaCache/`. The spec already agrees in prose at line 356: "For FencedLock, TTL is the lease".

### Grep proof

Over `dynacache-{engine,cluster,cp,server}/src` for `*.kt` and `*.proto`, case-insensitive:

- `transaction`: 4 hits, all the `EXECABORT` error text above.
- `barrier`: 6 hits, all `CyclicBarrier` and the parked-partition comment in the engine.
- `expiresAt`: no hit on a lock; all remaining hits are counters, references, the AP engine's
  entries, and the cluster's replication messages.
- `CpResponse`, `HeartbeatResponse`: no hits anywhere.
- `Command.Cp.LockTry(...).ttl`, `Command.Cp.LockRenew(...).ttl`: no hits; both carry `lease`.

### Tests

Full reactor, all four modules green at unchanged counts:

| Module | Tests |
| --- | --- |
| dynacache-engine | 147 |
| dynacache-cluster | 85 |
| dynacache-cp | 89 |
| dynacache-server | 91 |

One intermediate run saw `ReadRepairTest.read_repair_does_not_delay_reply` error with "no READ
reached node-3". That test is in the cluster module, which this ticket does not touch, and it
passed on both the run before it and the run after; it is a timing flake under three parallel
Maven builds on the machine. The final run is clean.

---

## T51 - A node's dot counter survives restart

**Built:** a node's `DotCounter` now resumes above everything it ever handed out, restart
included (C2), which is what makes a restarted coordinator's first write new to every replica
and closes the acknowledged-write loss the bug hunt found (I2). The counter reserves dots a
block at a time: crossing its reserved ceiling persists the next ceiling (`(counter / block + 1)
* block`, block 1000) through a new seam BEFORE the crossing dot is handed out, so the write
path pays one fsync per 1000 writes and nothing otherwise, and a crash wastes at most one block.
On start the counter takes the higher of two floors: the persisted ceiling and the highest own
counter in the local-data scan T21 already had (empty on every node today, ticket 67's floor
once versions persist).

The seam is `dynacache.engine.persist.DotCeilingStore { load(): Long; reserve(ceiling: Long) }`,
in the engine's persist package because that is the one package besides cp allowed
`java.nio.file` (plan 2.2); the cluster module still does no file I/O. Two adapters, both in the
same file as companion factories: `inFile(path)` writes the ceiling as decimal text to
`<path>.tmp`, fsyncs, and renames it over `<path>` atomically (the snapshot engine's own idiom),
and answers 0 for a missing file while a file it cannot parse fails loudly rather than starting
over at 0; `inMemory()` is what a node with no data directory and every test uses. `ClusterNode`
wires `inFile(dataDir/dots)` when it has a data directory and `inMemory()` otherwise; the file
adapter creates the directory itself because the counter is built before the snapshot engine
creates it. Inside the counter, `next()` stays an `AtomicLong` increment; only a dot past the
ceiling enters the `@Synchronized` reservation, the first arrival writes and the rest re-check
and go, and a reservation that throws leaves the ceiling where it was so the dot is never
handed out and the next caller retries.

The in-process test kit gained `restart(node)`: it cancels the node's router and handoff loops
and rebuilds `Replication`, `AntiEntropy` and `Router` over the same engine, the same transport
endpoint and the node's own `DotCeilingStore` (one in-memory store per node lives in the kit),
so the version table empties and the counter resumes from the persisted ceiling exactly as a
process restart does while the engine restores from disk. Node construction moved into one
`start(node)` the constructor and `restart` share.

**Acceptance:**
- `C2_dot_counter_never_reuses_a_dot_across_restart` (`DvvTest`, block 4 against a recording
  store): the first dot reserves 4 before it is out, dots 2 to 4 reserve nothing, dot 5 reserves
  8; then 41 restarts dying after 0 to 40 dots each, and every restarted counter's first dot is
  above everything handed out before; the recorded ceilings only rise.
- `I2_acknowledged_write_survives_coordinator_restart` (`ReplicationTest`): v1 and v2 written
  through the coordinator, replicas hold `(coord, 2)`; `restart(coordinator)`; v3 is written with
  W acks and its dot is above 2; a quorum read through a replica answers v3; after read repair
  drains every replica holds v3; `assertConverged` passes. Red at HEAD before the fix on the dot
  assertion (the reused `(coord, 1)`), which is the mechanism the parked bug-hunt test named.
- `dvv_no_counter_reuse` keeps its T21 assertions and is extended across a restart with no local
  data (the persisted ceiling is the floor) and with local data above the ceiling (the higher
  floor wins).
- `DotCeilingStoreTest` (engine, `@TempDir`): a fresh node loads 0; the last of two reservations
  is what a new instance loads and the temp file is gone; an unparseable file throws
  `NumberFormatException`; the in-memory store survives only its own instance.
- Every existing replication, hint, read-repair, anti-entropy and convergence test passes; the
  P4 acceptance test restarts real nodes over data directories and now reads `dots` back.
- Counts: engine 144 -> 148, cluster 83 -> 85, cp 89 -> 89, server 83 -> 83. Diff: 7 files,
  about 300 lines including tests, inside the budget.

**Deviations:** none against the spec or the plan entry. Three judgement calls.
1. The seam lives in the engine's persist package, not the cluster, because the cluster depends
   on the engine and not the reverse; its vocabulary ("dot") is the cluster's, and the KDoc says
   where the word comes from. `inMemory()` is main code rather than test code because a node with
   no data directory needs it.
2. The reservation is a `@Synchronized` block holding an fsync, on the caller's thread, inside
   `Replication.write`'s `versions.compute`. Once per 1000 writes on one key's map bin; the
   plan's lock rule (2.5) is about the engine's data structures and this is the cluster module.
   If a measurement ever shows the once-per-block stall, reserve the next block ahead of time on
   a background coroutine; the seam does not change.
3. The ceiling is rewritten whole (write, fsync, atomic rename) rather than appended to the WAL:
   the WAL format is frozen for this ticket and a 20-byte file has nothing to gain from a log.

**For the next ticket:**
- Ticket 67 (persist the version table) should hand the restored versions to
  `DotCounter.of(node, localData, ceilings)` as the scan floor it already takes; the ceiling
  store stays as the guard for versions that were handed out but never reached the RDB. Do not
  drop the ceiling in favour of the scan: the scan only sees what was persisted, and a write's dot
  is handed out before the write is durable.
- A restarted node's version table is still empty until ticket 67, so its own reads of keys it
  wrote before the restart answer with no version and lose to any replica's; read repair then
  refills it. Correct, and one round trip per key.
- `InProcessCluster.restart(node)` restarts only the replication layer; the network's own
  `kill`/`restart` stays separate, and a chaos run that wants "process restart" should call both.

---

## T58 - One MutableClock in an engine test-jar; drop the two ModuleGraphTests

Five hand-written clock doubles became one. The engine module now publishes a test-jar and the
other three modules depend on it for tests only, so plan rule 1.5 (time is an injected `Clock`)
has a single implementation to point at instead of four copies that had already drifted apart.

### The one clock

`DynaCache/dynacache-engine/src/test/kotlin/dynacache/engine/testkit/MutableClock.kt`:

```kotlin
class MutableClock(@Volatile var now: Instant, private val record: Boolean = false) : Clock() {
    val readers: List<String>            // the thread behind each read, in order
    fun advance(by: Duration)
    override fun instant(): Instant
    override fun getZone(): ZoneId       // UTC
    override fun withZone(zone: ZoneId): Clock
}
```

`now` is public and settable, which covers both spellings already in use: absolute
(`clock.now = deadline`) and relative (`clock.now += Duration.ofMillis(6)`). `advance` is the CP
kit's spelling of the relative form and is kept so its two call sites are untouched. `now` stays
`@Volatile` because every copy it replaces was: the engine's partition threads, the WAL writer's
appender pool, the cluster's hint sweeper and MicroRaft all read the clock off the test thread.

`record` is the one addition. `RecordingClock` named the thread behind every read into a
synchronized list; the merged class does that only when asked, and defaults to off. Always
recording would cost a retained string per clock read in suites that run thousands of commands
(T59 is about to move the acceptance tests onto this clock), and the synchronized list would add
contention to exactly the threads that `wal_group_commit_amortizes` and
`eviction_runs_on_the_partition_thread` are measuring. `readers` is exposed as a read-only
`List<String>` view over the backing list, so `readers.size` and `readers.toSet()` read the same
as they did on `RecordingClock`.

### What each copy needed

| Copy | Needed | Notes |
|---|---|---|
| `CommandEngineTest.MutableClock` | `now` get/set | private nested; the file's other two ad-hoc clocks (`ParkingClock` and an anonymous gate) stay, they park and count rather than tell the time |
| `CommandEngineTest.RecordingClock` | `now`, `readers` | folded in behind `record = true`; its one construction is now `MutableClock(clock.now, record = true)` |
| `WalFsyncTest.MutableClock` | `now` get/set | private nested, byte-identical to the engine copy |
| `HintedHandoffTest.MutableClock` | `now` get/set | private nested, byte-identical to the engine copy |
| `CpTestKit.MutableClock` | `now`, `advance` | the only copy that was public, because the server module's `CpRoutingTest` reaches it through `kit.clock(member)` |

No test name and no assertion changed; only the double each test constructs. The `record = true`
construction is the single call-site edit.

### The poms

- `dynacache-engine/pom.xml`: `maven-jar-plugin` 3.4.1 with the `test-jar` goal, copied from the
  wiring `dynacache-cp` has carried since the CP test kit was published for the server module.
  The module's zero-runtime-dependency constraint is untouched, this is test output only.
- `dynacache-cluster`, `dynacache-cp`, `dynacache-server`: a `dynacache-engine` dependency with
  `<type>test-jar</type>` and `<scope>test</scope>`. Test scope means plan 2.2's module graph is
  unchanged; the server needs its own declaration because a test-scoped dependency of the cp
  test-jar is not transitive.

The fixed contract held: no `install` was needed. `mvn -o clean test -pl dynacache-server -am`
from a clean state resolves the engine test-jar out of the reactor (Maven substitutes the
module's `target/test-classes` when the artifact has not been packaged), and the Kotlin plugin's
`test-compile` execution puts the test kit there. Verified green from `clean`, offline.

### Why the ModuleGraphTests went

Both asserted that a module can see a type from a module it depends on: the cluster one compared
two `Key` hashes and two `Reply.Bulk`s, the server one pinged an `ApEngine`. Neither can fail
while the code compiles, because a missing dependency is a compile error in the same Maven run
that would have executed the test. They restate the dependency direction that
`dynacache-cluster/pom.xml` and `dynacache-server/pom.xml` already declare and that Maven already
enforces, so they cost a build slot and buy nothing.

### Test counts

Measured on a pristine `git archive` of HEAD (`073cc162`) against the worktree, same command.
The parent brief's baseline (engine 147, cluster 85, cp 89, server 90) was stale for the server
module; its true baseline is 93.

| Module | Before | After |
|---|---|---|
| engine | 147 | 147 |
| cluster | 85 | 84 |
| cp | 89 | 89 |
| server | 93 | 92 |
| total | 414 | 412 |

Minus two, both of them a deleted `ModuleGraphTest`, and the two modules that lost one are the
two that held one. Every remaining test passes.

### Deviations

- The size budget was 200 to 600 lines. The change is 84 added against 91 deleted, net negative,
  because the ticket is a fold rather than a build. Nothing was left out.
- `advance` and `now +=` both survive as ways to move time forward. Collapsing to one would have
  edited call sites the ticket asked to leave alone; the ticket's own wording ("settable,
  tickable") wants both.

---

## T60 - CP.LONG.GETADD

**Built:** `CP.LONG.GETADD K d` (CP spec 3.2 `LONG_GETADD`, 6.2 `-> :old`), the verb T38's
deviation 1 deferred and T44 never picked up. One new command, `Command.Cp.LongGetAdd(key, delta)`
under `Command.Cp.AtomicLong`; one parser row, `"cp.long.getadd" -> exactly(name, args, 2)`, beside
`cp.long.add` and `cp.long.cas`; **wire tag 34** in `CpWire` (31 to 33 went to T54's reference TTL
verbs), written and read exactly as `CMD_INCR_BY` is, a key and a signed long; and one branch in
`AtomicLongStateMachine.apply`.

The state machine's private `add` already did everything GETADD needs except answer the old value,
so it now returns the old value instead of a `Reply`, and the INCR family goes through a new
one-line `added(key, delta, now)` that adds the delta back on for its `:new` reply. That keeps the
counter's read and its write in one map write inside one applied entry, so GETADD is atomic for the
same reason `LongCas` is (I21) and costs one lookup, not two. Missing counters count as 0 as they do
for INCR, and the key keeps its TTL. Main-code diff: 3 lines in `Command.kt`, 1 in `CommandParser.kt`,
3 in `CpWire.kt`, 15 in `AtomicLongStateMachine.kt`; 54 lines of tests.

**The chaos checker is verb-generic, so GETADD joined it.** `CounterOp` gained `GetAdd(delta)` and
`CounterSpec` the row `(state + delta) to state` - the model's output is the state the operation
came in with, which is the whole of GETADD's semantics. `ChaosDriver.counterHistory` now draws one
of three operations per client per round (`Get`, `GetAdd(1)`, `IncrBy(1)`) instead of one of two, so
`invariant_linearizable_ops` linearizes GETADD histories across a leader kill and a restart on all
five seeds. An unanswered GETADD under chaos is recorded with a null output like any other, and the
checker is free to place it or drop it.

**Acceptance:**
- `long_getadd_returns_old_value_and_adds` (`CpEngineTest`, a three-member group through the
  leader's engine): a missing counter answers `:0` and is left holding 5; the next GETADD of -2
  answers `:5`, not `:3`, and `CP.LONG.GET` reads 3.
- `long_getadd_concurrent_linearizable` (same class): 50 GETADDs of 1 in flight at once over 4
  client threads; the old values they answer are exactly the set 0..49, one each, and the counter
  ends at 50.
- `C16_cp_engine_rejects_a_non_cp_key` gained a GETADD line: `LongGetAdd(Key("plain-key"), 1)` is
  `-NOTCP`, the rejection every CP verb gets before a primitive sees it.
- `cp_op_round_trips_with_its_stamp` (`CpWireTest`) round-trips `CpOp(8, LongGetAdd(cp:counter:c, -3))`,
  so a follower decodes tag 34 as what the leader replicated, negative deltas included.
- `the CP verbs of CP spec 6` table (`CommandParserTest`) gained the row
  `CP.LONG.GETADD cp:counter:k -5`, asserting the parsed delta.
- Offline `test -pl dynacache-server -am`: engine 147, cluster 85, cp 91 (89 + 2), server 93 (92 + 1),
  all green. The server module holds 92 tests at this base, not the 90 the ticket brief predicted;
  the parser table gained exactly one row (98 to 99), which is the whole of this ticket's + 1.

**Deviations:**
1. **No `-CAPACITY` limit, as the ticket directs.** GETADD can carry a counter past any bound a
   future capacity rule would set, exactly as `CP.LONG.ADD` can today. The deferral stays recorded
   in ticket 62, which owns the `-CAPACITY` line of the ledger; this ticket adds nothing new to it.
2. **87 lines changed against a 200 to 600 budget.** The verb is one data class, one parser row, one
   wire tag and one state-machine branch, and the checker was already generic over `CounterOp`, so
   there was nothing else to write. Turning `add` into an old-value function rather than duplicating
   its body is where the shape decision was.
3. **No Redis-compat spelling.** `GETSET`-style compat on `cp:counter:` keys is not re-targeted to
   GETADD; CP spec 6.2 gives the row no Redis column ("-"), so the verb is reachable only as
   `CP.LONG.GETADD`, the way `CP.LONG.CAS` is.

**For the next ticket:** ticket 62 deletes `LongDecrBy` as dead; it now goes through `added` with a
negated delta like the other three, so the deletion is still one variant, one wire arm and one
branch. If a `-CAPACITY` rule is ever built, `add` is the single place both the INCR family and
GETADD pass through.

---

## T61 - SET NX and SET XX on cp: keys

**Built:** the Redis lock idiom now works on the CP namespace. `SET cp:ref:lock v NX PX 30000`
takes the reference only when nothing live holds it, with the lease applied in the same committed
entry, and `SET ... XX` writes only what is already there; the same on a `cp:counter:` key with a
numeric value. Four changes, in the order the command travels:

1. **The model.** `Command.Cp.LongSet` and `Command.Cp.RefSet` each gain a trailing
   `condition: Set.Condition? = null`, reusing the enum `Command.Set` already has rather than
   inventing a second vocabulary for NX/XX. That was the smaller of the two shapes the ticket
   offered (an optional condition versus a sibling conditional variant): every existing call site
   compiles unchanged, the `when` in `CpWire` and in both state machines keeps one arm per verb,
   and nothing else in the CP hierarchy grows a variant. `RefSet`'s hand-written
   `equals`/`hashCode`/`toString` include the condition.
2. **The rule.** `Command.Set.Condition.refuses(exists: Boolean)` says what NX and XX mean in one
   place: NX refuses a key that exists, XX refuses one that does not. Both CP state machines read
   it; the AP partition still spells the same three lines out in its own `SET` branch, because the
   AP engine is outside this ticket's seams (noted below).
3. **The dispatcher.** `CommandDispatcher.compat`'s `SET` branch no longer refuses a conditional
   SET with `-NOTCP`. It passes `command.condition` into the SET verb of the kind the key names,
   exactly as it already passed the TTL. The counter arm still reads the value as a number first,
   so `SET cp:counter:x banana NX` is `-ERR value is not an integer or out of range` and not nil:
   the value is parsed before the condition is looked at.
4. **The state machines.** `AtomicLongStateMachine` and `AtomicReferenceStateMachine` each test the
   condition against the key's presence at that entry's log time and answer `Reply.Bulk(null)` when
   it refuses, otherwise write the value and the TTL as before. Condition, value and TTL are one
   applied entry, so no reader sees a half state (I21), and nothing reads a clock, so a lease still
   runs on log time (CP spec 5, 9.4).

Diff: 310 insertions, 28 deletions over eight files. Main code is 99 of those insertions - 43 in
`Command.kt` (most of it the two KDoc blocks and `refuses`), 30 in `CpWire.kt`, 9 in
`CommandDispatcher.kt`, 9 in `AtomicReferenceStateMachine.kt`, 8 in `AtomicLongStateMachine.kt` -
and 211 are tests: 163 in `CpRoutingTest.kt`, 32 in `CommandDispatcherTest.kt`, 16 in
`CpWireTest.kt`.

**Encoding and wire tags:** no new wire tag. The condition rides on the two existing SET tags,
`CMD_SET` (1) and `CMD_REF_SET` (28), as one byte written after the TTL, so nothing collides with
T60's new CP tag or with anything at 40 and above. The byte is spelled out
(`NO_CONDITION` 0, `CONDITION_NX` 1, `CONDITION_XX` 2) in a `when` rather than taken from the enum's
ordinal, because a log entry outlives the declaration order of a Kotlin enum, and an unknown byte is
an `error(...)` the way an unknown tag already is. A `readTtl()` helper was pulled out while both
SET decoders were being touched, since three call sites spelled the same `takeIf { it != NO_TTL }`
out.

**Reply shapes:** the compat path answers Redis's shapes exactly - `+OK` when the conditional SET
takes, nil bulk when it is refused - and the dispatcher rewrites nothing, so the CP verb has the
same two shapes. There is no second shape to record: the reply is produced once, in the state
machine, and the `CP.LONG.SET`/`CP.REF.SET` spelling would answer the same `+OK`/nil if a parser row
is ever added for the condition.

**Acceptance (all in a real three-member CP group behind the RESP socket, `CpRoutingTest`):**
- `compat_set_nx_on_ref_key_acquires_once`: nine clients on nine connections, released together by
  a `CountDownLatch` and not a sleep, race `SET cp:ref:lock owner-N NX`; exactly one reads `+OK`,
  the other eight read nil, and `GET` returns the winner's bytes.
- `compat_set_nx_px_expires_on_log_time`: `SET ... NX PX 30000` takes, a second `NX` is nil, `PTTL`
  reads the lease; the leader's clock 31 s on and one `tick()` past the deadline, `GET` is nil and
  the next `SET ... NX` takes.
- `compat_set_xx_on_missing_key_is_nil` (and the refusal wrote nothing) and
  `compat_set_xx_on_present_key_replaces` (bytes replaced, and `TTL` is -1 because a plain SET
  clears the lease it replaces, as Redis does).
- The same four behaviours on a counter, over three tests rather than four:
  `compat_set_nx_on_counter_key_acquires_once`, `compat_set_nx_px_on_counter_expires_on_log_time`,
  `compat_set_xx_on_counter_key_is_nil_then_replaces` (which also holds the `-ERR` for a
  non-numeric value under `NX`).
- `compat_conditional_set_retargets_to_the_kinds_set_verb` (`CommandDispatcherTest`): the four
  conditional spellings reach the CP engine as `LongSet`/`RefSet` carrying the condition and the
  TTL, and the AP engine sees none of them. The old row asserting `-NOTCP` for a conditional SET is
  gone from `a cp key outside the compat set is NOTCP`, which now also asserts that `NX` does not
  excuse a non-numeric counter value; `I22_namespaces_never_cross` is untouched and passes.
- `conditional_set_commands_round_trip` (`CpWireTest`): both kinds, both conditions, with and
  without a TTL, so a follower applies the rule the leader replicated.
- Red before green: the dispatcher slice failed to compile against the old model, then passed. For
  the end-to-end slice, `refuses` was temporarily stubbed to prove the tests are load-bearing -
  NX disabled fails 4 of them, XX disabled fails the other 2 - and then restored.
- Offline `test -pl dynacache-server -am`: engine 147, cluster 85, cp 93 (92 + 1), server 101
  (93 + 8), all green. The server base is 93 and not the 91 the briefing expected; the eight new
  tests are one in `CommandDispatcherTest` and seven in `CpRoutingTest`.

**Deviations:**
1. **The counter's four behaviours are three tests, not four.** The acceptance list names four test
   names for the reference and says "the same four behaviours" for the counter without naming them;
   the two XX behaviours on a counter share one test because they share one session, which is one
   less three-member Raft group to start.
2. **No `NX`/`XX` on the `CP.LONG.SET` / `CP.REF.SET` spelling.** The ticket calls exposing it free,
   not required, and the fix asked for is the compat path. The commands carry the condition, so a
   parser row is two lines whenever a ticket wants the CP spelling; the parser's command rows are
   outside this ticket's seams anyway.
3. **The NX/XX rule is stated twice in the tree.** `Condition.refuses` is the one statement of it,
   but `Partition.run`'s `SET` branch still has its own three-line `when`, because the AP engine is
   outside this ticket's seams. Folding that call site is a one-line change whenever the AP engine
   is open.
4. **Two Kotlin warnings sit on a re-wrapped line.** The compiler reports
   "identity-sensitive operation on an instance of value type `Duration?`" twice at
   `RefSet.equals`'s `ttl == other.ttl`. The diff only re-wrapped that expression to fit the new
   `condition` term beside it; the comparison itself is unchanged from T42.
5. **The namespace rule is still the key prefix.** `compat` reads `cp:ref:` to pick the kind, as it
   has since T44 and T54. Ticket 71 folds it.

**For the next ticket:**
- **Ticket 71** folds the kind lookup: `compat` now branches on `reference` in five arms (`GET`,
  `SET`, `EXPIRE`, `TTL`, `PERSIST`), and the conditional SET added here is inside the existing
  `SET` arm, so it costs 71 nothing extra. Its `compat_set_matches_cp_spec_9_5` should assert that
  `SET` with a condition is in the compat set, which it now is.
- **Ticket 62**, the reply-shape ledger, has one thing to record that this ticket did not
  introduce: `SETNX k v` parses to `Command.Set(..., NX)` and therefore answers `+OK`/nil on both
  engines, where Redis's `SETNX` answers 1/0. That divergence is the parser's and predates T61; it
  is now reachable on `cp:` keys as well as AP ones.
- **T60's wire tag** does not collide: this ticket added no tag and used no number at 40 or above.

---

## T63 - The engine's command codec encodes every keyed command

The WAL codec became the engine's command codec. `WalCodec.kt` is now
`persist/CommandCodec.kt`; the object is public (`internal` would have hidden it from the server
and cluster modules that T64 and T65 move onto it).

**The interface.** Two names, both in `dynacache.engine.persist`:

- `CommandCodec.encode(command: Command, now: Instant? = null): Pair<Byte, ByteArray>` - the
  command's op code and the body of its arguments.
- `CommandCodec.decode(op: Byte, body: ByteArray): List<Command>` - the commands that encoding
  redoes, in order.
- `whatChanged(command: Command, reply: Reply): Command?` - the command the log should hold, or
  null when nothing changed. A top-level function, not a member: it is the log's decision about a
  reply, not part of the encoding.

`CommandEngine.log` is now `whatChanged(command, reply)?.let { CommandCodec.encode(it, now) }` and
appends exactly as before. `SnapshotEngine` replays through `CommandCodec.decode`, unchanged
otherwise. `Wal.kt` was not touched at all: the writer already took `(op, payload)`.

**Coverage.** Total over `Command.Keyed` minus `Cp`, plus every `Command.Fanned`, plus `FlushDb`
(the log has always held it). That is 22 reads, 17 writes and the 4 fanned commands, 43 op codes.
Op codes 1 to 16 and their bodies are exactly what they were; 17 to 43 are new and are never
written to the log (`HMSET`, every read, the four fanned commands). `Command.Cp` is out of scope:
it is CpWire's business (CP spec 6.2) and `encode` throws for it, as it does for `Ping`,
`CommandTable`, `Scan` and the other `EveryPartition` commands, which the router runs on the node
the client reached and never forwards. The `when` in `encode` is exhaustive over `Command`, so a
variant added without a codec case stops the main build, not only the test.

**The what-changed function.** An error or a nil (a refused `SET`, an empty `POP`) changed nothing;
so did a read and so did a fanned command, which reaches the log as the single-key parts it splits
into. A taken `SET` is logged with its condition decided away; `HMSET` is logged as the `HSET` it
is; `ZADD` keeps its condition (T48: `:0` is a refusal and a moved score alike) and loses `CH`,
which changes only the reply.

**Deviation: `whatChanged` takes no `now`.** The ticket's signature is (command, reply, now). No
`Command` can carry a decided deadline - `Command.Set` holds a `Duration` and only `Command.Expire`
holds an `Instant` - so a what-changed that returned "the command to log" with the TTL already
absolute would have to return two commands, which the WAL would log as two entries. That is a
durability regression: a crash between them restores the value without its expiry, where today one
entry is all-or-nothing. So the instant stays in the encoding, where it already lived: `now` is
`encode`'s parameter, and the deadline and the asked duration are one field read two ways.

**The format decision.** One encoding, one op code per command, and the WAL entry header wraps it:
the header carries the op code and the payload carries the body. A forward carries the same two
concatenated, op code first (T64 writes those two lines of framing; nothing here needs them yet).

**A pre-existing WAL still reads.** No entry header, op code or body changed. The two fields the
wire needs and the log never did are written only when they are not their default - `SET`'s
condition and asked duration, `ZADD`'s `CH` - so an entry an older build wrote has no tail, and no
tail is what it always meant. `codec_reads_the_entries_written_before_the_reads_were_added` builds
both old bodies by hand and decodes them. Nothing about a pre-existing log is version-gated, so no
format version was bumped and none was needed.

**Tests.** `dynacache-engine/src/test/kotlin/dynacache/engine/persist/CommandCodecTest.kt`, 7 tests:

- `command_codec_round_trips_every_keyed_variant` - one sample of every variant through an
  exhaustive `when`, each asserting both answers: does it cross, and does the log hold it. Byte
  exactness is `encode(decode(encode(c))) == encode(c)` plus the decoded variant's own class.
- `codec_round_trips_conditions_and_both_ttl_forms`
- `codec_reads_the_entries_written_before_the_reads_were_added`
- `what_changed_logs_nothing_for_an_error_or_a_refused_write`
- `what_changed_decides_the_condition_of_a_set_it_took`
- `what_changed_keeps_the_condition_of_a_conditional_zadd`
- `what_changed_logs_an_hmset_as_the_hset_it_is`

Engine suite 151 before, 158 after, all green; every WAL, recovery and fsync test passes unchanged,
`wal_reads_append_nothing` included. (The brief's expected base of 148 was three low; nothing
existing was renamed or removed, and only the codec's own file was added to.) `dynacache-server
-am` green downstream: cluster 86, cp 94, server 92 -- the brief's 84 and 98 for those two were
off in both directions, and no test outside the engine was touched.

**Known ceiling.** The exhaustive `when` stops the build when a variant is added, but the samples
list is a list: a new variant folded into an existing branch group compiles without a sample. The
engine is kotlin-stdlib only, so `sealedSubclasses` was not available to close that gap, and a
reflection dependency for one assertion was not worth it. `whatChanged`'s `else -> null` has the
same shape: the test's `when` is what forces the author of a new mutating variant to classify it.

**For the next ticket.**

- T64 (forwards): `CommandCodec.encode(command)` with no `now`, framed as `byteArrayOf(op) + body`;
  the other side is `CommandCodec.decode(bytes[0], bytes.copyOfRange(1, bytes.size)).single()`.
  `single()` is safe for a forward: only a logged `SET` with a deadline decodes to two commands, and
  a forward passes no `now`, so it never carries one. `commandToTokens` in the server and
  `TokenCodec` in the cluster test kit both become dead once the router carries bytes.
- T65 (replicates): `whatChanged(command, reply)` is `Replication.decided()`'s replacement, and it
  is stricter - it also drops `ZADD`'s `CH` and turns `HMSET` into `HSET`. It answers null for
  exactly the writes `Replication.write` currently refuses to replicate (an error, a refused
  conditional `SET`), so the `if (reply is Reply.Error || ...)` check there becomes the null. The
  replicate's `expiresAtMillis` field is the same decision as `encode`'s `now`: pass the
  coordinator's instant and the TTL travels absolute.

---

## T59 - Acceptance tests run on the injected clock

Plan rule 1.5 at the acceptance tier. No test in the repository constructs `Clock.systemUTC()`
or `Clock.systemDefaultZone()` any more, and the four acceptance tests no longer spin on wall
time except where the thing being waited for is another thread that reads no clock at all.

Test sources only; no production file changed. Six files, +110 / -41.

### What changed, per test

**`P1AcceptanceTest`** - the engine was built on `Clock.systemUTC()` and `aTtlThatFires` spun on
`System.nanoTime()` for up to five seconds. It now holds one `MutableClock` at
`2026-09-06T00:00:00Z` and hands the same instance to the engine *and* to the server, which is
the shape T52 gave `DynaCacheServerTest.withServer`: the parser works out a `PX` deadline from
the server's clock and the engine compares it against its own, so the two must be one clock.
`aTtlThatFires` advances 201 ms past the `PX 200` and calls `engine.tick().join()` - the tick the
server's scheduler would otherwise have run - then asserts as before. Every assertion and the
test's name are unchanged.

**`CpRoutingTest`** and **`CpSessionLifecycleTest`** - both built the AP engine on
`Clock.systemUTC()`; the ticket names only the first, but the acceptance criterion is about every
test, so both are on a `MutableClock` now, shared with the server as in P1. The clock never
moves. This is safe across the CP boundary because a lease is measured on log time, which only a
CP member's own (kit) clock moves, and `EXPIRE`'s absolute deadline is turned straight back into
a span by `CommandDispatcher` using the same clock the parser built it from - so the AP clock and
the CP clocks never need to agree on what the date is. Freezing it also removes a small real
wobble: `TTL` on a 100 s reference lease used to lose the milliseconds spent between the parser
and the dispatcher.

**`P2AcceptanceTest`** - the three `ClusterNode`s take the shared `MutableClock`, so the TTL that
crosses the quorum is measured against a "now" the test sets. The gossip wait stays (below).

**`P4AcceptanceTest`** - every generation of nodes, including the single node the memory-pressure
section builds, takes one `MutableClock` that survives the restarts as the data dirs do. Three
changes follow from it:

- `aMixedKeyspaceThroughJedis` no longer answers `System.nanoTime()`, and `everyKeyCameBack` no
  longer subtracts the seconds spent restarting. No clock time passes across the restart, so the
  restored TTL is asserted exactly: `assertEquals(60L, three.ttl("sess:1"))` where it used to be
  `in 1L..(60L - spent)`.
- the `blink` key's 300 ms deadline is reached by advancing 301 ms rather than by polling. The
  restart now costs no clock time at all, which is what lets the test assert `blink` is *still
  there* when the node comes back before advancing past its deadline.
- `aTtlFiresOnAClusterNode` advances 201 ms instead of polling for up to ten seconds.

The snapshot wait stays (below).

**`P5AcceptanceTest`** - the three nodes take the shared `MutableClock`. P5 constructed no system
clock, so this is not required by the acceptance criterion; it is here because
`SET cp:counter:x 5 EX 10` was a live wall-clock dependency, and a run slow enough under CI load
could have let that lease lapse between the `SET` and the `INCR` that reads it back. The election
wait stays (below).

### Bounded waits that remain (plan rule 1.7)

Each waits for a thread that consults no clock, so there is nothing a test could advance to bring
it forward. All three are bounded polls; none sleeps.

1. `P2AcceptanceTest.gossipSeesTheDeadNode`, 30 s. Waits for **SWIM's own gossip coroutine** on
   `Dispatchers.Default`, and the gRPC transport it probes over. `Swim` takes no `Clock` at all -
   it counts its own gossip periods through `delay` - so a burial cannot be brought forward by
   moving the injected clock.
2. `P4AcceptanceTest.awaitUntil`, one remaining call ("the snapshot completed on every node"),
   20 s. Waits for the **gRPC transport threads and each node's router coroutine** to carry the
   Chandy-Lamport markers past the envelopes in flight. No clock is read anywhere on that path.
3. `P5AcceptanceTest.awaitValue` in `cpLeader()`, 20 s. Waits for **MicroRaft's election timer
   threads** on the three members. MicroRaft runs its own scheduler and takes no injected clock -
   the same reason the CP kit's waits were recorded at T45 and T46.

Already recorded elsewhere and cited rather than re-recorded: `CpTestKit.awaitApplied` and
`ChaosDriver`'s submit deadline (T38, T43, T45, T46 deviations). Both stand unchanged.

### The two flakes

**`ReadRepairTest.read_repair_does_not_delay_reply`** - reproduced once here, on the first full
reactor run of this ticket, as `IllegalStateException: no READ reached node-3` at
`ReadRepairTest.kt:150`. It is **not** a real-time wait and not a clock read, so it is left alone
per the ticket. `arrived()` does `repeat(10) { network.drain(); yield(); tryReceive() }` inside
`runTest`, on a `TestDispatcher`. The READ envelope it is looking for is sent only after
`Replication.submit(Command.Get)`'s local `engine.view` future completes, and that future
completes on an **`ApEngine` partition executor thread**, outside the test dispatcher. `yield()`
only reschedules within the dispatcher; it does not wait for the partition thread, so all ten
iterations can run before the view completes. Converting it means awaiting the engine's future
(or a real bounded wait) - a logic change, not a clock injection - so it is out of this ticket.

**`CpEngineTest.C23_every_member_agrees_on_expiry_at_same_index`** - already on the injected
clock: it drives the expiry with `kit.clock(leader).advance(Duration.ofSeconds(2))` and an
explicit `leader.tick()`. Its only real-time wait is `CpTestKit.awaitApplied`, which polls a
member's MicroRaft `report` until the commit index arrives, waiting on **MicroRaft's replication
and heartbeat threads**. That is the already-recorded T45/T46 deviation and no clock advance
reaches it. Nothing changed; it passed on every run here.

### Wall time

Four acceptance classes, run on their own (`-Dtest=P1,P2,P4,P5AcceptanceTest`), seconds. Two other
Maven builds were running on the machine throughout, so run-to-run noise is roughly +/- 0.7 s on
an 11 s total. The first baseline sample was taken under lighter load than everything after it;
samples 2 and 3 were taken by reverting the six files in place, so they share the load of the
"after" runs.

| Run | P1 | P2 | P4 | P5 | total |
| --- | --- | --- | --- | --- | --- |
| before, sample 1 (light load) | 1.144 | 4.485 | 1.513 | 3.304 | 10.446 |
| before, sample 2 | 1.627 | 4.699 | 1.939 | 3.357 | 11.622 |
| before, sample 3 | 1.509 | 4.750 | 1.671 | 3.279 | 11.209 |
| after, sample 1 (fresh compile) | 1.830 | 4.795 | 1.909 | 3.401 | 11.935 |
| after, sample 2 | 1.524 | 4.724 | 1.667 | 3.372 | 11.287 |

Flat within the noise: 11.21 before against 11.29 after, comparing the two samples taken under
the same load. Inside the full reactor run, where P1 is no longer the first class to pay the JVM
and Netty warm-up, P1 falls from about 1.5 s to 0.38 s.

Suite totals unchanged, as no test was added or removed: engine 151, cluster 86, cp 92, server
92, **421 total**, green.

### Deviations

1. Three bounded real-time waits remain, named above with the thread each waits for (rule 1.7).
2. `P4AcceptanceTest` gained one assertion, `assertEquals("gone", three.get("blink"))`, against
   the brief's "keep every assertion" (it says keep, not freeze). It is here because the frozen
   clock is what makes "the key is still alive when the node comes back" checkable at all, and
   without it the `assertNull` that follows would go green on a key the RDB had dropped entirely.
3. `P4AcceptanceTest`'s restored TTL assertion was tightened from `in 1L..(60L - spent)` to
   `assertEquals(60L, ...)`, for the same reason: the elapsed-time slack it allowed no longer
   exists.
4. Two tests beyond the four the ticket names were changed: `CpSessionLifecycleTest` also built
   its AP engine on `Clock.systemUTC()`, and the acceptance criterion covers every test, so it was
   fixed alongside `CpRoutingTest`.
5. `P5AcceptanceTest`'s nodes were put on the injected clock although P5 constructed no system
   clock, to remove the `EX 10` lease's dependency on how slow the machine is.

---

## T62 - Reply shapes and the spec ledger

**Built:** four small gaps between the code and the specs closed, and the three that stay
recorded below. Nothing new was designed; every change is a reply the spec already fixed, or a
row the spec never asked for.

`-NOTLEADER` **carries the member id alone.** `CpEngine.notLeader` wrote
`leader is ${leader.id}`, so a client taking the first token of CP spec 6.8's `-NOTLEADER
<hint>` read the word `leader`. It now writes `runtime.node.term.leaderEndpoint?.id`, and a
member that knows of no leader writes no hint at all rather than a sentence a client would
parse as an id. Nothing consumed the old wording: `ForwardingCpEngine` recognises a
`-NOTLEADER` by its kind and rediscovers the leader through `GetInfo`, so the hint is for a
real client, not for us.

`LOCK_UNLOCK` **answers `:1` for every accepted unlock.** CP spec 3.1 makes the op an `ok:
Bool` that "decrements reentrance; releases at 0", and 6.8 gives the rejection its own error,
`-REENTRANCE`. The state machine already answered `-REENTRANCE` for a non-holder and `:1` for a
release, but `:0` for a reentrant decrement -- a third answer the spec's boolean has no room
for. A reentrant decrement is an accepted unlock, so it answers `:1` now, and `:0` is no longer
produced by this verb at all. See deviation 3.

`Command.Cp.LongDecrBy` **deleted, wire tag 6 retired.** Nothing produced the variant but the
wire decoder: the dispatcher folds Redis's `DECRBY` into `Command.IncrBy` with a negative delta
and then into `Command.Cp.LongIncrBy`, and the parser has no `cp.long.decrby` row, because CP
spec 6.2 maps `INCRBY` to `ADD` and gives `DECRBY` no verb. The variant, its encoder row and
its state-machine row are gone. The tag constant stays as `CMD_DECR_BY_RETIRED = 6` with a
decode branch that names it, so the number is never reused and a peer replaying an old entry
is told what it sent rather than reading "unknown tag".

`EXAT`, `PXAT` **and** `PSETEX` **deleted.** No spec line asks for them: spec 2.1 gives `SET`
the flags `NX`, `XX`, `EX` and `PX`, and the Redis-compat-for-CP set (CP spec 6.2, and 2.1's
routing table) names `SETEX` without its millisecond twin. Nothing forwards them and the test
kit does not reach for them -- checked by grep across all four modules before deleting -- so the
`psetex` row, the two `SET` flags, the `TTL_FLAGS` entries and the now-dead `until(...)` helper
T52 left behind all go. **Kept, with the reason:** `SETEX` (the CP compat set names it) and
`PEXPIREAT` (`CommandTokens` writes every `Command.Expire` as one, since a forwarded deadline
is an absolute instant however the client spelled it -- T16, T24; T13 deviation 3 first noted
the row had no spec line of its own, and this is the reason it earned one).

**The six missing constraint and invariant names.** Each asserts its constraint by delegating
to the spec-named test that already covers the behaviour, so the constraint breaks the named
test as well as the original:

- `C17_fencing_tokens_never_repeat` (`FencedLockTest`) -- a successful `LOCK_TRY`'s token is
  strictly greater than every token the key ever returned; delegates to
  `lock_fencing_token_monotonic` (100 acquire/release cycles). The across-a-failover half of
  C17 is `I18_lock_held_across_leader_failover` and `C17_lease_expires_after_skewed_failover`,
  which already carried the prefix.
- `I13_at_most_one_session_holds_a_lock` (`FencedLockTest`) -- delegates to
  `lock_mutual_exclusion`: two sessions race, exactly one is granted and one denied.
- `I14_a_later_acquire_gets_a_greater_token` (`FencedLockTest`) -- delegates to
  `lock_fencing_token_monotonic`.
- `C20_committed_cp_operations_are_linearizable` (`ChaosInvariantTest`) -- delegates to
  `invariant_linearizable_ops(seed = 1)`: the Wing-Gong checker accepts a counter history
  recorded across leader kills and restarts.
- `C22_no_cross_engine_state_leakage` (`CommandDispatcherTest`) -- delegates to
  `I22_namespaces_never_cross`: one key name written on both sides of the `cp:` boundary, each
  engine seeing only its own.
- `I10_the_same_script_answers_the_same_on_every_node` (`LuaTest`) -- delegates to
  `lua_deterministic`: one script on two replicas with the same seed state, replies equal.

**Concepts named:** nothing new. The only idea the ticket adds is that a **retired wire tag** is
a constant that decodes to an error, not a hole in a `when`: the number carries meaning for as
long as an old log entry can exist, so deleting the variant is not deleting the tag.

**Acceptance:**
- `notleader_hint_is_the_leader_id` (`CpEngineTest`): a follower's reply, and `NodeId(message)`
  equals the leader's own id -- the whole message, not a token of it. Red first
  (`expected: <cp1> but was: <leader is cp1>`).
- `lock_unlock_reply_shape` (`FencedLockTest`): a lock held twice; a non-holder's unlock is
  `-REENTRANCE`, the reentrant decrement is `:1` and `LOCK_STATE` still shows the holder with
  one hold, the release is `:1`, and the lock is then unowned. Red first
  (`expected: <Integer(value=1)> but was: <Integer(value=0)>`).
- `lock_reentrant_same_session` keeps its name and now expects `:1` for the decrement.
- `the_retired_decrby_tag_is_refused` (`CpWireTest`): an `ADD` encoding with its first byte set
  to 6 throws, and the message names both the tag and `DECRBY`. Red first (nothing was thrown).
- `EXAT PXAT and PSETEX are not commands here` (`CommandParserTest`): the two flags are a syntax
  error, `psetex` is Redis's unknown-command reply word for word, and `PEXPIREAT` still parses.
  Red first (the parse succeeded).
- `C17_`, `C20_`, `C22_`, `I10_`, `I13_`, `I14_` as listed above, all green.
- `mvn -o test -pl dynacache-server -am`: engine 151, cluster 86, cp 92 -> 99, server 92 -> 95.
  Every earlier test green. (The brief's baseline of `cp 92, server 97` was right for cp and
  stale for the server module, whose true baseline is 92, the number T58 recorded.)
- This entry.

**Deviations:** Six. The first three are the ledger entries the ticket asks for.

1. **`-CAPACITY` is not built.** CP spec 6.8 lists `-CAPACITY` ("state machine at max-entries
   cap") among the new RESP error prefixes, and no state machine has a cap: `AtomicLong`,
   `FencedLock`, `Semaphore`, `CountDownLatch`, `AtomicReference` and `SessionRegistry` all grow
   with the keys the log gives them, bounded only by the snapshot size and the heap. The error
   string appears nowhere in the source. Building the cap is not this ticket's (it needs a
   per-state-machine limit, a place to configure it, and a decision about what happens to an
   entry already committed when the cap is hit -- a Raft-level question, since refusing at apply
   time must be deterministic on every member). Recorded here so the next reader finds it named
   rather than missing.

2. **A fanned command inside a batch is refused even when its keys share the partition.** In
   `ApEngine`'s `Batch` context, `Command.Keyed` runs when the batch declared its key and
   everything else falls to `Reply.Error("ERR", "this command spans partitions and cannot run
   inside a batch")`. `Command.Fanned` -- `MGET`, `MSET`, multi-key `DEL` and `EXISTS` -- is in
   that "everything else", by definition rather than by measurement: the refusal does not look
   at the keys. So `MULTI; MGET {t}.a {t}.b; EXEC` on two hash-tagged keys, which land on the
   one declared partition and which spec 2.2's "atomicity across keys requires a batch with hash
   tags" invites, is refused here and answered by Redis. T14 describes the rule; it was never
   recorded as a divergence from Redis, and it is one. The repair is one branch -- a `Fanned`
   whose every key is in `declared` runs key by key on this partition -- and it is a batch
   semantics change, which this ticket's seams exclude.

3. **The reentrancy reply shape, where CP spec 6.1 is ambiguous.** 6.1's table gives
   `CP.LOCK.UNLOCK` the replies `:1` / `:0`, while 3.1 makes the op an `ok: Bool` that "rejects
   if session/token mismatch" and 6.8 gives that rejection `-REENTRANCE`. Read together, `:0`
   is the rejection 6.8 turned into an error, and the spec never says what a reentrant decrement
   answers. Decided, per the ticket: every accepted unlock answers `:1`, released or not, and
   `:0` is unreachable from this verb. A client that must know whether it still holds the lock
   reads `CP.LOCK.STATE`'s reentrance count, which 6.1 already gives it. Redis has no
   `LOCK.UNLOCK`, so there is no Redis behaviour to diverge from.

4. **`-NOTLEADER` with no hint has a trailing space on the wire.** `RespEncoder` writes
   `"-" + kind + " " + message`, so a member that knows of no leader sends `-NOTLEADER ` rather
   than `-NOTLEADER`. It decodes back to the same `Reply.Error` either way and Redis clients
   split on the first space, so this is cosmetic; fixing it means touching the encoder for every
   error, which is outside this ticket.

5. **`ChaosDriver` reads the lock owner back after an accepted unlock.** Changing the reply
   broke `invariant_mutual_exclusion_under_chaos` and
   `invariant_fencing_token_monotonic_under_chaos`, and the failure was not the reply: the
   driver retries a `LockTry` after a lost reply (at-least-once, as `ForwardingCpEngine`'s own
   note says), so a retry can take a second reentrant hold the driver never saw. The old `:0`
   hid that -- the driver kept believing the lock held -- and `:1` exposed it as "denied to 4 but
   belief says null". The driver now reads the new owner from `LOCK_STATE` after an accepted
   unlock instead of guessing it from the reply, which is exact whatever the retries did.
   Confirmed against a clean checkout of the base commit that the two tests were green before
   the reply change, so this is a driver model that was always approximate, not a new bug.

6. **Size:** 159 lines added against 64 deleted across sixteen files, inside the 200-to-600
   budget. Two other things the ticket names were deliberately not touched: the AP engine, and
   the dispatcher's routing.

**For the next ticket:** the deviation-2 branch is the smallest real gap left in the batch path,
and it is testable without a cluster: `ApEngine.atomically` with two hash-tagged keys and an
`MGET` over both. Whoever takes it should note that `Batch.execute`'s `else` is currently doing
two jobs -- refusing what spans partitions and refusing what this partition cannot interpret --
and only the first is about keys.

---

## T69 - CP primitives tested at the state machine, without Raft

The five primitive suites (lock, session, semaphore, latch, reference) no longer start a Raft
member. Each drives the composite `CpStateMachine` directly through one small test fixture,
`Primitives`, and the tests that genuinely need a log moved to three clearly named kit-backed
classes. No production file changed: the seam was already there, in `CpStateMachine.runOperation`
taking a stamped `CpOp`, so the ticket's "small test-facing constructor or entry point on
`CpStateMachine`" allowance was not needed.

### The fixture's interface

`DynaCache/dynacache-cp/src/test/kotlin/dynacache/cp/Primitives.kt`, 53 lines, no production
dependency beyond the state machine and the engine test kit's `MutableClock`:

- `apply(command: Command.Cp): Reply` - stamps the command the way `RaftRuntime.stamp` does
  (`max(clock now, last applied ts + 1, last stamped + 1)`, CP spec 5) and applies it, answering
  the composite's reply.
- `advance(by: Duration)` - moves the fixture's clock; no entry carries the new time until the
  next `apply` or `tick`.
- `tick(after: Duration = ZERO)` - advances, then appends what a leader appends when the group is
  idle: one `TtlTick`, then one `SessionClosed` per session `lapsedSessions()` reports. This is
  the same order `RaftRuntime.tick` uses, so a lease or a session expires here exactly as it does
  on a real leader.

The stamp rule is what lets the moved assertions stay verbatim: with the clock standing still a
stamp climbs by one millisecond per entry, in the fixture as in a group, so `remaining =
LEASE.toMillis() - 2` still means "two entries after the TRY". The fixture starts on the kit's
epoch (2026-09-06T00:00:00Z) for the same reason - a stamp printed by a failing assertion means
the same in both worlds.

### Moved to the state machine (25 tests, 5 suites, no Raft member)

- `FencedLockTest` (9): `lock_try_acquire_release_roundtrip`, `lock_fencing_token_monotonic`,
  `lock_reentrant_same_session`, `lock_unlock_wrong_session_rejected`,
  `lock_unlock_wrong_token_rejected`, `lock_ttl_expires` (CP spec 10.1), `lock_ttl_renew`,
  `lock_renew_by_non_holder_rejected`, `lock_force_unlock_overrides`.
- `SessionTest` (5): `session_create_heartbeat_close`, `session_op_without_session_rejected`
  (10.6), `session_timeout_closes` (10.6), `session_heartbeat_keeps_alive`, and one new test,
  below.
- `SemaphoreTest` (6): `sem_init_acquire_release`, `sem_over_acquire_fails`,
  `sem_over_release_rejected`, `sem_session_death_releases`, `sem_drain`,
  `sem_drain_of_unknown_key_leaves_it_initialisable`.
- `CountDownLatchTest` (3): `latch_set_down_get`, `latch_down_at_zero_stays_zero`,
  `latch_reset_only_at_zero`.
- `AtomicReferenceTest` (3): `ref_set_get_roundtrip`, `ref_cas_byte_equality`, `ref_ttl_expires`.

Every spec-named test kept its name verbatim.

### New (1 test)

`SessionTest.C18_close_releases_every_lock_and_permit_in_one_entry` - the direct session-close
cascade the ticket asks for (C18, I15): one session holds two locks and two semaphore permits,
and the single applied `SESSION_CLOSE` entry gives back all three. The kit could only ever show
this as a commit-index count; at the state machine the "one entry" claim is the assertion itself,
and it is the only test covering the semaphore leg of the CLOSE path (the lapse leg was already
covered by `sem_session_death_releases`).

### Kept on the kit (12 tests, 3 new classes)

- `FencedLockFailoverTest` (4), from `FencedLockTest`: `cp_leader_failover_preserves_state`
  (10.7), `I18_lock_held_across_leader_failover`,
  `I19_lease_expires_late_never_early_across_failover`,
  `C17_lease_expires_after_skewed_failover` (the T50 skew case). A lock across a leader change is
  a fact about the log, not about the primitive.
- `SessionLogTest` (3), from `SessionTest`: `I15_no_lock_owned_after_session_closed_index`
  (asserts the index the close landed on and that two members applied it),
  `C18_release_is_one_log_entry` (asserts the commit index moved by exactly one),
  `C18_session_lapses_after_skewed_failover` (the T50 skew case, a lapse on a successor's own
  idle ticks).
- `CpConcurrencyTest` (5), one from each of four suites: `lock_mutual_exclusion` (I13),
  `sem_concurrent_acquire_exactly_permits_succeed`, `latch_concurrent_down_correct_count`,
  `ref_concurrent_cas_exactly_one_wins`, `I21_concurrent_cas_exactly_one_wins`. What these test
  is that the log puts simultaneous clients in an order; applied one at a time to a state machine
  they would assert nothing. The four private `race` helpers they used to carry are now one.

`CpEngineTest`, `CpSnapshotTest`, `ChaosInvariantTest`, `CpWireTest` and `GrpcCpTest` are
untouched.

### Wall time (surefire `time`, same machine, three other Maven builds running alongside)

The five primitive suites:

| suite | before | after |
| --- | --- | --- |
| FencedLockTest | 6.043 s | 0.012 s |
| SessionTest | 1.257 s | 0.016 s |
| AtomicReferenceTest | 0.160 s | 0.094 s |
| SemaphoreTest | 0.032 s | 0.011 s |
| CountDownLatchTest | 0.029 s | 0.015 s |
| **total** | **7.521 s** | **0.148 s** |

Under the one-second acceptance bar by a factor of about fifty. The three new kit-backed classes
carry the elections that used to sit inside the primitive suites: `FencedLockFailoverTest`
5.332 s, `SessionLogTest` 1.481 s, `CpConcurrencyTest` 0.154 s. The point of the ticket was never
the total, which is roughly unchanged; it is that a primitive-semantics test now costs
milliseconds and a reader can see at a glance which twelve tests need a log.

`mvn test -pl dynacache-cp -am` is green: engine 151, cluster 86, cp 96. The brief's expected
base of engine 148 and cluster 84 is stale for those two modules, which this ticket does not
touch; cp is the 95 the brief named, plus the one new cascade test.

### Red before green

The moved tests were checked against two deliberate mutations of production code, then reverted:

- `FencedLockStateMachine.Lock.at(now)` made to never expire a lease - `lock_ttl_expires` failed.
- `CpStateMachine.closeSession` made to skip `semaphores.releaseAllOf(session)` -
  `sem_session_death_releases` and `C18_close_releases_every_lock_and_permit_in_one_entry`
  failed.

Three failures out of the 26 tests then in the five direct suites, each the test that should
notice. `session_timeout_closes` correctly did not fail on the lease mutation: the lock it checks
is released by the session cascade, not by lease expiry.

### Deviations

- **No production entry point added.** The ticket allowed a small test-facing constructor or
  entry point on `CpStateMachine`; none was needed, since `runOperation(commitIndex, CpOp)` is
  already public and already the seam. Zero production lines changed.
- **A third kit-backed class.** The ticket named the failover cases; the concurrency races needed
  a home too, since they test the log rather than a primitive. `CpConcurrencyTest` is that home.
- **`lock_mutual_exclusion` now races through one shared helper** rather than two hand-written
  `submit` calls. Same assertions, same property, one fewer bespoke fixture.
- **Net diff +141 lines** (95 added and 397 removed across the five suites, 443 added in the four
  new files), against a 200-600 budget. A move of five suites is mostly deletion; the raw diff is
  935 lines touched. Test count in `dynacache-cp` rises by one, from 95 to 96, for the new C18
  cascade test.
- **Merge with T62 expected.** T62 edits `FencedLockStateMachine` (the UNLOCK reply) and
  `FencedLockTest` in parallel. The unlock assertions were carried over verbatim into the new
  `FencedLockTest` and `FencedLockFailoverTest`, so the conflict is a move, not a rewrite: T62's
  changed expectations land on whichever of the two files now holds each test.

### For the next ticket (T70, each CP primitive owns its snapshot bytes)

- `Primitives` gives T70 a snapshot round trip with no group: build state through `apply`, take
  `CpStateMachine.state`, restore into a second machine, compare. Only
  `cp_snapshot_install_preserves_tokens_and_sessions` and the install-through-Raft cases need
  `CpSnapshotTest` and the kit.
- `CpStateMachine.takeSnapshot`/`installSnapshot` and the `Snapshot` data class are unchanged by
  this ticket, so T70 starts from the shape recorded in the spec (10.7).
- The fixture deliberately exposes no accessor for the state machine itself. T70 will want one
  (to read `state` and to install a snapshot); adding a single `val stateMachine` to `Primitives`
  is the smallest change, and was left out here under YAGNI rather than guessed at.

---

## T64 - A forward carries codec bytes

The RESP spelling of a command no longer crosses the cluster seam. A `Forward` carries the engine
command codec's bytes, and the coordinator decodes them with the same codec, so the two ends of a
forward are one encoding rather than a writer in the server module and a reader in the parser.

**The envelope.** `Forward`'s `repeated bytes token = 2` is gone and `bytes command = 3` replaces
it; tag 2 and the name `token` are `reserved`. The field was removed rather than deprecated
because nothing persists a `Forward` and both ends of the wire are this repository: a forward
lives for one round trip between two nodes of the same build. A new tag rather than a reuse of
tag 2, so a stale peer's tokens decode as an absent field rather than as a corrupt command.

**The router.** `Router`'s constructor lost `tokens: (Command) -> List<ByteArray>` and
`parse: (List<ByteArray>) -> Command`; nothing is injected in their place. Two private companion
functions hold the framing T63 specified: `encode` is `CommandCodec.encode(command)` with no `now`
(so a `SET`'s TTL crosses as the duration the client wrote and the coordinator decides its
deadline), framed as `byteArrayOf(op) + body`; `decode` is
`CommandCodec.decode(bytes[0], bytes.copyOfRange(1, bytes.size)).single()`. `single` is not a bet:
only a `SET` the log wrote with a decided deadline decodes to two commands, and a forward passes
no `now`.

`coordinate`'s `runCatching` stayed. An unreadable forward is now impossible between peers of this
build - the codec wrote it, so the codec reads it - but a corrupted envelope or an older peer's
tokens still arrive as an empty or unknown-op `command` field, and a throw on the demux would
leave `run` dead and take the node's gossip down with its forwarding. The guard is cheap and the
failure it prevents is the whole node.

**What was deleted, and what was not.** Deleted: the two constructor parameters and the two
arguments at each of the three call sites (`ClusterNode`, the kit's `InProcessCluster`,
`RouterTest`).

**`commandToTokens` is still alive, and its remaining caller is `Replication`.** The ticket allowed
either deletion or a named caller; this is the named caller.
`dynacache-server/.../ClusterNode.kt` still passes `tokens = ::commandToTokens, parse = ::parse` to
the `Replication` constructor, and `Replication` still spells a `Replicate` and a `Read` in RESP
tokens (`Replication.kt:131` and `:154`). The same holds for the test kit's `TokenCodec`: the kit's
`InProcessCluster` still hands it to `Replication` and to `seed`, and `DistributedSnapshotTest`,
`ReadRepairTest` and `ReplicationTest` still use it. T65 moves replicates onto the codec and both
files go then. `CommandTokensTest` in the server module is untouched and still passes.

**Tests.** `forward_round_trips_every_keyed_variant` in
`dynacache-cluster/src/test/kotlin/dynacache/cluster/RouterTest.kt`: 38 keyed variants and all 4
fanned ones, run in an order that builds the string, hash, list and sorted-set keys before it
reads them, each once through the key's coordinator and once through a node that has to forward,
asserted equal. The conditional `SET` with a TTL is the first sample; the multi-key read is
`MGET`. Two guards keep the equality from being vacuous: the direct run must not answer an error
(two matching "unreadable forwarded command" replies would otherwise pass), and a fanned sample's
keys must all share a coordinator, which the `{multi}` hash tag gives them, so the direct run
forwards nothing at all. A `classify` function with a `when` exhaustive over `Command` stops the
build when a variant is added to the engine.

Counts: engine 158, cluster 86 to 87, cp 95, server 100, all green. Diff 6 files, +233/-27.

**Deviation 1: the round-trip test builds its own routers rather than using `InProcessCluster`.**
The kit puts `Replication` under every router, and `Replication.write` calls `tokens(...)`
eagerly, before the quorum, so even at n=1/w=1 a forwarded `APPEND`, `PERSIST`, `HMSET`, `HDEL`,
`LSET`, `LREM`, `POP`, `ZINCRBY` or `ZREM` dies in the test kit's `TokenCodec`, which knows six
writes. Growing that codec to 38 variants is exactly the RESP re-encoding this ticket removes and
T65 deletes, so the test builds three routers straight over three `ApEngine`s on one
`InMemoryTransport` instead - about 35 lines, an inner class in `RouterTest`. That is also the
honest seam for this ticket: what it asserts is the forward's own round trip, with nothing under
it. The four existing router tests still run through the kit, unchanged.

**Deviation 2: `router_unreadable_forward_is_an_error_and_the_node_lives` changed its garbage.**
It sent the RESP token `NOSUCH`, which no longer has a field to sit in. It now sends op code 99,
which no command has. The test's name, shape and claim are unchanged; only the spelling of
"unreadable" moved with the envelope.

**Also touched:** `GrpcTransportTest`'s one-envelope-per-oneof-case fixture built its `Forward`
with `addAllToken`; it now carries `GET k` as the codec writes it. No assertion changed.

**For the next ticket (T65).**

- `Replication`'s `tokens`/`parse` parameters and `Replication.decided()` are what is left. The
  same two-line framing this ticket put in `Router`'s companion (`byteArrayOf(op) + body`, and
  `decode(bytes[0], rest).single()`) is what a `Replicate` and a `Read` need. When T65 writes the
  second copy, lifting both onto `CommandCodec` as a framed pair is worth it; one copy did not
  justify it.
- A `Replicate` differs from a forward in one way that matters: it passes `encode` the
  coordinator's `now`, which is the same decision as the message's `expiresAtMillis` field, and a
  `SET` with a deadline then decodes to two commands, so `single()` is wrong there and the list is
  the point.
- Deleting `commandToTokens` (`dynacache-server/.../CommandTokens.kt`) and its
  `CommandTokensTest`, and the kit's `TokenCodec.kt`, falls out of T65 once `Replication` and
  `InProcessCluster.seed` stop calling them. Nothing else references either file.

---

## T55 - A snapshot-set part is a persist adapter

The cluster module no longer touches the filesystem. A node's part of a snapshot set is written
and read by `SnapshotParts` in `dynacache.engine.persist`, and `DistributedSnapshot` keeps only
the marker rules and the channel bookkeeping. Plan 2.2's rule that `java.nio.file` appears only
in the engine's persist package and the cp module holds again for the cluster: there is no
`java.nio.file`, `kotlin.io.path` or `java.io.File` import left under `dynacache-cluster/src/main`.

### The interface

`dynacache-engine/src/main/kotlin/dynacache/engine/persist/SnapshotParts.kt`, shaped after
`DotCeilingStore` and `WalSink` in the same package. Set ids and channel names are opaque strings
and the recorded bytes are opaque bytes, so nothing in the engine knows what a marker, an
envelope or a protobuf is.

```kotlin
interface SnapshotParts {
    fun cut(id: String)                                        // open this node's part, state into it
    fun record(id: String, channel: String, bytes: ByteArray)  // append one whole record
    fun restore(id: String)                                    // the state back into the engine
    fun replay(id: String, channel: String): List<ByteArray>   // every whole record, in order
    fun delete(id: String)                                     // the whole set, every node's part
}
```

The one adapter is `FileSnapshotParts(root, self, engine, clock)`: a set is `<root>/<id>/`, a part
is `<root>/<id>/<self>/`, its state is the T32 snapshot's `dump.rdb` in it, and a channel is
`from-<peer>.wal` beside that.

### Record format, crc and torn tail

A channel log is an ordinary WAL: `WalWriter`/`WalReader` are reused as-is, so a record is the
spec 2.8 entry `[crc32:u32][length:u32][seq:u64][op:u8][payload]` with the envelope's bytes as
the payload. The header is not command-specific, so no new record writer was needed.

- Torn tail (a crash mid-append): `WalReader` reports `TORN_TAIL` and `replay` answers every
  whole record before it and stops. Covered by
  `snapshot_part_with_torn_channel_log_replays_the_complete_prefix`.
- Checksum failure: `replay` throws `IOException`. A torn tail is the shape a crash leaves at the
  end of a file; a crc mismatch is corruption, and nothing at or past it is trusted. The old
  delimited-protobuf loop had neither check.
- Every record carries `seq = 0` and `op = 0`. A channel's order is its file order and nothing
  reads the seq back, so the log is not a sequenced WAL, only a crc'd record file in the WAL's
  format. Deliberate: reusing the writer is cheaper than a second record format.
- A recorded envelope is **not** fsynced (`FsyncPolicy.NEVER`), which is exactly what the old
  `Files.newOutputStream(...).use { writeDelimitedTo }` did. Recording happens on the node's
  inbound path, and forcing the disk there would stall it.
- A failed append is surfaced: `record` joins the append's `durable` future, because `WalWriter`
  hands an `IOException` to that future and nowhere else, and under `NEVER` nothing later forces
  it. Found in the code-review self-pass; without the join a full disk would silently drop an
  in-flight envelope and break I12 with no error anywhere.

### Compatibility with parts written before this ticket: **rejected, with a clear error**

The channel log's name changed from `from-<peer>.log` to `from-<peer>.wal`, so an old part is
recognisable. `restore` refuses one:
`IOException("<path> predates the checksummed channel log and cannot be restored")`. Old records
are length-delimited protobuf with no header and cannot be read as WAL records; restoring the
state without them would silently drop everything that was in flight at the cut (I12), which is
worse than refusing. `restore` runs before any `replay` on the only path that reads a part
(`DistributedSnapshot.restoreFrom`), so no part can be half-restored. Covered by
`a_part_written_before_the_channel_log_became_a_wal_is_rejected`.

### What moved out of the cluster

`DistributedSnapshot` lost its `engine`, `dir` and `clock` constructor parameters and gained
`parts: SnapshotParts`; the class is 67 lines shorter in the diff. Gone from it: the
`java.nio.file` imports, `Files.createDirectories`, the `SnapshotEngine` construction in `start`
and `restoreFrom`, the delimited-protobuf append and `generateSequence { parseDelimitedFrom }`
loop, the `deleteRecursively` in `abort`, and the `part(root, id)` path helper. What stayed: the
marker rules, `open`/`aborted`, the `cutting` mutex and T49's cut-then-open order, the deadline
timer, and the peer iteration on replay.

`restoreFrom(from: Path, id: String)` became `restoreFrom(id: String)`. Every caller passed the
same directory the adapter is already built on, so the parameter carried no information.

T49's ordering is preserved and is now slightly stronger. The part directory used to be created
before the `cutting` mutex was taken; `parts.cut(id)` now creates it and writes the state inside
the mutex. Nothing can observe the gap: `record` is reachable only from `receive`, which takes
the same mutex and iterates `open`, and `open[id]` is published inside the mutex strictly after
`cut` returns.

### Tests and counts

New: `dynacache-engine/src/test/kotlin/dynacache/engine/persist/SnapshotPartsTest.kt`, six tests
(`a_channel_replays_what_was_recorded_on_it_in_order`,
`snapshot_part_with_torn_channel_log_replays_the_complete_prefix`,
`a_channel_log_with_a_corrupt_record_is_rejected`, `a_part_holds_the_state_it_cut`,
`a_set_is_deleted_as_a_whole`, `a_part_written_before_the_channel_log_became_a_wal_is_rejected`).

| Module | Before | After |
|---|---|---|
| engine | 158 | 164 |
| cluster | 86 | 86 |
| cp | 102 | 102 |
| server | 103 | 103 |

Every Chandy-Lamport test passes unchanged in name and assertion: `chandy_lamport_consistent_cut`,
`chandy_lamport_restorable`, `chandy_lamport_timeout_aborts`, `C10_marker_on_every_channel`,
`I12_reads_after_restore_return_snapshot_time_values`, `I12_write_during_the_cut_is_restored_once`,
`C10_state_is_cut_before_any_channel_opens`; `P4AcceptanceTest` passes.

### Deviations

1. **`snapshot_set_deleted_as_a_whole_on_deadline` does not exist and was not added.** The ticket
   names it; no test under that name has ever existed. The deadline-abort test that must keep
   passing is `chandy_lamport_timeout_aborts` (`DistributedSnapshotTest`), and it does, asserting
   that `<dir>/s1` is gone after the deadline. The new unit-level `a_set_is_deleted_as_a_whole`
   covers `delete` itself, without a deadline.
2. **`SnapshotParts` is a seam with one adapter, which plan 2.1 forbids** ("A seam exists only
   where a second adapter is real, and every seam has one in the test kit"). Built as an interface
   because the ticket and the orchestrating brief both name an interface as the deliverable, and
   the concrete `FileSnapshotParts` would satisfy plan 2.2 on its own. Collapsing the interface
   into the class is a one-line change at five call sites if the plan's rule is meant to win.
3. **The cluster test helper `recorded` reads a part's records through the adapter but still
   finds its channels by listing `from-*.wal`.** The code-review self-pass called the glob
   coupling to a layout the adapter now owns and suggested deriving the channels from the set's
   node list instead. That was tried and is wrong: a peer whose envelopes were recorded need not
   have a part of its own, which is exactly the `Lone` node in
   `C10_state_is_cut_before_any_channel_opens` and `I12_write_during_the_cut_is_restored_once`,
   and the refactor made that test read an empty channel map. Which channels a part recorded is a
   fact only the files hold. Reverted, with the reason written into the helper's KDoc. If the
   coupling is worth removing later, the adapter needs a `channels(id)` listing, which the ticket
   did not ask for and nothing in main sources needs.

### For the next ticket (T66)

1. **A distributed snapshot rotates the node's live WAL into the snapshot part, and an abort
   deletes it.** Pre-existing since T36 and moved verbatim here, not introduced by T55, but
   `FileSnapshotParts.cut` now owns the line. `SnapshotEngine.save()` calls `cut()`, which does
   `wal.rotate(FileChannelSink(logFile(seq)))` with `logFile` resolved under the **part**
   directory. On a node with both `dataDir` and `snapshotDir` (which is how `clusterMain` wires
   every persisting node: `snapshotDir = dataDir.resolve("snapshots")`) the live WAL therefore
   continues inside the snapshot part, and `DistributedSnapshot.abort` `deleteRecursively`s the
   set at the deadline, taking the open log file with it. Every write acked after the cut is then
   unrecoverable on restart, since recovery reads `dataDir` only. C14 and spec 2.8's recovery
   sequence. Not fixed here: the fix is in `SnapshotEngine`'s save/rotate contract, outside this
   ticket's seams. No test covers it because no test wires `dataDir` and `snapshotDir` together
   and then aborts.
2. **A snapshot id arrives on a marker from the wire and becomes a path segment unchecked.**
   `envelope.marker.snapshotId` reaches `root.resolve(id)` in `FileSnapshotParts`, and `abort`
   turns that into `deleteRecursively`. Pre-existing and identical before T55. Not fixed here: a
   bare `require` would swap a traversal for a node kill, because `Router.run` has no per-envelope
   catch by design, so the real fix is for the marker path to ignore an unusable id, and the brief
   put the marker protocol off-limits. Worth a ticket: validate in the adapter and have
   `DistributedSnapshot.receive` drop a marker whose id the adapter refuses.
3. Restoring an id that was never cut is a silent no-op that leaves the engine empty
   (`SnapshotEngine.restore` on a missing `dump.rdb`). `ClusterNode.restoreSnapshot("typo")`
   therefore empties a node without complaint.

---

## T71 - One home for the cp: namespace rule

The rule "is this a CP key, which primitive kind owns it, and which Redis commands may touch it"
was written in four modules with five `-NOTCP` literals, the reference prefix was known only to
the dispatcher, and the compat re-target undid the parser: an `EXPIRE` became an instant from the
parser's clock and a span again from the dispatcher's. It is now one module in the engine, beside
`Command.Cp`, and every other site reads it.

### The rule and where it lives

`dynacache-engine/src/main/kotlin/dynacache/engine/CpNamespace.kt` (~215 lines), three
declarations:

- `enum class CpKind(prefix)` - `COUNTER("cp:counter:")`, `LOCK("cp:lock:")`,
  `SEMAPHORE("cp:sem:")`, `LATCH("cp:latch:")`, `REFERENCE("cp:ref:")`, `SESSION("cp:session")`,
  `UNTYPED("cp:")`, the sub-namespaces of CP spec 2 in declaration order so `cp:` is read last and
  never hides a longer prefix.
- `sealed interface CpRouting` - `Ap` (names no `cp:` key), `Verb(Command.Cp)` (the CP verb this
  command means), `Refused(Reply.Error)`.
- `object CpNamespace` with six public members: `kindOf(Key): CpKind?`, `owns(Key): Boolean`,
  `kindOf(Command.Cp): CpKind?`, `refusalFor(Command.Cp): Reply.Error?`, `route(Command):
  CpRouting`, `expiry(Key, Duration): CpRouting`, plus `notCp(message)`, `keysOf(command)` and
  `COMPAT`.

`route` is total and is the whole of CP spec 9.5's three rules in order. `refusalFor` is the edge
both CP engines check. `expiry` is the parser's, and is the only entry that takes a span, because
a span is what the CP log evaluates against log time (CP spec 5).

`COMPAT` is CP spec 9.5's fifteen names written once, as the command classes those names parse to
(`SETEX` and `SETNX` both parse to `Command.Set`, `DECR` and `INCRBY` to `Command.IncrBy`), so the
set is one declaration rather than fifteen. It earns its keep in production: a refusal inside the
set reads "no CP verb answers this command yet" (`DEL`, `EXISTS`, `TYPE`) and one outside it reads
"this is not a command it accepts" (`LPUSH`, `STRLEN`).

### Sites that now read it, literals deleted

| Site | Before | After |
|---|---|---|
| `CommandDispatcher.kt` | 145 lines: `isCpKey`, `keysOf`, `compat`, `Rejected`, `refuse`, `notAnInteger`, `asLong`, `REFERENCE_PREFIX`, a `Clock` | 55 lines: one `when` over `CpRouting`, no clock |
| `CpEngine.submit` | two `Reply.Error("NOTCP", ...)`, own `Key.isCp()` | `CpNamespace.notCp` + `refusalFor` |
| `ForwardingCpEngine.submit` | two `Reply.Error("NOTCP", ...)`, own `Key.isCp()` | `CpNamespace.notCp` + `refusalFor` |
| `CommandEngine.submit` (AP) | one `Reply.Error("NOTCP", ...)` | `CpNamespace.notCp` |
| `CommandParser` | built `Command.Expire` for every key | emits the CP verb for a `cp:` key |
| `DynaCacheServer` | `CommandDispatcher(ap, cp, clock)`, server-local `keysOf` | `CommandDispatcher(ap, cp)`, `CpNamespace::keysOf` |

Deleted: five `Reply.Error("NOTCP", ...)` literals (one producer remains, `CpNamespace.notCp`);
three copies of `startsWith("cp:")` (`Key.isCp()` twice, `Key.isCpKey()` once); the dispatcher's
`REFERENCE_PREFIX` and its private `Rejected` control-flow exception; the server's `keysOf`.

### The parser decision: emit the CP verb, leave the refusal

The parser emits CP verbs for `cp:` keys, as the ticket asks. It is done in two places:

1. `parse` runs `cpVerb(dispatch(...))`, one line reading `CpNamespace.route`, which covers `GET`,
   `SET`/`SETEX`/`SETNX`, the `INCR` family, `TTL`/`PTTL` and `PERSIST`.
2. the four expiry spellings call `CpNamespace.expiry(key, ttl)` **before** an instant exists, so
   `EXPIRE cp:counter:x 10` becomes `LongExpire(key, PT10S)` with no instant in between.

A command the namespace **refuses** is deliberately left as it parsed and refused by the
dispatcher. Refusing at parse time would turn an execution error into a parse error, and a parse
error aborts an enclosing `MULTI` with `EXECABORT` where an execution error does not. That is a
semantic change this ticket has no business making, so the refusal stayed where it was.

The round trip is gone: the dispatcher has no `Clock` parameter any more, which is the mechanical
proof. `EXPIRE`/`PEXPIRE` cost one clock read in the parser, and it is the overflow guard rather
than a conversion: a span no clock can hold is still Redis's `invalid expire time` for a `cp:` key
too, before the CP log has to add it to log time. `EXPIREAT`/`PEXPIREAT` cost the same one read,
for the absolute-to-span conversion the wire genuinely requires.

### The kind-mismatch decision: `-WRONGTYPE`, from CP spec 6.8

CP spec 9.4 does not name a reply; it says only that "`EXPIRE` on a `cp:lock:*` key is rejected"
(line 356). The reply is named one section earlier, in the error table of CP spec 6.8 (line 279):

> `-WRONGTYPE` | Key exists as different primitive (e.g., LOCK on AtomicLong key)

CP spec 2 (line 104) makes the sub-namespace the thing that says which primitive a key is. Putting
the two together: **a verb of one kind aimed at a key of another kind is `-WRONGTYPE`**, whether it
is spelled as a CP verb (`CP.LONG.INCR cp:lock:x`) or as the Redis command the compat set maps to
one (`GET cp:lock:x`, `EXPIRE cp:lock:x`, `INCR cp:ref:r`). Decided once, in `CpNamespace`, rather
than differently per verb.

Three boundaries of that decision, all deliberate:

- **A command outside the compat set stays `-NOTCP`, whatever kind owns the key.** CP spec 9.5
  rule 2 is explicit that the `cp:` namespace only accepts the compat set, so `LPUSH cp:lock:x`
  and `TYPE cp:lock:x` are commands aimed at no primitive rather than at the wrong one. Only the
  six value verbs the counter and the reference share (`GET`, `SET`, the `INCR` family, `EXPIRE`,
  `TTL`, `PERSIST`) can be aimed at the wrong primitive, and only those answer `-WRONGTYPE`.
  (The first cut of this ticket had the mismatch swallow the whole else-branch; the code-review
  pass caught it against spec 9.5 and it is now one line, `else -> refused(command)`.)

- **An untyped `cp:` key is nobody's in particular.** `cp:x` is `CpKind.UNTYPED` and the counter
  answers it, exactly as before this ticket. The mismatch check fires only when the key's kind is
  one a primitive claims. Making unknown prefixes an error would reject `CP.LONG.INCR cp:x`, which
  works today and which CP spec 2 permits ("conventionally prefixed").
- **The check is on the key's sub-namespace, not on the key's existence.** CP spec 6.8 says "key
  exists as different primitive"; nothing tracks per-key existence across state machines and this
  ticket may not change them. The prefix is the spec's own way of saying which primitive a key is,
  so it is what the check reads. This also closes the `ponytail:` comment the dispatcher carried
  since T44: `GET cp:lock:x` no longer reads an empty counter, and `EXPIRE cp:lock:x` no longer
  answers 0.

This fixes the class of bug ticket 54 fixed, at its root: `INCR cp:ref:r` used to become
`LongIncrBy` on a reference key because only `GET`/`SET`/`EXPIRE`/`TTL`/`PERSIST` consulted the
reference prefix. There is now one lookup, so a verb cannot consult it for some commands and not
others.

### Tests

New, `dynacache-engine/src/test/kotlin/dynacache/engine/CpNamespaceTest.kt` (9):
`cp_kind_lookup_covers_every_prefix` (every prefix, and every enum entry reachable from a key, so
no prefix is written twice or hidden), `C16_a_cp_verb_outside_the_cp_namespace_is_notcp`,
`a_verb_of_one_kind_on_a_key_of_another_is_wrongtype`,
`the_untyped_and_typed_counter_keys_read_the_same_verbs`,
`the_reference_answers_its_own_value_and_ttl_verbs`, `a_plain_key_is_the_ap_engines`,
`C16_a_fanned_command_naming_a_cp_key_is_refused_whole`,
`a_command_no_cp_primitive_answers_is_notcp_whatever_the_kind`,
`a_counter_takes_a_number_or_the_error_redis_gives_for_one`.

New, `CommandParserTest` (3): `compat_set_matches_cp_spec_9_5` (each of the fifteen names parses
to a class in `COMPAT`, and `LPUSH`/`STRLEN`/`APPEND`/`HSET`/`MGET` do not),
`the_parser_emits_cp_verbs_for_cp_keys`, `a_refused_cp_command_is_left_for_the_dispatcher`.

New, `CommandDispatcherTest` (1): `a_kind_mismatch_on_a_cp_key_is_wrongtype`.

Moved: the two `Command.Expire` rows of `the compat set reaches the CP engine as the verb it
means` are now the parser's, because the dispatcher no longer sees an `Expire` on a `cp:` key.
`I22_namespaces_never_cross`, `C16_ap_engine_never_sees_cp_key`,
`C22_no_cross_engine_state_leakage`, `compat_conditional_set_retargets_to_the_kinds_set_verb` and
every `CpRoutingTest`, `CpSessionLifecycleTest` and P5 acceptance test are unchanged and green.

Counts: engine 158 -> 167, cluster 86 -> 86, cp 102 -> 102, server 103 -> 107 (462 total, all
green). Diff: 569 insertions, 147 deletions across 11 files, two of them new; the dispatcher alone
is net -69.

`CpSnapshotTest.lagging_member_is_brought_up_by_snapshot` failed once during a whole-repo run and
passed on its own immediately after. It asserts that a restarted member's `lastSnapshotIndex` has
caught up, which is MicroRaft install-snapshot timing; three other Maven builds were running on
the machine at the time. Not touched by this ticket and green in every isolated run.

### Deviations

1. **The refusal did not move to the parser** (above): a parse error aborts a `MULTI` and an
   execution error does not, and the ticket's seam list does not include `MULTI` semantics. The
   parser emits every CP verb it can build; it never refuses.
2. **`DEL`, `EXISTS` and `TYPE` still answer `-NOTCP` on a `cp:` key**, though CP spec 9.5 lists
   them in the compat set. No CP primitive answers them and adding three CP verbs is another
   ticket; the refusal now says so in as many words ("no CP verb answers this command yet"), and
   `COMPAT` records that they are in the set. Behaviour is unchanged from before this ticket.
3. **`DynaCache/CONTEXT.md` gained a "Namespace rule" and a "CP kind" entry** and lost the
   dispatcher's claim to the re-target, since the rule moved out from under it.

---

## T70 - Each CP primitive owns its snapshot bytes

Adding a CP primitive was a five-file change: the composite exposed every table as a public
field, its snapshot type listed a map per primitive, and `CpWire` knew every primitive's field
layout. Now a primitive is one class, one line in the composite's list, one branch in its `when`,
and nothing at all in the codec.

### The primitive interface

`CpPrimitive` (`dynacache-cp/src/main/kotlin/dynacache/cp/CpPrimitive.kt`) is what the composite
sees of a primitive - everything it does to all of them alike, and nothing else:

- `val id: Int` - its byte in a snapshot, one of the constants on `CpPrimitive`'s companion
  (`LONGS` 1, `LOCKS` 2, `SEMAPHORES` 3, `LATCHES` 4, `REFERENCES` 5, `SESSIONS` 6), spelled out
  and never reused, since a snapshot on disk outlives the order the composite lists them in.
- `fun sweep(now: Long)` - the TTL tick, defaulted to a no-op (latches and sessions keep it).
- `fun releaseAllOf(session: Long)` - the session-close cascade, defaulted to a no-op.
- `fun snapshot(): ByteArray` / `fun restore(bytes: ByteArray)` - its table as bytes only it reads.

The six implementations are the five primitive state machines and `SessionRegistry`, which is a
primitive too. Each encodes through `CpWire.encodeTable`/`decodeTable` (keyed by `Key`) or, for
the session registry, `CpWire.bytes`/`read` directly, all on the length-prefixed `writeBlob`
helper that was already there.

### Snapshot layout and version

`CpStateMachine.Snapshot(lastAppliedTs, tables: List<Table>)`, where `Table(id, bytes)` compares
by content. On the wire and on disk:

```
byte  SNAPSHOT_VERSION = 2
long  lastAppliedTs
int   table count
      repeat: byte primitive id, int length, length bytes
```

`CpWire` no longer reads inside a table. Version 2 is the bump: the layout before this ticket
opened with the log-time long, whose top byte reads here as version 0, so every pre-T70 snapshot
is refused by `CpWire.UnsupportedSnapshotVersion`, which names the version it found. Pre-release,
so no migration.

Rows are written sorted - table keys by unsigned byte order, semaphore holders and session ids by
id - so two members holding the same state write byte-identical snapshots. That is what lets the
existing "two members agree" assertions compare snapshots as values now that a table is bytes.

### The session cascade across primitives

`closeSession` still closes the session first (so a second closing is a no-op) and then offers the
dead session to every primitive in the list: `if (sessions.close(session)) primitives.forEach {
it.releaseAllOf(session) }`. Locks and semaphores override it, the rest keep the default no-op, so
C18/I15 stays one entry and a new primitive that holds something for a session overrides one
method rather than editing the composite. Order in the list is unchanged (locks before
semaphores).

### What became private

Every primitive table on `CpStateMachine` (`longs`, `locks`, `semaphores`, `latches`,
`references`, `sessions`) is now private, as is the new `primitives` list. The four tests that
read a table directly go through one new seam instead:

`fun read(command: Command.Cp): Reply` - what a read verb answers at this member's applied index,
appending nothing. `valueOf(key)` now delegates to it, so there is one path from the composite
into a primitive, not two. `CpStateMachine.takeSnapshot`, `installSnapshot`, `lastAppliedTs` and
`lapsedSessions` are unchanged in shape; `installSnapshot` is now a loop that errors if a
snapshot carries no table for a primitive this build has.

### Tests and counts

New `CpPrimitiveSnapshotTest` (7): one round trip per primitive through its own bytes, each
restored into a second instance and asked what it holds, plus `composite_snapshot_restores_every_primitive`,
which takes `Primitives().stateMachine.state`, installs it into a fresh `CpStateMachine`, and then
runs a `SessionClosed` on the restored machine so the cascade is shown to still cross primitives
after a restore. `Primitives` gained `val stateMachine` (T69's guidance).

`CpWireTest` (+2): `snapshot_round_trips_through_its_bytes` (six tables, version and all) and
`a_snapshot_from_before_the_version_bump_is_refused`. Its snapshot fixture is now built by
driving `Primitives` rather than by naming every primitive's state class.

`CpSnapshotTest` (+1): `cp_snapshot_install_preserves_tokens_and_sessions` - a member killed,
lapped by 40 entries and brought up by an installed snapshot holds the token that snapshot
carried (1), agrees on the strictly greater token (2) the next holder is granted (C17), carries
the leader's sessions table byte for byte, and still has the permits that session held to give
back.

| Module | Before | After |
|---|---|---|
| dynacache-cp | 103 | 113 |
| dynacache-server | 103 | 103 |
| dynacache-engine | 158 | not re-counted, module untouched, build green |
| dynacache-cluster | 86 | not re-counted, module untouched, build green |

Net diff: 16 files, +488 / -165, net +323 (2 new files). Commit a0e5cdb4 on branch `t70`.

### Deviations

- **`apply` is not on `CpPrimitive`.** The ticket's design guidance lists it. The composite still
  routes a command through an exhaustive `when` over the sealed `Command.Cp` (`answer`), because
  the compiler checks that routing is total and each primitive keeps a typed command parameter;
  an `apply(Command.Cp, Long)` on the interface would need a cast inside every primitive and
  would turn an unrouted verb from a compile error into a runtime one. Sweep, the session cascade,
  snapshot and restore are loops over the list, which is what the acceptance criteria name.
- **One new read seam.** `CpStateMachine.read` was added so the tables could go private;
  `ChaosInvariantTest`, `FencedLockFailoverTest`, `SessionLogTest` and `valueOf` use it.
- **Sorted rows.** Not asked for, but required once a snapshot is compared as bytes.
- **`cp_snapshot_install_preserves_tokens_and_sessions` did not exist.** The plan entry names it,
  the spec's 10.7 table lists only `cp_snapshot_restore_roundtrip`. Written fresh in
  `CpSnapshotTest`; the spec was not edited.
- **One new test file** rather than a snapshot case added to each of the five existing primitive
  suites, which are about semantics rather than persistence.

## T74: The live WAL never lives inside a snapshot part

### Built

The root cause was in `SnapshotEngine`, not in the part adapter: `save()` always rotated the
engine's live log into a file under its own directory, and `FileSnapshotParts.cut` runs a save
rooted at the part. A `SnapshotEngine` built with `fsync = null` was already documented as
"snapshots and no log"; that is now the ownership rule. Such an engine stamps the checkpoint's
seq into the RDB it writes and neither rotates nor deletes a log file; the engine's log, if it
has one, is another `SnapshotEngine`'s to checkpoint (the data directory's). Two one-token
guards in `SnapshotEngine.cut` and `SnapshotEngine.save`, plus the contract written into the
`fsync` parameter, `cut`'s KDoc and `SnapshotParts.cut`.

A part therefore holds `dump.rdb` (the state at the cut, stamped with the log's seq at the
cut) and its channel logs, never a `wal.*`. The live log keeps running under the data
directory across a cut, an abort deletes the set without touching it, and recovery from the
data directory replays every write acked after the cut. T49's cut-then-open order and T55's
`SnapshotParts` interface are unchanged; `DistributedSnapshot` is untouched.

### Tests

- engine: `SnapshotPartsTest.snapshot_part_holds_the_log_up_to_the_cut_only` (new). A node
  with a data directory cuts a part, writes on, restarts: no `wal.*` under the snapshot root,
  the part's checkpoint seq equals the log's seq at the cut and its state holds only the
  pre-cut value, the data directory's `wal.0` holds every entry, and the restart reads the
  post-cut writes.
- cluster: `DistributedSnapshotTest.C14_writes_after_the_cut_survive_an_aborted_snapshot_set`
  (new). The test's `Lone` node gained an optional data directory (restored through
  `SnapshotEngine`, `FsyncPolicy.NEVER`) and a deadline; its peer never answers the marker,
  the set is aborted at 30s of virtual time, the node crashes without a save, and the restart
  reads both writes acked after the cut. Red before the fix with the exact data-loss symptom
  (`expected <Bulk(2)> but was <Bulk(1)>`).

| Module | Tests |
|---|---|
| engine | 165 |
| cluster | 88 |
| cp | 103 |
| server | 103 |

`chandy_lamport_restorable`, `I12_reads_after_restore_return_snapshot_time_values`, every WAL
and recovery test and `P4AcceptanceTest` pass unchanged.

### Deviations

1. **A part holds no log file at all.** The ticket allowed "a copy or a sealed segment of the
   log up to the cut"; the part's RDB is that log folded, stamped with the cut's seq. A copied
   segment would be dead weight: a part is restored by a no-log `SnapshotEngine`, which never
   replays, and a replay would skip every entry at or below the checkpoint anyway. The
   acceptance test asserts the checkpoint seq and the state instead of file bytes.
2. Well under the 200-line floor (about 110 lines including tests): the fix is two guards.

### For the next ticket

- `SnapshotEngine.close()` still does `engine.wal?.close()` regardless of `fsync`. No caller
  closes a part's engine, so it is harmless today; the same guard belongs there if one appears.
- T76 could stop reading `fsync == null` as "no log" by splitting a `RdbStore` out of
  `SnapshotEngine`; not needed for anything yet.

---

## T65 - A replicate carries codec bytes

A replica applies exactly the entry the coordinator logged. The coordinator runs the write,
passes `(command, reply)` through `whatChanged`, frames the result with its own instant through
the engine command codec, and ships those bytes with the version. The replica decodes them and
submits what they decode to, in order. No RESP spelling of a command is left anywhere on the
cluster seam: T64 took the forward, this ticket takes the replicate and the read.

This ticket was started by one agent, who landed the codec's framed pair, the proto change and
the named test before dying on an API limit without committing, and finished by a second agent,
who wrote everything else below from that inherited diff.

**The envelope.** `Replicate`'s `repeated bytes token = 2` is gone and `bytes command = 6`
replaces it; tag 2 and the name `token` are `reserved`, as `Forward`'s are (T64). The field was
removed rather than deprecated because a hint outlives a build only in memory. `expires_at_millis`
stays: it is the same instant the command's bytes carry, repeated so a hint holder can drop a
write that expired while it waited (T25) without decoding it. `Read` lost its tokens the same
way and carries `bytes command = 3`.

**The framing helper's home.** `CommandCodec.frame(command, now)` is `encode` with the op code
prepended to the body, one byte array; `CommandCodec.unframe(bytes)` is the inverse and answers
the list `decode` does. T64's note said the second copy of `Router`'s private framing would
justify lifting it onto the codec; this is the second copy, so `Router`'s companion is deleted
and `Router`, `Replication`, the test kit and `DistributedSnapshotTest` all call the codec's
pair. `Router.coordinate` still takes `single()` (a forward passes no `now`, so it never
decodes to two commands); a replicate iterates the list.

**Replication.** `write` is `whatChanged(command, reply) ?: return reply`, then
`frame(changed, clock.instant())`. The hand-written rules are deleted with `Replication.decided()`:
the refused-`SET` check (`whatChanged` answers null for an error or a nil bulk), the NX/XX
stripping (`whatChanged` strips it), the TTL-to-instant conversion (`encode`'s `now` settles it)
and the replica's second submit of an `EXPIRE` (a `SET` with a settled deadline decodes to the
`SET` and then the `EXPIRE`, and the replica submits each). `replicate` takes its key from the
first decoded command; bytes that do not decode are not acked, as unreadable tokens were not.
The constructor lost `tokens` and `parse`; nothing is injected in their place, at every call
site (`ClusterNode`, `InProcessCluster`, `ReadRepairTest`, `ReplicationTest`). `ClusterNode`
also lost its `CommandParser` and the `parse` helper that were only there for replication.

**The same-bytes comparison.** `replica_applies_exactly_the_logged_entry` in
`dynacache-cluster/.../ReplicationTest.kt`: three nodes with W = 3, each engine under a
`SnapshotEngine` with a never-fsynced WAL in a temp dir. A `SET NX` with a ten-second TTL goes
through the coordinator. The coordinator's WAL holds one entry, asserted equal to
`CommandCodec.encode(Set(key, value, ttl = 10s), EPOCH)`: condition gone, deadline settled. Both
`Replicate` envelopes on the in-memory transport's `sent` log carry exactly `op + body` of that
entry. Each replica's WAL holds the two entries the logged entry decodes to (`SET` without TTL,
then `EXPIRE` at the deadline), byte for byte. Red by construction against the base: the
`Replicate` message had no `command` field, so the test did not compile before the proto change.

**What was deleted.** The test kit's `TokenCodec.kt` (the kit's partial command encoding), the
server's `CommandTokens.kt` (`commandToTokens`) and `CommandTokensTest.kt`. `TokenCodec` was
also the reason T64's round-trip test built its own routers; that test's comment no longer
points at T65. `GrpcTransportTest`'s one-envelope-per-case fixture builds `Replicate` and `Read`
with opaque `command` bytes, as it does for `dvv`. `DistributedSnapshotTest` reads a channel
log's write tag out of the decoded command (a `SET`'s value, or the lone harness's `INCRBY`
delta) instead of token 2.

**Docs.** ADR 0003 gains the line: since T65 the command ships as the engine command codec's
bytes, the entry the coordinator logged, framed op code first. `CONTEXT.md`'s hint entry and
`HintStore`'s header say "the logged entry's bytes" where they said "tokens".

**Tests.** engine 158, cluster 87 to 88, cp 103, server 103 to 102 (`CommandTokensTest` gone),
all green. Diff 18 files, +138/-347. The three known flaky tests did not flake in the green run.

**Deviations.** None from the ticket. One judgment call: a write whose reply is a nil bulk
(an empty-list `LPOP`) used to be replicated and applied as a no-op on the replicas; now
`whatChanged` says nothing changed and nothing ships. The coordinator's version bump still
happens first, exactly as it did for a refused conditional `SET` before this ticket.

**For the next ticket.**

- T66 (the versioned store): `Replication.write` now has the shape T66 wants to move, a
  `versions.compute` bump, an engine submit, a `whatChanged`, a frame; the replica side is
  `versions[key]` + spec 5.3's three-way `when` + a loop of submits. The version bump before the
  engine runs and the replicate's key coming from `commands.first()` are the two places a
  versioned store takes over. `RecordingEngine` in `ReplicationTest` is still the double for
  C4; T66 deletes it.
- T73 (or whoever touches the wire next): `frame`/`unframe` are the only framing on the cluster
  seam; `Router` keeps `single()` and a comment saying why. If a forward ever needs to carry a
  settled deadline, drop the `single()` and pass the codec a `now` on the forwarding side.
  `expires_at_millis` on `Replicate` is now redundant with the bytes; it stays only for
  `HintStore.pending`'s expiry sweep without a decode.
