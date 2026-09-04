# T17 lens 1: concurrency and invariants

Reviewer: a fresh Fable 5.1 subagent with one lens, reading every lock, `NonCancellable`,
`StateFlow` write, `catch`, permit and dispatcher entry for the interleaving that breaks it.
Read-only: nothing was built or run. Where a claim rests on the SSH library, it was checked
against the pinned `jsch-2.28.7-sources.jar` in the local Maven cache, and the file and line in
that jar is given so the adjudicator can look rather than trust.

Scope read in full: spec 3.3, 4, 5.3, 7.3, 7.6, 9, 11.2, 17.1; progress.md's open-seams table, C13,
R1, R2, T13, T16; every main source file in `core`, `quarkus` and `testkit`; the tests only to see
what they already prove (`CancellationLadderTest`, `LadderReviewTest`, `PoolReviewTest`,
`PoolShutdownTest`, the two Lincheck classes, `PartitionMatrixTest`, `AdversaryTest`).

What is good, so the findings are read in proportion. Every `catch (Throwable)` in the module
rethrows; every `catch (Exception)` is preceded by a `CancellationException` rethrow, except
`consume`'s, which re-checks `ensureActive()` before treating the exception as the block's. The
permit is released exactly once on every exit of `acquire` I could trace, including R1's
granted-at-the-instant-of-cancellation path, whose `granted` flag is still there. `dial` tells the
registry under `NonCancellable`, and the transport's orphan close is still in place. The registry's
one lock never holds I/O: `sweep(takeRoom)` is a CAS, nothing under the lock touches a socket or a
meter, and `PoolMeters` read a volatile snapshot. `withLease`'s `.also { release() }` is skipped only
when the ladder threw, because `supervisorScope` and a same-dispatcher `withContext` resume their
caller undispatched, so a landed result is not replaced by a cancellation on the way back; only a
dispatcher switch does that, which is R1's row and is honoured everywhere. The transfer permit is a
kotlinx `Semaphore` taken before the lease and given back between tries, so no permit is ever held
across a backoff. `InFlightSet.admit` is a bare `Semaphore.acquire` with no scope around it that
could drop the permit; `settle` is a CAS and `withdraw` cannot double-release. `produce` with a
`CoroutineExceptionHandler` behaves exactly as the comment says: the handler fires only when the
channel could not carry the failure. Under `SKIP`, `latest.isActive` stays true through the tick's
`withdraw`, so the ticker cannot skip against a tick that has finished but not yet given back.
Resilience4j's retry catches `CancellationException` but its predicate answers no, so it is
rethrown without a delay; the breaker ignores it. The Quarkus `stop` reads correctly: a
`withTimeoutOrNull` around a `NonCancellable` block waits for the block, so the hook holds the
shutdown thread for the whole close and the WARN fires after, which is what its KDoc says. Nothing
collects `Lease.state` or `PoolEntry.state` anywhere in main or test (grepped; every reader is
`.value`), so the undispatched-collector row is still theoretical.

## Findings, by severity

### CRITICAL

**C1. A call blocked writing to a dead tunnel is bounded by none of the three tiers, because both
remaining tiers need the SSH session's write lock, and the writer holds it.**
`JschTransport.kt:263-270` (`abort`), `:150-151` (the keepalive), `CancellationLadder.kt:71-79`,
`SftpPool.kt:317-326` (`cutEverythingHeld`), `SftpConnectorLifecycle.kt:81-90`.

Breaks: spec 5.3 ("the keepalive ladder is what actually bounds a hung server"; the forced tier
"disconnects the session from another thread"), spec 11.2 and I9 ("`close()` returns within
`drainTimeout + cancelGrace`"), and D6's premise that leak detection need not force release
because the ladder bounds a live call.

Mechanism, verified in the 2.28.7 sources. `Session._write` encodes and writes the packet inside
`synchronized (lock)` (`Session.java:1870-1880`), and the write is `out.write` then `out.flush` on
the socket (`IO.java:71-72`), which has no timeout. An upload's `sendWRITE` reaches `_write` through
`Session.write(packet, channel, length)` (`ChannelSftp.java:2551-2587`, `Session.java:1757-1845`);
the SSH-window wait in that method is outside `lock` and does watch `c.close`, so a server that
stops consuming is handled, but a proxy or peer that stops *reading TCP* blocks the thread inside
`_write` with `lock` held. From there:

- The keepalive tier: `Session.run` turns the read timeout into `sendKeepAliveMsg()`
  (`Session.java:1916-1919`), which is `write(packet)` and then `_write` (`:3268-3277`), so the
  reader thread parks on `lock` behind the stalled writer and never reaches the second timeout
  that ends the session.
- The forced tier: `Session.disconnect()` (`Session.java:2244-2307`) first disconnects every
  channel; `Channel.disconnect` calls `close()`, which sends `SSH_MSG_CHANNEL_CLOSE` through
  `getSession().write(packet)` (`Channel.java:519-541`), which is `_write` again. Only after every
  channel, and after a second `synchronized (lock)` for the connect thread (`:2285`), does it reach
  `socket.close()` or `proxy.close()` (`:2304-2307`). `abort()` therefore blocks on `lock` before it
  ever touches the socket.

Interleaving: `client.upload(8 MiB)` through the proxy; the tunnel stops reading upstream after
the first few hundred KiB; the IO thread fills the peer's window and its own send buffer and parks
in `socket.write` inside `_write`. Nothing else on that session can be sent. The caller's
`transferTimeout` (or its own `withTimeout`) fires: `onTheClock` cancels, `carry` waits the grace,
`bringToAStop` calls `entry.cutLoose()`, and `abort()` parks on `lock` on the *caller's* thread,
under `NonCancellable`. The lease is never handed back (the `supervisorScope` in `carry` waits for
`work`, which waits for the thread), the pool is one place smaller, `sftp_pool_leak_total` fires
after ten minutes and the log says the session "is being cut apart to get its thread back", which
it is not. Then `close()`: the drain runs out, `cutEverythingHeld` calls the same `abort()` on the
closing thread and parks there before `settled(cancelGrace)` is reached; `SftpConnector.close()`
never returns; in Quarkus, `stop` holds the shutdown thread forever because its `withTimeoutOrNull`
cannot return before the `NonCancellable` inside `pool.close()` does. A second held session would
not be cut either, because the cuts are sequential (`SftpPool.kt:326`).

Why nothing has seen it: the testkit's `LoopbackConnectProxy.stall()` keeps reading and discards
(`LoopbackConnectProxy.kt:193-198`, "so the sender's own buffers never fill"), so by construction
every T8 and R1 measurement was a read-side stall; every partition row in T15/T16 is a download, a
stat or a rename, and the adversary's fake has no socket. R1 finding 5 named the close packet as a
possible block on a wedged peer and left it as having "no fix inside the library's API"; the lock
makes it worse than that, and there is a fix inside the API (below).

Failing tests, both against the embedded server with a proxy variant that stops *reading* the
client (a `stallReads()` on `LoopbackConnectProxy` that parks the `toClient = false` copy thread
before its next `read`, or Toxiproxy's `timeout` toxic on `UPSTREAM`, which stops draining the
client socket once its channel is full):

- `CancellationLadderTest.a cancelled upload on a tunnel that stopped reading is cut within the grace`:
  `keepAlive = 500 ms`, `cancelGrace = 300 ms`, `retry.maxAttempts = 1`; start a 64 MiB upload from
  a generated stream; stop the proxy reading once the client has sent 1 MiB; cancel the caller;
  assert the caller returns within `cancelGrace * 20`, `pool.stats().total == 0`, and
  `evictedAsPoisoned() == 1.0`. Expected red: the caller never returns.
- `PoolShutdownTest.I9_close returns within the bound while an upload is stalled on a full send buffer`:
  same staging, `drainTimeout = 1 s`; call `pool.close()` from another coroutine; assert it returns
  within `drainTimeout + cancelGrace + 2 s` with every entry `Closed`. Expected red: `close()` hangs.

Fix shape, contained in the adapter: give the session a `SocketFactory` that remembers the socket it
created (`Session.java:239-254` uses it for the direct socket and hands it to
`ProxyHTTP.connect(socket_factory, ...)`, `ProxyHTTP.java:70-80`, so the proxy path is covered), and
make `abort()` close that socket first and only then call `session.disconnect()`. Closing the
socket from another thread fails the blocked `write` with a `SocketException`, which releases
`lock`, after which the library's own disconnect proceeds and the keepalive thread exits. The
orderly `close()` on the bounded dispatcher (`JschTransport.kt:245-256`) has the same shape and is
only safe because it runs after the cut; it should stay orderly, but the same retained socket lets
it be bounded if the maintainer ever wants that.

Consequences to record with the fix: the ladder's `abort()` runs on the cancelling caller's own
thread under `NonCancellable`, which T14's KDoc says only of `close()` (see M1); and every wait in
the module that is described as "bounded by the ladder" - `BorrowedSession.handItBack`'s lock
wait, the drain, `carry`'s `supervisorScope` - is bounded only if the cut actually unblocks the
thread, which is the property this finding removes.

### HIGH

**H1. A listing's cooperative tier watches the channel, not the coroutine, and its forced tier
cannot unpark the thread at all; a slow collector is the only bound, and it can hold `close()`
past I9.** `SftpClient.kt:366` (`handOn` is `trySendBlocking`), `:94-103`, `JschTransport.kt:174-186`,
`Resilience.kt:140` and `:178-188` (the time limiter on `list`), `CancellationLadder.kt:71-79`,
`SftpPool.kt:308`.

Breaks: spec 5.3's cooperative row ("listings run with an `LsEntrySelector` that returns `BREAK`"
once the coroutine is cancelled); I9 in the shutdown variant.

Mechanism: `trySendBlocking` is `trySend` and then `runBlocking { send(element) }`. That
`runBlocking` has its own job; it observes only the channel. The channel is closed when the
*collector* of `channelFlow` leaves, which is why every existing test (a collector that stops, a
`take`, a cancelled collection) sees the listing stop cleanly. It is not closed when the cancellation
comes from inside `attempting`: the time limiter's `withTimeout` cancels the `withLease` block and
the ladder's `work`, but the `channelFlow` producer is their parent and stays alive, so the IO thread
stays parked in `send`. `abort()` closes the socket, which a parked `send` does not notice. The
selector is never asked, so `BREAK` is never answered.

Interleaving: `client.list(dir).collect { slowThing(it) }` from a request handler whose client stops
reading, with the default `transferTimeout` of fifteen minutes (`ConnectorDsl.kt:444`) and a
directory of more than the channel's 64-entry buffer. At fifteen minutes the limiter cancels; the
grace passes; the healthy session is cut apart (`sftp_pool_evicted_total{reason=poisoned}` rises, the
WARN says the thread is being taken back); the thread stays parked; `carry` waits for the collector.
When the collector finally takes one entry, JSch hits the closed socket, `work` ends with
`SessionLost`, and the collector receives `OperationTimeout` saying "the request may still land, so
the session is not kept" about a directory listing. Shutdown variant: with `maxSize = 1` (or as many
parked listings as the pool is wide) `close()` cuts, waits the grace, and then `closeEverything`'s
hang-ups need an IO slot that the parked thread holds; `coroutineScope { launch { finish } }` at
`SftpPool.kt:308` waits for the collector, past `drainTimeout + cancelGrace`.

The shipped source is not exposed: `walk` collects into a list and `take(maxFilesPerPoll)` closes the
channel, so the tick's listing never parks for long. The exposure is the public `SftpClient.list`,
whose KDoc invites exactly the slow-consumer use ("a consumer that is busy stops the server sending
more").

Failing tests, against the embedded server (the fake answers on the caller's coroutine and cannot
park a thread, T16 deviation 4):

- `SftpClientTest.a listing whose collector stalls is stopped by the time limiter without destroying its session`:
  200 files, `transferTimeout = 500 ms`, `cancelGrace = 200 ms`, `retry.maxAttempts = 1`; collect,
  take one entry, then suspend on a `CompletableDeferred` for 3 s; assert the collection fails with
  `OperationTimeout` within `transferTimeout + cancelGrace + 1 s` and that
  `evicted{reason=poisoned}` stays zero. Expected red on both: the failure arrives at 3 s and the
  counter reads one.
- `PoolShutdownTest.I9_close returns within the bound while a listing is parked on its collector`:
  `maxSize = 1`, same collector; call `connector.close()` with `drainTimeout = 300 ms`,
  `cancelGrace = 200 ms`; assert `withTimeoutOrNull(2.seconds) { close() }` is not null. Expected red.

Fix shape: `handOn` takes the job of the coroutine that owns the lease (captured with
`currentCoroutineContext().job` inside the `attempting` block, which is the ladder's `work`) and
parks in short slices: `runBlocking { while (job.isActive) { if (withTimeoutOrNull(50.milliseconds)
{ send(entry) } != null) return@runBlocking true }; false }`. A cancelled lease then answers `STOP`
within a slice, JSch closes the handle cleanly, and the session goes back healthy - which is the
cooperative tier spec 5.3 describes. (`job.onJoin` in a `select` would not do: the job cannot
complete until the parked thread returns.)

### MEDIUM

**M1. The ladder's `abort()` runs on the cancelling caller's thread, and the adapter documents that
only for `close()`.** `CancellationLadder.kt:64-79` (under `NonCancellable`), `JschTransport.kt:258-262`
(KDoc: "runs on the caller's own thread"), `SftpConnectorLifecycle.kt:73-76` (worker thread for
`close()` only). Spec 11.2 says an event loop must not block on a socket close; the same socket
close happens on every cancelled `download` or `upload` whose caller is on an event loop, which in
Quarkus is a suspending reactive route. R1 finding 5 said "T14 should know"; T14 covered the
shutdown path and not this one. With C1 the block is unbounded; without it, it is T8's under-a-second
on the loop. Fix: `withContext(Dispatchers.IO) { entry.cutLoose() }` in `bringToAStop` - the
unlimited IO pool, not the bounded view the contract forbids - or a sentence in the adapter's KDoc.
Owner: the ticket that fixes C1, since it rewrites `abort()`.

**M2. The housekeeper reserves room before it hangs up, and the hang-ups run on the bounded
dispatcher without a pool place.** `SftpPool.kt:359-375` (`sweep`: `retired.forEach { finish }` and
only then `openForTheShelf`), `SessionRegistry.kt:281-287` (`takeRoom` inside the same round),
`JschTransport.kt:245` (`close` on `io`). The seams row says everything on the bounded dispatcher
holds a pool place; a session retired from the shelf holds none, so its hang-up waits for a slot
when every slot is pinned. It cannot deadlock - nothing pinned waits on the housekeeper - but while
it waits, the spares the same round reserved sit registered as `Connecting` with their permits
taken: `sftp_pool_active` counts them, `entries.size` counts them, and a caller refused meanwhile
reads "most of the pool is stuck opening sessions, so look at the server and the network"
(`SftpException.kt:251-252`) for a pool that is stuck hanging up. Interleaving: `maxSize = 3`,
`minIdle = 2`, one long download, two idle of which one expired; the round retires one, reserves
one, and its hang-up queues behind the download's thread only if the third slot is also busy -
which is a hang-up from an earlier round on a slow peer, or C1. Test on the fake with a `close`
hook that suspends: `HousekeeperTest.a round whose hang-up waits does not hold room it has not
dialled`, asserting `stats().connecting == 0` while the close is held. Fix: dial first, close after,
or reserve after the closes. Owner: whoever next touches `sweep`.

### LOW

- **L1.** `SftpSource.kt:153-158`: `catch (stopped: CancellationException)` also catches the
  `AbortFlowException` that `take`, `first` and `collectWhile` throw through `emit`, passes
  `ensureActive()`, and logs "ended because the connector stopped it". Behaviour is unchanged
  (`consumeEach`'s `finally` cancels the channel and the abort's owner completes normally anyway);
  the line lies. Narrow the catch to the two cases it means.
- **L2.** `SftpPool.kt:385-393`: `openForTheShelf` swallows an `Error` that is not a cancellation and
  logs it as a failed spare, while `close(connection, entry)` (`:443-452`) and `housekeep` (`:346`)
  deliberately let an `Error` through. One of the two policies is wrong; either is defensible.
- **L3.** `PoolEntry.kt:129-132`: `cutLoose` reads `connection` outside the lock against `retire`'s
  write of null (`SessionRegistry.kt:312-313`) during `closeEverything`. Both orderings end with the
  session closed - a null skips the abort and the drain hangs up; a non-null aborts a session the
  drain also closes - so it is benign, but it is the only unguarded read of a field the class
  comment says is written only under the lock.
- **L4.** `SessionRegistry.kt:281-287` sets `borrowedAt` on a spare "so it does not look like a
  lease nobody gave back", but `HOLDABLE` (`:269`, defined at `:346`) excludes `Connecting`, so a spare stuck
  in its dial is never reported; `connectTimeout` bounds it, so nothing is lost except the comment's
  promise.
- **L5.** `SftpConnector.kt:119-120` builds the pool and client, and so registers their gauges, before
  the probe; a refused start leaves gauges on the host registry bound to the dead pool, and a later
  successful start against the same endpoint on the same registry (a host that retries, dev-mode
  reload) registers the same ids and reads the dead pool forever. The seams row about two
  connectors covers the mechanism; this is a third way to get there with one connector.
- **L6.** `SftpPool.kt:326`: the drain's cuts are sequential on one thread. Fine while a cut is a
  socket close; with C1 fixed it stays fine; recorded so nobody makes them wait for anything.

## Inventory

Every lock, permit, `NonCancellable`, `StateFlow` write, broad catch, dispatcher entry and scope.
"Place" is whether the caller holds a pool permit when it enters the bounded dispatcher.

| Site | What | Protects / does | Released by, on which exits |
|---|---|---|---|
| `SessionRegistry.kt:39` | `Mutex` | `entries`, `idle`, `retiring`, `closing`, `published`, every `PoolEntry` field but `unfitAfterCancelling` | `withLock` in every method (`:98,128,137,157,180,192,201,206,217,241,299`); no suspension inside except the lock itself; `sweep(takeRoom)` runs a CAS |
| `BorrowedSession.kt:33` | `Mutex` | one call at a time, and `mine` | `withLock` per call (`:44`); `handItBack` (`:42`) under `NonCancellable` from `SftpClient.kt:300` |
| `InFlightSet.kt:26` | `synchronized` | `inFlight`, `excluded` | `holds`/`enter`/`exit`/`size`, all non-suspending; model-checked (T16) |
| `SftpSource.kt:86` | synchronized set | one watch per directory | `finally` at `:159-161`, entered only after the claim succeeded |
| `SftpPool.kt:74` | `Semaphore(maxSize)` | I1 | `admit` (`:229-266`) takes; `freeRoom` (`:273`) from `giveBack`'s `finally` (`:439`), `acquire`'s catch (`:165`), `admit`'s catch when granted (`:260`); housekeeper `tryAcquire` in `sweep` (`:360`) freed by `giveBack` on every path (`:373,384,386`) |
| `Resilience.kt:94` | `Semaphore(maxConcurrentTransfers)` | transfers on the wire | `withPermit` (`:170`), inside the retry, outside the lease |
| `InFlightSet.kt:29` | `Semaphore(maxInFlight)` | I7/I8 backpressure | `admit` (`:44-52`) takes, gives back on the second look; `leave` (`:54`) from `release` on ack, nack, gone, withdraw |
| `SftpPool.kt:163` | `NonCancellable` | discard and `freeRoom` after a failed or cancelled acquire | - |
| `SftpPool.kt:181` | `NonCancellable` | `registry.filled` and the hang-up of a dial nobody wanted | - |
| `SftpPool.kt:214` | `NonCancellable` | discard after a failed proof | - |
| `SftpPool.kt:301` | `NonCancellable` | the whole of `close()` | bounded by two `withTimeoutOrNull`s, then the hang-ups (unbounded on a wedged peer: C1, R1 f5) |
| `SftpPool.kt:370` | `NonCancellable` | hang-ups of a round's retired sessions | R1 fix; `toOpen` given back in `finally` (`:373`) |
| `SftpPool.kt:386` | `NonCancellable` | give back a spare that failed to open | - |
| `SftpPool.kt:433` | `NonCancellable` | `giveBack`: hand back, hang up, free the permit last | - |
| `CancellationLadder.kt:64` | `NonCancellable` | wait the grace, cut, read the outcome | `carry`'s `supervisorScope` then waits for `work` (C1, H1: only if the cut unblocks the thread) |
| `JschTransport.kt:96` | `NonCancellable` | hang up on a session that finished its handshake into a cancelled caller | R1 fix, held |
| `JschTransport.kt:245` | `io + NonCancellable` | orderly close | needs an IO slot (M2) |
| `SftpClient.kt:300` | `NonCancellable` | `handItBack` waits for a call in flight | bounded by the ladder or keepalive floor (C1) |
| `SftpConnector.kt:133` | `NonCancellable` | cancel the scope and close the pool on a refused start | - |
| `PoolEntry.kt:147` | `MutableStateFlow.value =` | entry state | every caller is a registry method under the lock; no collector exists (grepped) |
| `SftpPool.kt:106,159,259` | `catch (Throwable)` | release lease / discard / free permit | all rethrow |
| `SftpPool.kt:385` | `catch (Throwable)` | give back a spare | rethrows cancellation, swallows the rest including `Error` (L2) |
| `JschTransport.kt:84` | `catch (Throwable)` | disconnect a session whose channel failed | rethrows |
| `SftpSource.kt:310` | `catch (Throwable)` | withdraw handed-over files | rethrows |
| `ClientMeters.kt:34`, `SourceMeters.kt:41` | `catch (Throwable)` | stop the timer | rethrow |
| `SftpPool.kt:210-212,344-346,446-448`, `JschErrorMapper.kt:52-57` | cancellation rethrown, then `catch (Exception)` | validation, housekeeping round, hang-up, mapping | sound |
| `SftpSource.kt:178-181` | `catch (Exception)` then `ensureActive()` | consumer block | sound; an inner timeout is a nack, as documented |
| `SftpSource.kt:240-267` | `Flow.catch` | tick failures | sound; transparent to downstream and to the tick's own cancellation, ISE on a foreign cancellation |
| `JschTransport.kt:78,165,174,188,212,225,232,236` | `withContext(io)` | every JSch call | place held: `connect` from `dial` under `acquire` or a reserved spare; the rest through a lease |
| `JschTransport.kt:245` | `withContext(io + NonCancellable)` | `close` | place held from `giveBack`, `discard`, `dial`, the orphan path; **not** from `sweep`'s retired idle sessions or `closeEverything` (M2, by design at shutdown after the cut) |
| `JschTransport.kt:263` | `abort()` on the caller's thread | forced tier | not on `io`; blocks on the peer (C1, M1) |
| `SftpClient.kt:366` | `runBlocking` on the IO thread via `trySendBlocking` | listing backpressure | unparked only by the channel (H1) |
| `CancellationLadder.kt:57-58` | `supervisorScope` + `async` | the borrowed call as one watched child | waits for the child on every path |
| `SftpPool.kt:308` | `coroutineScope` + `launch` per retired | parallel hang-ups at shutdown | after the cut |
| `SftpSource.kt:150` | `background.produce(handler)` | the ticker | cancelled by the collector's `consumeEach` or the connector's scope |
| `SftpSource.kt:226` | `launch` per tick inside the producer | one tick | a failure cancels the producer and sibling ticks; `withdraw` in the tick's catch |
| `SftpConnector.kt:124,140` | `CoroutineScope(SupervisorJob)`, `launch { housekeep() }` | background work | `scope.cancel()` in `close()` and on a refused start |
| `SftpConnectorLifecycle.kt:62,81` | `runBlocking`, `runBlocking(Dispatchers.IO)` | start on the CDI thread; close on IO | the close holds the shutdown thread for its whole duration (correct; forever under C1) |

## Ranked list

Critical and High, fix before production with the test that found it:

1. **C1** - a stalled write is bounded by nothing; `abort()`, the keepalive and `close()` all wait on
   JSch's session lock behind the writer. Fix in the adapter with a retained socket.
2. **H1** - a listing's cancellation cannot reach the thread parked in `trySendBlocking`; the time
   limiter destroys a healthy session and the wait is the collector's; `close()` can overrun.
   Fix in `handOn` by polling the lease's job.

Medium, with a proposed owner:

- **M1** - `abort()` on the cancelling caller's thread; document it for the adapter or move the
  cut to the unlimited IO pool. Owner: the C1 fix.
- **M2** - a round reserves room before it hangs up, and the hang-up holds no place. Owner: whoever
  next touches `sweep`; one reordering.

Low: L1 to L6 above.

## Verdict on R1 after T11 to T16

R1's three fixes held under every path retraced: the orphan close on a cancelled handshake, the
`finally` that gives back a round's reserved spares, and the granted flag in `admit`. Its two
standing rows - a dispatcher switch drops a produced resource, and a cancelled `withLease` is no
proof the call did not land - are honoured by T11 to T13 and are the reason `withLease`, `dial`,
`carry` and `close()` are sound. Its beliefs about the registry, the lock discipline and the ladder's
orderings are confirmed by mechanism. Where R1's conclusions no longer hold is one step past its
finding 5: "`abort()` may block the aborting thread on a wedged peer" was recorded as a pathology
of the close packet with no fix inside the library's API, and T13 then built the drain on "a cut
does not wait for anything". Both are true only for a call blocked in a *read*. A call blocked in a
write holds the session's lock, and with it every remaining rung of the ladder and the drain; the
harness that measured the ladder cannot produce that case, because its stall keeps reading; and the
socket factory is the fix inside the API. Nothing in T11 to T16 widened or narrowed this - the
retry, the time limiter and the shutdown each assume the ladder's bound and inherit its hole - and
T16's adversary, having no socket, was never in a position to find it.

## Adjudication (T17, lens 1)

Fixed and recorded by the T17 owner (Fable). The reviewer's text above is unchanged.

- **C1 (= lens 2 H1): fixed.** `JschTransport.abort()` now closes the socket JSch dialled - kept
  through a `SocketFactory` set on the session, honoured on the direct path and through `ProxyHTTP`
  alike - before `session.disconnect()`, so a call blocked writing to a dead peer is cut without
  waiting on the session's write lock. Spec 5.3 and D47 record the new guarantee: all three tiers
  now bound a blocked call, a write as much as a read. Test kit gained `LoopbackConnectProxy`'s
  black-hole fault (`blackHoleClientAfter`, which stops reading from the client); the ladder cut is
  proved by `CancellationLadderTest.a cancelled upload on a tunnel that stopped reading is cut
  within the grace`, and I9 by `ShutdownAgainstServerTest.I9_closing while an upload is black-holed
  on a full send buffer returns within the bound`. Commit `SFTP T17 C1`.

- **H1: fixed.** `SftpClient.handOn` now watches the lease's coroutine (`currentCoroutineContext().job`
  captured in the `attempting` block) as well as the channel, parking in 50 ms slices; a listing
  cancelled from inside the call - the time limiter's cancellation - reaches the parked IO thread
  within a slice and answers `STOP`, so JSch closes the handle cleanly and the session goes back
  healthy. Proved by `CancellationLadderTest.a listing whose collector stalls is stopped by the time
  limiter without destroying its session`. The shutdown variant the report names is not added as a
  separate test: a raw parked listing at shutdown is not bounded by this fix (its coroutine is not
  cancelled by `pool.close()`), and a listing whose owning scope *is* cancelled closes its channel,
  which the pre-fix `trySendBlocking` already stopped cleanly - so there is no red-without-fix
  shutdown case to write; the time-limiter test is the one that exposed the bug. Also folded in
  ticket 18's request: `SftpSource.filesUnder` caps `maxFilesPerPoll` through a shared `FileBudget`
  and each listing's own `maxEntries` instead of a `.take`, so a capped listing records
  `sftp_op_seconds{op=list,result=ok}`, not `cancelled`
  (`SftpSourceTest.a recursive poll capped by maxFilesPerPoll records every listing as ok`). Commit
  `SFTP T17 lens 1 H1`.

- **M1: documented.** `abort()` runs on the cancelling caller's own thread under `NonCancellable` -
  now stated in spec 5.3 and in the `SftpConnection.abort` KDoc. The C1 fix owns it (it rewrote
  `abort()`); on a reactive host it is a socket close on the event loop, which spec 11.2 already
  routes `close()` off a loop for. No behaviour change.

- **M2: fixed.** `SftpPool.sweep()` now dials the spares a round reserved before it hangs up on the
  round's retired sessions, so the reserved spares are `Connecting` only while they are really being
  opened; a retired session's slow hang-up no longer sits while they misread as "stuck opening".
  Both the hang-ups and the leftover reservations run in a `NonCancellable` finally (rooms given
  back first, then closes), preserving R1 finding 2's cancellation safety. Proved by
  `HousekeeperTest.a round whose hang-up waits does not hold room it has not dialled`. Contained to
  `SftpPool.sweep`; `SessionRegistry`'s lock is untouched, so the Lincheck tier is unaffected.
  Commit `SFTP T17 lens 1 M2`.

- **Lows.** L1 (`SftpSource.kt:153` catch narrowing): owner is the failure-semantics agent, whose
  region includes `SftpSource.watch`'s catch; not touched here. L2 (`openForTheShelf` swallows an
  `Error`): either policy defensible; recorded, unchanged. L3 (`cutLoose` reads `connection` outside
  the lock): benign data race, = lens 2 L5; recorded. L4 (`Connecting` excluded from `HOLDABLE`):
  comment-only; recorded. L5 (gauges registered before the probe): third path to the meter-identity
  seam (lens 2 M3), recorded with that owner. L6 (drain's cuts sequential): still fine after C1 -
  a cut closes the socket first and does not wait; recorded. The lens 5 Low "a cancelled acquire
  evicts a healthy session as poisoned" (`SftpPool.acquire`'s catch, = R1 finding 4): judged **not**
  a safe one-liner - keeping the session would have to exclude a dial that never landed and a
  validation the ladder cut, and T4's `a session that opens into a cancelled caller is closed`
  pins `Connect, Close` for the filled case, so a change here edits an earlier ticket's test for a
  cost (one handshake per cancelled borrow, no leak) R1 already weighed. Recorded, not fixed.
