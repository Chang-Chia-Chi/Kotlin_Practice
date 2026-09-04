# T17 lens 2: resource lifecycle

Reviewer: a fresh Fable 5.1 subagent with one lens, tracing every socket, JSch thread, `.part`
file, caller stream, coroutine scope, pool entry, in-flight slot and permit from where it is
created to where it is released, on every exit path: normal return, a mapped failure, an
unclassified failure, cancellation at each suspension point, shutdown, and the forced disconnect
of spec 5.3 tier 3. Adjudication (what was fixed, what was recorded) belongs in an "Adjudication"
section at the end; the report above it is the reviewer's.

Scope read in full: the ticket, spec 4, 5.1 to 5.3, 6.1 to 6.3, 7.3, 8, 11, 17.1; progress.md's
open seams table, R1, R2, T13 and the T6/T8 test names; every main source file in `core`,
`quarkus` and `testkit`; the tests only to see what is already proven. JSch's `Session`, `Channel`,
`ChannelSftp` and `ProxyHTTP` were read from the pinned 2.28.7 sources jar in `~/.m2` where a
finding depends on what the library does, because R1 finding 5 was recorded from memory and this
lens turns on exactly that path. Nothing was built or run.

What is sound, so the findings are read in proportion. R1's two fixes held on every path traced:
`JschTransport.connect` keeps the session on the producing side and hangs up on an orphan
(`JschTransport.kt:246-276`), and `SftpPool.sweep` closes a round's retired sessions under
`NonCancellable` and gives back every reserved room in a `finally` (`SftpPool.kt:359-375`). R2's
`BorrowedSession` mutex holds: the loan cannot end while a call is on the wire and a call escaping
the block is cut by the ladder like any other (`BorrowedSession.kt:42-50`). The permit is freed
exactly once on every exit of `acquire`, `withLease`, `admit`, `openForTheShelf` and `giveBack`;
the `.part` file is removed on every exit of `StagingArea.receive` including a transfer the pool
had to cut, and a stale `.part` of the same name from an earlier run is truncated by the next
download (`StagingArea.kt:117-122`); `writeFrom` and `readTo` leave the caller's stream open, that
contract is written at the interface (`SftpTransport.kt:79-80, 90-91`) and JSch's `_put` and `_get`
confirm it (neither closes the stream it is handed); the only production caller of `writeFrom`
that owns a stream closes it with `use` (`SftpClient.kt:211`) and the other hands it a
`ByteArrayInputStream` (`StartupProbe.kt:301`). The housekeeper's eviction cannot race a lease:
`sweep` sees only the idle deque and `checkOut` pops from the same deque under the same lock. The
in-flight slot is given back exactly once by whichever of ack, nack, gone and withdraw settles it
first (`InFlightSet.kt:91-94`). `close()` on the pool and the connector is idempotent and
uncancellable. Every JSch failure on `Session.connect` runs the library's own `disconnect()`
before it is thrown (`Session.java:555-580`), so a refused handshake leaks nothing.

## Resource table

| Resource | Created | Released | Exit paths checked |
|---|---|---|---|
| JSch `Session` + socket + `Connect thread` | `JschTransport.openSession` `JschTransport.kt:290-325`, via `SftpPool.dial` `:178-187` (acquire) or `openForTheShelf` `:381-395` (housekeeper) | `JschConnection.close` `:416-427` on `io + NonCancellable`; `abort` `:434-441` on the caller's thread; JSch's own `disconnect()` on a failed handshake | Normal: `finish` `SftpPool.kt:422-431` from `handBack`/`closeEverything`. Cancel during handshake: orphan closed `:264-275`. Cancel after `filled`: discarded `POISONED` `:159-168`. Shutdown: cut `:317-327` then closed in parallel `:308`. Late dial into a closed pool: `filled` returns it to be hung up on `SessionRegistry.kt:128-134`. **Tier 3 on a stuck writer: H1.** |
| `ChannelSftp` | `JschTransport.kt:252-254`; on failure the session is disconnected `:258` | With the session, `:418-424` | One channel per session; no path hands a session back with a second channel open. Remote handles are closed by JSch on BREAK and on a monitor stop (T8's ladder tests against the server). |
| Pool permit | `admit` `SftpPool.kt:229-267` (`tryAcquire`/`acquire` under `withTimeoutOrNull`, granted flag), `sweep(takeRoom)` `:360` | `freeRoom` `:273-276`: `acquire`'s catch `:163-166`, `giveBack`'s `finally` `:436-440`, `admit`'s catch `:260`, `sweep`'s `finally` `:373` | Every exit traced by R1 re-traced; unchanged. |
| `PoolEntry` state / `retiring` count | `checkOut` `SessionRegistry.kt:98-119`, `sweep` `:283-288` | `retire` `:306-315` then `closed` `:180-186` | Balanced on every path except an `Error` out of `connection.close()`: L3. |
| `Lease` release-once | `acquire` `:153-157` | `Lease.giveBack` `:527-537` | Double release is refused; a `release()` that throws is followed by a misleading "given back twice" WARN: L4. |
| `.part` file | `StagingArea.receive` `StagingArea.kt:114-122` | `finally { deleteIfExists }` `:144`, after `use` closed the stream `:125` | Normal (moved `:137`), `IncompleteTransfer`, `SessionLost`, unclassified local failure, cancellation cooperative/keepalive/cut (I13 proven three ways), shutdown (S9, P6). Not unique per transfer: L1. `deleteIfExists` throwing masks: L2. |
| Caller `InputStream` (`writeFrom`) | `SftpClient.upload` `SftpClient.kt:211`; probe `StartupProbe.kt:301`; `BorrowedSession.writeFrom` `:60` pass-through; fake `FakeSftpTransport.kt:247-250` | `use` at the one site that opens a file; contract at `SftpTransport.kt:90-91` | Cancel during `put`: `withContext(io)` waits for JSch, `use` closes after. Sound. |
| Caller `OutputStream` (`readTo`) | `StagingArea.Tally` `:116-124` | `use` `:125` | Sound. |
| Connector scope | `SftpConnector.start` `:124` | `close` `:84`; refused start `:133-136` | Not joined (T13 deviation 2); every child either finishes memory work or runs under `NonCancellable`. Sound. |
| Watch producer | `background.produce` `SftpSource.kt:150` | Collector leaves: `consumeEach` cancels the channel and the producer `:152`; connector closes: scope cancel `:157-158`; directory claim released `:160` | Tick jobs are children of the producer `:226`; every `handedOver` slot is withdrawn on any exit `:310-316`. Sound. |
| `channelFlow` listing | `SftpClient.list` `:87` | Collector leaves: channel closed, `trySendBlocking` fails `:366`, selector answers BREAK `JschTransport.kt:353` | Blocked in the callback: unblocked by the channel close. Blocked in a socket read: ladder. Sound. |
| Ladder child | `carry` `CancellationLadder.kt:226-236` | `supervisorScope` waits for the child on every exit; cut after grace `:240-249` | Every ordering R1 walked re-walked. Sound, except that the cut itself can block: H1. |
| In-flight slot | `admit` `InFlightSet.kt:44-52` | `leave` `:54-57` via `settle` once `:91` | Cancel while waiting for room: no slot taken; earlier slots withdrawn. Slot from a completed tick survives `close()`: recorded seam. Sound. |
| Transfer permit | `Resilience.kt:94`, `withPermit` `:170` | `withPermit` | Cancellable acquire; sound. |
| Breaker, gauges, timers | `Resilience.kt:100`, `PoolMeters.kt:399-401`, `SourceMeters.kt:330` | Never; the registry owns them | Identity by endpoint only; a closed or refused connector's gauge shadows the next one on the same registry: M3. |
| Probe marker | `StartupProbe.kt:201, 301` | `tidyAway` `:316-319, 350-356` on the probe's own session | Left behind on a wire failure and on cancellation: M1. |
| Housekeeper coroutine | `SftpConnector.kt:140` | Scope cancel | Retired closes under `NonCancellable`; sound. |
| Quarkus close | `SftpConnectorLifecycle.kt:81-90` | `runBlocking(Dispatchers.IO)` under `withTimeoutOrNull` | The timeout cannot shorten an uncancellable close; it only decides whether the WARN prints. With H1 the hook blocks past its bound. |
| Testkit sockets/threads | `LoopbackConnectProxy.kt:140-164`, `EmbeddedSftpServer.kt:452` | `close()` `:125-131`, `:395-397` | Daemon threads; a held tunnel is released before the sockets close. Sound. |

## Findings, by severity

### HIGH

**H1. The forced tier cannot cut a session whose writer is blocked inside the socket write, and
neither can the keepalive; the only bound is the kernel's TCP retransmission timeout.**
`JschTransport.kt:434-441` (`abort` is `session.disconnect()`), read against the pinned
`Session.java:2244-2310` and `Channel.java:519-575`. `Session.disconnect()` first calls
`disconnect()` on every channel; `Channel.disconnect()` calls `close()`, which sends
`SSH_MSG_CHANNEL_CLOSE` through `Session.write` and therefore `_write`, which takes
`synchronized (lock)` around the socket write (`Session.java:1870-1885`). The socket is closed only
after that loop returns (`:2300-2308`). The reader thread's keepalive is the same write under the
same lock (`sendKeepAliveMsg` `:3268-3276`, called from the read-timeout branch at `:1916-1919`).

Exit path: an `upload` (`SftpClient.kt:206-212`, JSch `_put`) over a link that drops packets
rather than resetting - a firewall state that expired, a proxy whose upstream stopped reading, a
NAT that forgot the flow. The channel window is 2 MiB but the kernel send buffer of a socket that
sees no ACKs stays at its initial few tens of kilobytes, so the io thread blocks inside
`out.write` while holding `lock`. The caller is then cancelled (a consumer walking away, the
transfer time limit at `Resilience.kt:178-188`, or `SftpPool.close()`). Tier 1 never runs: the
progress monitor is asked between chunks and this thread is inside one. Tier 2 never fires: the
reader thread times out its read, enters `sendKeepAliveMsg`, and blocks on `lock` behind the
writer; it is now in a write, not a read, and no timeout is armed on it. Tier 3 blocks:
`cutLoose()` (`PoolEntry.kt:129-132`) calls `abort()`, which walks into `Channel.close()` and
blocks on the same lock before it reaches `socket.close()`. Three threads are now stuck until the
kernel gives up on the connection (order of fifteen minutes on Linux defaults): the io thread in
the write, the JSch reader thread, and whichever thread called `abort()`. Under
`CancellationLadder.bringToAStop` that thread belongs to the cancelled caller's coroutine and the
`supervisorScope` in `carry` waits behind it. Under `SftpPool.cutEverythingHeld`
(`SftpPool.kt:326`) it is the closing thread: `close()` overruns `drainTimeout + cancelGrace` by
the kernel's timeout, I9 and spec 11.2's bound are broken, and in Quarkus `stop()`
(`SftpConnectorLifecycle.kt:81-90`) blocks the shutdown for the same time, because
`withTimeoutOrNull` cannot shorten an uncancellable close. The spec's tier-3 promise (5.3: "the
cancellation handler calls `abort()`, which disconnects the session from another thread") is void
for exactly the call shape that most needs it. R1 finding 5 recorded this from memory as
"pathological, no fix inside the library's API"; read from the source it is the ordinary blackhole
case for uploads, and there is a fix inside the API.

Reasoned from the source, not reproduced; the testkit cannot stage it today because
`LoopbackConnectProxy.stall()` keeps reading precisely so that the sender's buffers never fill
(`LoopbackConnectProxy.kt:193-194`).

Fix shape: hold the socket. `openSession` sets a `SocketFactory` on the session that records the
socket it creates (honoured through the CONNECT proxy: `ProxyHTTP.java:70-80` and
`Session.java:244-254` both use the factory when one is set; the factory then owns the connect
timeout, since `Util.createSocket`'s timed dial is bypassed). `abort()` closes that socket first
and then calls `session.disconnect()`. Closing the socket makes the blocked `out.write` throw,
which releases `lock`; the reader thread's keepalive then throws, the reader loop ends and JSch's
own `disconnect()` runs to completion; `_put` throws, the mapper reads the `IOException` cause as
`SessionLost`, the ladder's child ends, and the entry is `POISONED` as it is today.

Failing test I would write: `CancellationLadderTest.a cancelled upload whose link drops packets is
cut loose within the grace`. `LoopbackConnectProxy` gains `stopReadingFromClient()`: the upstream
copier parks on a latch instead of reading (released by `close()`), and the accepted client socket
gets `setReceiveBufferSize(8 KiB)` before the tunnel starts so the client's send buffer fills
within a few hundred kilobytes on every host. A `JschTransport` through the proxy with
`cancelGrace = 300 ms`, `keepAlive = 500 ms`; `client.upload` of a 16 MiB file inside `async`;
once `bytesDelivered` upstream passes 256 KiB the proxy stops reading; cancel the upload and
assert the `async` completes within `cancelGrace + 2 s`, that `server.liveSessions` reaches zero
and that no `Connect thread ` is alive (the helper in `JschTransportTest.kt:119`). Red today: the
upload, the cut and the reader thread all outlive a ten-second bound. A second assertion in
`ShutdownAgainstServerTest` with `connector.close()` in place of the cancel pins I9 on the same
stall.

### MEDIUM

**M1. A start-up that fails or is cancelled during the marker rename leaves the marker in the
watched directory or the action target, and the next start hands it to the consumer as a file.**
`StartupProbe.kt:293-320` with `:350-356` and `:362-367`; the lister has no name filter
(`SftpSource.kt:325-341`, `PollingConfig` carries none). Two exit paths. Wire failure: the
`rename(home, parked)` at `:309` loses its reply, `checking` turns the `SessionLost` into a
`ConfigurationError`, and the `finally` runs `tidyAway` on the same session, which is dead; both
deletes fail, are caught as `SftpException` and logged at DEBUG, and `start` closes the pool.
Cancellation: the rename completes on the wire within the grace and the session is healthy, but
`tidyAway` calls `delete` on a cancelled coroutine, `withContext(io)` refuses it with
`CancellationException` at `JschTransport.kt:403`, and the `finally` throws past the marker. On
the next start a differently named `.sftpconnector-probe-<name>-<uuid>` sits in `inbound/` or in
`temp/`; the poll lists it, readiness passes a zero-byte file, and the consumer receives it. The
KDoc at `:196-198` says the marker "survives only a start-up that was killed mid-check"; a wire
failure is not a kill. Fix shape: skip the marker prefix in `filesUnder`, so a leftover is inert
whoever left it, and run `tidyAway` under `NonCancellable` for the cancellation path. Failing
test: `StartupProbeTest.a marker left by an earlier start is never handed to the consumer`: fake
transport with `inbound/.sftpconnector-probe-x-<uuid>` staged, `poll("inbound/")`, assert no
`FileSeen` names it. Owner: the ticket; it is a small change in two places.

**M2. `SftpPool.close()` after the cut is bounded only if `abort()` returned, which H1 shows it
may not; and the hang-ups of step 5 are unbounded on the same peer.** `SftpPool.kt:301-310`. The
`coroutineScope` at `:308` waits for every `finish`, and `JschConnection.close()` at
`JschTransport.kt:416-427` is `NonCancellable` and runs the same `Channel.close()` write. For an
idle session over a dropped link the write lands in a non-full send buffer and returns, so idle
hang-ups are fine; for a session whose writer is stuck (H1) the hang-up blocks behind the same
lock. With H1's fix the cut closes the socket and every later `disconnect()` returns at once, so
this collapses into H1. Recorded separately because the bound in the KDoc ("returns within the
drain timeout plus one cancel grace, whatever the sessions are doing") should say what it rests
on: every `abort()` returning promptly. Owner: whoever fixes H1; the test is H1's `close()` variant.

**M3. A closed or refused connector's gauges shadow every later connector to the same endpoint on
the same registry.** `PoolMeters.kt:399-401` and `:456`, `Resilience.kt:100`,
`SourceMeters.kt:330`, all registered in the constructors that `SftpConnector.start` runs before
the probe (`SftpConnector.kt:119-125`). Micrometer returns the existing meter when an id is
already registered and discards the new supplier. Exit path: `start` refuses (the comment at
`:130-132` names "a host that starts connectors on demand") or a host closes a connector and
starts another; the new pool's `lastCount` and the new breaker are never read, the first pool's
registry is retained by the gauge for the life of the process, and the numbers describe a pool
that no longer exists. The seams row "a second connector for one endpoint on one registry" names
two concurrent connectors; this is the sequential case through the module's own restart path,
and it is the one an on-demand host will hit first. Fix shape is the row's: a `name` tag beside
`endpoint`, which is spec 13's identity and the maintainer's call; or, inside the module, removing
the connector's meters from the registry in `close()`. Failing test: start a connector on a
`SimpleMeterRegistry`, close it, start a second against the same fake, borrow one session, assert
`sftp_pool_active{endpoint}` reads 1. Owner: the existing row's owner, with this path added to it.

### LOW

- **L1.** The `.part` name is `<target>.part` and not unique per transfer (`StagingArea.kt:114`).
  Two concurrent downloads onto one target (two watched directories holding the same name with
  the default target, which the KDoc at `SftpClient.kt:139-143` warns about, or a consumer calling
  `download()` twice on one event) open the same file: Windows refuses the second with a sharing
  violation, which is unclassified and evicts a healthy session; POSIX interleaves the writes, and
  the loser's `finally` deletes the winner's file from under its `Files.move`. No resource leaks;
  a random suffix would make each transfer's cleanup its own.
- **L2.** `Files.deleteIfExists` in the `finally` at `StagingArea.kt:144` can throw (a scanner
  holding the file on Windows) and then replaces the failure that ended the transfer. A
  `SessionLost` becomes an `IOException`: no retry, and the `.part` is the one thing the
  `finally` exists to remove. A `runCatching` with a WARN keeps the cause.
- **L3.** An `Error` out of `connection.close()` is let through on purpose (`SftpPool.kt:448-452`),
  and on that path `registry.closed(entry)` at `:430` never runs: the entry stays `Evicting`,
  `retiring` is never decremented, `isQuiet()` is never true, `close()` waits its full bound and
  the entry never reaches `Closed`. Error-only; noted so I9's "every entry `Closed`" is read with
  that qualification.
- **L4.** A `release()` that throws (the same `Error` path) is followed by `releaseAfter` in
  `withLease`'s catch (`SftpPool.kt:106-108`), which logs "given back twice" at `:529-533`
  about a lease that was given back once and failed. The permit is freed exactly once; the log
  line is the only casualty.
- **L5.** `cutLoose()` reads `entry.connection` outside the registry lock (`PoolEntry.kt:131`),
  which is written only under it (`SessionRegistry.kt:313`). A handback racing the cut either
  skips the abort (already retired, being closed) or aborts a session concurrently being closed;
  JSch's `disconnect()` returns at once when `isConnected` is false, so both outcomes are
  harmless. A data race by the letter; noted, not a finding.
- **L6.** A caller's filter that throws inside the listing selector (`SftpClient.kt:97`) escapes
  JSch's `ls`, reaches the mapper as a non-JSch exception and is classified `Unknown`, which
  poisons a healthy session and increments `sftp_error_unmapped_total`. Not a leak; the session is
  closed. Lens 5's to classify.
- **L7.** The accounting argument at `SftpClient.kt:352-358` says "a session being hung up on"
  holds a pool place. A session the housekeeper retired from the idle deque holds none (idle
  entries hold no permit: `freeRoom` runs after the handback to the shelf). Its close can wait
  behind `maxSize` leased operations for an io thread. Delay, never deadlock: the leased
  operations are bounded by the ladder and the keepalive, and the drain's cut frees threads. The
  sentence should say "or is a retired session waiting its turn".

## Answers to the lens questions not already covered

- `.part` after a cancelled download, and the next download of the same name: removed on every
  exit by `receive`'s `finally`, after `use` has closed the handle; a `.part` that survived a
  process kill is opened with `TRUNCATE_EXISTING` by the next download of that name. Spec 6.3's
  "no partial file survives a run" holds; nothing sweeps a stale `.part` from a previous run
  whose name is never downloaded again, and the spec does not ask for that.
- A rename compensation that leaves both names: there is none. `moveOnto` is rename, look, clear
  the target, rename again (`Compensation.kt:301-323`); its residual is a cleared target with the
  source in place, recorded as a seam by R2. The probe's marker (M1) is the one place both a
  `home` and a `parked` name can be left.
- A session returned to the pool with a channel still open: one channel per session by
  construction; the remote handles a listing or transfer holds are closed by JSch on BREAK and on
  the monitor's stop, which T8's ladder tests prove against the real server.
- A JSch session whose reader thread outlives `disconnect`: on the ordinary paths it ends when
  the socket closes, proven by `JschTransportTest`. On H1's path it outlives everything.
- `writeFrom` call sites: two in production, both correct, contract documented at the interface
  and inherited by `BorrowedSession`. Not restated at either site; `use` at the upload site makes
  the ownership plain, and the probe's stream has nothing to close.
- Scopes that outlive `close()`: the connector's scope is cancelled and not joined; a tick's
  `withdraw` and a housekeeper's `NonCancellable` hang-ups may still be running for a moment.
  Both are bounded and hold nothing the pool does not also track. A source built without the
  connector gets a scope nothing stops, by its own documentation.
- The housekeeper's eviction racing a lease: impossible by the deque and the lock.

## Ranked list

1. **H1** (High): tier 3 and tier 2 both block behind a writer stuck in the socket write; the
   cut, the reader thread, the io thread and `close()` wait for the kernel. Fix in the adapter
   with a socket-holding `SocketFactory`; test needs a proxy fault that stops reading.
2. **M1** (Medium, owner: this ticket): the probe's marker survives a wire failure or a
   cancellation and is delivered as a file on the next start. Skip the prefix in the lister; tidy
   under `NonCancellable`.
3. **M2** (Medium, owner: whoever fixes H1): `close()`'s bound rests on `abort()` returning;
   say so, and H1's fix makes it true.
4. **M3** (Medium, owner: the existing meter-identity seam's owner): a closed or refused
   connector's gauges shadow the next one on the same registry, through the module's own
   on-demand restart path.
5. **L1 to L7** as listed.

## Verdict

The lifecycle work of T3 through T13 and the two Fable reviews holds up: every resource the
connector creates has one owner and one release, cleanup runs under `NonCancellable` where a
cancelled coroutine would otherwise skip it, and the paths the earlier reviews found were
re-traced without finding a regression. The one High is a promise the spec makes that the library
cannot keep as the adapter uses it: the forced disconnect goes through JSch's orderly channel
close, which needs the same lock a stuck writer holds, so a blackholed upload defeats all three
tiers and turns a bounded shutdown into a kernel timeout. It is fixable inside the adapter in a
few lines and is the one thing on this list that should be closed before the connector meets a
network that drops packets. The Mediums are an operator-visible leftover from the probe, a
sentence in the shutdown's contract, and a metrics identity problem the seams table already
half-names. The Lows are bookkeeping.

## Adjudication (T17, lens 2)

Fixed and recorded by the T17 owner (Fable). The reviewer's text above is unchanged.

- **H1 (= lens 1 C1): fixed.** `JschTransport.abort()` closes the retained socket before
  `session.disconnect()`, so a session whose writer is blocked in the socket write is cut without
  waiting on the write lock the writer holds - all three tiers now bound a blocked call, a write as
  much as a read. The socket is kept via a `SocketFactory` set on the session (honoured on the
  direct dial and through `ProxyHTTP`); the factory owns the connect timeout, since setting one
  bypasses JSch's own timed dial. Spec 5.3 and D47 record it. Test kit gained the black-hole fault
  the report asked for (`LoopbackConnectProxy.blackHoleClientAfter`, which stops reading from the
  client); proved by `CancellationLadderTest.a cancelled upload on a tunnel that stopped reading is
  cut within the grace` and `ShutdownAgainstServerTest.I9_closing while an upload is black-holed on
  a full send buffer returns within the bound`. Commit `SFTP T17 C1`.

- **M1: fixed.** The probe's `tidyAway` now runs under `NonCancellable`, so a probe cancelled after
  the marker was written still deletes it on a session that is still alive
  (`StartupProbeMarkerTest.a probe cancelled after the marker is written leaves no marker`). And the
  lister skips the whole marker prefix (`PROBE_MARKER_PREFIX`) in `SftpSource.filesUnder`, so a
  marker left by a dead session - whoever left it - is inert to the poll rather than handed over as
  a file (`SftpSourceTest.a marker left by a dead session is never handed to the consumer`). Commit
  `SFTP T17 lens 2 M1`.

- **M2: collapses into C1, as the report says.** `SftpPool.close()`'s bound rested on `abort()`
  returning; with C1's socket-close-first fix every `abort()` and every later `disconnect()` returns
  promptly, so the shutdown bound holds for a write-blocked session too. No separate change; the
  KDoc's promise is now true, and `ShutdownAgainstServerTest.I9_...black-holed...` pins it.

- **M3: recorded as a seam, with the meter-identity owner.** A closed or refused connector's gauges
  shadow the next connector to the same endpoint on the same registry, through the module's own
  on-demand restart path. Same fix as the existing "second connector for one endpoint on one
  registry" seam - a `name` tag beside `endpoint` (spec 13's identity, the maintainer's call), or
  removing the connector's meters in `close()`. Added to that seam's row rather than closed here;
  lens 1 L5 (gauges registered before the probe) is a third path to the same place and is recorded
  with it. Not this ticket's to change: it is spec 13's meter identity.

- **Lows.** L1 (`.part` not unique per transfer), L2 (`deleteIfExists` in the `finally` can mask the
  cause), L4 (misleading "given back twice" WARN): in `StagingArea`/`Lease`, outside this ticket's
  regions; recorded, unchanged. L3 (`Error` out of `connection.close()` skips `registry.closed`) and
  L5 (`cutLoose` reads `connection` outside the lock, = lens 1 L3): benign / error-only; recorded.
  L6 (a caller filter throwing inside the selector is classified `Unknown`): lens 5's to classify;
  recorded. L7 (the accounting sentence should say "or is a retired session waiting its turn"): a
  comment; recorded. No Low was a safe behaviour-preserving one-liner inside this ticket's regions.
