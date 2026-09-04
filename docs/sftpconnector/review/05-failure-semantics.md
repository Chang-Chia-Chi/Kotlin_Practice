# T17 lens 5: failure semantics

Reviewer: a fresh Fable 5.1 subagent with one lens, reading every `catch`, every retry and
compensation site, and the breaker's inputs against spec 6.1, 8.3, 9, 10 and 17. Adjudication
(what was fixed, what was recorded) belongs in an "Adjudication" section the ticket owner appends;
the report above it is the reviewer's.

Scope read in full: spec 5.3, 5.4, 6.1, 6.2, 7.2, 8.3, 9, 10, 11.2, 17.1, 17.2, 17.3; progress C4,
C13, the open-seams table, R2, T11, T12, T13, T15, T16; every main source file under `core` and
`quarkus`; `FakeSftpTransport` as the test boundary. Where the behaviour depends on Resilience4j
2.4.0, it was read off the jar's bytecode (`javap`), not remembered: the Kotlin module's
`CircuitBreaker.executeSuspendFunction`, `Retry.executeSuspendFunction`,
`TimeLimiter.executeSuspendFunction`, `CancellationKt.isCancellation`, and
`CircuitBreakerConfig`'s default constants.

What is sound, so the findings are read in proportion: the failure model is *one* mechanism and
every layer obeys it. The retry predicate reads `disposition.retry == IMMEDIATELY`
(`Resilience.kt:197-198`) and the breaker predicate reads `countsAgainstTheBreaker`
(`Resilience.kt:200-201`), with no class list anywhere; that makes spec 10.2's table true row by
row: wire failures (`ConnectFailed`, `SessionLost`, `OperationTimeout`, `IncompleteTransfer`,
`Unknown`) are retried on a fresh lease and counted; answered failures (`NoSuchFile`,
`PermissionDenied`, `ServerFailure`) are neither retried in-call nor counted and keep their session;
`Fatal` stops, uncounted; `PoolExhausted`, `OverwriteRefused`, `UnsafeFileName` are neither; and
`CircuitOpen` never reaches the breaker's `onError` at all. The S5 rule holds exactly:
`NoSuchFile` from a download is thrown on try one, never retried, never counted, and becomes `GONE`
(`SftpSource.kt:357-366`). The `NoSuchFile`-after-a-retry rules for `delete` and `rename` are
gated on "a try before this one reached the server", recorded before the request is sent, never
inferred from a failure class (`SftpClient.kt:243-246`, `Compensation.kt:50-52`). `mkdir` is
idempotent by looking. `withSession` has no retry. Cancellation is honoured at every boundary: the
mapper rethrows it before classifying (`JschErrorMapper.kt:52-56`), the ladder rethrows it after
deciding the session's fate (`CancellationLadder.kt:61-66`), the time limiter asks the coroutine
before believing a timeout (`Resilience.kt:181-183`), and Resilience4j's breaker releases its
permit on any `CancellationException` rather than recording it (bytecode: `isCancellation` is
`job.isCancelled || t is CancellationException`, then `releasePermission()`). A cancelled try is
never retried, because the retry's own `delay` is cancellable. No `Error` is ever turned into an
`SftpException`. The `.part` file is deleted on every exit of `StagingArea.receive`, so a
download retry always starts into a fresh partial, and a transfer the monitor stops reports
cancellation, never `IncompleteTransfer` (`JschTransport.kt:212-215`).

## Findings, by severity

### HIGH

**H1. The breaker opens on *slow successes*, which spec 9 never lets it count.**
`Resilience.kt:78-92`. The breaker is built with `failureRateThreshold`, `slidingWindowSize`,
`minimumNumberOfCalls`, `waitDurationInOpenState`, `clock`, `permittedNumberOfCallsInHalfOpenState`
and `ignoreException`, and inherits everything else. Read off the 2.4.0 jar:
`DEFAULT_SLOW_CALL_DURATION_THRESHOLD = 60` seconds and `DEFAULT_SLOW_CALL_RATE_THRESHOLD = 100`.
A call that *succeeds* after more than 60 s is recorded as a slow success, and a window in which
every call is slow opens the breaker with no failure having occurred. The connector's own clock on
a transfer is `transferTimeout`, default 15 minutes (`ConnectorDsl.kt:444`), so the connector
declares a 14-minute download healthy while the breaker it wraps declares it slow. The measured
duration includes the wait for a transfer permit, because `throughTheTransferLimit` runs *inside*
`throughTheBreaker` (`Resilience.kt:138-139`).

Spec row contradicted: 9, "Circuit breaker: counts as failure: recoverable errors only; fatal
errors are surfaced, not counted"; 10.2 has no row on which a success counts against anything.

Sequence, all defaults except `PostAction.Noop` (or a consumer that downloads its batch first and
acks afterwards): a tick lists 25 files of a size the tunnel moves in 90 s each; the consumer
downloads them one after another. After the 21st download the 20-call window holds only slow
successes, slow rate 100 %, breaker `OPEN`; download 22 fails with `CircuitOpen`; under `consume`
that is a nack with redelivery and a nack action that also fails with `CircuitOpen`; every tick for
`waitInOpen` (1 min) is `PollSkipped(BREAKER_OPEN)`; the half-open probe is the next tick's `list`,
fast, so the breaker closes; the next 20 downloads open it again. A pipeline moving large files on
a slow link is throttled to twenty files per minute of outage, with `sftp_breaker_state` reading 2
and nothing in the log saying why (lens 4 H3). With `Move` actions between downloads the rate
stays at 50 % and the fault is invisible until someone changes the ack action.

Failing test: the breaker measures duration with `System.nanoTime()` (the injected `Clock` is used
only for the wait in open), so a boundary test needs real seconds. Two tests, either is enough:
(1) `ResilienceTest.a slow success is not a failure` in `core` (same module, `Resilience` is
internal): build `Resilience` with `transferTimeout = 2.minutes` and assert the breaker's
`circuitBreakerConfig.slowCallDurationThreshold >= transferTimeout` through an internal accessor
the fix adds; red today (60 s). (2) The real-time twin, `@Tag("measure")`: `slidingWindow = 2`,
a fake whose `readTo` sleeps 61 s of wall time twice, then `client.stat`; assert it is not
`CircuitOpen`. Smallest fix: `.slowCallDurationThreshold(settings.transferTimeout)`, since anything
longer is already `OperationTimeout`; better, also
`.currentTimestampFunction({ it.millis() }, MILLISECONDS)` on the injected clock so the twin runs
on virtual time.

**H2. Under `REPLACE`, a retry after a lost reply takes the file it was meant to replace for its
own landed rename, reports success, and leaves the source where it was.**
`Compensation.kt:50-51` and `:63-68`, reached from `SftpClient.kt:230-234` and the ack path
`SftpSource.kt:406-410`. On a retry, before sending anything, `holdsTheMovedFile(size)` stats the
target and answers yes for *any* file of the expected size. Under `REFUSE` the target was found
free before the first request, so that reading is nearly always right. Under `REPLACE` the target
is, by the policy's own premise, usually occupied: yesterday's `report.csv`, last run's marker, the
previous version of a fixed-format extract, an empty file. A file of the same size there is the
common case, not a stranger.

Spec rows contradicted: I15, "every file that was acked is at the ack target"; 6.1's rename row is
followed literally ("existing with the expected size counts as success"), which is why this is
raised as a spec change and not a code slip: the rule was written for a target expected to be free
and D38 refined which path is missing, but nothing refined what "expected" means once the target
is expected to be occupied.

Sequence, fake server (no POSIX rename extension) or any server: `archive/daily.csv` holds
yesterday's 4,096-byte file; today's `inbound/daily.csv` is also 4,096 bytes; ack action
`Move("archive", REPLACE)`. Try 1: `moveOnto` sends the first rename; the session dies before the
reply (`SessionLost`); nothing landed. Try 2, fresh session: `reachedTheServer` is true, so the
target is looked at first: `archive/daily.csv`, 4,096 bytes, "the rename whose reply was lost had
landed; reported as success". The ack returns, the slot is released, `sftp_ack_total{outcome=ack}`
moves. `inbound/daily.csv` is still in `inbound`; the next tick lists it, the in-flight set does
not hold it, `consume` downloads and processes it again, and this time the move replaces the old
file. One file processed twice, one ack that lied, and no line in the log says either.

Failing test, `RetrySemanticsTest.I15_under REPLACE a file already at the target with the source's
size is not taken for the landed one`: `fakeServer { if (it.isFirstOf(Rename)) throw
SessionLost(...) }.file(FROM, CONTENT).file(TO, OTHER_OF_SAME_LENGTH)`;
`client.rename(FROM, TO, Overwrite.REPLACE, expectedSize = CONTENT.length.toLong())`; assert
`server.snapshot()` has no `FROM` and `server.bytesAt(TO)` is `CONTENT`. Red today: `FROM` is
still there and `TO` holds the other bytes. Smallest sound fix: a rename that landed preserves the
source's mtime and removes the source, so the discriminator should be `(size, mtime)` rather than
size alone; the source layer already has `file.modifiedAt` beside `file.size`, and `RenameTries`
measures the source once when the caller passes nothing, so it can capture both in the same stat.
The fake needs a per-file mtime for that test (today it is one fixed instant); a fake change is
inside the boundary.

### MEDIUM

**M1. A transfer that received *more* bytes than the listing said is retried three times on fresh
sessions and counted three times, when the evidence is a file still being written.**
`StagingArea.kt:57-63` raises `IncompleteTransfer` for `count != expectedSize`, and
`SftpException.kt:140-141` makes every instance poison. D28's argument is that "a short read and a
half-dead session are indistinguishable from here" - true of a short read, not of a long one: a
socket cannot deliver bytes the file did not have, so `count > expectedSize` means the server
answered honestly about a file that grew since it was listed, which is an uploader still writing.
Spec 10.2's own rationale for the third row (D41) - "a failure the server answered proves the
request arrived and was understood... counting it charges the connector for a healthy server doing
its job" - applies to it word for word.

Sequence, defaults, `MinAge(1.minutes)` readiness, an uploader writing 200 MB files over ten
minutes: a tick lists a file at 50 MB after a minute of quiet; the consumer downloads it; 51 MB
arrive; `IncompleteTransfer`, session evicted `poisoned`, breaker +1; retry after 1-2 s on a fresh
handshake fetches 52 MB; same again; third try, same; the caller gets `IncompleteTransfer` after
three full downloads. Ten such files in one tick are thirty counted failures in a window of
twenty: the breaker opens on a healthy server, and every other file in the tick is
`CircuitOpen`. The next tick redelivers all of them.

Failing test, `RetrySemanticsTest.a download that received more than the listing said is not tried
again and keeps its session`: `fakeServer { if (it.operation == Read) file(REMOTE, CONTENT +
"more") }.file(REMOTE, CONTENT)`; `client.download(RemoteFile(REMOTE, size = CONTENT.length,
...))`; assert `IncompleteTransfer`, `retries("download")` is 0, exactly one `Connect`, and
`pool.stats().idle == 1`. Red today: two retries, three connects, three closes. Proposed decision
(owner: the maintainer, D28 amendment): a long read is `IncompleteTransfer` with `poisons = false`
- answered, kept, next tick - and only a short read poisons; the class stays, the KDoc's
"stalled or still-writing uploader" reading becomes the disposition it already describes.

**M2. `watch` swallows a `CancellationException` the collector raised for itself and ends the flow
normally, saying the connector stopped it.** `SftpSource.kt:153-158`. The catch around
`consumeEach { emit(it) }` cannot tell a cancelled *producer* (the connector closing, which is what
it is for) from a `CancellationException` thrown *through* `emit` by the collector's own block: a
`withTimeout` the block let escape, or a flow operator's abort. `ensureActive()` passes because the
collector's job is not cancelled, and the flow completes.

Spec row contradicted: 10.1, "`CancellationException` is never caught or wrapped"; and T12's own
rule for the same shape inside a tick, "a check that lets its own timeout escape... ends the watch
as a bug" (`SftpSource.kt:241-248`), is applied to the tick and inverted for the collector.

Sequence: `runBlocking { source.watch("inbound", 1.hours).collect { withTimeout(30.seconds) {
process(it) } } }`; `process` overruns; `TimeoutCancellationException` leaves the block, reaches
`emit`, is caught at `:153`, `ensureActive` passes, INFO "The watch of inbound ended because the
connector stopped it", `collect` returns normally, `runBlocking` returns, the pipeline is gone and
the process's main thread has exited with status 0. Files the tick had handed over are withdrawn
correctly (the producer is cancelled by `consumeEach`), so nothing is lost on the server; what is
lost is the failure.

Failing test, `SftpWatchTest.a timeout the collector lets out of its own block ends the watch with
that timeout, not normally`: on the fake, `runTest`, `source.watch(DIR, 1.minutes).collect {
withTimeout(1.milliseconds) { delay(1.seconds) } }`; `assertThrows<TimeoutCancellationException>`.
Red today: `collect` returns. Smallest fix: catch only when the *producer* is done -
`events.isClosedForReceive` (or check `stopped` is the channel's cancellation by catching around
`receiveCatching`) - and rethrow anything else. Owner: T17's adjudicator; it is one condition.

**M3. The start-up probe relabels a wire failure mid-check as `ConfigurationError`, so a
`RETRY_ON_A_FRESH_SESSION` failure leaves as `STOP_THE_CONNECTOR`.** `StartupProbe.kt:209-214`
(`checking` catches every `SftpException`). This is lens 4's H1 seen from the class rather than
the message. A `ConnectFailed` on the dial is *not* affected - it is raised from `withLease`
inside `once`, outside any `checking`, and reaches the host as itself - but a `SessionLost`,
`OperationTimeout` or `Unknown` during any of the eight checks becomes `Fatal`, uncounted,
unretried, and in Quarkus a refused deployment naming a remedy about path spelling. Spec 11.1's "a
failed probe is fatal" supports the outcome; spec 10.2 does not support the *class*: a host that
retries start-up on `Recoverable` and gives up on `Fatal` (the natural reading of 10.1) cannot tell
a proxy hiccup from a wrong directory. Failing test: as lens 4 H1 - a fake whose first `Realpath`
throws `SessionLost`; assert `SftpConnector.start` throws `SessionLost`. Red today:
`ConfigurationError`. Smallest fix: in `checking`, convert only failures whose disposition is
`RETRY_ON_THE_NEXT_TICK` or `ACCEPT_THE_REFUSAL` (the server, or the connector, *answered*), and
let a wire failure through as itself. Owner: adjudicate together with lens 4 H1; one fix serves
both.

**M4. A rename whose reply is lost on the last permitted try is reported as failed with the
knowledge that would settle it thrown away, and the source's WARN then asserts the file is where it
was.** `SftpClient.kt:230-234` and `Resilience.kt:126-147`: when the retry budget is spent (or the
breaker opens between tries), `attempting` rethrows the wire failure; `RenameTries` holds
`reachedTheServer = true` and an `expectedSize`, and nobody asks it. `SftpSource.kt:194-198` then
logs "so it is still where it was and will be handed over again", which for a lost reply is
unknown, and false whenever the rename landed: the file is at the action target, will never be
listed again, and the consumer's ledger has a failed ack for it. T16 raised this as I15's bound and
proposed the spec wording; this lens adds that the code can close most of it at the cost of one
`stat`. Sequence: `maxAttempts = 3`; try 1 rename lands, reply lost; try 2 dial `ConnectFailed`;
try 3 dial `ConnectFailed`; caller gets `ConnectFailed`; the file is in `archive/`; the consumer
records a failure. Failing test, `RetrySemanticsTest.a rename whose reply is lost on the last
permitted try is looked into before it is reported`: `maxAttempts = 1`, `fakeServer { if
(it.isFirstOf(Rename)) landAndLoseTheReply(it) }`; assert `rename` returns normally and
`server.calls` contains `Stat` of `TO` on session 2. Red today: `SessionLost`. Proposed shape:
`SftpClient.rename` catches the final wire failure when `tries.reachedTheServer`, runs
`holdsTheMovedFile` once through `resilience.once`, and returns success or rethrows; the WARN at
`:194` says "may or may not have moved" for `RETRY_ON_A_FRESH_SESSION` failures. Owner: the
maintainer for the I15 wording; the adjudicator for the look, which is small.

### LOW

- **L1. A cancelled acquire evicts the healthy session it had just been given, counted
  `poisoned`.** `SftpPool.kt:151` and `:159-168`: `ensureActive()` after a `Reuse` or a `Prove`
  that completed throws, and the catch discards `claimed` as `POISONED` - the comment says "while
  the pool can still put the session back itself", the code throws it away. Contradicts
  `Lease.releaseAfter`'s rule (`SftpPool.kt:512-518`) that a cancellation says nothing about the
  session, and costs a handshake per cancelled caller. Test: one warm idle entry past
  `validationBypass`; `val job = launch { pool.withLease {} }`; a fake whose `Realpath` hook calls
  `job.cancel()` and returns; assert `sftp_pool_evicted_total{reason=poisoned}` is 0 and
  `stats().idle == 1`. Red today: 1 and 0.
- **L2. `openForTheShelf` swallows an `Error`.** `SftpPool.kt:385-394` catches `Throwable`,
  rethrows only `CancellationException`, and logs the rest as "Opening a spare session failed";
  `close(connection)` at `:446-453` deliberately lets an `Error` through. One of the two is wrong;
  the second is right.
- **L3. An exception thrown by a caller's `list` filter is reported as `ServerFailure` "status
  4:" with an empty message.** `JschTransport.kt:176-184` runs `onEntry` inside JSch's selector;
  JSch wraps any exception from it as `SftpException(SSH_FX_FAILURE, "", cause)`, and
  `JschErrorMapper.kt:129-142` maps that to `ServerFailure`. Disposition is right by accident
  (kept, not retried, not counted); the class and message blame the server for a caller's bug,
  and the real exception is `failure.cause.cause`. Reproduces only against the embedded server;
  the fake calls `onEntry` on the caller's coroutine.
- **L4. `SSH_FX_NO_CONNECTION` (6) and `SSH_FX_CONNECTION_LOST` (7) map to `ServerFailure`.**
  `JschErrorMapper.kt:129-142`. No server sends them and JSch does not raise them today, so no
  sequence; two rows to `SessionLost` would cost nothing and protect against a library that starts
  to.
- **L5. With defaults the breaker cannot judge before twenty tries.** `Resilience.kt:84` sets
  `minimumNumberOfCalls = slidingWindow` (20). A dead server costs one `list` of three tries per
  tick, so on the hourly pipeline the breaker opens on the seventh hour, stays open one minute, and
  closes on the next tick's probe if the server is back. Consistent with spec 9; recorded because
  S3's "each tick until half-open" reads differently at hourly intervals than at P5's one second.
- **L6. A breaker skip inside `consume` is counted as a nack.** `SftpSource.kt:173-187`: a block
  whose `download()` throws `CircuitOpen` is nacked, `sftp_ack_total{outcome=nack}` moves, and the
  nack action runs into the same `CircuitOpen`. Spec 10.2 gives `CircuitOpen` "emits PollSkipped";
  per file there is no row. Redelivery is right; the counter is the lie.

## Every `catch`, with its disposition

Disposition: R = rethrow unchanged, W = wrap into another type, M = map to a connector class,
S = swallow (with what is logged). CE = can a `CancellationException` enter, and what happens.
Err = can an `Error` enter, and what happens.

| Site | Caught | Disposition | CE | Err |
|---|---|---|---|---|
| `SftpConnector.kt:129` | `Throwable` | close scope and pool under `NonCancellable`, R | R after cleanup | R after cleanup |
| `StartupProbe.kt:188` | `NoSuchFile` | M to null | passes | passes |
| `StartupProbe.kt:200` | `SftpException` | S, DEBUG (tidy-away) | passes | passes |
| `StartupProbe.kt:212` | `SftpException` | W into `ConfigurationError` (M3) | passes | passes |
| `Resilience.kt:164` | `CallNotPermittedException` | M to `CircuitOpen` | passes | passes |
| `Resilience.kt:181` | `TimeoutException` | `ensureActive()` then M to `OperationTimeout` | rethrown by `ensureActive` when the caller was cancelled | passes |
| Resilience4j `Retry.executeSuspendFunction` (bytecode) | `Exception` | predicate `worthAnotherTry && stillWorthRetrying`; delay or R | R (predicate false); a CE from the backoff `delay` is outside the try | passes |
| Resilience4j `CircuitBreaker.executeSuspendFunction` (bytecode) | `Throwable` | `isCancellation` -> `releasePermission()`, R; else `onError` (ignored unless `countsAgainstTheBreaker`), R | released, not recorded | `onError`, ignored (not an `SftpException`), R |
| Resilience4j `TimeLimiter.executeSuspendFunction` (bytecode) | `Throwable` | `TimeoutCancellationException` -> W into `TimeoutException`; other CE -> R without `onError`; else `onError`, R | see left | `onError`, R |
| `SftpClient.kt:174` | `InvalidPathException` | M to `UnsafeFileName` | n/a | n/a |
| `SftpClient.kt:249` | `NoSuchFile` | S if an earlier try reached the server (INFO), else R | passes | passes |
| `SftpClient.kt:329` | `ServerFailure` | S if a directory is there, else R | passes | passes |
| `Compensation.kt:55` | `NoSuchFile` | S if the target holds the moved file, else R | passes | passes |
| `Compensation.kt:91` | `ServerFailure` | compensate (look, clear, rename) or R | passes | passes |
| `Compensation.kt:97` | `ServerFailure` | W into `ServerFailure` (same class, "cleared" wording, cause kept) | passes | passes |
| `Compensation.kt:125` | `NoSuchFile` | W into `NoSuchFile` naming `to` when the source is there, else R | passes | passes |
| `Compensation.kt:146` | `NoSuchFile` | S, DEBUG | passes | passes |
| `Compensation.kt:155` | `NoSuchFile` | M to null | passes | passes |
| `ClientMeters.kt:34`, `SourceMeters.kt:41` | `Throwable` | record label, R | R (`cancelled`) | R (`fatal`) |
| `CancellationLadder.kt:61` | `CancellationException` | `bringToAStop` under `NonCancellable`, R; `supervisorScope` waits for the child | R | passes (from `await`) |
| `SftpSource.kt:153` | `CancellationException` | `ensureActive()`; S with INFO when the collector is alive (M2) | S or R | passes |
| `SftpSource.kt:178` | `Exception` | `ensureActive()`; nack | nack when it is the block's own (deliberate) | passes, ends `consume` |
| `SftpSource.kt:192` | `SftpException` | R if `watch == STOP`, else S with WARN | passes | passes |
| `SftpSource.kt:240` | flow `catch`, all upstream | CE with live job -> W into `IllegalStateException`; non-`SftpException` -> ERROR, R; else by `disposition.watch` | W (deliberate, T12) | ERROR, R |
| `SftpSource.kt:310` | `Throwable` | withdraw handed-over slots, R | R | R |
| `SftpSource.kt:357` | `NoSuchFile` | settle `GONE`, null | passes | passes |
| `SftpPool.kt:106` | `Throwable` | `releaseAfter(failure)`, R | R; lease kept unless the ladder marked it | R; lease evicted |
| `SftpPool.kt:159` | `Throwable` | discard `claimed` as `POISONED`, free room, R (L1) | R | R |
| `SftpPool.kt:210`, `:212` | CE; `Exception` | R; discard as `VALIDATION`, return false | R | passes to `:159` |
| `SftpPool.kt:259` | `Throwable` | free room if granted, R | R | R |
| `SftpPool.kt:344`, `:346` | CE; `Exception` | R; S with WARN and continue | R | ends the housekeeper |
| `SftpPool.kt:385` | `Throwable` | give back `POISONED`; CE -> R; else S with WARN (L2) | R | S |
| `SftpPool.kt:446`, `:448` | CE; `Exception` | R; S with WARN | R | passes |
| `JschErrorMapper.kt:52`, `:57` | CE; `Exception` | R; M by `classify` (non-JSch -> `Unknown`) | R | passes |
| `JschTransport.kt:84` | `Throwable` | `session.disconnect()`, R | R (cannot arise on the IO thread) | R |
| `JschTransport.kt:93`, `:97` | CE; `Exception` (from the orphan's close) | close orphan under `NonCancellable`, R; S with WARN | R | passes |
| `JschTransport.kt:266` | `Exception` | S with WARN (abort) | n/a | passes |
| `SftpConnectorLifecycle.kt:82` | none (`withTimeoutOrNull`) | the block is `NonCancellable`, so the timeout cannot return early; the WARN fires after the close | n/a | n/a |

Disposition sites that are not `catch` blocks: `Lease.releaseAfter` (`SftpPool.kt:516-525`) reads
`disposition.lease`; unclassified failures and `Error`s evict, `NONE_HELD` keeps; a cancellation
keeps unless `unfitAfterCancelling`. `CancellationLadder.bringToAStop` (`:70-92`) sets that flag
from the completed work's exception class or from cutting.

Answers to the lens question. A `CancellationException` is *wrapped* once, deliberately, into
`IllegalStateException` for a tick cancelled by nobody (`SftpSource.kt:243-247`); *swallowed* once,
by mistake, at `SftpSource.kt:153-158` (M2); *converted to a nack* once, deliberately, at
`SftpSource.kt:178-183`. It is never turned into an `SftpException`. No `Error` is ever classified;
one is swallowed (L2).

## Every retry and compensation site against 6.1 and 8.3

| Operation | 6.1 row | What the code retries | What it compensates | Can a retry duplicate or lose a side effect |
|---|---|---|---|---|
| `list` (`SftpClient.kt:94`) | blind retry | until the first entry is handed on (T11 deviation 5) | none | no; a listing that died after entries fails the poll |
| `stat`, `exists` (`:119-122`) | blind retry | every wire failure | none | no |
| `download` (`:147-154`, `StagingArea.kt:45-75`) | restart from zero into a fresh `.part` | every wire failure and `IncompleteTransfer` | `.part` deleted on every exit, including cancellation | no duplicate; M1: a long read is retried when it should not be |
| `upload` (`:201-213`) | restart, remote partial overwritten | every wire failure | the `REFUSE` look runs once; `OVERWRITE` on every try | no duplicate; a stranger arriving at the target during the backoff under `REFUSE` is replaced (open seam, stands) |
| `rename` (`:230-234`, `Compensation.kt:35-69`) | on `NoSuchFile` naming the source after a retry, stat `to`; expected size is success; naming the target means nothing landed | every wire failure | look at the target before sending on a retry; look again on `NoSuchFile` naming the source; `moveOnto` clears only a file, only without the extension | H2: an old same-size target is taken for the landed file under `REPLACE`; M4: a lost reply on the last try is never looked into |
| `delete` (`:242-254`) | `NoSuchFile` after a retry is success | every wire failure | `NoSuchFile` is success only after a try reached the server | no |
| `mkdir` (`:266-268`, `:326-332`) | already-exists is success | every wire failure | `ServerFailure` followed by a look; a directory there is the outcome | no |
| `withSession` (`:292-303`) | no retry | none; breaker only | the loan ends after the last in-flight call, under `NonCancellable` | no |

Retry only ever follows a wire failure, so "a rename retried after a partial success" can only
mean the replace sequence's delete landed and the second rename's reply was lost; the retry's look
finds the target empty and sends a plain rename, which lands. "A delete retried after the file is
gone" is success by `:249-251`. "An upload retried onto a `.part` from the first attempt" cannot
happen: uploads write to the target directly and downloads delete their partial per try.

## The `NoSuchFile`-before-retry rule and every other "do not retry"

S5: `download` -> `attempting` -> `NoSuchFile` from `readTo` -> `worthAnotherTry()` is false
(`RETRY_ON_THE_NEXT_TICK`) -> rethrown after one try -> breaker `ignoreException` -> not recorded
-> `FileHandling.download` settles `GONE`, releases the slot, returns null; `FileGone` follows
`FileSeen` in a live poll. `sftp_retry_total` untouched. Holds, and holds on try two as well.

Fatal: not retried, not counted, `watch` ends (`SftpSource.kt:262-265`). `PoolExhausted`: not
retried, not counted, `PollFailed`. `OverwriteRefused`, `UnsafeFileName`: not retried, not counted,
lease returned (the latter never held one). `CircuitOpen`: not retried, `PollSkipped`. A
cancelled try: not retried, because the backoff `delay` throws. All as 10.2 says.

## The breaker

What counts: exactly `RETRY_ON_A_FRESH_SESSION` (`Disposition.kt:25`), because
`ignoreException { !it.countsAgainstTheBreaker() }` treats everything else as neither success nor
failure and releases the permit (`Resilience.kt:88-90`, confirmed against
`CircuitBreakerStateMachine`'s ignore-then-record order). A cancellation never counts, even when
the ladder's cut raised a `SessionLost` underneath it, because `carry` replaces that with the
cancellation before the breaker sees anything. An `OperationTimeout` counts, and is raised after
the ladder has decided the lease, so `LeaseFate.EVICTED` on it is advisory (open seam, stands). A
client error (`PoolExhausted`, `OverwriteRefused`, `UnsafeFileName`, an unclassified exception)
does not count and does not open it. An open breaker is `CircuitOpen`, `SKIP_THE_TICK`,
`PollSkipped(BREAKER_OPEN)`. The breaker sees *tries*, not calls, as spec 9's order requires, so
one failing operation is three counted failures. What also counts, and should not: a success slower
than 60 s (H1).

## Ranked list

1. **H1** - breaker opens on slow successes (`Resilience.kt:78-92`). Fix: set
   `slowCallDurationThreshold` to `transferTimeout`; put the timestamp function on the injected
   clock.
2. **H2** - `REPLACE` retry takes an old same-size target for its landed file
   (`Compensation.kt:51`, `:63-68`). Fix: discriminate on `(size, mtime)`; spec 6.1 and I11 wording
   follow.
3. **M1** - a long read is retried and counted (`StagingArea.kt:57-63`). Owner: the maintainer,
   D28 amendment; code is one flag.
4. **M2** - `watch` swallows a collector-raised cancellation (`SftpSource.kt:153-158`). Owner:
   adjudicator; one condition.
5. **M3** - probe relabels wire failures `Fatal` (`StartupProbe.kt:209-214`). Owner: adjudicator,
   with lens 4 H1.
6. **M4** - last-try lost reply on a rename is not looked into; WARN asserts "still where it was"
   (`SftpClient.kt:230-234`, `SftpSource.kt:194-198`). Owner: the maintainer for I15; adjudicator
   for the one look.
7. **L1** to **L6** as listed.

## Verdict on R2 after T11 to T16

R2's five fixed findings still hold and nothing since has weakened them: paths are escaped once in
the transport; `REPLACE` clears only a file, only without the extension, and passes a refusal on
as given with the extension; `NoSuchFile` names the path that is missing and `RenameTries` reads
that name; the borrowed session's loan ends after its last call under `NonCancellable`; the
"cleared" wording survives a retry because retries are sent only after wire failures and the
second refusal is answered, not retried. R2's four beliefs remain true, and the two seams R2 left
for T11 - `REFUSE` refused by its own success, and a cancelled `withLease` proving nothing - are
closed by "decided once" and "reached the server", both verified here against every try ordering.
What R2 could not examine, because T7 had no retry, is the discriminator the retry rests on, and
that is where the one High of this lens sits: R2's I11 guidance said "a file of the expected size
there is the landed rename", written for a `REFUSE` target that was free a moment ago, and T11
applied it to `REPLACE`, whose target is expected to be occupied. R2's residual-loss row (a
non-extension server clearing a healthy target) stands unchanged. The other High is not in R2's
territory at all: the breaker's inherited slow-call rule was never configured and never tested,
because every breaker test drives it with failures.

Counts: Critical 0, High 2, Medium 4, Low 6.
