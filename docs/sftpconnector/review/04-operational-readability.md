# T17 lens 4: operational readability

Reviewer: a fresh Fable 5.1 subagent with one lens, reading every log line, exception message and
meter as an on-call engineer meets it cold. Adjudication (what was fixed, what was recorded) is
in the "Adjudication" section at the end; the report above it is the reviewer's, unedited except
for headings.

Scope read in full: spec 5.4/9/10/11/12/13, the brief, progress.md (open seams, T4, T5 deviations,
T9, T14), and every main source file in `core`, `quarkus`, `testkit`. The testkit has no log calls
at all, so nothing to report there.

What is good, so the findings are read in proportion: exception messages are rendered in exactly
one place (`Attempt.describe`), every `SftpException` raised at runtime carries
`endpoint=host:port, op=, path=, attempt=`; the pool-exhaustion three-way reading and every
StartupProbe remedy are genuinely actionable; no runtime message or log line cites a spec section
or ticket; every meter name and tag value matches spec 13 exactly; every meter carries `endpoint`;
`sftp_breaker_state` maps 0/1/2 correctly including `DISABLED`/`METRICS_ONLY` to 0 and
`FORCED_OPEN` to 2; leaks are reported once per borrow with a stack; no gauge goes absent after
`close()`.

## Findings, by severity

### HIGH

**H1. A transient wire failure during the start-up probe is reported as a configuration fault
with a path-spelling remedy.** `StartupProbe.kt:362-367` (`checking` catches every
`SftpException`) with `:232-236`. The probe runs on one session through `resilience.once` (no
retry). A `SessionLost` during `realpath` renders as: *connector "vendor-drop" cannot start:
resolving the watched directory inbound/ failed. The path is sent to the server as it is written
here, so a leading slash makes it absolute ... The server's answer was: the connection broke under
the request: Connection reset (endpoint=..., op=realpath, path=inbound/, attempt=1)*. The
operator is told to fix the spelling of a path that is fine, the class is `ConfigurationError`
(Fatal), and in Quarkus the deployment refuses to boot. Same shape for `PoolExhausted`,
`PermissionDenied` and `ServerFailure` under every `checking` block. Only `NoSuchFile` and the
generic status genuinely mean "configuration". Test: a probe over a fake whose `realpath` throws
`SessionLost`; assert the thrown type is `SessionLost`, or a `ConfigurationError` whose message
does not say "leading slash" and does say the connection broke.

**H2. A consumer's exception inside `consume` is swallowed, and the only record of it has no
stack trace.** `SftpSource.kt:380-385` (`nack`), reached from `consume` at `:178-183`. Rendered:
*The consumer could not process inbound/a.csv and it will be handed over again on a later poll:
java.lang.NullPointerException*. `reason.toString()` is passed as a formatted argument, not as the
trailing `Throwable`, so slf4j prints no stack. `consume` never rethrows it. With `redeliver =
true` the same line repeats every tick without the stack. Test: capture the logger, run `consume`
with a block that throws; assert the captured event's throwable is that exception.

**H3. Circuit-breaker state transitions are never logged.** `Resilience.kt:434-448`: no
`eventPublisher.onStateTransition` listener. When the breaker opens, the only traces are the gauge
and, if a watch is running, an INFO per skipped tick. A caller using `SftpClient` directly gets
`CircuitOpen` and nothing in the log. The moment it opened, the rate that opened it, half-open and
closed again are absent. Test: drive `slidingWindow` counted failures; assert one WARN containing
the endpoint and "open".

### MEDIUM

**M1. `HostKeyRejected` shows no fingerprint, key type or known-hosts path.**
`JschErrorMapper.kt:72-78`. Verified against mwiede JSch 2.28.7: the message is `reject HostKey:
<host>` or `HostKey has been changed: <host>`; the fingerprint appears only in JSch's interactive
prompt, never in the exception. Nothing says which key was offered or which file was consulted;
the "changed" case (possible MITM) is not distinguished in the connector's own words.

**M2. `AuthenticationFailed` names neither the user nor the method the connector used.**
`JschErrorMapper.kt:89-90`. The quoted `'password,keyboard-interactive,publickey'` is what the
server offers; the connector only ever sends `password`. The user name is not in the message.

**M3. A `ConnectFailed` through the HTTP CONNECT proxy never names the proxy.**
`JschErrorMapper.kt:79-81` and `:107-108`. `endpoint=` is the target; the address that refused
(`proxy.internal:3128`) appears nowhere. The operator pings the wrong host.

**M4. `Attempt` carries a number but no budget.** `SftpException.kt:18-35`. `attempt=1` after a
`NoSuchFile` (never retried) and `attempt=3` after a `SessionLost` (exhausted) look alike to a
reader who does not know `maxAttempts`. The retry WARN says "tried again in PT2S" but not "2 of 3".

**M5. The watch's "failure the connector has no name for" is logged at ERROR without a stack
trace.** `SftpSource.kt:250`, `:263`, `:208`: `failed.toString()` as an argument, not a trailing
throwable.

**M6. Pool, ladder and lease lines carry no endpoint; `PoolEntry.toString()` is `session #7
(InUse)`.** `SftpPool.kt:213, 349, 388, 405-413, 452, 529-533`; `CancellationLadder.kt:241-247`.
A host with two connectors cannot tell whose session #7 it is. The `Unknown` WARN and the cut WARN
do carry the endpoint, so the layer is inconsistent with itself.

**M7. Every source-layer line is keyed by directory only, never by endpoint or connector name.**
`SftpSource.kt:157, 194, 208, 223, 255, 259, 263, 361, 380, 416`; `SftpClient.kt:251`;
`Compensation.kt:66`. Two connectors watching `inbound/` on two servers are indistinguishable by
grep. Also `"A {} of {}"` renders "A ack of".

**M8. A shutdown cannot be reconstructed from the log.** `SftpPool.kt:301-310`. Shipped:
*Connector "x" is closing.* then silence up to 30 s, optionally *3 still out when the pool ... had
to close*, silence up to 5 s, *The pool ... is closed.* Missing: how many leases were out when the
drain began, whether it timed out or settled early, how many came back during the grace, how many
sessions were hung up on.

**M9. `PollSkipped(OVERLAP)` is WARN once per interval for as long as a consumer is slow.**
`SftpSource.kt:223`. Under the default `SKIP` with a consumer slower than the interval this fires
every interval; the breaker skip beside it is INFO.

**M10. The lease double-release WARN describes a bug but gives no way to find it.**
`SftpPool.kt:529-533`: no stack attached, so "the code that did it" is unidentifiable.

**M11. Lazily registered meters, and where absent-vs-zero is (not) documented.** Registered on
first use: `sftp_pool_evicted_total{reason}` (documented, T5 deviation 1), `sftp_pool_leak_total`
(recorded as debt; the KDoc an operator finds does not carry the warning), `sftp_retry_total{op}`
(`Resilience.kt:492`, inline, no KDoc, not recorded anywhere), `sftp_error_unmapped_total`
(`JschErrorMapper.kt:162-165`, lazy; spec 13 says "any non-zero value is a table entry to add",
and an alert written as `> 0` never fires on an absent series), `sftp_op_seconds{op,result}`
(`ClientMeters.kt:406-408`), `sftp_poll_seconds{result}` (`SourceMeters.kt:489-490`). Registered
eagerly for all values: `sftp_poll_files{state}`, `sftp_ack_total{outcome}`, `sftp_inflight`,
`sftp_breaker_state`, the three pool gauges, `sftp_pool_acquire_seconds`,
`sftp_pool_acquire_timeout_total`, `sftp_pool_created_total`. No gauge goes absent after
`close()`; `sftp_inflight` may read above zero (recorded seam).

**M12. Quarkus: a refused start surfaces only through CDI's exception chain, and nothing is
logged about what was configured.** (Plausible, not run.) `SftpConnectorLifecycle.kt:41` and
`:61-63`. Nothing at boot logs the effective host, proxy, host-key policy, watched directories or
staging dir; the core's start line gives host and a directory count only.

**M13. No line says a watch started, and no line at any level says a tick completed.**
`SftpSource.kt:140-163, 218-230, 275-318`. The only per-tick lines are for failure or skip. The
operator asking "is it polling at all?" has the `sftp_poll_seconds` timer and nothing else.

### LOW

- **L1.** Java `Duration` ISO rendering (`PT1M`, `PT2.371S`) in `Resilience.kt:541` and `:488`,
  where everything else reads `30s`, `10m`.
- **L2.** `ConnectorDsl.kt:73` says the name "tags every metric and log line it produces"; no
  metric is tagged by name and most log lines do not carry it.
- **L3.** The `Unknown` WARN (`JschErrorMapper.kt:153-161`) omits path, attempt and the stack.
- **L4.** `JschTransport.kt:281` orphan hang-up WARN has no endpoint.
- **L5.** Readiness `ConfigurationError`s (`Readiness.kt:265, 266, 304, 322`) carry no connector
  name.
- **L6.** Server-supplied names go into messages unescaped (`SftpClient.kt:180`, every
  `failure.message` from JSch). A listed name containing a newline forges a log line.
- **L7.** Retry at WARN per try (`Resilience.kt:488`); `sftp_retry_total` already counts them.
- **L8.** Validation evictions are DEBUG only (`SftpPool.kt:213`).
- **L9.** Fatal watch end at ERROR without a stack (`SftpSource.kt:263`).
- **L10.** Citations: none found in any runtime message.

## Answers to the lens questions not already covered

- Consistency: exceptions have one renderer, greppable on `endpoint=`. Logs are free prose; the
  endpoint is spelled three ways or absent. The connector name appears only in `SftpConnector`
  and the DSL/probe.
- `sftp_breaker_state` mapping correct (`Resilience.kt:560-564`). Names and tags: all exact.
- `OverwriteRefused`, `PoolExhausted` (three-way reading, `closing` variant), `IncompleteTransfer`
  (received vs listed), `UnsafeFileName`, the six-fault `ConfigurationError`, every probe refusal
  (path, check, remedy): good. H1 is the exception.
- Leak: once per borrow, with stack. Second watch refused: good message, not logged.

## Appendix: inventory of every log call and exception construction site

Rendered text with placeholders filled by example.

**error/SftpException.kt** 28-35 render `<detail> (endpoint=sftp.example:22, op=download,
path=inbound/a.csv, attempt=2)`; 153 Unknown; 231 PoolExhausted closing; 249 PoolExhausted no
stats; 260-262 PoolExhausted three-way; 298 CircuitOpen.

**transport/jsch/JschErrorMapper.kt** 75 HostKeyRejected; 80 ConnectFailed (proxy); 90
AuthenticationFailed; 96 SessionLost (`session is down`); 101 ConnectFailed (no SFTP channel);
108 ConnectFailed (`java.net.`); 114 ConnectFailed (closed before handshake); 126 SessionLost
(IOException cause); 131 NoSuchFile; 134 PermissionDenied; 136-141 ServerFailure; 153-161 WARN
unmapped (no throwable).

**transport/jsch/JschTransport.kt** 281 WARN orphan hang-up failed; 449 WARN cut failed.

**pool/SftpPool.kt** 156, 207 IllegalStateException (lent without a connection / parked without
a session); 189, 252 PoolExhausted; 213 DEBUG validation replaced; 309 INFO pool closed; 320-325
WARN N still out at close; 349 WARN housekeeping round failed (+stack); 388-393 WARN spare failed;
405-413 WARN leak (+stack); 452 WARN hang-up failed; 529-533 WARN given back twice.

**pool/CancellationLadder.kt** 241-247 WARN cut after grace.

**client/SftpClient.kt** 178-181 UnsafeFileName; 251 INFO delete landed. **client/Compensation.kt**
66 INFO rename landed; 101-107 ServerFailure cleared; 127-131 NoSuchFile naming target; 147 DEBUG
already gone; 167-170 OverwriteRefused. **client/StagingArea.kt** 230-234 IncompleteTransfer.
**client/BorrowedSession.kt** 331-334 IllegalStateException kept past the block.

**resilience/Resilience.kt** 488 WARN retry; 521 CircuitOpen; 539-543 OperationTimeout.

**SftpConnector.kt** 83, 86 INFO closing/closed; 141-147 INFO up.

**StartupProbe.kt** 232-315 ConfigurationError per check (trying, remedy, server's answer); 354
DEBUG probe file did not need clearing.

**source/SftpSource.kt** 142 IllegalArgumentException interval; 144 IllegalStateException closed;
145-147 IllegalStateException already watched; 157 INFO stopped by connector; 194-198 WARN answer
could not be carried out; 208 WARN failed as collector left; 223 WARN skipped overlap; 243-247
IllegalStateException cancelled by something inside; 250 ERROR no name (no stack); 255 WARN tick
failed; 259 INFO skipped breaker; 263 ERROR ending fatal (no stack); 270-272
IllegalArgumentException not configured; 296 DEBUG not ready; 361 INFO gone; 380-385 WARN nack
(no stack); 416-421 WARN answered twice.

**source/Readiness.kt** 265, 266, 304, 322 ConfigurationError. **config/ConnectorDsl.kt** 226
ConfigurationError aggregate; 239-246 WARN AcceptAll. **quarkus/SftpConnectorLifecycle.kt** 83-88
WARN close overran.

## Adjudication

Ticket 17, lens 4. Every finding above is answered here as **Fixed** (with the commit and the test
that holds it), **Seam** (with an owner), or **Rejected** (with why). The report above this line is
the reviewer's and is unedited.

Four commits:

| | |
|---|---|
| `73782e3` | H1: a wire failure during the probe is not a configuration fault |
| `f5c64a7` | H2: a consumer's exception in `consume` is logged with its stack |
| `fcb7421` | H3: circuit-breaker transitions are logged |
| this one | the Mediums and Lows below that are message or log-line changes |

### High

**H1 - Fixed** (`73782e3`). `StartupProbe.checking` now converts only the failures the server
*answered* - `NoSuchFile`, `PermissionDenied`, `ServerFailure` - into a `ConfigurationError` with
that check's remedy. That is spec 10.2's own line, and it is read off
`Disposition.RETRY_ON_THE_NEXT_TICK` rather than off a list of classes, so a class added later is
sorted by the answer it already gives. `ServerFailure` stays converted deliberately, against the
report's second sentence and with its first: the featureless status is exactly what a
cross-filesystem rename is refused with, and catching that is the reason `proveTheMoveInto`
exists. Everything else - `SessionLost`, `ConnectFailed`, `OperationTimeout`, `PoolExhausted`,
`Unknown`, and the two `Fatal`s - propagates as itself.

*Which of spec 10.2 and 11.1 wins, and why:* both, and they do not conflict. 11.1's "a failed probe
is fatal at startup" is about the *start-up*, and the start-up still refuses - the exception leaves
`SftpConnector.start` unchanged, and the pool the checks borrowed from is closed on the way out.
10.2 is about the *class*, and it says a lost session is `Recoverable`; re-labelling one `Fatal`
because of where it happened would tell an operator that nothing will ever make the next attempt go
differently, when the truth is that the next attempt may well work. So the connector does not start
and the failure says the connection broke.

Test: `SftpConnectorTest.a session lost during the probe is reported as itself, not as a path to
respell` - the fake transport with `realpath` throwing `SessionLost`; asserts the thrown type, that
the message says the connection broke, that it does not mention a leading slash, and that no
session is left open.

**H2 - Fixed** (`f5c64a7`), logging only. T12's progress entry and spec 7.2 both say `consume`
catches the block's exception and nacks it: "a throwable escaping the collect block now ends the
poll and withdraws every unanswered file as `cancelled` - correct for a consumer that has died,
wrong for one file that failed to parse." That is deliberate and is unchanged. What was wrong is
that the only record of the swallowed exception had no stack: `reason.toString()` went in as a
formatted argument. The throwable itself is now the trailing argument and the third placeholder is
gone. Test: `SftpWatchTest.the consumer's exception is logged with its stack when consume nacks`,
which asserts a frame of the throwing block appears in the captured output.

**H3 - Fixed** (`fcb7421`). One WARN per `onStateTransition`, naming the endpoint and both states in
spec 9's words ("half-open", not the library's `HALF_OPEN`), with the consequence sentence appended
only for the states that actually refuse calls. Test:
`BreakerTransitionsTest.the breaker names the endpoint and the states it moved between when it
opens`.

### Medium

**M1 - Fixed, in the half that exists.** `JschErrorMapper` is now given the configuration, and
`HostKeyRejected` says which of the three refusals it was - the fork has
`JSchUnknownHostKeyException`, `JSchChangedHostKeyException` and `JSchRevokedHostKeyException`, so
the man-in-the-middle case is distinguished by type rather than by wording - and names the
known-hosts file the key was compared against. The fingerprint is *not* added and the message says
so out loud: the reviewer's finding is right that JSch carries the offered key only into its
interactive prompt, and inventing one would be worse than saying there is none. Assertions added to
`JschErrorMappingTest.a host key the connector was not told to expect is fatal`.

**M2 - Fixed.** `AuthenticationFailed` names the account and states that the connector offers a
password and nothing else, so the methods JSch quotes are readable as the server's offer rather
than as what was tried. The secret is not in the message and `AuthMethod.Password` is still not a
data class. Assertions in `JschErrorMapperTest.a rejected credential names the account the
connector offered` and in `JschErrorMappingTest`'s S10.

**M3 - Fixed.** Both `ConnectFailed` sites that can be reached through a tunnel - the
`JSchProxyException` one and the `java.net.` one - now name the proxy the connector actually dials.
Assertions in `JschErrorMapperTest.a proxy that will not open a tunnel is a failure to connect, and
names the proxy` and in `JschErrorMappingTest.a proxy with nothing behind it is a failure to
connect`.

**M4 - Fixed.** `Attempt` carries an optional `budget`, and `describe` renders `attempt=2 of 3`.
`Resilience.attempting` is the only thing that sets it, because it is the only thing that decides
to try again; `once`, the probe and a direct call render `attempt=1` as before, which is honest -
nothing is retrying them. The budget travels to the transport and the pool through `CurrentAttempt`
like the number does. The retry WARN now reads "failed on try 2 of 3". Assertion in
`RetrySemanticsTest.a failure names which try it was...`.

**M5, L9 - Fixed.** Every place in `SftpSource` that logged `failed.toString()` at WARN or ERROR
now passes the throwable: the collector-left handler, the "no name for" ERROR and the fatal
watch-end ERROR. The two lines that report a connector failure the reader is meant to act on rather
than debug (`Tick n failed`, `The answer for ... could not be carried out`) keep the rendered
message, which already carries endpoint, op, path and attempt.

**M6 - Fixed at the root.** `PoolEntry.toString()` is now `session #7 to sftp.example:22 (InUse)`,
so every pool, ladder and lease line that names an entry names the server without any of them being
edited. `PoolEntry` and `SessionRegistry` take the endpoint for this and nothing else. Assertion in
`HousekeeperTest`'s leak test.

**M7 - Fixed.** `SftpSource` renders every path through one helper, `at(path)`, which appends the
endpoint, so a line about `/drop` says which server's `/drop`. `"A {} of {}"` is now `"The {} of
{}"`. Assertion in the H2 test.

**M8 - Fixed.** `SftpPool.close` counts the leases out before the drain starts, remembers whether
the drain settled or ran out, and the closing INFO carries both plus how many sessions were hung up
on. Assertion in `PoolShutdownTest`'s I9.

**M9 - Seam.** Rate-limiting the `PollSkipped(OVERLAP)` WARN is a behaviour change and needs a
policy (log once per run of skips? every nth? a configurable level?), which is a knob and not a
message. Owner: whoever next owns `OverlapPolicy` - the same person as the already-recorded seam
"the ticker's own `send` under `SKIP` delays it". Note for them: the line is now at least
greppable per endpoint, so a rate limit can be added without changing what it says.

**M10 - Fixed.** The double-hand-back WARN carries an `IllegalStateException` as its trailing
argument - constructed, never thrown - so the stack names the code that did it. Assertion in
`SftpPoolTest.a lease given back twice is ignored the second time`.

**M11 - Fixed by documentation.** Spec 13 gains "Absent is not zero": which meters are registered
lazily, which are registered eagerly for every label value, and how to write an alert on a lazy one
(`absent(x) or x > 0`, or a rate). No code change: registering every counter eagerly would mean
enumerating every `op` and every `reason` at start-up, which is a behaviour change and a worse one,
since a label value nobody has ever produced would then read as a real zero.

**M12 - Seam.** A Quarkus boot line describing the effective host, proxy, host-key policy, watched
directories and staging dir is a new log line in the Quarkus module about configuration this lens
did not read, and the finding is marked "plausible, not run". Owner: whoever owns
`SftpConnectorLifecycle` (the Quarkus adapter ticket). The core's own start line already gives host
and a directory count, and a refused start now propagates the honest class (H1), which is half of
what M12 asks for.

**M13 - Fixed.** `watch` logs at INFO when a watch claims its directory, with the interval; each
tick logs at DEBUG what it listed, handed over and found not ready when it finishes. Assertion on
the start line in the H2 test.

### Low

**L1 - Fixed.** The retry WARN and the `OperationTimeout` message convert the library's
`java.time.Duration` to a Kotlin one, so they read `2s` and `30s` rather than `PT2S` and `PT30S`,
like everything else.

**L2 - Fixed.** The DSL no longer claims the connector's name "tags every metric and log line it
produces", which is false: meters are tagged by endpoint (spec 13) and most lines name a path and a
server. It now says what the name is actually for - the start-up, shutdown and probe messages.

**L3 - Fixed.** The unmapped-failure WARN carries the path, the attempt number and the throwable.

**L4 - Fixed.** The orphan hang-up WARN in `JschTransport` names the endpoint.

**L5 - Rejected, and recorded as a seam.** `SizeStable`, `MinAge` and `MarkerFile` throw from their
own constructors, which run before any connector exists and can run with no connector at all - a
readiness check is a public class a host may build on its own. There is no name to carry. Making
the DSL catch and re-wrap them so the aggregate at `ConnectorDsl` names the connector is a
behaviour change to configuration validation. Owner: whoever next touches the configuration
aggregate.

**L6 - Seam, and lens 3's.** Escaping server-supplied names before they reach a log format is a
behaviour change on every listing, and it is a security finding (log forging) rather than a
readability one. Owner: the security lens's adjudicator; if that lens did not raise it, whoever
owns `SftpClient`'s listing path.

**L7 - Rejected.** The retry WARN stays at WARN. A retry is a failure that was survived, and the
counter answers "how often" while the line answers "which operation, against which server, on which
try, with what wait" - which M4 and L1 have just made it do properly. A pipeline retrying enough
for the line to be noise has something worth the noise.

**L8 - Seam.** Raising validation evictions above DEBUG is a level change with a volume cost on a
pool that replaces idle sessions routinely; `sftp_pool_evicted_total{reason=validation}` already
counts them. Owner: whoever tunes the connector's log volume against a real deployment.

**L10 - No action.** The reviewer found no citations in any runtime message, which is the intended
state.
