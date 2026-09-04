# T17 lens 6: spec conformance

Reviewer: a fresh Opus 5 subagent with one lens, reading the module against `spec.md` and the
deviations recorded in `progress.md`. Method is the `mattpocock-skills:code-review` spec axis -
requirements missing or partial, behaviour nobody asked for, requirements implemented wrongly -
applied to the whole module rather than to a diff, because the ticket asks for a conformance
sweep and not a branch review. No sub-agents, no git, no build.

Scope read in full: `spec.md` sections 1 to 17; `progress.md` in full, with the open-seams table
and every entry's Deviations section; every main source file in `core`, `quarkus` and `testkit`;
the test tree by name, with `I<n>_` and `S<n>` ids grepped out of it.

**What conforms, so the findings are read in proportion.** Every one of the 42 decisions in the
log has code behind it or an honest deferral - there is no D-number that was written down and
then forgotten. All thirteen metric names in spec 13 exist, with the tag sets spec 13 fixes and
`endpoint` on every one; the five `sftp_pool_evicted_total` reasons and the four
`sftp_op_seconds` results are closed enums, not strings written at each site. Every configuration
key in spec 12's example exists in the DSL and has a Quarkus property; all six of spec 12's
validation rules are enforced, and the Quarkus mapping restates none of the builder's defaults.
All twelve scenarios S1 to S12 have a named test collected into one suite; all six partition rows
P1 to P6 have one; fourteen of the fifteen invariants have an `I<n>_` test. The layering spec 3.1
draws is enforced by ArchUnit rather than by review. The resilience order of spec 9 and D15 is
built exactly as written, with D42's substitution recorded. The three cancellation tiers, the
glob escaping of D37, the rename compensation of D38 and D40, the look-once policy of D29, and
the `IncompleteTransfer`/`OverwriteRefused` classes of D28 and D30 are all present and match
their decision entries word for word.

The findings below are almost entirely **documentation drift**: the code is ahead of the spec in
several places and the open-seams table is behind the code in two. One spec sentence has no code
behind it at all.

---

## Table (a): every D-number against the code

| ID | Implemented at | Verdict |
|---|---|---|
| D1 | `pool/SessionRegistry.kt:39-48` (one `Mutex`, flat `entries`, idle `ArrayDeque`), `pool/PoolEntry.kt:8-42` | Built |
| D2 | `transport/SftpTransport.kt:1-171`; `transport/jsch/JschTransport.kt`; `testkit/FakeSftpTransport.kt` | Built |
| D3 | `core/src/test/kotlin/sftp/connector/ArchitectureTest.kt:35-60`; slf4j throughout core | Built, build-enforced |
| D4 | `transport/jsch/JschTransport.kt:60` `Dispatchers.IO.limitedParallelism(config.pool.maxSize)` | Built |
| D5 | `pool/SessionRegistry.kt:109-115` (bypass window), `pool/SftpPool.kt:206-219` (`realpath(".")`, replace on failure) | Built |
| D6 | `pool/SftpPool.kt:403-414` - reports and never forces | Built |
| D7 | `core/pom.xml:24-29` `com.github.mwiede:jsch` | Built |
| D8 | `config/SftpConnectorConfig.kt:54-63`; DSL refuses an unset policy (`ConnectorDsl.kt:95-99`) and warns on `AcceptAll` (`:238-247`) | **Partial.** `Fingerprint(sha256)` is not built; honestly open on the seams table with an owner |
| D9 | `pool/CancellationLadder.kt:1-97`; `JschTransport.kt:286-293` (monitor), `:263-270` (`abort`) | Built |
| D10 | `resilience/Resilience.kt:118-147` (fresh lease per try) with `client/Compensation.kt` | Built |
| D11 | `client/StagingArea.kt:419-457` | Built |
| D12 | `source/SftpSource.kt:107-110`, `:143-163` - `flow` / `channelFlow`, never `SharedFlow` | Built |
| D13 | `source/InFlightSet.kt:24-95`; `SftpEvent.FileSeen.ack/nack` | Built |
| D14 | `source/InFlightSet.kt:7-23` - the only state kept | **Partial.** The `SeenRepository` SPI spec 8.3 promises alongside it does not exist. See S2 |
| D15 | `resilience/Resilience.kt:134-146` - retry, breaker, transfer limit, clock, lease | Built |
| D16 | `pool/SftpPool.kt:301-310`, `:317-327`; `client/StagingArea.kt:450-456` | Built |
| D17 | `source/SftpEvent.kt:440-491` - metadata only | Built |
| D18 | `config/SftpConnectorConfig.kt:196-200`; `source/SftpSource.kt:325-341` | Built |
| D19 | `StartupProbe.kt:140-167`; `polling.startupProbe` knob | Built |
| D20 | `error/Disposition.kt:37` `RETRY_ON_THE_NEXT_TICK` | Built |
| D21 | `config/ConnectorDsl.kt:344` (`maxSize = 5`), `:301` (`DEFAULT_CONCURRENT_TRANSFERS = 4`) | Built |
| D22 | `client/StagingArea.kt:466-490` (`Tally`), `LocalFile.digest` | Built |
| D23 | `transport/jsch/JschErrorMapper.kt:151-167` | Built |
| D24 | - | **No such decision.** The log skips it; see S13 |
| D25 | - | **No such decision.** The log skips it; see S13 |
| D26 | `JschTransport.kt:150-151`; no `socketTimeout` anywhere | Built |
| D27 | `error/SftpException.kt:124-125` `poisons = false` | Built |
| D28 | `error/SftpException.kt:140-141`; raised at `StagingArea.kt:438-444` | Built |
| D29 | `client/SftpClient.kt:205-210` (upload), `client/Compensation.kt:46-49` (rename) | Built |
| D30 | `error/SftpException.kt:277-280`; `Disposition.ACCEPT_THE_REFUSAL` | Built |
| D31 | `JschTransport.kt:107-114` `UNANSWERED_KEEPALIVES = 1`; no `socketTimeout` in the DSL | Built |
| D32 | `StartupProbe.kt:78-91` - `realpath` then `insistOnADirectory` | Built |
| D33 | `client/Overwrite.kt:1-37`; `PostAction.Move(target, overwrite: Overwrite)`; `targetUnder` resolves in one place | Built |
| D34 | `testkit/.../pressure/AdversaryTest.kt`, `InFlightSetLincheckTest.kt`, `SessionRegistryLincheckTest.kt`, `SoakTest.kt` | Built |
| D35 | Measurement tables in the T15 and T16 progress entries; `@Tag("measure")` / `@Tag("soak")` | Built |
| D36 | `source/Readiness.kt:65-100` - batched, inside one poll, one wait per poll | Built |
| D37 | `JschTransport.kt:306-308` `literally()`, applied at `:176, :189, :193, :196, :226, :233`; not at `:237` (mkdir) or `:166` (realpath) | Built, exactly as specified |
| D38 | `client/Compensation.kt:122-133` `renameNamingWhatIsMissing` | Built |
| D39 | `client/BorrowedSession.kt:1-70`; `SftpClient.kt:292-303` under `NonCancellable` | Built |
| D40 | `client/Compensation.kt:88-110` - `renameReplaces`, directory and null all pass the refusal on | Built |
| D41 | `error/Disposition.kt:37`; `Resilience.kt:197-201` reads the disposition and keeps no class list | Built |
| D42 | `resilience/Resilience.kt:94`, `:169-170` - kotlinx `Semaphore`, permit taken before the dispatcher switch | Built |
| D43 | `pool/SftpPool.kt:301-310` `withContext(NonCancellable)`, drain then one grace, `cutEverythingHeld` cuts all at once | Built |
| D44 | `testkit/.../partition/HalfOpenPartitionTest.kt:34`; spec 17.3 carries the grace | Built |

**Count: 42 decisions, 40 fully built, 2 partial (D8, D14), 0 forgotten. D24 and D25 do not
exist.**

---

## Table (b): the open-seams table, row by row

Thirty-one rows. Fifteen are struck through and genuinely closed. The interesting column is the
last one.

| Row (abridged) | Recorded state | Actual state |
|---|---|---|
| `housekeep()` has no production caller | Closed by T9 | Correct - `SftpConnector.kt:140` |
| Refused start-up leaves sessions open | Closed by T13 | Correct - `SftpConnector.kt:127-138` |
| `PostAction.Delete` / `Move.overwrite` have no consumer | Closed by T10 | Correct - `SftpSource.kt:406-413` |
| `NoSuchFile` from `download` converted outside the client | Closed by T11 | Correct |
| **`SizeStable` observes across polls** | **Open, owner "the maintainer"** | **STALE. Ruled on by C11, applied before T11, and `Readiness.kt:76-96` batches inside one poll. See S1** |
| `FileGone` is an event of the live poll only | Open, owner "whoever builds a concurrent-download consumer helper" | Correct - `SftpSource.kt:354-366` |
| Readiness constructor faults are not aggregated | Open, owner "whoever next touches DSL validation" | Correct - `Readiness.kt:67-70, 106-108, 124-126` throw at construction |
| `socketTimeout` is dead configuration | Closed by T8 | Correct - the knob is gone |
| `Lease.connection` hands out a full `SftpConnection` | Closed by T13 | Correct - `SftpPool.kt:483` is `SftpSession` |
| A cut session counts as `reason=poisoned` | Open, owner "whoever revisits the five labels" | Correct - `PoolEntry.kt:130-132` |
| Path traversal in `download`'s default target | Closed by the pre-T11 hotfix | Correct - `SftpClient.kt:169-182` |
| `HostKeyPolicy.Fingerprint` unimplemented | Open, owner "the first ticket needing pinning" | Correct - two policies ship, the `when` is exhaustive |
| `sftp_pool_leak_total` registers on first use | Open, owner "the ticket that revisits T4's meters assertion" | Correct - `PoolMeters.kt:81-83` |
| `Attempt.number` always 1 | Closed by T11 | Correct - `CurrentAttempt`, `Attempt.inside` |
| `Retirement.SHUTDOWN` has no producer | Closed by T13 | Correct - `SessionRegistry.kt:162-164` |
| `OperationTimeout` has no producer | Closed by T11 | Correct - `Resilience.kt:183-187` |
| `MutableStateFlow.value` under the registry lock | Open, owner "any ticket that collects entry/lease state" | Correct and still theoretical - `PoolEntry.kt:146` is called under the lock; nothing collects |
| Byte-count mismatch raised `SessionLost` | Closed by C7/T7 | Correct |
| A self-decided refusal raised `ServerFailure` | Closed by C8/T8 | Correct |
| **`writeFrom` and the `SftpSession`/`SftpConnection` split are not in spec 5.1 or 6.1** | **Open, owner "the maintainer"** | **STALE. Spec 5.1 now carries both paragraphs and spec 6.1's table declares `withSession(block: suspend SftpSession.() -> T)`. See S5** |
| A borrower can call `abort()` | Closed by T13 | Correct |
| Only a call inside `withLease` is on the ladder | Open, owner "every later ticket" | Correct - `Resilience.once` also routes through `pool.withLease`, so both probe and client paths are covered |
| **A cut session is counted `reason=poisoned`** | Open | **Duplicate of an earlier row, near-verbatim. See S12** |
| The IO dispatcher is as wide as the pool | Open, standing rule | Correct - `JschTransport.kt:60` |
| `withContext(dispatcher)` drops a resource on cancellation | Open, standing rule | Correct - `JschTransport.kt:75-105` holds and closes |
| A cancelled `withLease` is not proof it did not land | Closed by T11 | Correct |
| A retry under `REFUSE` refused by its own success | Closed by T11 | Correct - `Compensation.kt:41, 46-49` |
| `REPLACE` clears a target for a non-target reason | Open, owner "the maintainer" | Correct - `Compensation.kt:97-108` names the cleared target |
| A local I/O failure inside a transfer reads as `SessionLost` | Open, owner "whoever next touches the mapper" | Correct - `JschErrorMapper.kt:124-127` |
| A local failure inside a lease evicts a healthy session | Open, owner "whoever has cause" | Correct - `StagingArea` opens the file inside `resilience.attempting` |
| `OperationTimeout` says `EVICTED` after the fate was decided | Open, owner "whoever next revisits `Disposition`" | Correct - `CancellationLadder.kt:88-92` decides first |
| `ServerFailure` counts against the breaker | Closed by C13/T12 | Correct |
| `consume` re-runs its block for a file whose ack keeps being refused | Open, owner "whoever first sees the WARN repeat" | Correct - `SftpSource.kt:189-200` |
| A watch on a stopped connector ends normally | Closed by T13 | Correct - `SftpSource.kt:144` |
| A skipped tick delays the ticker under `SKIP` | Open, owner "whoever measures it" | Correct - `SftpSource.kt:224` |
| Under `REFUSE`, a retry's window is the backoff | Open, owner "whoever has cause" | Correct |
| A file held from a completed tick stays in flight across `close()` | Open, owner "whoever builds `ackWait`, or nobody" | Correct |
| A failing assertion before `close()` hangs a `runTest` | Open, owner "whoever next writes one" | Correct |
| The breaker and `sftp_breaker_state` are per `SftpClient` | Ruled on by T14 | Correct |
| Two connectors on one registry read each other's gauges | Open, owner "whoever first hosts two" | Correct - `PoolMeters.kt:151-155`, `Resilience.kt:100` identify by endpoint alone |
| `lifetime()` draws from an unseeded `Random` | Open, owner "whoever needs a replayable run" | Correct - `SessionRegistry.kt:318-322` |
| `HostKeyPolicy.Strict` does not check its known-hosts file (T14 sub-table) | Open, owner "whoever next touches DSL validation" | Correct |

**Nothing on the table is forgotten. Two rows are stale (S1, S5) and one is a duplicate (S12).
The gap is on the other side: three things deferred in a T10 deviation and three raised by T16
never reached the table at all (S2, S8, S9).**

---

## Table (c): spec Sec 16, the open items

| Item | Recorded state | What actually closes it |
|---|---|---|
| 1. Producer-side completeness convention | Open in the spec; T15 analysed it and could not close it | **Still open, and correctly so - no code can close it.** T15's entry is the best artefact on it: what the shipped default protects against, what it does not (an uploader paused over a minute mid-file, a burst writer, a server clock behind ours), and the exact question for the upstream team. `markerFile(suffix)` ships (`Readiness.kt:122-133`) as the deterministic answer once they reply. **What is missing:** spec 16 item 1 still reads as an unanswered question, with none of T15's analysis in it, and no seams row carries it. **Proposed decision entry:** *"D45: the shipped readiness default is a heuristic with a stated blind spot - an uploader paused for longer than `minAge` mid-file passes it. Closing item 1 needs the upstream team's convention, and `markerFile` is where the answer lands. Until then the blind spot is documented in spec 7.5 and the T15 entry, not designed around."* |
| 2. Temp folder ownership | Struck as open; T15 records it closed by T9's code | **Closed, correctly.** `StartupProbe.kt:108-128` handles both ownership models: `createActionTargets = true` runs `mkdir -p` and refuses with the "set it false" remedy; `false` insists the folder is there and refuses with "createActionTargets is off". Tests: `StartupAgainstServerTest` (three cases). **What is missing:** spec 16 item 2 is not struck through the way item 3 is. One-line edit. |
| 3. JSch error wording | Struck through, closed by T2 | **Closed, and the closure text is itself now partly wrong.** T2's nine-row measured table stands and every row has an embedded-server test; T15 added a tenth wording and T16 mapped it. But the closure note says the mapper matches host key and proxy failures by type, which spec 5.4 was never updated to reflect. See S3. |

---

## Spec 12: configuration keys, DSL, Quarkus properties

Every key in spec 12's example maps three ways. Nothing exists on one side only.

| Spec 12 key | DSL | Quarkus property |
|---|---|---|
| `endpoint.host/port`, `proxy.httpConnect` | `EndpointBuilder`, `ProxyBuilder` | `sftp.connector.endpoint.*` |
| `auth.password(user, secret)` | `AuthBuilder` | `auth.user`, `auth.password` |
| `hostKey` | `SftpConnectorBuilder.hostKey` | `host-key.policy`, `host-key.known-hosts` |
| the fourteen `pool.*` knobs | `PoolBuilder:344-357` | `PoolProperties:84-97`, all fourteen |
| `retry.maxAttempts`, `backoff` | `RetryBuilder` | `retry.max-attempts`, `backoff-initial/max/jitter` |
| `circuitBreaker.*` | `CircuitBreakerBuilder` | `circuit-breaker.*` |
| `bulkhead.maxConcurrentTransfers` | `BulkheadBuilder` | `resilience.max-concurrent-transfers` |
| the eleven `polling.*` knobs | `PollingBuilder:361-425` | `PollingProperties:100-111` |

Keys in the code that spec 12's example does not show, all justified elsewhere in the spec or in
a deviation: `pool.drainTimeout` (spec 11.2), `pool.housekeepingInterval` (spec 4.5),
`resilience.operationTimeout`/`transferTimeout` (spec 9's time limiter), `polling.recursive`
(spec 7.4). Keys in spec 7 with no DSL entry at all: `sortBy` (S9), `ackWait` (spec 14.3
defers it).

Validation: all six of spec 12's rules are enforced (`ConnectorDsl.kt:101-223`), plus ten
positive-duration checks, a jitter range check, a self-move check and the `drainTimeout >
cancelGrace` rule of T13 deviation 3. Spec 12's rule list names none of the extras (S11).

## Spec 13: metrics

All thirteen names exist with the specified types and tag values. `endpoint` is on every one.
Lazily registered (absent until first use): `sftp_pool_evicted_total{reason}`,
`sftp_pool_leak_total`, `sftp_retry_total{op}`, `sftp_error_unmapped_total`,
`sftp_op_seconds{op,result}`, `sftp_poll_seconds{result}`. Eagerly registered for every tag
value: the three pool gauges, `sftp_pool_acquire_seconds`, `sftp_pool_acquire_timeout_total`,
`sftp_pool_created_total`, `sftp_poll_files{state}`, `sftp_ack_total{outcome}`, `sftp_inflight`,
`sftp_breaker_state`. No metric exists that spec 13 does not name. Lens 4 covers the
absent-versus-zero documentation question; nothing here contradicts spec 13.

## Spec 17.1 and 17.2: invariants and scenarios to tests

| ID | Test |
|---|---|
| I1 | `I1_idle plus inUse plus connecting never exceeds maxSize` |
| I2 | `I2_an entry is handed to at most one lease at a time`, `I2_a call still in flight when the block ends keeps the session until it finishes` |
| I3 | `I3_a poisoned entry never returns to the idle deque` |
| I4 | `I4_every permit is released exactly once on every exit path`, `I4_a housekeeper cancelled while opening spares gives back every room the round reserved` |
| I5 | `I5_no transport call executes while the registry lock is held` |
| I6 | `I6_a session past its lifetime is closed when it comes back and never lent again` |
| I7 | `I7_a file in flight is not handed over by any poll`, `I7_a file two waiting polls both want is handed over once`, `I7_I8_the in-flight set's lock is linearizable across interleavings` |
| I8 | `I8_cancelling a collector with unacked files gives every place back` |
| I9 | `I9_close returns within the drain plus one grace and leaves every entry closed` |
| I10 | `I10_a fatal failure stops the watch and no other failure does`, `I10_a recoverable failure is reported and the watch goes on` |
| I11 | four `I11_` tests plus partition row P3 |
| I12 | `I12_ack and nack are each accepted once per file` |
| I13 | five `I13_` tests |
| I14 | `I14_a keepalive or an idle timeout that outlasts the path...` |
| **I15** | **No `I15_` test.** Two assertions inside `AdversaryTest.kt:431` and `:493`. See S7 |

S1 to S12 are all named `S<n>_` and collected in `AcceptanceScenarios.kt`, checked on every build
by `AcceptanceScenariosTest`. P1 to P6 are all in `HalfOpenPartitionTest` and `PartitionMatrixTest`.

---

## Findings

### HIGH

**S1. The open-seams table still tells a maintainer that `SizeStable` waits a whole poll, which
stopped being true before T11.** `progress.md:206` (the row) against `core/.../source/Readiness.kt:65-100`.
The row reads *"`SizeStable` observes across polls, not inside one, so the shipped default is
ready on the second poll ... On the hourly pipeline the default readiness adds an hour of latency
per file ... needs a ruling, not a workaround"*, owner "the maintainer". That ruling was made -
C11, `progress.md:148-163` - and applied: the class stats every candidate, waits one `interval`
once with the listing's session released, stats again, and the across-poll memory and its
`synchronized` are gone. The row was never struck through. This is the single most misleading
line on a table whose entire purpose is to survive session boundaries, and it points a maintainer
at a decision that has already been taken. **Amendment:** strike the row through and record
*"Closed by C11 and the pre-T11 hotfix. `SizeStable` batches inside one poll: every candidate is
stated, one `interval` elapses, every candidate is stated again, with the listing's session
already released. Spec 7.5 and D36 record it."*

**S2. Spec 8.3 says a `SeenRepository` SPI is provided. There is none, anywhere, and no seam owns
it.** `spec.md:8.3` against the whole tree - `grep -ri seenrepository` over `sftpconnector`
returns nothing. The spec sentence is present tense and unqualified: *"A `SeenRepository` SPI
with an in-memory LRU default is provided for callers that cannot move files and want the
connector to filter; it is not used by the Sec 1.1 pipeline."* Spec 14 does not defer it, and the
open-seams table has no row for it. The only record anywhere is one clause in T10 deviation 10
(`progress.md:1990-1993`), which groups it with `ackWait` as *"off by default and not in this
ticket"* - an inaccurate description, since an SPI that does not exist is not off by default. A
reader of the spec today is told the connector ships a filtering hook it does not have.
**Either** build it, **or** amend spec 8.3 to say it is deferred and add a seams row.
**Proposed spec amendment:** move the sentence into a new spec 14.5, *"A `SeenRepository` SPI for
callers that cannot move files is not built. The in-flight set is the only state the connector
holds; a caller needing persistent filtering does it above the source."*, with a seams row owned
by *"the first caller that cannot move or delete files"*. **Failing test if it is built instead:**
`@Test fun I16_a file the seen repository already holds is not handed over again()` - a fake
transport listing one file, a `SeenRepository` pre-loaded with its key, one `poll`, assert the
event sequence is `PollStarted` then `PollCompleted(seen = 1, emitted = 0)`.

### MEDIUM

**S3. Spec 5.4's table of message prefixes names three strings the mapper does not match on, and
one it does not match at all.** `spec.md:5.4` against `transport/jsch/JschErrorMapper.kt:61-118`.
The spec says the mapper works by *"a maintained table of message prefixes (`Auth fail`,
`timeout`, `session is down`, `ProxyHTTP`, `UnknownHostKey`, `channel is not opened`)"*. Of those
six: `Auth fail` (`:89`), `session is down` (`:95`) and `channel is not opened` (`:100`) are
matched as written. `ProxyHTTP` and `UnknownHostKey` are matched **by exception type** instead -
`JSchProxyException` at `:79` and `JSchHostKeyException` at `:72` - which is strictly better and
is exactly what spec 16 item 3's closure note records, and spec 5.4 was never updated to say so.
`timeout` is matched by **nothing**: T2 measured the connect-phase failures arriving as
`Session.connect: java.net.SocketTimeoutException: Read timed out`, caught by the `java.net.`
marker at `:107`, and deliberately left `timeout in waiting for rekeying process.` to fall
through to `Unknown`. So a reader of spec 5.4 is told to look for a prefix that is not there and
will not find why. **Amendment to spec 5.4:** *"`JSchException` carries only a message and maps
by a maintained table of message prefixes (`Auth fail`, `session is down`, `failed to send
channel request` / `channel is not opened`, `connection is closed by foreign host`) plus the
`java.net.` marker JSch leaves in a stringified socket failure. The host key and proxy failures
have exception types of their own in this fork and are matched by type, so a rewording cannot
reclassify them."* The measured table in the T2 entry is the authority; this sentence should
point at it.

**S4. Spec 7.2 says a cancelled collector's files are "treated as nack with redelivery"; the code
redelivers them without running the nack action.** `spec.md:7.2` against
`source/SftpSource.kt:394-399` (`withdraw`) and `:310-316`. `withdraw` settles the slot as
`CANCELLED`, counts it, and releases - `perform(polling.onNack, ...)` is never called. T10
deviation 3 (`progress.md:1944-1955`) records this and gives a good reason: running a configured
`move("failed/")` for files the consumer never looked at would file every unprocessed message as
a failure on every shutdown, inside a cancelled coroutine. Spec 13's own `cancelled` label beside
`nack` reads as the same distinction. The deviation stands; the spec sentence was never amended,
so spec 7.2 and the code disagree in writing. **Amendment to spec 7.2:** *"Cancellation of the
collector with unacked files makes each eligible again on a later tick, as a nack with
redelivery would - but the nack action does not run: nobody said those files failed. They are
counted `sftp_ack_total{outcome=cancelled}`."*

**S5. The `writeFrom` / `SftpSession` seam row is closed by the spec and not struck through.**
`progress.md:212` (the row) against `spec.md:5.1` and `spec.md:6.1`. The row says *"Spec 5.1
still names `openWrite` and spec 6.1 still declares `withSession(block: suspend Connection.() ->
T)`"*. Neither is true any more: spec 5.1 carries the `readTo`/`writeFrom` paragraph and the
`SftpConnection` = `SftpSession` + `close()` sentence, and spec 6.1's table row reads
`withSession | (block: suspend SftpSession.() -> T): T` with a paragraph of D39 behind it. The
code matches (`transport/SftpTransport.kt:189, :203, :253`; `client/SftpClient.kt:292`). Strike
the row through: *"Closed. Spec 5.1 carries the transfer-operation paragraph and the
session/connection split; spec 6.1 declares `withSession` over `SftpSession` with D39."*

**S6. `UnsafeFileName` is a top-level failure class that spec 10.1's hierarchy and 10.2's table do
not have a row for.** `error/SftpException.kt:291-294` against `spec.md:10.1`. It sits beside
`PoolExhausted`, `CircuitOpen` and `OverwriteRefused` with disposition `ACCEPT_THE_REFUSAL`, and
it is raised at `client/SftpClient.kt:178-181`. The hotfix that added it is on the seams table
(the path-traversal row) and struck through, but the row describes the *fix*, not the *new
class*, so nothing points a spec reader at it. The same applies to `ACCEPT_THE_REFUSAL` itself,
which C8 introduced as a seventh disposition. `FailureModelTest`'s `rowOf` is an exhaustive
`when`, so the code cannot drift - only the document can. **Amendment to spec 10.1:** add
`UnsafeFileName` under the `OverwriteRefused` line, *"a listed name that cannot be a local file
name under the staging directory; nothing was sent and no session was borrowed"*; and to spec
10.2's table a row *"| UnsafeFileName | No | Not counted | n/a | Emits `PollFailed`, continues |"*.

**S7. I15 is the one invariant with no `I15_` test, and spec 17.1 says invariant tests are named
`I<n>_<description>`.** `spec.md:17.1` against `testkit/.../pressure/AdversaryTest.kt:431` and
`:493`. What exists is two `describedAs("I15: ...")` assertions inside the adversary's
per-operation and ledger checks - genuinely stronger coverage than one scenario test, since they
run after every operation of thousands of sequences. But `grep I15_` finds nothing, so the one
mechanical check that every invariant has a home does not see it, and a reader running
`-Dtest=I15*` finds nothing. **Failing test to write:** extract the ledger comparison into
`@Test fun I15_every acked file is at the ack target and no landed move is reported as failed()`
in `AdversaryTest`, running one fixed seed end to end, so the id is greppable; leave the
per-operation assertions where they are.

**S8. T16 raised three spec findings with proposed wordings under C9's protocol. None is applied
and none is on the seams table.** `progress.md:3545-3557`. They are:
(i) *I15's phantom-failure clause is bounded by the retry budget and the breaker* - a rename that
landed and lost its reply on the **last** permitted try is reported as a failure while the file
sits at the target. Spec 17.1's I15 still says "no landed move is reported as failed" flatly.
(ii) *Tier D's recovery bound omits the tick interval and the breaker's wait in open* - spec 17
layer 6 says "recovery time by bound" and states no formula, so there is nothing to correct, but
the formula the tests use should be written down.
(iii) *`InFlightSet`'s class comment says a duplicate "never" queues for room it will not use* -
`source/InFlightSet.kt:40-42` still says "never", and its own next sentence explains the case
where it does. T16 proposed "seldom".
Each is a small edit and each will be lost when this session ends. **Amendments:** apply T16's
three wordings verbatim - they are already drafted - and add a seams row for anything that
outlives the edit.

**S9. `sortBy` and `RenameClaim` are named in spec 7 as built-ins and exist nowhere, recorded only
in a T10 deviation.** `spec.md:7.4` (*"`sortBy` requires materialization and is honored only
together with `maxFilesPerPoll`, as Camel does"*) and `spec.md:7.5` (the built-ins table lists
`RenameClaim`) against `config/ConnectorDsl.kt:361-425` and `source/Readiness.kt`, neither of
which has either. T10 deviations 5 and 10 record both with reasons - `sortBy` needs
materialisation and a design nobody asked for; `RenameClaim` proves nothing on Linux by spec
7.5's own caveat and is spec 14.2's multi-consumer hook. Both reasons are good. Neither reached
the coordinator's seams table, so both are one session away from being forgotten. **Amendments:**
spec 7.4 - *"`sortBy` is not built: it needs materialisation and nothing in scope asks for it"*;
spec 7.5 - mark the `RenameClaim` row *"not built; see 14.2"*. Add one seams row each, owned by
*"whoever first needs deterministic poll order"* and *"whoever builds 14.2's claim step"*.

**S10. Spec 11.1 step 3 says the pool fills to `minIdle` in the background at start-up. Nothing
fills until one `housekeepingInterval` has passed.** `spec.md:11.1` against
`SftpConnector.kt:140` and `pool/SftpPool.kt:339-352`. `start` launches `pool.housekeep()`, whose
loop is `delay(interval)` **before** `sweep()`, so with the shipped defaults the first top-up
happens 30 seconds after the connector reports itself up. The spec's "readiness does not wait for
it" is honoured; "fill to `minIdle` in the background" is not, on any timescale a reader would
expect. It is invisible today because `minIdle` defaults to 0, which is exactly why it would stay
invisible until a deployment set it. **Failing test:** `@Test fun the pool fills to minIdle
without waiting for the first housekeeping round()` - start a connector over the fake with
`minIdle = 2` and `housekeepingInterval = 5.minutes` on a test scheduler, advance by nothing,
assert `pool.stats().idle == 2`. **Or**, if the delay-first loop is deliberate, amend spec 11.1
step 3 to *"The housekeeper fills to `minIdle` on its first round, one `housekeepingInterval`
after start-up; readiness waits for neither."*

**S11. Spec 12's validation-rule list is a strict subset of what the builder enforces.**
`spec.md:12` against `config/ConnectorDsl.kt:101-223`. The spec names six rules. The builder also
enforces: ten durations must be positive (`:106-119`), `validationBypass >= 0` (`:123`),
`maxLifetimeJitter in 0.0..1.0` (`:126`), **`drainTimeout > cancelGrace`** (`:133`, recorded as
T13 deviation 3, which notes a reviewer thought `drain = 1s, grace = 5s` is a configuration
somebody could want), `maxSize >= 1`, `minIdle >= 0`, `maxInFlight >= 1`, `maxFilesPerPoll >= 1`,
a move target that names no folder, host and port ranges, and `operationTimeout`/`transferTimeout
> acquireTimeout` (`:207-213`). The last is load-bearing and subtle - it stops a full pool being
reported as a server timing out and counted against the breaker - and it is nowhere in the spec.
Nothing here is wrong; the spec's list simply reads as exhaustive and is not. **Amendment:**
replace spec 12's rule sentence with the six current rules plus *"`drainTimeout > cancelGrace`,
`operationTimeout` and `transferTimeout` longer than `acquireTimeout`, and every duration
positive. `ConnectorDsl.build()` is the authority and reports every fault at once."*

### LOW

**S12.** The open-seams table carries the "a cut session counts as `reason=poisoned`" row twice,
near-verbatim, both left by T8 (`progress.md:207` and `:216`). One says the owner is *"whoever
revisits the five fixed labels with the maintainer"*, the other *"whichever ticket revisits spec
13's five eviction labels"*. Merge them.

**S13.** D24 and D25 do not exist. The log runs D1 to D23, then D26 to D44, and nothing says the
two were withdrawn. A reader chasing a citation to either finds silence. One line in spec 15
- *"D24 and D25 were withdrawn during the design review and are not reused"* - closes it.

**S14.** Spec 14.1 says *"`Connection.openRead` exists on the transport so a streaming download
that pins a lease for the consumer's read can be added without changing the pool"*. It does not
exist (`transport/SftpTransport.kt:143-246`), and spec 5.1 contradicts 14.1 directly: *"`openRead`
is not `readTo` renamed - it is the streaming download of Sec 14.1, deferred out of v1 by Sec
1.3, and whichever release builds it adds it alongside."* Spec 5.1 is right. Amend 14.1 to
*"`openRead` is the streaming download; the transport interface has room for it beside `readTo`
and it is not built."*

**S15.** Spec 3.2 names the modules `sftp-core`, `sftp-quarkus`, `sftp-testkit`; the artifact ids
are `sftpconnector-core`, `sftpconnector-quarkus`, `sftpconnector-testkit` under a `sftpconnector`
parent. Cosmetic, and worth one edit so a search for the module name finds it.

**S16.** Spec 6.3 says download writes `<stagingDir>/<name>.part`. `client/StagingArea.kt:425`
resolves the partial file as a sibling of whatever `target` it was given, which is the staging
directory only when `localTarget` is null (`SftpClient.kt:148`). A caller naming its own target
gets the `.part` beside that target. This is the better behaviour - it keeps the atomic rename on
one filesystem - and I13's five tests cover it. Amend 6.3 to *"beside the target, which by
default is `<stagingDir>/<name>`"*.

**S17.** Spec 4.2 says the acquire timeout throws `PoolExhaustedException` and spec 9 says the
open breaker raises `CircuitOpenException`; spec 10.1 and the code call them `PoolExhausted` and
`CircuitOpen`. T2 deviation 5 noted the clash and let the hierarchy's names win. Two words in
spec 4.2 and 9.

---

## Ranked

**Critical:** none.

**High**
1. **S1** - the seams table's `SizeStable` row is stale and points the maintainer at a decision
   already taken (C11). Owner: the coordinator, one strike-through.
2. **S2** - spec 8.3 promises a `SeenRepository` SPI that does not exist, with no seam and no
   deferral. Owner: the maintainer - build it or move the sentence to spec 14.

**Medium** (each with a proposed owner)
3. **S3** spec 5.4's prefix list against the measured mapper - *whoever next touches the mapper*.
4. **S4** spec 7.2's "treated as nack" against `withdraw` - *the maintainer* (T10 deviation 3 is
   the ruling; only the spec sentence is outstanding).
5. **S5** the `writeFrom`/`SftpSession` seams row is closed and unstruck - *the coordinator*.
6. **S6** `UnsafeFileName` and `ACCEPT_THE_REFUSAL` missing from spec 10.1/10.2 - *the maintainer*.
7. **S7** I15 has no `I15_` test - *the T17 owner*, a test-rename-and-extract.
8. **S8** T16's three proposed spec findings unapplied and unlisted - *the coordinator*.
9. **S9** `sortBy` and `RenameClaim` unbuilt, off the seams table - *the coordinator*, two rows.
10. **S10** `minIdle` is not filled until the first housekeeping round - *the T17 owner*: this is
    the one Medium with a behavioural question behind it, and it wants either the failing test or
    an amended spec 11.1, not both.
11. **S11** spec 12's validation list is incomplete - *whoever next touches DSL validation*.

**Low:** S12 (duplicate seams row), S13 (D24/D25 do not exist), S14 (spec 14.1 vs 5.1 on
`openRead`), S15 (module names), S16 (`.part` location), S17 (`PoolExhaustedException` vs
`PoolExhausted`).

---

## Verdict

The code conforms to the spec. Every decision in the log has an implementation or an honest
deferral with an owner, every metric and configuration key matches, the resilience order and the
failure model are built exactly as decided, and fourteen of fifteen invariants plus every
scenario and partition row have a named test. Where the code departs from the spec it departs
deliberately, and in every case a progress deviation says so with a reason that reads better than
the sentence it replaced - the batched `SizeStable`, the answered-failure disposition, the
withdraw that runs no nack action, the escaped paths. This is a module whose builders left a
trail.

What has drifted is the paperwork, in two directions. The spec is behind the code in eight places
(S3, S4, S6, S9, S11, S14, S16, S17), each a sentence that would mislead a reader building
against it, and one place where the spec claims a feature that does not exist (S2). The
open-seams table is behind the code in two (S1, S5) and is missing rows for six things that were
deferred in a ticket's deviations and never promoted to it (S2, S8, S9). Since the table exists
precisely because *"each was correctly deferred by the ticket that found it"* and is the thing
most likely to be lost between sessions, a stale row on it is worse than an absent one: it costs
the next maintainer a decision they have already made. That is why S1 is the highest finding here
despite being a documentation edit.

Nothing found by this lens blocks production. The list a maintainer must still decide before
production is unchanged in substance by this review: `Fingerprint` pinning, the `reason=poisoned`
label covering cut sessions, `ackWait`, two connectors on one registry, and spec 16 item 1's
producer-side convention. This lens adds one item to it - whether `SeenRepository` is built or
struck (S2) - and one behavioural question worth a test - whether `minIdle` should be filled at
start-up or on the first round (S10).

Counts: **Critical 0, High 2, Medium 9, Low 6.**

---

## Adjudication (T17 owner)

Every finding was adjudicated as valid. The reviewer's text above is untouched; this section is
the disposition, one row per finding, with the commit that carries it.

| ID | Sev | Disposition |
|---|---|---|
| S1 | High | **Fixed** - `bdcc729`. The `SizeStable` row is struck through with what closed it: C11's ruling, applied by the pre-T11 hotfix, `Readiness.kt:65-100`, spec 7.5 and D36 |
| S2 | High | **Deferred with a decision, and recorded as a seam** - `9f9e1f0`. Not built. An interface with one implementation, no consumer in the Sec 1.1 pipeline, and an in-memory LRU that filters nothing after the restart it exists for is a second ledger inside the connector against D14. Spec 8.3 now defers it to a new spec 14.5, which says what a caller that cannot move files does instead - filter above the source and ack what its own ledger holds, which spec 7.2 already permits. T10 deviation 10's "off by default" clause is corrected. Seams row owned by *the first caller that cannot move or delete files* |
| S3 | Medium | **Fixed** - `597658d`. Spec 5.4 lists the four prefixes and the `java.net.` marker the mapper really matches, says the host key and proxy failures are matched by exception type, and points at T2's measured table as the authority. `timeout` is gone from the list |
| S4 | Medium | **Fixed** - `597658d`. Spec 7.2's cancellation bullet says the nack action does not run, with T10 deviation 3's reason and the `outcome=cancelled` label |
| S5 | Medium | **Fixed** - `bdcc729`. The `writeFrom`/`SftpSession` row is struck through; spec 5.1 and 6.1 already carry both paragraphs |
| S6 | Medium | **Fixed** - `597658d`, with one deliberate departure from the proposed wording. `UnsafeFileName` is in spec 10.1's hierarchy and 10.2's table, and `ACCEPT_THE_REFUSAL` gets the paragraph C8's seventh disposition never had. The 10.2 row reads *"Raised to whoever called `download`; the poll is untouched"* rather than *"Emits `PollFailed`, continues"*: the class is thrown by `SftpClient.download` (`:178`) to its caller, and `SftpSource.FileHandling.download` catches only `NoSuchFile`, so no poll event is produced. Lens 3's H1 is a different defect on the remote side (a listed name that is not one path component reaching `rename`/`delete`) and its fix skips the entry at the listing rather than raising this class; if that lands differently, this row is the one to revisit |
| S7 | Medium | **Fixed, with the test** - `e04c6bc`. `I15_every acked file is at the ack target and no landed move is reported as failed` in `AdversaryTest`, one fixed seed end to end through the same harness; the per-operation assertions stay where they are. Verified red by breaking `reconcile`'s expected ledger |
| S8 | Medium | **Fixed** - `597658d`. T16's three wordings applied verbatim: spec 17.1's I15 bounds the phantom-failure clause by the retry budget and the breaker; spec 17 layer 6 carries `2 x keepAlive + max backoff + interval + waitInOpen`; `InFlightSet`'s comment says "seldom" and spec 7.3 names the second look. Nothing outlives the edits, so no seams row was needed |
| S9 | Medium | **Fixed and recorded as seams** - `bdcc729` (rows) and `597658d` (spec). Spec 7.4 says `sortBy` is not built and spec 7.5's `RenameClaim` row says *"not built; see 14.2"*. Seams rows owned by *whoever first needs a deterministic poll order* and *whoever builds spec 14.2's claim step* |
| S10 | Medium | **Deferred with a decision** - `3ea1270`. The delay-first housekeeper loop is deliberate and already recorded as T9 deviation 3, which weighed an immediate fill against changing the timing three of T5's tests are written against. `SftpConnectorTest.the pool fills to its minimum in the background, and the connector works before it has` already asserts both halves, so the proposed failing test would have contradicted a passing one. Spec 11.1 step 3 now says the fill happens on the housekeeper's first round, cites the deviation, and notes `minIdle` defaults to 0 |
| S11 | Medium | **Fixed** - `597658d`. Spec 12's rule list carries `drainTimeout > cancelGrace`, both timeouts longer than `acquireTimeout` with the reason, the jitter range, the non-negative bypass and the positive durations, and names `ConnectorDsl.build()` as the authority |
| S12 | Low | **Fixed** - `bdcc729`. The two near-verbatim `reason=poisoned` rows are one row with both owners' wording |
| S13 | Low | **Fixed** - `597658d`. One line under spec 15's table: D24 and D25 were withdrawn during the design review and are not reused |
| S14 | Low | **Fixed** - `597658d`. Spec 14.1 says `openRead` is the streaming download, that the interface has room for it beside `readTo`, and that it is not built. Spec 5.1 was already right |
| S15 | Low | **Fixed** - `597658d`. Spec 3.2 and the ArchUnit sentence name the artifact ids |
| S16 | Low | **Fixed** - `597658d`. Spec 6.3 puts the partial file beside the target, which by default is `<stagingDir>/<name>`, with the one-filesystem reason |
| S17 | Low | **Fixed** - `597658d`. Spec 4.2 and 9 use `PoolExhausted` and `CircuitOpen`, the hierarchy's names |

**Spec 16.** Item 1 stays open and is now the only one: D45 records that the shipped readiness
default is a heuristic with a stated blind spot, that no code can close it, and that
`markerFile(suffix)` is where the upstream team's answer lands. Item 2 is struck with T9's probe
and its three `StartupAgainstServerTest` cases. Item 3 was already struck; its closure note now
points at the amended spec 5.4 and T15's tenth wording.

**What this adds to the maintainer's list before production:** nothing new. `SeenRepository` (S2)
is deferred with a decision and an owner rather than left open, and `minIdle` (S10) turned out to
be a recorded deviation rather than a question.
