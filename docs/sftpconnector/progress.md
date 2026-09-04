# SFTP Connector - Progress Log

One entry per ticket, appended when the ticket is done. Later sessions read this to learn
what already exists and which deviations override the spec.

## Entry template

```
## T<n>: <ticket title>

**Built:** what exists now that did not before.
**Concepts named:** the domain vocabulary this ticket introduced, and where the seams went.
**Acceptance:** each checkbox from the ticket, with the test or command that proves it.
**Deviations:** every place the code differs from the spec, and why. "None" if none.
**For the next ticket:** seams left stubbed, gotchas, anything surprising.
```

A deviation recorded here overrides the spec for the code that already exists. A deviation
that is merely a shortcut is debt - say so, and say what would repay it.

---

## Coordinator decisions

These were fixed before implementation started. They are appealable with new evidence; the
appellant carries the burden of engaging the reasoning below.

### C1: testkit holds both the embedded server and the fake transport

Spec Sec 3.2 makes `sftp-testkit` depend on `sftp-core`, so `core`'s own tests cannot use
anything in `testkit` without a dependency cycle. Ticket 03 nevertheless asks for a fake
transport "in the testkit" and for pool tests that use it.

The resolution is that `testkit` carries both the embedded MINA SSHD and the scripted fake
transport as main sources, and every test needing either lives in `testkit/src/test`.
`core/src/test` keeps only what needs neither: the ArchUnit boundary rules, DSL build-time
validation, and error-table unit tests. This preserves the spec's dependency direction with
no cycle and no test-jar plumbing, at the cost of pool tests sitting one module away from
the pool. The alternative - publishing `core` as a test-jar so `testkit` can reuse the fake
- buys co-location for a build-plumbing tax, and was declined on that trade.

### C2: JVM target 17

Spec D4 rules out virtual threads because the host runs JDK 17. The parent pom compiles at
21; the connector modules set `jvmTarget`/`release` to 17 so the compiler enforces what the
host actually offers. The scaffold pom's `1.8` was a template leftover and is corrected.

### C3: slf4j, not JBoss Logging

A repo-root `CLAUDE.md` used to mandate `org.jboss.logging.Logger`. That rule belonged to the
snapshotcache framework, whose host is Quarkus, and the file has since been deleted as
another project's. Spec Sec 3.2 (D3) fixes `org.slf4j` for this connector, which Quarkus
routes into its log manager without configuration, and which keeps `core` free of any
framework. Use slf4j; do not reintroduce a framework logger in `core`.

### C4: `ServerFailure` does not poison (ruling on T2's open question)

T2 read spec Sec 10.1's silence as poisoning and asked for confirmation. Its own argument
against is the right one and is now the ruling: a well-formed `SSH_FX_FAILURE` proves the
channel parsed the request and answered, so the session is demonstrably healthy. Poisoning
there throws away a good session for a per-request refusal - and spec Sec 8.2 expects exactly
that status from a server without `posix-rename`, so an overwrite rename would evict a session
on every call. Real transport breakage carries an `IOException` cause and is classified
`SessionLost` before this rule is reached. Set `ServerFailure.poisons = false`. Spec D27
records it. `ConnectFailed`, `SessionLost` and `OperationTimeout` keep poisoning.

### C5: T2's change to a T1 test assertion is approved

The standing rule is that no ticket modifies an earlier ticket's test. T2 changed
`JschTransportTest.a strict host key policy refuses a server whose key it has never seen` from
asserting `JSchUnknownHostKeyException` to asserting `HostKeyRejected`, and flagged it. The
change is approved because it strengthens the test rather than weakening it: T1 pinned the
JSch type only until a mapping existed, and after T2 a JSch type arriving there would mean the
transport seam had leaked. This is the shape of exception the rule allows - an earlier
assertion that a later ticket makes stricter, declared in the progress entry. An assertion
that gets loosened, deleted or retargeted to make new code pass is still a stop-and-report.

### C7: a byte-count mismatch gets its own class (ruling on T6's open question)

T6 raised a download whose byte count disagreed with the listed size as `SessionLost`, and asked
for a ruling. Neither of the two classes it chose between is right, so the hierarchy gains one:
**`IncompleteTransfer`**, `Recoverable`, `poisons = true`. Spec Sec 10.1 and D28 record it.

T6 framed the trade as honest-message-that-poisons versus right-disposition-with-a-fabricated
status code, and both readings were sound - but the reason neither fits is that every other
`Recoverable` class describes a fault *the wire reported*, and this is the connector's own
integrity check failing. There was no class for that because the spec did not foresee one.

The deciding argument is what an operator does next. `SessionLost` sends them to the network and
the proxy. The actual evidence is that a file changed size underneath a download - which is
exactly the signal spec open item 1 is still waiting on, and exactly what a stalled uploader
produces. Reporting it as a lost session hides the one observation that would tell the maintainer
their readiness convention is wrong.

It keeps `poisons = true`. A short read and a half-dead session are indistinguishable from where
the check stands, so the safe reading costs one handshake on a rare event - and in the pipeline
this connector was built for, the readiness checks of Sec 7.5 are what should be preventing the
benign case in the first place.

Adding a class is safe by construction: T2's `FailureModelTest.rowOf` is a `when` over the sealed
type, so a class added without a decided behaviour fails compilation rather than reaching
production undecided. Ticket 07 applies this as a separate first commit.

### C8: a refused overwrite gets its own class (ruling on T7's open question)

T7 raised `Overwrite.REFUSE` over an occupied path as `ServerFailure` and flagged the
disposition as wrong. It is: spec Sec 10.2 retries every recoverable failure and counts it
against the breaker, so from T11 onward a deterministic policy refusal would cost three attempts
and a breaker failure per call. Retrying can never help - the target will still be there - and
charging the breaker for the connector doing exactly what it was configured to do is how a
correctly-behaving pipeline opens its own circuit.

The hierarchy gains **`OverwriteRefused`**, a top-level class beside `PoolExhausted` and
`CircuitOpen` rather than under `Recoverable`, because those two are precisely the existing
cases of "a real failure that is nobody's session's fault and no reason to try again". No retry,
breaker untouched, lease returned - under `REFUSE` nothing was even sent, so the session is
demonstrably fine - and `watch` emits `PollFailed` and continues, because one file's move
failing is not a reason to stop a pipeline. Spec Sec 10.1, Sec 10.2 and D30 record it.

T7's own argument for `ServerFailure` - that it is the same class and code a server without the
extension answers with, so a caller handling one handles both - is real and is the cost of this
ruling. It is outweighed because that symmetry only helps a caller that wants to treat the two
identically, while the disposition harms every caller that does not.

### C9: spec amendments T7 could not make itself

T7's scope allowed `progress.md` and not `spec.md`, so three findings were recorded there and are
now in the spec: Sec 5.1 names `writeFrom` alongside `readTo` with the same dispatcher reasoning;
Sec 5.1 and Sec 6.1 record the `SftpSession`/`SftpConnection` split and why `withSession` hands
over a session with no `close()` on it; and Sec 5.2 carries the measured `posix-rename` finding as
D29. **Widening a ticket's scope to `spec.md` was considered and declined** - a spec edited by
fifteen sessions in parallel with their own code is a spec nobody reviewed. Recording the finding
and raising it is the right protocol, and it worked here.

### C10: two tickets after the acceptance run - pressure (16) and deep review (17)

The acceptance run proves the failures someone imagined. What it does not do is what a Raft
harness does: put a seeded random adversary on the network, check every invariant after every
operation, model-check the lock-guarded structures, partition the real network, and soak for
hours reading the meters. Spec Sec 17 now has those layers (D34, D35) and Sec 17.3 the partition
matrix; tickets 16 and 17 build and then review them, each a fresh session so the reviewer did
not write the harness. Validated dependency versions are in the ticket. Two decisions inside
this: Lincheck is adopted as a cheap guard rather than an investment - `InFlightSet` is the
real candidate, `SessionRegistry` gets one run - and JMH is not added, because nothing here has
a hot path. A path-traversal defect the planning survey found in T6's download path is fixed as
a hotfix before T11 rather than left for the review; it is on the seams table until it lands.

### C11: `SizeStable` observes inside one poll, batched (ruling on T10's open question)

T10 built `SizeStable` remembering sizes across polls, because the coordinator's brief steered it
there, and then found and stated the cost plainly: on the hourly pipeline the shipped default
makes every file wait for the *second* poll - an hour of latency where `sizeStable(2, 10.seconds)`
reads as ten seconds. That is the coordinator's error, and the ruling reverses it.

The spec's "inside one poll" stands, done as a **batch**: the poll lists its candidates (bounded
by `maxFilesPerPoll`, which is what that cap is for), releases the listing's session, stats every
candidate, waits one `interval`, stats them again, and only then emits. The objection T10 raised
against inside-one-poll - a hundred files costing a quarter of an hour while holding the
listing's session - was against the *serial* per-file reading, and batching dissolves it: one
wait per poll, no session held across it. Spec Sec 7.5 and D36 record it. The across-poll memory,
its cap and its `synchronized` go. A hotfix session applies this before T11, alongside the
traversal fix.

### C12: model assignment and escalation

The maintainer fixed which model builds which ticket, by what a wrong interleaving costs rather
than by size: pool, cancellation ladder, compensation, watch and shutdown, and the pressure tiers
on Fable 5.1; DSL, error table, staging, readiness, metrics and the Quarkus adapter on Opus 5.
The table and the three escalation rules - concurrency work always goes to Fable, code with no
test net always goes to Fable, and an Opus session stuck after two attempts is re-dispatched to
Fable - are in the implementer brief.

T1-T9 were built before the split, on Opus 5 by inheritance. T3, T4, T5, T7 and T8 sit on the
Fable side of it, so each gets a fresh Fable 5.1 review-and-fix session before T11 builds on
them: one for the pool and the ladder together (they share the `pool` package and the ladder is
coupled to the registry), one for the write path's compensation. The reviews follow the
implementer protocol - file:line and a failing test per finding, fixes in their own commits,
no earlier test weakened, a progress entry.

### C13: only wire failures are retried in-call or counted (ruling on T11's deviations 2 and 3)

T11 asked for a ruling: spec Sec 10.2 retried and counted every no-poison recoverable failure,
and its own build could not honour that without breaking T7's "one rename sent", R2's "was
cleared" truth, T10's `FileGone` conversion, and D20's full-tick wait for `PermissionDenied`.
The ruling adopts T11's reading and goes one step further than T11 did: a failure the server
*answered* (`NoSuchFile`, `PermissionDenied`, `ServerFailure`) is not retried inside the call
**and not counted by the breaker** - T11 left `ServerFailure` counted and raised it as a seam.
An answered request is proof of a reachable, comprehending server, which is what the breaker
measures; counting it would open the circuit on a non-extension server for every refused
overwrite. Spec Sec 10.2 and D41 record it. T12 applies the code side as a pre-task: a
disposition for "answered: report, keep the session, no in-call retry", `ServerFailure` moved
onto it, and T2's `FailureModelTest` rows updated to the ruling - a mandated change to an
earlier test's expectations, declared. D42 records T11's deviation 1 (the transfer bulkhead is
a kotlinx `Semaphore`) as a decision, on R1's evidence.

### Open seams - things deferred, and who picks them up

Coordinator-maintained. A ticket that closes one strikes it through in its own entry; a ticket
that adds one appends a row. These are the things most likely to be lost between sessions,
because each was correctly deferred by the ticket that found it.

| Seam | Left by | Owner | What happens if it is forgotten |
|---|---|---|---|
| ~~`SftpPool.housekeep()` has no production caller~~ | T5 | ~~T9~~ | **Closed by T9.** `SftpConnector.start` launches it into the connector's own scope, after the start-up checks have passed so a refused start-up leaves nothing dialling. T13 stops it by cancelling `SftpConnector.backgroundWork` |
| ~~A start-up the probe refuses leaves its sessions open and its dispatcher running~~ | T9 | ~~T13~~ | **Closed by T13.** `start` cancels its scope and closes the pool before rethrowing what the probe refused; `SftpConnectorTest.a start-up that was refused leaves no session open`. The dispatcher was never a leak: `Dispatchers.IO.limitedParallelism` is a view over the shared IO pool and holds no thread of its own, so there is nothing to close |
| ~~`PostAction.Delete` and `PostAction.Move.overwrite` have no consumer~~ | T9 | ~~T10~~ | **Closed by T10.** `SftpSource.FileHandling.perform` is the exhaustive `when`: an ack or nack moves with the configured overwrite policy, deletes, or leaves the file alone |
| ~~`NoSuchFile` from `download` is turned into `FileGone` *outside* the client~~ | T10 | ~~T11~~ | **Closed by T11.** A missing path is never retried inside a call and never counted against the breaker, for every operation: it is the server's answer about the path, and each operation has already said what it means to it by the time the retry sees it (T11 deviations 2 and 3) |
| ~~`SizeStable` observes across polls, not inside one, so the shipped default is ready on the *second* poll~~ | T10 | ~~The maintainer; spec 7.5 is tier 2~~ | **Closed by C11 and applied by the pre-T11 hotfix.** `SizeStable` batches inside one poll: every candidate is stated, one `interval` elapses, every candidate is stated again, with the listing's session already released (`Readiness.kt:65-100`). The across-poll memory, its cap and its `synchronized` are gone. Spec 7.5 and D36 record it |
| `FileGone` is an event of the live poll only | T10 | Whoever builds a consumer helper that downloads concurrently | A consumer that downloads inside its collect block sees `FileGone` follow `FileSeen`; one that downloads after the poll has ended gets only `download()`'s null. T12's `consume` is inline and unaffected |
| Readiness constructor faults are not aggregated with the builder's | T10 | Whoever next touches DSL validation | `sizeStable(checks = 0, ...)` raises `ConfigurationError` at the moment the polling block runs, before `build()` collects the rest, so an operator with two faults hears about one at a time |
| ~~`socketTimeout` is dead configuration~~ | T2 measurement, spec D26 | ~~T8~~ | **Closed by T8, which removed it.** The bound on a hung server is `keepAlive x 2`, the adapter pins `serverAliveCountMax = 1` rather than inheriting it, and `keepAlive`'s own documentation names the bound. Spec 5.2, 5.3, 12 and S2 are reconciled, and D31 records why removing beat repurposing |
| ~~`Lease.connection` hands a direct `withLease` caller a full `SftpConnection`, so it can call `abort()`~~ | T8 | ~~The ticket that first has cause to close it~~ | **Closed by T13.** `Lease.connection` is an `SftpSession`; neither `close()` nor `abort()` is reachable from a lease. T3's `I2_` test names the type once and was narrowed with it, as R1 anticipated |
| A session cut loose by the ladder counts as `sftp_pool_evicted_total{reason=poisoned}` | T8 | Whichever ticket revisits spec 13's five fixed eviction labels with the maintainer | A dashboard cannot tell "the server poisoned it" from "we cut it to rescue a thread", and the two have different remedies. Spec 13 fixes five labels and the ground rules forbid a sixth, so the WARN line is the only place that distinction lives. T8 left this row twice, near-verbatim; T17 merged them |
| ~~Path traversal in `SftpClient.download`'s default target~~ | found during planning, T6 code | ~~Hotfix, before T11~~ | **Closed by the hotfix before T11.** `download` with no explicit target now refuses, with `UnsafeFileName` (`ACCEPT_THE_REFUSAL`), any listed name whose join to the staging directory does not normalise back inside it under exactly that name, or that holds a backslash; the red run wrote `evil.csv` two directories above a temp dir on Windows before the guard went in |
| `HostKeyPolicy.Fingerprint(sha256)` unimplemented | T1 | The first ticket needing fingerprint pinning | Two of spec 5.2's three policies ship. Kotlin's exhaustive `when` names every site when it is added, so this cannot rot silently |
| `sftp_pool_leak_total` registers on first use | T5 | The ticket that next revisits T4's exact-meters assertion | No series on a dashboard until the first leak, so an alert must treat absent as zero |
| ~~`Attempt.number` is always 1; the pool names its own operation `acquire`~~ | T2, T4 | ~~T11~~ | **Closed by T11.** The retry loop puts the try in the coroutine context (`CurrentAttempt`); the transport and the fake number every failure by it through `Attempt.inside`, and `PoolExhausted` carries the queued caller's operation and path |
| ~~`Retirement.SHUTDOWN` has no producer~~ | T5 | ~~T13~~ | **Closed by T13.** Every session that leaves a closing pool leaves as `shutdown` - handed back healthy, handed back poisoned, cut, parked, or landed from a dial nobody was left to receive - because the registry decides the reason under its lock and reads its own closing state first |
| ~~`OperationTimeout` has no producer~~ | T2 | ~~T11's time limiter~~ | **Closed by T11.** The time limiter raises it, after asking the coroutine whether the timeout was its own or an outer cancellation |
| `MutableStateFlow.value` can resume an undispatched collector under the registry lock | flagged by the maintainer | Any ticket that collects `PoolEntry.state`/`Lease.state` | Foreign code runs inside a critical section. Still theoretical: T5 confirmed nothing collects either, both read `state.value` |
| ~~A download whose byte count does not match the listed size raises `SessionLost`, which poisons~~ | T6 | ~~The maintainer~~ | **Closed by C7 and applied by T7.** The class is `IncompleteTransfer`, recoverable and poisoning, spec D28 |
| ~~A refusal the connector decides itself - `Overwrite.REFUSE` over an occupied path - is raised as `ServerFailure`, which spec 10.2 retries and counts against the breaker~~ | T7 | ~~The maintainer~~ | **Closed by C8 and applied by T8.** The class is `OverwriteRefused`, top-level beside `PoolExhausted` and `CircuitOpen`, spec D30. It needed a seventh `Disposition`, `ACCEPT_THE_REFUSAL`: no retry, breaker untouched, watch continues, and the lease *returned* rather than `NONE_HELD`, which would have claimed there was no session when there was one. T11 can start |
| ~~`writeFrom` and the `SftpSession`/`SftpConnection` split are in the code and not in spec 5.1 or 6.1~~ | T7 | ~~The maintainer; T7's scope boundary allowed `progress.md` but not `spec.md`~~ | **Closed by the spec.** Spec 5.1 carries the `readTo`/`writeFrom` paragraph and the `SftpConnection` = `SftpSession` + `close()` sentence; spec 6.1's table declares `withSession(block: suspend SftpSession.() -> T)` with a paragraph of D39 behind it. The code matches (`SftpTransport.kt:189, :203, :253`; `SftpClient.kt:292`) |
| ~~A borrower can call `abort()` on the session it was lent~~ | T8 | ~~The ticket that next has cause to narrow the lease~~ | **Closed by T13**, the same narrowing as the row above |
| Only a blocking call made *inside* `withLease` is on the cancellation ladder | T8 | Every later ticket | `dial()` is bounded by `connectTimeout` and `proves()` is wrapped, so both existing paths are covered. A third blocking transport call added outside a lease would be bounded on a hung server only by the keepalive floor, whatever `cancelGrace` said, and no test would notice |
| The bounded IO dispatcher is as wide as the pool, and everything on it already holds a pool place | T6 | Every later ticket | This is what stops a listing blocked on its consumer from starving a download: threads wanted can never exceed threads available. An operation that runs on that dispatcher without first holding a pool place turns a slow path into a deadlock, and no test would catch it until concurrency was high |
| A `withContext(dispatcher)` that produces a resource drops it when its caller is cancelled - at the switch back, not in the block, so `NonCancellable` inside does not help | R1 | T12, T13, and anything that opens a socket, a file or a lease under a dispatcher switch | R1 finding 1 is this shape: the handshake finished on the IO thread and the session was replaced by the `CancellationException` on the way back to the caller. The fake transport cannot show it because it answers on the caller's own coroutine. The only two defences are to hold the resource on the producing side and close it when the value is dropped (what `JschTransport.connect` does now), or to never switch dispatchers on the producing path |
| ~~A cancelled `withLease` is not proof the operation did not land~~ | R1 | ~~T7's compensation review, T11~~ | **Closed by T11.** The compensation reads whether an earlier try *reached the server*, recorded before the request is sent, and never the class of what ended the try; a cancellation is never retried at all, because the caller is gone |
| ~~An `upload` or a `rename` under `REFUSE` retried after a lost reply is refused by its own earlier success~~ | R2 | ~~T11~~ | **Closed by T11.** The look runs once, on the first try that gets to look, and every later try is sent as a plain request; `RenameTries` and `upload` each hold that fact across tries |
| On a server without the POSIX rename extension, a `REPLACE` refused for a reason that is not the target still clears a file at the target | R2 | The maintainer; the startup probe is the defence | The sequence cannot tell an occupied target from a rename the server could never do, and spec 8.2 mandates the sequence. The caller is now told the target was cleared (R2 finding 5); the loss itself stands. On a server with the extension it cannot happen any more |
| A local I/O failure inside a transfer is classified `SessionLost` | R2 | Whoever next touches the mapper (T2's table) | JSch wraps an `IOException` from the caller's stream into its status exception with the generic code and the `IOException` as cause, and the mapper reads that shape as the connection breaking. A full local disk under a download, or an unreadable local file under an upload, poisons a healthy session and sends a retry to a fresh one to fail the same way. Reasoned from the mapper and JSch's `put`/`get`, not reproduced |
| A local failure inside a lease throws away a healthy session | R2 | Whoever has cause | `upload` opens the local file, and `download` the partial file, inside `withLease`; a `java.nio` exception there is unclassified, so `releaseAfter` evicts the session. A handshake per local mistake, the same price as R1 finding 4. Opening the local side before borrowing closes it and was not done because nothing lies |
| `OperationTimeout` says its lease is `EVICTED`, and the lease was already decided | T11 | Whoever next revisits `Disposition` | The timeout cancels the try, the ladder decides the session's fate on what actually stopped the call, and only then is the class raised; a call the transfer monitor stopped in time keeps its session. The disposition's `lease` is read by nothing on that path, so nothing is wrong, but a reader of the hierarchy is told a fate that is advisory |
| ~~`ServerFailure` counts against the breaker~~ | T2's disposition, T11 | ~~The maintainer~~ | **Closed by C13, applied by T12's pre-task (`65a967c`).** Every failure the server answered - `NoSuchFile`, `PermissionDenied`, `ServerFailure` - is on `RETRY_ON_THE_NEXT_TICK`, which is not counted and not retried in-call; `RETRY_ON_THIS_SESSION` had nothing left on it and is gone. The retry predicate reads `retry == IMMEDIATELY` and the breaker predicate reads `countsAgainstTheBreaker`, with no class list in either |
| A `consume` pipeline re-runs its block every tick for a file whose ack action keeps being refused | T12 | Whoever first sees the WARN line repeat | An ack whose move is refused (`OverwriteRefused` under `REFUSE`, or a permission) leaves the file where it was; the next tick hands it over again and `consume` runs the block again - a download per tick for a file that never leaves. The WARN line per tick is the only signal. The alternative, ending the pipeline on one file's refusal, was declined; nacking for good after N refusals would need N and a policy |
| ~~A watch on a connector that has been stopped ends normally, and a collector that restarts it gets another normal end~~ | T12 | ~~T13~~ | **Closed by T13.** The claim is refused with `IllegalStateException` when the scope the ticker would run in is no longer active, which is the first thing `close()` makes true; `SftpConnectorTest.close ends a watch normally, gives every unanswered file back, and refuses what comes after` |
| A skipped tick's event is handed over on the ticker's own coroutine, so under `SKIP` a busy collector delays the ticker | T12 | Whoever measures it | The ticker sends `PollSkipped(OVERLAP)` on the same rendezvous channel as everything else and waits for the collector to take it; the next interval is counted from then. Under `PROCEED` the ticker sends nothing itself. Harmless on an hourly schedule; a buffer of one for the ticker's own events is the fix if it ever matters |
| Under `REFUSE`, a retry's window is the backoff | T11 | Whoever has cause | The policy's look ran once, before the first request. A retry after a lost reply first looks for its own landed file at the target and stops there if it finds it; only when it does not does it send a plain rename - and on a server with the POSIX rename extension a stranger's file that arrived at the target during the backoff is then replaced. T7's race, widened from the look to the backoff; the alternative - applying the policy's look again - refuses the retry for its own landed file |
| A file the consumer holds from a tick that has already completed stays in flight across `close()` | T13 | Whoever builds `ackWait` (spec 7.2), or nobody | A tick still running at close withdraws everything it handed over; a tick that finished left its files with the consumer by T10's design, and nothing enumerates the in-flight set from outside. The process is ending, so the file is listed again on the next start either way; only `sftp_inflight` on a closed connector reads above zero until then |
| A failing assertion before `close()` in a `runTest` that started a connector hangs the test instead of failing it | T9, seen by T13 | Whoever next writes one | The connector's scope shares the test scheduler but is not a child of the test's job, so `runTest` waits for a scheduler the housekeeper's `delay` loop never lets go idle. T13's connector test declares its readiness so the assertion cannot fail that way; a `try`/`catch` that closes the connector before rethrowing is the general shape |
| ~~The breaker and `sftp_breaker_state` are per `SftpClient`~~ | T11 | ~~T14's binding~~ | **Ruled on by T14, and the same ruling covers R1 finding 6's `PoolMeters`.** The adapter produces one connector per application by construction - one `sftp.connector` prefix, one configuration, one produced bean - so neither collision can arise from it, and no guard was added for a shape the module cannot make. The colliding gauges themselves are untouched: a host that builds a second connector by hand still gets them, and the row below records what that costs |
| A second connector for one endpoint on one registry silently reads the first one's pool gauges and breaker state | T14, restating T11 and R1 finding 6 | Whoever first hosts two connectors against one server | Registering a gauge whose id already exists returns the existing gauge, and `PoolMeters` and `ClientMeters` identify themselves by endpoint alone. Nothing throws; the numbers lie. The fix is a tag - the connector's name beside the endpoint - which is a change to spec 13's meter identity and therefore the maintainer's, not an adapter's. The adapter's own defence is that it cannot produce the second connector |
| `SessionRegistry.lifetime()` draws its jitter from an unseeded `Random`, and the retry's jitter is Resilience4j's | T16 | Whoever next needs a replayable run of the connector | A seeded harness has to set `maxLifetimeJitter = 0.0` and `jitter = false` or its seed does not replay; a `Random` handed in with the clock would keep both the spread and the replay |
| `sortBy` is named in spec 7.4 and is not built | T10 deviation 5, promoted by T17 | Whoever first needs a deterministic poll order | The listing is a `channelFlow` that never materialises a directory, and `sortBy` needs materialisation plus a design nobody asked for. Spec 7.4 now says it is not built; without this row the sentence and the reason part company at the next session |
| `RenameClaim` is a row in spec 7.5's built-ins table and is not built | T10 deviation 10, promoted by T17 | Whoever builds spec 14.2's claim step | It proves nothing on Linux by spec 7.5's own caveat - a rename succeeds while a writer holds the file open - so as a readiness check it would be a check that passes. Its real use is the multi-consumer claim of spec 14.2, where it stops being a readiness check at all |

### C6: spec Sec 5.3 amended - the middle cancellation tier is `keepAlive`

T2 measured that `socketTimeout` bounds nothing: JSch implements `serverAliveInterval` by
setting the socket read timeout, so a positive `keepAlive` always overwrites `session.timeout`.
A stall took 60 s under `socketTimeout = 500 ms` with the default `keepAlive = 30 s`, and 1.2 s
under `socketTimeout = 5 s` with `keepAlive = 300 ms`. This is a measurement, so it outranks
the document: spec Sec 5.3 and D26 were rewritten before ticket 08 could build a ladder on the
false premise. `socketTimeout` stays in the DSL as the knob a reader reaches for, but the real
bound on a hung server is `keepAlive x (serverAliveCountMax + 1)`. Ticket 08 owns deciding
whether to make `socketTimeout` mean something or to remove it.

---

## T1: Walking skeleton: one session through the transport seam

**Built:** `sftpconnector` is now an aggregator with two modules under it. `sftpconnector/core`
(`sftpconnector-core`) holds the configuration DSL, the transport seam and its JSch adapter;
`sftpconnector/testkit` (`sftpconnector-testkit`) holds an embedded Apache MINA SSHD server and a
minimal HTTP CONNECT proxy. A session opens against a real server - directly or through a proxy -
resolves a path and closes, all through the transport interface, and the ArchUnit rules that keep
JSch inside its adapter and frameworks out of the connector run from this ticket onward.

Versions pinned in the parent `dependencyManagement`: mwiede JSch 2.28.7, resilience4j-kotlin
2.4.0, micrometer-core 1.17.1, slf4j 2.0.18, Apache MINA SSHD 2.19.0, ArchUnit 1.3.0.

**Concepts named:**

- **Transport** (`sftp.connector.transport`) is the seam. `SftpTransport.connect()` returns an
  `SftpConnection` offering `realpath` and `close`. Above it there are only paths and strings;
  which SSH library dials the socket, which threads it blocks and how a call in flight is
  cancelled live entirely below it. The remaining operations of spec Sec 5.1 join this interface
  as the tickets that need them arrive - absent rather than stubbed, so nothing above can call a
  method that is not there yet.
- **The JSch adapter** (`transport.jsch`) is the only place a JSch type is named, and it owns the
  bounded IO dispatcher. Its width is `pool.maxSize`, so a server that stops answering pins that
  many threads and no more.
- **Host key policy** is a sealed type with no default: `Strict(knownHosts)` and `AcceptAll`.
  Choosing nothing is a build-time failure, which is the strongest reading of "AcceptAll is never
  the default" and makes the choice a decision someone made rather than one they inherited.
- **The DSL** (`sftp.connector.config`) is the module, not the data classes it produces. Its
  narrow surface is `sftpConnector(name) { ... }`; behind it sits every validation rule, and it
  reports all the faults it finds in one exception rather than one per restart.
- **The testkit** carries `EmbeddedSftpServer` (a real SSH and SFTP server on loopback, key
  generated per instance, port chosen by the OS) and `LoopbackConnectProxy` (reads CONNECT,
  dials, answers 200, copies bytes).

**Acceptance:**

- *Module builds in the reactor with the pinned dependencies* - `mvn -B -fae test` builds
  aggregator, core and testkit green. All five named dependencies are `core` compile
  dependencies.
- *Transport interface; JSch adapter the only implementation and the only package importing
  JSch* - `ArchitectureTest.JSch stays inside the adapter built around it`.
- *Testkit starts embedded MINA SSHD on loopback with a temp-directory filesystem and password
  auth* - `EmbeddedSftpServer`, exercised by every test in `JschTransportTest`.
- *A test opens a session, runs realpath, closes; the reader thread is gone afterwards* -
  `JschTransportTest.a session opens, resolves a path and closes` and `.the session's reader
  thread is gone once the connection is closed`. The latter takes the thread JSch started, closes
  the connection, then joins it: the join returns the instant the thread ends, so it waits on the
  fact under test rather than on a duration, and no test sleeps.
- *ArchUnit: core never imports Quarkus; only the JSch transport package imports com.jcraft* -
  `ArchitectureTest`, three tests. The third asserts the importer actually found the connector's
  classes, because a rule whose subject was never imported passes without checking anything.
- *DSL builds an immutable config; AcceptAll warns at build time and is not the default* -
  `ConnectorDslTest`, seven tests.

**Deviations:**

1. **`HostKeyPolicy.Fingerprint(sha256)` is not implemented.** Spec Sec 5.2 names three policies;
   this ticket ships two. It has no caller and no test in this slice, and adding it as a case the
   adapter refuses at connect time would be a configuration the DSL accepts and the connection
   rejects - the trap this ticket removed elsewhere. Adding it later breaks no one: Kotlin checks
   a sealed `when` for exhaustiveness, so the compiler names every site that must handle it. Debt,
   repaid by the ticket that first needs fingerprint pinning.
2. **The AcceptAll warning is emitted at build time, not at connector startup.** Spec Sec 5.2 says
   "logs a warning at startup" and the ticket's checkbox says "at build time". There is no startup
   path yet, so build time is the earliest the connector exists at all. When the startup probe
   lands, decide whether the warning should repeat there: it is more use in the log at the moment
   the connector begins running than at the moment its config object was made.
3. **The parent pom's new dependency versions are literals, not properties.** Every pre-existing
   entry uses a `${...}` property, but this ticket's scope allowed the parent's `<modules>` and
   `<dependencyManagement>` only, and a property is neither. A later ticket touching the parent
   more freely should convert them. Note also that `archunit-junit5` 1.3.0 is now managed here
   *and* versioned individually by `snapshotcache`, `SimpleEtl` and `gauntlet`, so an ArchUnit
   bump is more than one edit. Converging those was out of scope.
4. **`pool { maxSize }` arrives before the pool.** Three of the block's four knobs
   (`connectTimeout`, `socketTimeout`, `keepAlive`) are consumed by the adapter this ticket
   writes. `maxSize` is consumed too - it is the width of the bounded IO dispatcher, which spec
   Sec 3.3 defines as `pool.maxSize` - but it is otherwise ticket 03's. Defaults follow spec
   Sec 12; the rest of the block is ticket 03's to add.

**For the next ticket:**

- **Errors are raw.** `JschTransport` lets `JSchException` and `SftpException` out untouched.
  `sftp.connector.error.ConfigurationError` exists as a bare `RuntimeException` because the DSL
  needed something to throw; ticket 02 should fold it into the sealed hierarchy under `Fatal`
  keeping the name, which is all `ConnectorDslTest` asserts. The transport tests assert JSch
  exception *types*, never message text - mwiede JSch throws `JSchUnknownHostKeyException` with
  the message "reject HostKey: ...", not the "UnknownHostKey:" wording older JSch used. That is
  spec open item 3 confirmed against the pinned version before a line of the mapping table was
  written: build the table against 2.28.7, never against remembered wording.
- **The ArchUnit JSch rule sees `core`'s main classes only.** It cannot catch a JSch type leaking
  through a *signature* into a caller in another module, which is what raw JSch exceptions
  escaping `connect()` currently are. Ticket 02 closes that by mapping them.
- **`abort()` is absent from `SftpConnection`.** Spec Sec 5.1 lists it and spec Sec 5.3 makes it
  the third cancellation tier; it belongs to ticket 08 with the rest of the ladder.
- **`LoopbackConnectProxy` is reusable.** It is testkit main source, so a ticket needing a stalled
  or half-open proxy can add fault hooks to it rather than reach for Toxiproxy.
- **The full reactor cannot go green on a machine without Docker.** `snapshotcache`'s
  Testcontainers tests fail with "Could not find a valid Docker environment", which skips
  `SimpleEtl`, `composed-host-example` and `etl-host` behind it. Unrelated to this connector and
  untouched; use `mvn -B -fae test` to see the connector modules build in the full reactor.
- **Size:** 900 lines of Kotlin, 596 of them neither blank nor comment; the gap is the KDoc the
  comment rule asks for. At the top of the budget, so ticket 02 should not assume room to spare.

---

## T2: Error model and JSch message mapping

**Built:** Nothing JSch raises leaves the transport any more. `sftp.connector.error` holds the
sealed failure hierarchy, and `sftp.connector.transport.jsch.JschErrorMapper` turns every JSch
exception into one of its classes. `JschTransport.connect()`, `realpath` and `close` all run
through the mapper, so a caller in another module sees the connector's own types and never
`JSchException` or `com.jcraft.jsch.SftpException`. That closes a hole the ArchUnit rule cannot
see: it inspects what `core`'s main classes import, not what their methods throw.

**Concepts named:**

- **`Attempt`** (`error`) is one try at one operation against one server: endpoint, operation,
  path, number. Every failure raised while the connector is running carries one and folds it into
  its own message, so one log line places a failure without reading the lines around it.
  `ConfigurationError` is the single failure without one, because nothing had been attempted yet.
- **`Disposition`** (`error`) is the seam this ticket's design work went into. Spec Sec 10.2 is a
  table of four decisions - retry, breaker, lease, what `watch` does - and the four only make
  sense together. Rather than exposing `recoverable`, `poisons` and `fatal` for callers to
  combine, every failure answers with one of six named `Disposition` constants, each carrying all
  four answers (`Retry`, `LeaseFate`, `WatchReaction`). A caller reads one value and obeys; the
  day a row changes, it changes in one place instead of in every caller that guessed. `poisons`
  survives on `Recoverable` because spec Sec 10.1 fixes it there, but it is now an input to the
  disposition rather than something a caller is expected to interpret.
- **The mapper** is a deep module with one method. `translating(attempt) { ... }` is its whole
  surface, and running a JSch call through it is what makes forgetting the cancellation rule
  impossible rather than merely unlikely - there is no way to reach the table without it. It lives
  in `transport.jsch` because that is the only package allowed to name a JSch type, which is also
  the only sensible home for the knowledge of what JSch's messages mean.
- **Two testkit fault hooks.** `EmbeddedSftpServer.start(offersSftp = false)` authenticates and
  then refuses the sftp subsystem; `LoopbackConnectProxy.stall()` stops relaying bytes while
  keeping both sockets open and still draining the sender, so the peer neither answers nor hangs up.

**The table, as measured against mwiede JSch 2.28.7.** Every row below was produced by staging the
real condition and reading what came out. Nothing here is remembered wording.

| Condition staged | JSch threw | Message observed | Maps to |
|---|---|---|---|
| wrong password | `JSchException` | `Auth fail for methods 'password,keyboard-interactive,publickey'` | `AuthenticationFailed` |
| strict policy, empty known_hosts | `JSchUnknownHostKeyException` | `reject HostKey: [127.0.0.1]:59131` | `HostKeyRejected` |
| socket accepted, nothing ever spoken | `JSchException` | `Session.connect: java.net.SocketTimeoutException: Read timed out` | `ConnectFailed` |
| port with nothing listening | `JSchException` | `java.net.ConnectException: Connection refused: getsockopt` | `ConnectFailed` |
| name that does not resolve | `JSchException` | `java.net.UnknownHostException: no.such.host.invalid` | `ConnectFailed` |
| proxy port with nothing behind it | `JSchProxyException` | `ProxyHTTP: com.jcraft.jsch.JSchException: java.net.ConnectException: Connection refused: getsockopt` | `ConnectFailed` |
| server without the sftp subsystem | `JSchException` | `failed to send channel request` | `ConnectFailed` |
| tunnel stalls under a live request | `SftpException` id 4 | `java.io.IOException: inputstream is closed` | `SessionLost` |
| server killed under a live session | `SftpException` id 4 | `java.io.IOException: Pipe closed` | `SessionLost` |

Two of these are worth keeping in mind. The host key and proxy failures have exception types of
their own in this fork, so they are matched by type rather than by wording and a rewording cannot
silently reclassify them. And **the two transport breakages arrive as `SftpException` with the
generic `SSH_FX_FAILURE` code and an `IOException` cause** - the same type and code the server
uses for its own refusals. Mapping by status code alone would have called a dead socket a server
failure and handed a broken session to the next caller, so the mapper checks the cause first.

The three connect-phase rows show JSch stringifying the underlying socket exception into its own
message and then replacing the cause with a copy of itself. The text is the only place the real
fault survives, which is why the `java.net.` marker in the message is what that row matches on.

**Acceptance:**

- *Sealed hierarchy exactly as spec Sec 10.1, each class carrying a poisons flag where applicable* -
  `error/SftpException.kt`. `FailureModelTest.every failure class lands on the row the failure
  model puts it on` walks all twelve; its `rowOf` is a `when` over the sealed type, so a class
  added later without a decided behaviour fails compilation rather than reaching production
  undecided.
- *Mapper is one class; a table entry for auth fail, unknown host key, connect timeout, socket
  timeout, session down, proxy failure, channel not opened* - `JschErrorMapper`, one class, one
  public method.
- *One embedded-server test per table row triggers the real condition* -
  `JschErrorMappingTest`, nine tests, all against the embedded server or a real socket. Its
  `failureFrom` helper insists on the connector's own exception type, so every row also proves the
  transport seam holds. One row is not staged there and says so in the code: `session is down` is
  unit-tested in `JschErrorMapperTest` instead, because the transport opens its channel during
  connect and nothing yet holds a live session to ask a second channel of.
- *Unmapped message maps to Unknown with the raw message preserved, WARN logged,
  sftp_error_unmapped_total incremented* - `JschErrorMapperTest.a wording the table has never seen
  keeps its raw text, warns, and is counted`, which reads the warning off standard error the way
  an operator would, and `.a wording the table knows is not counted as unmapped`.
- *CancellationException is never wrapped* - `JschErrorMapperTest.a cancellation passes through
  untouched` asserts identity, not just type.
- *Progress entry appended* - this.

Two named scenarios are proven at this layer: `S10_` (wrong password is fatal, breaker untouched,
watch stops) and `S2_` (a stalled tunnel is `SessionLost`, poisoned, counted). `I10_a fatal failure
stops the watch and no other failure does` runs over every class in the hierarchy.

**Deviations:**

1. **One assertion in T1's `JschTransportTest` was changed.** `a strict host key policy refuses a
   server whose key it has never seen` asserted `JSchUnknownHostKeyException`; it now asserts
   `HostKeyRejected`. T1's own note said the JSch type was worth pinning only "until" the mapping
   existed, and that mapping is this ticket. The assertion is stronger afterwards, not weaker: the
   JSch type arriving there would now mean the seam had leaked. Flagged because the standing rule
   is not to touch an earlier ticket's tests.
2. **`ServerFailure.poisons = true` is a reading, not a ruling - please confirm.** Spec Sec 10.1
   annotates `poisons = false` on `PermissionDenied` and `NoSuchFile` and marks `Unknown` as
   poisoning, and says nothing about `ConnectFailed`, `SessionLost`, `OperationTimeout` or
   `ServerFailure`. This ticket read silence as poisoning. For `ServerFailure` that is arguably
   wrong: a well-formed `SSH_FX_FAILURE` proves the channel parsed the request and answered, so
   the session is demonstrably healthy and is being thrown away for a per-request refusal - and
   spec Sec 8.2 expects exactly that status from a server without `posix-rename`. Real transport
   breakage no longer needs this flag to be caught, because the `IOException`-cause rule sends it
   to `SessionLost` first. Left as-is rather than decided alone; a one-word change if the
   maintainer agrees.
3. **`OperationTimeout` has no producer.** It is in the hierarchy because spec Sec 10.1 puts it
   there, and it is exercised by the failure-model tests, but nothing raises it yet: it belongs to
   the time limiter in the resilience ticket.
4. **`Attempt.number` is always 1 from the transport.** The transport is told nothing about
   retries. The layer that decides to try again is the layer that knows which try it is, and it
   will have to build the `Attempt` or renumber the failure; `connect()` has no parameter to
   thread it through, and the signature is fixed.
5. **`PoolExhausted` and `CircuitOpen` take only an `Attempt`** and carry a fixed message. The
   tickets that raise them should add whatever detail is worth saying - how long the acquire
   waited, how long the breaker has been open. Note also that spec Sec 9 calls the second one
   `CircuitOpenException` while spec Sec 10.1 calls it `CircuitOpen`; the hierarchy's name won.
6. **`sftp_error_unmapped_total` is registered as a literal metric name**, not in Micrometer's
   dotted convention, because spec Sec 13 fixes the name and the ground rules forbid inventing one.
   A registry that renames meters by convention would leave this one alone.
7. **Size.** About 660 lines of Kotlin that is neither blank nor comment across three main files
   and three test files, against a 200-600 budget. Twelve of those classes are the hierarchy spec
   Sec 10.1 fixes and are one or two lines each; the largest single file is the nine-row
   embedded-server test. Nothing here looked like it would get simpler by being smaller, but the
   ticket did run over and the next one should not assume slack.

**For the next ticket:**

- **`keepAlive`, not `socketTimeout`, is what unblocks a stalled read - spec Sec 5.3 is wrong about
  this, and it is ticket 08's to settle.** JSch implements `serverAliveInterval` *by* setting the
  socket read timeout, so it overrides `session.timeout` whenever it is set - and the DSL requires
  `keepAlive` to be positive, so it is always set. Measured: with `socketTimeout = 500ms` and the
  default `keepAlive = 30s`, a stalled tunnel took **60 seconds** to fail, which is
  `keepAlive x (serverAliveCountMax + 1)` and has nothing to do with `socketTimeout`. With
  `socketTimeout = 5s` and `keepAlive = 300ms` the same stall failed in 1.2 seconds. So today
  `socketTimeout` is dead configuration, and the real bound on a hung server is the keepalive
  ladder. Spec Sec 5.3's "socket timeout" tier should be restated in those terms before the
  cancellation ladder is built on it.
- **The same value bounds the key exchange**, which is a trap for any test that shortens it. A
  `keepAlive` below the handshake time fails `connect()` with
  `timeout in waiting for rekeying process.` instead of failing the read. That wording was
  observed but is deliberately *not* in the table: it only appears under a misconfiguration, it
  cannot be staged reliably, and `Unknown` already handles it correctly and visibly. The `S2_` test
  works around it with a throwaway connection that warms the first key exchange in the JVM; four
  consecutive full runs were clean.
- **A `@Test fun x() = runBlocking { ... }` whose last expression is not `Unit` is silently not
  run.** JUnit 5.11 does not report it - the class simply shows fewer tests than it has. Four of
  the nine tests in `JschErrorMappingTest` were being skipped this way and were only found by
  counting. Every test in that file now says `runBlocking<Unit>`. Worth doing everywhere.
- **`NoSuchFile` is retried and counted against the breaker**, because spec Sec 10.2 puts every
  recoverable failure in the "counted" column. Scenario S5 wants a file that vanished between list
  and download to be `FileGone` with no error and no retry, so the source or client layer has to
  turn `NoSuchFile` into `FileGone` *before* it reaches the retry ladder. Left as the spec has it;
  a directory another system is writing into would otherwise open the breaker on its own.
- **Run every JSch call through `translating`.** It rethrows `CancellationException` by identity,
  leaves `Error` alone, and returns an already-classified failure untouched, so nesting it is
  safe - a decided failure is never reburied inside an `Unknown`.
- **`LoopbackConnectProxy.stall()` and `EmbeddedSftpServer(offersSftp = false)`** are testkit main
  source and available to any ticket that needs a silent peer or a server that refuses SFTP.

---

## T3: Pool core: registry, entry states, acquire and release

**Built:** `sftp.connector.pool` exists. A caller borrows a session from a bounded pool and gives it
back, the pool opens one when there is nothing on the shelf, and every session it holds is counted in
one place under one lock. `sftpconnector/testkit` gained `FakeSftpTransport`, a transport that answers
from a script, so the pool is proven with no socket, no server and no wall clock.

A separate first commit applied coordinator decision C4: `ServerFailure.poisons` is now `false`, with
`FailureModelTest`'s row table moved to match and the mapper test strengthened to assert the session
survives a status refusal. T2's open question is closed.

**Concepts named:**

- **`SessionRegistry`** (internal to `core`) is where this ticket's design went. It *decides and never
  acts*: every method answers a question - which session does this caller get, may this one go back on
  the shelf - and returns before anything slow happens. The lock is private to it and it is handed no
  transport, so dialling a server while the pool is locked cannot be written by mistake; it would first
  have to be made possible by giving the class a transport it has no other use for. `handBack` returns
  *the connection to close* rather than closing it, which is how the one piece of I/O a handback implies
  gets carried out of the lock. This is what makes I5 structural rather than remembered.
- **`Checkout`** is the answer `checkOut` gives: `Reuse` or `Dial`. A sealed answer rather than a
  session plus a flag, so the caller has no decision left to take - it does the one thing its answer
  names. Ticket 05 adds the third case, `Prove`.
- **`PoolEntry` and `EntryState`.** The entry outlives any one borrowing, which is what lets the pool
  talk about a session it has not opened yet or one it has decided to throw away but not yet hung up
  on. The three transitional states are the whole reason no lock is held across a round trip.
- **`Lease`** never asks its holder what state the session is in. `releaseAfter(failure)` reads
  `failure.disposition.lease` and obeys it; there is no poison flag for a caller to set, and so no
  caller can set it differently from the caller next door. Anything unclassified - an application
  error, a cancellation - evicts, because a session nobody has vouched for is not worth handing on.
- **`PoolStats`** is one consistent reading taken under the lock, not three reads of a moving target.
  It is also the observation point I1 is asserted at, and the question whose *answer arriving at all*
  proves I5.
- **`FakeSftpTransport`** has one hook, `answer: suspend (Call) -> Unit`, and it is the whole scripting
  surface. Suspending in it is a slow server, throwing from it is a failing one, and asserting in it is
  a test of what the caller may do while it waits - which is exactly how I1 and I5 are proven. Its
  `Operation` is an enum, not a string, because a test that filters the call record for a misspelled
  operation finds nothing and reports that nothing happened, which is what a passing test looks like.

**Acceptance:**

- *Entry states as a StateFlow per entry* - `EntryState` has all six; `an entry publishes the states it
  passes through` walks InUse to Idle, then InUse to Closed through `Lease.state`. `Validating` has no
  producer yet and belongs to ticket 05.
- *Acquire pops the most recently used idle entry or registers a Connecting entry and connects outside
  the lock* - `the first caller opens a session and the next one gets it back` (one connect for two
  borrows, LIFO from the deque's end) and `a session that never opened is not left occupying the pool`.
- *Release pushes to the idle deque and releases the permit last* - `what a failure says about the
  session is what happens to it`, and `a lease given back twice is ignored the second time`, which
  proves no permit was invented by asserting the next acquire beyond capacity still blocks.
- *Fake transport with scripted connect success, failure and delay via hook points* -
  `FakeSftpTransport`; failure in `a session that never opened...`, delay in `a connect cancelled
  halfway leaves the pool all of its capacity`.
- *I1, I2, I5* - `I1_idle plus inUse plus connecting never exceeds maxSize`, `I2_an entry is handed to
  at most one lease at a time`, `I5_no transport call executes while the registry lock is held`.
- *Progress entry appended* - this.

**How the three invariants are enforced, not merely asserted.** Each was checked by breaking the pool
and confirming that its own test - and, for I5, only its own test - went red.

- **I1** is the semaphore, taken before an entry exists and released after it is handed back, so a
  session being opened occupies capacity exactly as much as one being used. Widening it to
  `maxSize + 2` fails I1 at the hook with `sessions accounted for during Call(operation=Connect...)`.
- **I2** is the idle deque: an entry leaves it under the lock and returns only through a handback, so
  two callers cannot hold the same one. Changing `removeLastOrNull()` to `lastOrNull()` fails I2 with
  `fake session 5 was already lent to someone else`.
- **I5** is the private lock plus the transport-free registry, and the test proves it with the one
  question only an unlocked pool can answer: `stats()` needs the same non-reentrant mutex, so a
  transport call made from inside it could never get a reply. Adding a method that closes a connection
  under the lock fails I5 and nothing else. The timeout turns what would be a deadlock into a red test
  rather than a hung one.

**Deviations:**

1. **Two of ticket 04's checkboxes are done here, deliberately and declared.** `withLease` releasing on
   every exit path, and a second release being logged and ignored, are 04's checkbox 2. They are here
   because "release it back" is this ticket's own deliverable and there is no safe way to express or
   test it otherwise: without the use-block every test leaks a permit on its failure path, and without
   the release-once guard a double release silently invents capacity. Poison eviction (04's checkbox 3)
   is likewise here, because this ticket's own statement says "connect **and close** happen in the
   transitional states outside the lock" - the close path is the Evicting mechanism. **Ticket 04 still
   owns:** the bounded wait and `PoolExhausted` with statistics, the meters, I3, I4, and the
   embedded-server demo. Its checkboxes 2 and 3 should be read as done and re-verified, not rebuilt.
2. **Acquire waits without a bound.** `acquireTimeout` is ticket 04's checkbox 1 and is not implemented,
   so `capacity.acquire()` suspends until a permit frees. No caller of the pool exists yet, so nothing
   can hang on this today, but it must not stay that way past 04.
3. **Validation on borrow is not here.** It is ticket 05's checkbox 2 verbatim, so `validationBypass`,
   `Checkout.Prove` and the realpath round trip were built, reviewed against the ticket boundary, and
   removed again. `EntryState.Validating` stays in the enum because this ticket fixes the six states,
   and `SessionRegistry.stats` already counts a validating entry as in use, so 05 adds a producer
   rather than an accounting rule. The pool takes no `Clock` yet for the same reason.
4. **No pool meters.** `sftp_pool_active`, `_idle`, `_pending`, `_created_total` are 04's checkbox 6.
   `PoolStats` is the shape they will read from, and it has no `pending` count yet because nothing
   counts waiters until the bounded wait exists.
5. **No `close()` on the pool.** Sessions opened stay open. Graceful shutdown is ticket 13 and I9 is
   its invariant; adding a half-shutdown here would be a seam nobody had designed. Absent rather than
   stubbed, following T1's precedent for transport operations.
6. **No new configuration knob**, so the standing rule about knobs landing in the DSL with build-time
   validation has nothing to apply to. `pool.maxSize` was already there from T1.
7. **Size.** 674 lines across five files, 385 of them neither blank nor comment, inside the 200-600
   budget on the measure the earlier entries used. The first draft was about 890 and over the top of it;
   the overage was precisely the two tickets' worth of work removed in deviations 2 and 3, which is
   worth knowing: the budget noticed the scope error before the review did.

**For the next ticket:**

- **Read deviation 1 before starting 04.** Two of its six checkboxes are already green.
- **`releaseAfter` evicts on `LeaseFate.NONE_HELD`,** which the failure model marks "n/a" rather than
  "evicted" for `PoolExhausted` and `CircuitOpen`. Neither can be raised by a caller that is holding a
  lease, so the case is unreachable today and the reading is the safe one. Ticket 04 owns the lease
  contract and should either confirm it or make `NONE_HELD` keep the session.
- **`Semaphore.acquire()` is cancellation-safe** and gives the permit back itself if the coroutine is
  cancelled between being granted one and resuming, so 04's bounded wait can be `withTimeoutOrNull`
  around it without leaking. The cleanup path already in `acquire` runs under `NonCancellable`, which
  it must: taking the registry's lock is a suspension, and a cancelled coroutine cannot wait for a lock.
  Anything 04 or 13 adds to a release path needs the same treatment.
- **`close()` in the pool catches `Exception`, never `Throwable`.** An `AssertionError` thrown by a
  testkit hook inside a close would otherwise be swallowed and the invariant tests would pass on the
  close path without checking anything. That is not a style choice; it was found by making I5 fail.
- **`FakeSftpTransport` is testkit main source** and takes one hook. A ticket needing a session that
  dies while parked, a connect that hangs, or an assertion about what the caller was holding at the
  moment of a call should reach for that hook rather than add a second mechanism.
- **`kotlinx-coroutines-test` is now managed in the parent pom** and is a test dependency of `testkit`.
  Virtual time is how a test proves something did *not* happen without waiting for it.
- **The `autoCreate` to `createActionTargets` rename landed while this ticket was being written** and
  is not part of it. Nothing in the pool reads that knob, so the two changes do not touch.

---

## T4: Lease contract, acquire timeout and poison eviction

**Built:** Waiting for a session now ends. `acquire` takes its permit within `pool.acquireTimeout` or
raises `PoolExhausted` carrying what the pool looked like at that moment and, in words, which of three
different faults those numbers describe. The pool publishes the six meters spec Sec 13 names for it,
through a `MeterRegistry` seam that defaults to a private `SimpleMeterRegistry`, following the
transport's precedent. I3 and I4 have tests. One real leak was found and closed on the way: a session
that finished opening into a coroutine cancelled a moment earlier was left open and unowned.

**Concepts named:**

- **Admission** (`SftpPool.admit`) is the door, and the design work went into what it says when it
  refuses. "The pool was full" is the class, not the message; full because the server has stopped
  completing handshakes, full because the work already holding the sessions is not finishing, and full
  because there are not enough sessions for the load are three faults with three different remedies,
  and an operator woken at three in the morning has to be able to tell them apart from one line. The
  first is visible in `connecting` against `inUse`; the other two are separated by **whether room came
  free at all during the wait**, which is the one fact the three counts alone cannot supply. So the
  pool keeps a monotonic count of rooms freed, a waiter reads it at both ends of its wait, and
  `explainExhaustion` turns the four numbers into the sentence naming which fault they are. The
  statistics are the message; `PoolExhausted` carries them as fields as well, but no operator should
  have to reach for them.
- **`freeRoom()`** is the single place a permit goes back. Two paths let a caller go - a handback and
  a failed acquire - and having them share one method is what stops the count of rooms freed from
  drifting from the permits that were actually freed. The first draft had the count on one path only,
  which made a waiter that had queued behind failing connects report the wrong one of the three faults.
- **Pending is contention, not traffic.** `admit` tries for the permit without queueing first and only
  joins the waiters if that fails, so `sftp_pool_pending` sits at zero on a pool that is keeping up. A
  gauge that ticks whenever anything happens is one nobody can alert on.
- **`SessionRegistry.lastCount`** is the reading a gauge takes. `stats()` still suspends and still
  takes the lock, because I5's test is built on the fact that only an unlocked pool can answer it -
  but a Micrometer gauge is sampled from a thread that cannot suspend and must never be made to wait
  on the pool, so the registry republishes its count under the lock after every change and the gauges
  read that. Between two changes the published reading is not stale, it is still true, because nothing
  else can alter what it counts. Only the waiters move on their own, and they are counted fresh.
- **`PoolMeters`** owns the names, the endpoint tag and the gauge wiring, and exposes four
  intention-named methods rather than its meters. It lives in `pool` rather than in a `metrics`
  package because it is one pool's private instrumentation; the later layers' meters belong beside
  their own layers for the same reason, and a `metrics` package would be a bag of unrelated counters.

**The ruling on `LeaseFate.NONE_HELD`, which T3 left open: it keeps the session.**

T3 read the unreachable case as evicting and asked for confirmation. The reading is now the other one.
Two reasons. `NONE_HELD` is the failure's own statement that what went wrong was not about any session,
which is exactly what spec Sec 10.2 means by "n/a" in the lease column for `PoolExhausted` and
`CircuitOpen` - it is an answer, not silence to be filled in. And the one way such a failure can reach
a lease holder is a second acquire failing inside the first: destroying a healthy session at the moment
the pool has just told somebody it has none would feed the shortage that caused the failure. Anything
the connector never classified at all - an application error, a cancellation, an `Error` - still
evicts, because that is a session nobody has vouched for, which is a different case entirely.
`I3_a poisoned entry never returns to the idle deque` pins the ruling: putting `NONE_HELD` back with
`EVICTED` fails it at `PoolExhausted`.

**Acceptance:**

- *acquire waits at most acquireTimeout then throws PoolExhausted with pool statistics* -
  `LeaseSemanticsTest.a caller that cannot be served is turned away rather than left queueing`. It
  asserts the wait against the scheduler's own clock rather than against the exception's account of
  itself, because a bound that was never applied would report the configured timeout just as happily
  as one that was. `.the exhaustion message names which of the three reasons the pool was full` stages
  all three: a pool stuck dialling, a pool whose holder is not finishing, and one session wanted by
  two callers.
- *use-block releases in finally; a second release is logged and ignored* - **built by T3, verified
  here, not rebuilt.** T3's `a lease given back twice is ignored the second time` and `what a failure
  says about the session is what happens to it` still pass unchanged, and the double release is
  exercised again as one of the exit paths in `I4_`.
- *A poisoned lease's entry transitions to Evicting on release and is closed outside the lock* -
  **built by T3, verified here, not rebuilt.** T3's `an entry publishes the states it passes through`
  and `I5_no transport call executes while the registry lock is held` still pass unchanged. `I3_` adds
  the assertion that the entry reaches `Closed` and that its session is never handed to anyone again.
- *Cancellation during Connecting releases the permit and closes the half-open entry* - T3's `a connect
  cancelled halfway leaves the pool all of its capacity` **already covers the case where cancellation
  lands while the dial is in flight**, and it does still pass. It does not cover the other half, and
  that half was broken: cancellation arriving in the gap between `connect()` returning and the entry
  being told about it left a live session that no entry owned, so nothing ever closed it. `a session
  that opens into a cancelled caller is closed rather than left running` stages it with the transport
  hook cancelling its own caller, and it fails without the fix.
- *I3 and I4* - `I3_a poisoned entry never returns to the idle deque` and `I4_every permit is released
  exactly once on every exit path`. See below.
- *The six meters* - `the pool publishes what a dashboard needs to watch it fill up` reads every one of
  them off a `SimpleMeterRegistry`, asserts the registry holds those six and no others, and asserts
  every one carries the endpoint tag.
- *Demo against the embedded server* - `PoolAgainstServerTest.two callers hold two sessions at once and
  the third is told why there is no more`. Two real JSch sessions to a real MINA SSHD, both answering
  `realpath` at the same time, a third caller refused after a real 300 ms wait, and the pool still
  serving the moment one comes back. It is the one test here whose wait is not virtual, because the
  wait is the subject.
- *Progress entry appended* - this.

**How I3 and I4 are enforced, not merely asserted.** Both were checked by breaking the pool and
watching the right test go red.

- **I3** is `handBack`: eviction moves the entry to `Evicting`, drops it from the registry and takes it
  out of the deque, all under one lock, and a handback is the only way back onto the shelf. The test
  proves it by asking for the session again, which is the only thing "never returns to the deque"
  actually means to a caller: an evicted one is gone for good and a kept one comes straight back.
  Changing `idle.remove(entry)` to `idle.addLast(entry)` fails I3 at `ConnectFailed`. The loop walks
  every failure class and asks each one what it wants rather than hard-coding a table, and it first
  asserts that the classes between them ask for all three fates - otherwise it would be checking one
  branch twelve times.
- **I4** is the two release sites, both now going through `freeRoom()`, plus the lease's release-once
  guard. The test runs ten exit paths - success, a returning failure, a poisoning failure, an
  unclassified error, a hand-released lease, a double release, a failed connect, cancellation during
  the dial, cancellation after it, and a wait that ran out - and then asks the pool what it can still
  lend. Filling to `maxSize` proves nothing was lost; being refused at `maxSize + 1` proves nothing was
  invented. Removing `ensureActive()` fails it, and so does the deque change above.

**Deviations:**

1. **`sftp.connector.error` now imports `sftp.connector.pool.PoolStats`**, so the two packages know
   about each other. Accepted deliberately. `PoolStats` is the type for "what the pool holds", and the
   alternative to importing it was a second stats-shaped type in `error` holding the same four numbers,
   or four loose ints in the exception's signature - both worse than one import inside one module. Note
   that `error` already carries pool vocabulary in the other direction: `LeaseFate` exists only to tell
   the pool what to do with a session. Appealable, and the appeal would have to say what the second
   type buys.
2. **`sftp_pool_active` counts `inUse + connecting`.** Spec Sec 13 fixes the six names and the ground
   rules forbid inventing a seventh, so there is no gauge for half-open sessions. Lumping them in with
   the lent-out ones is what makes `active + idle` equal everything the pool holds, so no session can
   go missing from a dashboard by sitting in a state no gauge was given. The distinction the gauges
   cannot show is in `PoolStats` and in the exhaustion message, which is where an operator looks once
   a gauge has told them something is wrong.
3. **`sftp_pool_acquire_seconds` records only the waits that ended in a session.** A wait that timed
   out lasted exactly `acquireTimeout` by construction, and mixing that constant into the distribution
   would drag every percentile toward a number the configuration already fixes. The refusals are
   counted separately, which is what `sftp_pool_acquire_timeout_total` is for.
4. **`PoolExhausted`'s three new parameters have defaults**, so `PoolExhausted(attempt)` still compiles
   and still produces its old fixed sentence. That is what keeps T1 and T2's `FailureModelTest`
   untouched, which the standing rule requires. `PoolStats.pending` has a default for the same reason:
   T3's tests compare against three-argument `PoolStats` values. Both defaults are honest rather than
   merely convenient - the fallback message claims nothing it does not know.
5. **The pool builds its own `Attempt`, with the operation `acquire`.** Nothing tells it what the
   caller was going to do with the session. The endpoint and the operation are both true - the pool was
   acquiring - but a log line would read better naming the caller's own operation, and that needs a
   parameter on `acquire()` that no caller exists to pass yet. Debt, repaid by the ticket that first
   has a caller with an operation to name.
6. **One test method was added to T1's `ConnectorDslTest`** rather than a second DSL test class, for
   the new knob's build-time validation. Additive only; no existing test in that file was touched, and
   no assertion anywhere was weakened.
7. **`explainExhaustion`'s third reading can be too generous.** "Room came free and other callers took
   it" is also what a pool churning through poisoned sessions looks like, and the remedy there is not a
   bigger pool. It is the residual branch after the two the numbers can identify, and it states what it
   observed before drawing its conclusion, so the counts beside it still tell the truth.
   `sftp_pool_created_total` is the meter that separates the two, and a later ticket wanting the third
   branch sharper should read it here rather than add a fourth count.
8. **Size.** About 380 lines that are neither blank nor comment across three new files and seven
   modified ones, inside the 200-600 budget. Two of the six checkboxes were already green, which is
   most of why.

**For the next ticket:**

- **`SessionRegistry` now takes a `pendingWaiters: () -> Int`.** It counts what the pool holds; the
  waiters are not something it holds, so it is told that number rather than guessing zero. Ticket 05's
  `Validating` producer changes nothing here - `stats` already counts a validating entry as in use.
- **`stats()` and `lastCount` answer the same question two ways, on purpose.** `stats()` suspends and
  takes the lock, and I5's test depends on exactly that. `lastCount` does not suspend, because a
  metrics gauge cannot. Add a new meter by reading `lastCount`; add a new assertion by reading
  `stats()`.
- **Anything that releases a permit must go through `freeRoom()`.** The count it keeps is what a waiter
  reads to decide which fault to report, and a release that skips it makes the pool tell an operator to
  look in the wrong place. Ticket 13's shutdown will have permits to release.
- **`acquire()` now ends with `coroutineContext.ensureActive()`**, so a cancelled caller is never
  handed a lease it will not release. Ticket 05's validation loop goes between `filled` and that line,
  and a failed validation that loops back to `checkOut` must keep the permit, per spec Sec 4.2 step 3.
- **The three-way reading in `explainExhaustion` is a contract two tests assert on by wording.** If a
  later ticket rewords it, `LeaseSemanticsTest.the exhaustion message names which of the three reasons
  the pool was full` and `PoolAgainstServerTest` both need the new words. The wording is the
  deliverable, so that is the right place for it to be pinned.
- **`FakeSftpTransport`'s single hook took every staging this ticket needed**, including a hook that
  cancels its own caller (`onCall = { afterConnect.cancel() }`) to land cancellation inside a
  one-statement window. A mutable `var onCall` reassigned between phases is how one pool exercises ten
  exit paths.
- **A pool with something idle does not dial.** `I4_`'s first draft failed because a bare release left
  a session on the shelf and the next three phases reused it instead of taking the dialling paths they
  were written for. A test about connecting has to empty the shelf first, and assert that it did.

---

## T5: Housekeeper, lifetime jitter, keepalive and validation on borrow

**Built:** The pool now looks after itself. A session that has been parked long enough to have been
dropped without either end noticing is asked whether it is still there before it reaches a caller, and
replaced when it is not. A session past its own lifetime is retired the moment it comes back. A
coroutine on a timer retires spares nobody has wanted, keeps the number of spares the pool was told to
keep, and says where a lease was taken that nobody has given back. The nine knobs all of that needs
landed in the DSL with build-time validation, including the two the proxy's idle cutoff dictates.

**Concepts named:**

- **`Retirement`** is why a session left, and it is where this ticket's design went. `handBack` used to
  take a boolean, which forced its caller to decide whether a healthy-looking session could go back on
  the shelf - and a caller cannot know, because it does not hold the clock. It now takes a reason or
  null, and the registry supplies `LIFETIME` itself when the entry has outlived its own. That is what
  makes I6 structural: handing back is the only door onto the shelf, and the door checks. The reason
  then travels out to `sftp_pool_evicted_total{reason}`, whose five labels are the enum's own, so two
  eviction sites cannot spell one number two ways and halve it on a dashboard.
- **`Checkout.Prove`** is T3's third case, filled in. A parked session is claimed but not handed over
  until it has answered, which is why the claim happens under the lock and the question does not.
- **`SessionRegistry.sweep`** is one method that decides a whole housekeeping round - what to retire,
  who has held too long, how many to open - and carries out none of it. One decision because it is
  taken against one reading: retiring sessions and then asking separately how many are left would read
  a pool that had moved in between, and top up for a shape it was never in.
- **`Leak`** carries the entry, how long it has been out, and the stack trace that took it, all
  captured under the lock. The trace read afterwards can be gone, because the caller may have handed
  the session back in the meantime - and a leak report without its trace says only what the pool's
  numbers already said.
- **`SftpPool.housekeep()`** is the whole housekeeper: one suspend function, no parameters, running
  until cancelled. Not a `Housekeeper` class, because its implementation would have been `delay` plus
  a call, and the complexity would have moved rather than vanished.

**Acceptance:**

- *maxLifetime with per-entry uniform jitter; idleTimeout honoured only above minIdle; minIdle top-up
  in the background* - `HousekeeperTest`: `lifetime jitter never retires a session early and never
  keeps one past the window`, `the housekeeper hangs up on a spare nobody has wanted since the idle
  timeout`, `the spares the pool was told to keep survive the idle timeout`, `the housekeeper opens
  sessions until the pool holds the spares it was told to keep`, and `the housekeeper never opens a
  session the pool has no room for`.
- *Validation on borrow after validationBypass via realpath; failed validation closes the entry and
  acquire loops with the permit held* - `ValidationOnBorrowTest`, three tests. The permit is proved by
  reading `sftp_pool_acquire_seconds`: two acquires, two trips through the door, not three.
- *Keepalive set on every session at the configured interval* - **built by T1, proved here, not
  rebuilt.** `JschTransport` has set `serverAliveInterval` since T1 and nothing tested it, because the
  only proof is a peer hearing it. `SessionHealthAgainstServerTest.a session keeps speaking on its own
  at the interval it was given` gives the embedded server a global-request observer and waits for an
  idle session to speak unprompted.
- *Leak detection logs the acquire stack trace once and never forces release* - `HousekeeperTest.a
  lease held past the threshold is reported once, with the stack that took it, and is not taken back`.
  It reads the warning off standard error the way an operator would, counts the occurrences over
  eleven rounds of housekeeping, and then uses the session and gives it back by hand.
- *DSL validation rejects keepAlive >= idleCutoff and idleTimeout >= idleCutoff (I14)* -
  `ConnectorDslTest.I14_a keepalive or an idle timeout that outlasts the path's idle cutoff is
  refused`, added to T1's file, additive only.
- *I6* - `HousekeeperTest.I6_a session past its lifetime is closed when it comes back and never lent
  again`.
- *Demo against the embedded server* - `SessionHealthAgainstServerTest.a session the server killed
  while it was parked is replaced before the caller sees it`. A real session, killed server-side while
  on the shelf, and the next caller is handed a working one with
  `sftp_pool_evicted_total{reason=validation}` at 1.
- *Progress entry appended* - this.

**How I6 and I14 are enforced, not merely asserted.** Both were checked by breaking the pool and
watching the right test - and only the right test - go red.

- **I6** is the lifetime check inside `handBack`, which is the only way back onto the idle deque.
  Replacing `failed ?: Retirement.LIFETIME.takeIf { now >= entry.expiresAt }` with `failed` fails
  `I6_` at "expected: Closed but was: Idle", and takes the jitter test's upper bound with it, which is
  the same rule read from the other side. The test asks for the session again afterwards, because
  "never reused" means nothing except to the caller who would have been handed it.
- **I14** is two lines in the DSL. Making both conditions `false` fails `I14_` with "Expecting code to
  raise a throwable" and nothing else. The test also asserts that the shipped defaults already sit
  under the cutoff, so a connector nobody tuned is correct rather than merely valid.

Two more breaks were staged for rules this ticket added rather than inherited. Removing the
housekeeper's `entries.size < config.maxSize` bound fails `the housekeeper never opens a session the
pool has no room for` with three sessions in a pool of two - an I1 violation, because the spares the
housekeeper opens are held by no caller and so nothing a caller does can bound them. Making a failed
validation release and retake its permit fails `a session that cannot answer is replaced without the
caller losing its place` at three trips through the door instead of two.

**On watching entry state, which the maintainer flagged.** Nothing here collects `Lease.state` or
`PoolEntry.state`, undispatched or otherwise. The housekeeper and the validation loop both read
`state.value`, which is a plain volatile read and resumes nobody. The hazard - assigning
`MutableStateFlow.value` synchronously running an undispatched collector's body on the setter's stack,
inside the registry's lock - is therefore still theoretical after this ticket, and still worth the
next one's attention.

**Deviations:**

1. **`sftp_pool_evicted_total` and `sftp_pool_leak_total` are registered on first use, not at
   startup.** For the eviction counter this is Micrometer's own way with a tagged counter and it is
   right on its own terms: a dashboard then shows the reasons this deployment has actually seen rather
   than every reason the connector can name, and `reason=shutdown` has no producer until ticket 13
   anyway. The leak counter follows it for a weaker reason - registering it eagerly would have made
   T4's `the pool publishes what a dashboard needs to watch it fill up` fail, and that test asserts
   the pool's meters exactly. The cost is real: a dashboard shows no series for leaks until the first
   one, so an alert has to treat absent as zero. Debt, repaid by whichever ticket next has cause to
   revisit that assertion with the maintainer.
2. **The housekeeper does not flag in-use entries for eviction.** Spec 4.5 says entries in use past
   `maxLifetime` are flagged and evicted on release; `handBack` computes expiry from the clock
   instead, which needs no flag, no second mechanism and no housekeeping round to have run. The
   observable behaviour is identical except that it is strictly more timely - an entry that expires
   between the last round and its release is retired, where a flag would have missed it.
3. **`housekeep()` has no production caller.** Spec 4.5 asks for one coroutine per pool running every
   `housekeepingInterval`, and a pool that starts one in its constructor is a pool nothing can stop.
   The connector owns a scope with a `SupervisorJob` (spec 11.2) and that scope belongs to ticket 13
   or 14, so the function is here and the launch is theirs. Tests drive it with `launch { }` and
   virtual time.
4. **`maxLifetimeJitter` is capped at 1.0, which spec 12 does not ask for.** Spec 4.5 fixes the window
   as `[0, maxLifetimeJitter x maxLifetime]` and puts no ceiling on the multiplier. A jitter above 1.0
   would let a session outlive twice the value called `maxLifetime`, which reads as the opposite of
   what that name promises. Added deliberately; a ticket that wants wider spread should raise the cap
   rather than work around it.
5. **A stack trace is captured on every acquire.** Spec 4.4 keeps the trace "when leak detection is
   on", and the DSL requires `leakDetectionThreshold` to be positive, so it is always on and the
   capture is always needed. The cost is a stack fill-in per borrow against a network round trip that
   follows it, so it was not worth inventing an off switch nobody asked for. If a profile ever
   disagrees, the switch is a nullable threshold.
6. **`idleCutoff` never reaches runtime.** It exists only to cross-validate `keepAlive` and
   `idleTimeout`, which is what spec 4.6 asks of it: it describes the network, not the connector.
   Nothing reads it after `build()`.
7. **Three review findings were declined.** `discard(entry, reason)` was called a middle man; it has
   two callers and names an operation `giveBack` does not, so it stays. `proves()` was called a
   mysterious name for a predicate with a side effect; its first KDoc line says it replaces the
   session, and the boolean is exactly "may the caller have this one". `capturingStandardError` is now
   a third copy of the same eight lines, but the other two live in `core` and this one is in
   `testkit`, so sharing it needs a cross-module test artifact that nothing else wants yet.
8. **Size.** About 690 lines that are neither blank nor comment, roughly 390 of them tests, against a
   200-600 budget. Over the top of it, and honestly so: eight checkboxes and fourteen tests, of which
   the housekeeper's own file is the largest single piece. Nothing here looked like it would get
   simpler by being smaller, but the next ticket should not read this as slack.

**For the next ticket:**

- **`SftpPool.housekeep()` needs launching.** Whoever builds the connector's `CoroutineScope` owns
  that, and owns cancelling it during shutdown. Until then the housekeeper does nothing in production
  and `minIdle` is a knob with no effect.
- **`Retirement` is internal, and `SHUTDOWN` is its unused fifth value**, waiting for ticket 13. The
  five labels are the closed set spec 13 fixes for the eviction counter's tag, so add a producer
  rather than a sixth spelling.
- **`handBack` takes a reason, not a boolean, and `giveBack` with it.** Anything that gives a session
  back now says why it is going, or passes null and lets the registry decide. A release path that
  wants a session gone for a new reason adds a `Retirement` value; it does not add a flag.
- **`SessionRegistry.sweep(takeRoom)` runs a caller's lambda under the lock, and that is safe by
  type.** `takeRoom` is `() -> Boolean`, not `suspend () -> Boolean`, and every operation this
  connector can perform against a server is a suspend function, so a round trip cannot be written
  there. A later ticket that widens the parameter to a suspending one has removed I5's guarantee, not
  merely bent it.
- **`EmbeddedSftpServer` gained two hooks**, both testkit main source. `killLiveSessions()` cuts every
  session the server holds and returns once the server side is really closed, so a test never races
  the kill it asked for. `start(onGlobalRequest = ...)` reports the name of every global request a
  client sends, which is the only way a keepalive is observable; the observer answers `Unsupported`,
  so the server behaves exactly as it would without one.
- **`TestScope.virtualClock()`** in `testkit/src/test` reads the scheduler, so one `advanceTimeBy`
  moves the housekeeper's next round and the age of everything it looks at together. It is in test
  sources because `kotlinx-coroutines-test` is test-scoped; a ticket wanting it in main source has to
  widen that scope first.
- **The keepalive test warms the JVM's first key exchange with a throwaway connection**, because
  `keepAlive` also bounds the handshake and a short one on a cold JVM fails `connect()` with "timeout
  in waiting for rekeying process" instead of proving anything. Five consecutive runs were clean at
  400 ms. Any later test that shortens `keepAlive` needs the same warm-up.
- **The jitter test pins the bounds, not the spread.** It proves no session is retired before
  `maxLifetime` and none survives the jitter window, over two independent draws. Proving that two
  entries drew *different* lifetimes is either probabilistic or needs the entry's expiry made public,
  and neither was worth it. If a later ticket exposes `PoolEntry.expiresAt`, that test becomes exact.

---

## T6: Client read path: list, stat, exists, download with staging and digest

**Built:** `sftp.connector.client` exists, and the connector can now read. A caller streams a
directory without the directory ever being held in memory, asks what the server says about a path,
and fetches a file onto local disk where it arrives complete, under its final name, with the digest
of the bytes that came over the wire. Every one of those borrows a session from the pool and gives
it back however it ends. The transport grew the three operations they need - `list`, `stat` and
`readTo` - and nothing else; `openWrite`, `rename`, `delete`, `mkdir` and `abort()` are still absent
rather than stubbed, following T1.

**Concepts named:**

- **`RemoteFile`** (`transport`) and **`LocalFile`** (`client`) are two things, not one thing at two
  moments, and separating them is where most of this ticket's design went. A `RemoteFile` is the
  server's *claim* about a path when it was asked: it can be stale before it is read, and nothing
  built on it may assume otherwise. A `LocalFile` is a *fact* about a file this connector wrote and
  counted - a path, a byte count and a digest - and it cannot exist for a file that is half there.
  The download is the only thing that turns one into the other, and it is the only place the
  server's claim is checked against what actually arrived.
- **`StagingArea`** is the deep module. Its whole surface is one call: hand it a place to put a
  file, a promise about the size, and something that writes bytes, and it either gives back a
  `LocalFile` or leaves the directory exactly as it found it. A caller is never told the partial
  file exists, and so cannot forget to clean it up - that is what makes I13 structural rather than
  remembered. Counting and digesting are the same pass as writing, because the bytes are going past
  anyway: reading the file back to digest it would double the I/O, and asking the filesystem for the
  size afterwards would answer about the disk rather than about the transfer.
- **`Listing`** (`CONTINUE` / `STOP`) is the answer the transport's listing callback gives, and it
  is the reason a hundred-thousand-entry directory costs what a thousand-entry one does. The seam
  had to be a callback rather than a returned collection: a returned collection is a materialised
  directory however the caller then filters it.
- **`ClientMeters`** owns `sftp_op_seconds`, the endpoint tag and the four result labels, and
  exposes a timing block rather than its timer - a caller that had to stop the timer itself would be
  the caller deciding what "recoverable" means. It does not sort failures into buckets of its own:
  every failure already answers what is to be done about it, and this reads that answer. It lives
  beside the client for the reason `PoolMeters` lives beside the pool.
- **`Endpoint.address`** is the one spelling of `host:port`. Three classes were building it by hand,
  and two spellings of it would split one server's numbers across two series on a dashboard.

**How the push callback became a cold flow, which was the hard half.** The SSH library reports
entries to a selector on the thread reading the socket, and that thread cannot suspend. So the
selector hands each entry to `channelFlow`'s bounded channel with `trySendBlocking`, the library's
own primitive for exactly this - a blocking callback feeding a channel - which answers with a result
instead of throwing. Blocking that thread *is* the backpressure: the server is not asked for the
next batch while this one has nowhere to go. A consumer that has stopped collecting closes the
channel, `trySendBlocking` reports failure, and the selector answers `STOP`, which closes the remote
handle cleanly and leaves the session healthy - the cooperative tier of the cancellation ladder,
arrived at from the listing side. Nothing in this path catches a cancellation.

That thread is one of the connector's bounded few, and the reason holding it cannot starve the rest
of the connector is an accounting one worth writing down, because a later ticket could take it away
without noticing: **the bounded dispatcher is exactly as wide as the pool, and everything that runs
on it is already holding a place in the pool** - a listing, a download, a dial, a hang-up. So the
number of threads wanted can never exceed the number there are. An operation added later that runs
on that dispatcher without first holding a place turns a slow path into a deadlock.

**Acceptance:**

- *list returns a cold Flow fed by the transport's per-entry callback through a bounded channel;
  maxEntries stops the listing early; directories are skipped by default* -
  `SftpClientTest.a listing hands on the files of a directory and leaves the directories out`,
  `.a listing stops after the entries the caller asked for` (which asserts what the *server* was
  asked to report, not merely what the consumer received), `.a filter keeps entries away from the
  consumer without ending the listing`, `.nothing is listed until somebody collects` for coldness,
  and `.a consumer that stops collecting stops the listing and gives the session back`.
- *download writes name.part in the staging directory, verifies byte count against the listed size,
  renames atomically, returns LocalFile with digest (SHA-256 default, MD5 selectable)* -
  `StagingAreaTest`, seven tests, and `SftpClientTest.a download lands under its final name with the
  digest of the bytes that arrived`. The expected digests are read off `sha256sum` and `md5sum`, not
  computed by the code under test.
- *Abort or failure during download deletes the partial file (I13)* - three `I13_` tests in
  `StagingAreaTest` and one in `SftpClientTest`. See below.
- *Listing 100k entries with maxEntries 1000 stops after 1000 with flat memory (S11, against the
  embedded server)* - `ReadPathAgainstServerTest.S11_a hundred thousand entries with a limit of a
  thousand stops after a thousand`. See below.
- *Meters sftp_op_seconds{op,result}* - `SftpClientTest.the client publishes how long each operation
  took and how it went`, which reads the timers off a `SimpleMeterRegistry` and asserts the op and
  result tags of every one, including a failing download tagged `recoverable`.
- *Progress entry appended* - this.

Four more tests prove the seam against a real server rather than a script:
`ReadPathAgainstServerTest` lists a real directory, downloads real bytes and checks their digest
against `sha256sum`, answers `stat` and `exists` about a path that is there and one that never was,
and stages a file deleted between the listing and the download.

**How I13 is enforced, not merely asserted.** There is one method that creates a partial file, and
exactly two ways out of it: the atomic move, which takes the partial file away itself, and
everything else. The `finally` deletes it, so the successful path finds nothing to delete and every
other path - a transfer that threw, a coroutine that was cancelled, a byte count that did not add
up, an error nobody expected - is cleaned by the same line. Replacing that line with a no-op fails
all three `I13_` tests in `StagingAreaTest` and nothing else. The tests assert the directory is
*empty* rather than merely that the partial file is gone, because the failure that matters most is a
final name left over half a file: whatever finds the final name takes what is under it for a whole
one.

The one arm of I13 not proved here is the word "abort": `abort()` is ticket 08's third cancellation
tier and does not exist yet. What is proved is that cancelling the coroutine running the transfer
already leaves nothing behind, which is the tier below it, so 08 inherits a cleanup that is correct
rather than one it has to add.

**What S11's memory measurement actually showed.** The assertion is not on heap. It is on the number
of entries that reached the connector at all, counted inside the callback the server's own batches
drive: **exactly 1000 of the 100,000**. That is both the stronger statement and the deterministic
one - a listing that read the whole directory and handed on the first thousand would pass a heap
check on a good day and fail it on a bad one, but it could never report 1000. Memory is bounded
because the work is, and the count is the work.

The heap was measured anyway, on a real run: **1.5 MB** of live heap between a settled reading
before the listing and a settled reading after (6.5 MB to 8.0 MB), while collecting 1000 entries out
of the 100,000-entry directory. That 1.5 MB is essentially the thousand `RemoteFile` objects the
test itself keeps in its own list. No assertion was written on it, deliberately: a gc-based delta
loose enough not to flake is too loose to catch what it is supposed to catch, since 100,000 of these
entries would only be about twelve megabytes and the bound would have to sit above the measurement
noise. The number is recorded here rather than asserted in a test because it is an observation.

**Deviations:**

1. **The transport's read operation is `readTo(path, sink)`, where spec 5.1 names `openRead`.** Not
   a rename for its own sake: `openRead` returning a stream would put every blocking socket read on
   whatever thread the caller happened to be on, and spec 3.3 requires them all on the bounded
   dispatcher. Handing the transport a sink keeps the whole transfer inside one call on that
   dispatcher, which is also where ticket 08 hangs its progress monitor. Spec 14.1 keeps `openRead`
   for a *streaming* download that pins a lease for the consumer's read, and spec 1.3 defers that
   out of v1 - so `openRead` is not this operation renamed, it is a different operation with no
   caller yet. Whichever ticket builds streaming adds it beside this one.
2. **A byte count that does not match the listed size is raised as `SessionLost`, and it is the one
   reading in this ticket I would most like ruled on.** The failure hierarchy is fixed by spec 10.1
   and holds no class for the connector's own consistency check failing, so the choice was between
   the classes that exist. `SessionLost` fabricates nothing and its message says exactly what
   happened, but it poisons - so a file that grew between the listing and the download costs a
   handshake, and C4's own reasoning (a channel that answered is demonstrably healthy) argues the
   session should be kept. The alternative inside the hierarchy is `ServerFailure`, whose disposition
   is right in every respect, but it carries a wire status code and the server sent none: inventing
   one would put a fabricated number in a field an operator may one day read. So the choice was no
   fabricated data, at the price of a handshake on a rare event, and an open seam below for the
   class that would end the trade. From a dashboard this looks like
   `sftp_pool_evicted_total{reason=poisoned}` rising alongside
   `sftp_op_seconds{op=download,result=recoverable}`.
3. **`list` never reports directories, where spec 7.4 says they are "skipped by default".** Read as
   the sentence's other half - "`recursive` descends" - the default that can change is `recursive`,
   which is ticket 10's, and not the filter. A `filter` default that excluded directories would
   silently put them back the moment a caller passed a filter of its own, which is a foot-gun rather
   than a default.
4. **`sftp_op_seconds{op=list}` spans the consumer's work, not the server's.** The listing blocks on
   the consumer by design, so there is no separable server time to measure. What it reports is how
   long the operation held a session, which is the number that matters to a pool of five - but it is
   not the server's latency and must not be read as one.
5. **`PoolExhausted` and `CircuitOpen` are tagged `result=recoverable`.** Spec 13 fixes four labels
   and neither failure is any of them: both are recoverable in the sense that the next tick tries
   again, and calling them `fatal` would say the connector should stop, which is the one thing they
   do not mean. The discriminator is the failure's own `WatchReaction.STOP`, so a class whose
   behaviour changes moves label without this file being edited.
6. **`polling { staging { } }` arrives before the poller.** Spec 12 puts the staging knobs inside
   `polling`, so they went where they belong rather than somewhere ticket 10 would have to move them
   from; the block holds nothing else yet. `staging.dir` defaults to the JVM's temp directory rather
   than being required, because a required knob with no default would have made every configuration
   in every earlier ticket's tests fail to build, and a default that exists and is writable wherever
   the connector runs keeps the new validation rule honest without that.
7. **A caller-supplied `localTarget` puts the partial file beside it, not in the staging directory.**
   The move has to be atomic, so the partial file has to be on the same filesystem as the final one;
   staging elsewhere and copying would give up the one guarantee this exists for. Spec 6.3's
   `<stagingDir>/<name>.part` is what the default target produces, and a caller naming its own target
   has taken responsibility for where it points.
8. **Two test methods were added to T1's `ConnectorDslTest`** for the new knobs' build-time
   validation. Additive only; no existing test in that file was touched and no assertion anywhere was
   weakened.
9. **Three review findings were declined.** The duplicated content and its digest across three test
   files stay duplicated on purpose: each test states the digest it expects, read from `sha256sum`,
   and a shared constant would let one test's change move another test's expectation without anyone
   noticing. The directory-prefix join in the JSch adapter and the same rule inside
   `FakeSftpTransport` stay separate, because sharing them means making a private helper public
   across a module boundary to serve a fake. And `ClientMeters` reading `failure.disposition.watch`
   is not envy: reading the failure's own answer instead of sorting failures a second time is the
   whole point of T2's disposition.
10. **Size.** About 700 lines that are neither blank nor comment, roughly 400 of them tests, against
    a 200-600 budget. Over the top of it. Six checkboxes, two new domain types, three new transport
    operations and twenty-six tests; the honest reading is that the read path is a large slice, not
    that anything here would be simpler for being smaller. The next ticket should not read this as
    slack.

**For the next ticket:**

- **A file that has gone comes back as `NoSuchFile` from `download`, carrying the remote path.** That
  is the seam ticket 10 needs for S5: it can catch that one class before the retry ladder and turn it
  into `FileGone` without guessing, because every other download failure is a different class. Proved
  both against the fake and against a real server whose file was deleted between the listing and the
  fetch.
- **The bounded dispatcher is exactly as wide as the pool, and everything running on it holds a pool
  place first.** That is what stops a listing blocked on its consumer from starving a download. An
  operation added later that touches the io dispatcher without holding a place breaks it, and the
  symptom is a deadlock rather than a slow path.
- **`trySendBlocking` is the right way to feed a channel from a blocking callback**, and it is why no
  cancellation is caught anywhere in the listing path. Anything else pushing into a flow from a
  library's own thread should reach for it rather than for `runBlocking` and a catch.
- **`FakeSftpTransport` now holds contents.** `file(path, bytes)`, `directory(path)` and
  `remove(path)` stage what a listing reports and what a download delivers, in insertion order, and
  calling `remove` while a listing is in flight is how a file that vanishes underneath a consumer is
  staged. Its single `answer` hook still does everything else.
- **Nothing retries yet.** `list`, `stat`, `exists` and `download` each make one attempt on one lease.
  Spec 6.1's per-operation retry semantics and `Attempt.number` staying at 1 are ticket 11's, and the
  read path is shaped so a retry wraps a whole operation rather than reaching inside one: a download
  that fails restarts from an empty partial file, because the staging area deleted the old one on the
  way out.
- **`withSession` is absent, not stubbed.** Spec 6.1 lists it and no caller needs it yet; T1's rule is
  that an operation nothing calls should not be reachable. The ticket that needs several operations on
  one lease adds it.
- **The S11 test creates 100,000 files and takes about 50 seconds**, which is most of the connector
  suite's wall time. It is the one slow test here, and the cost is creating the directory rather than
  listing it.

---

## T7: Client write path: upload, rename with overwrite, delete, mkdir, withSession

**Built:** the connector can now write. A caller sends a local file to a remote path, moves a file
with a policy for what to do about anything already at the target, removes a file, makes sure a
directory exists with its parents, and runs a sequence of operations on one held session. The
transport grew the four operations those need - `writeFrom`, `rename`, `delete`, `mkdir` - and
nothing else; `abort()` is still absent rather than stubbed, following T1, and belongs to T8.

A separate first commit applied coordinator decision C7: a byte count that disagrees with the
listed size is now `IncompleteTransfer` rather than `SessionLost`, with its row added to T2's
`FailureModelTest` and T6's staging check and its test retargeted. T6's open question is closed and
its seam is struck through above.

**Concepts named:**

- **`Overwrite`** (`client`) is where most of this ticket's design went, and the point of it is that
  it reads like a flag and is not one. SFTP version 3 has no way to say "put this here and replace
  whatever is there"; the POSIX rename extension adds one, and a server without it can only be told
  to clear the path and aim at it again. So `REPLACE` is not a bit on a request, it is a short
  sequence of requests with a gap in the middle, and the gap is the part a caller has to know
  about. Each operation's own documentation says what its gap looks like, because they are not the
  same gap: a rename's is the moment between the delete and the second rename when the target path
  holds nothing, and an upload's is the length of the transfer, during which the target holds a
  partial file.
- **`SftpSession`** (`transport`) is everything one session can be asked to do, and `SftpConnection`
  is now that plus `close()`. The split is the answer to what a `withSession` caller may do with the
  session it is handed: the pool lends the same session out again afterwards, so a caller that hung
  up on it would break the *next* caller's work rather than its own. Splitting the interface means
  the block is handed something with no hang-up on it, rather than being asked not to use one.
  Every existing implementor was unaffected - `SftpConnection` still names the whole set.
- **`BorrowedSession`** is the other half of that answer: the loan ends when the block does. A
  reference stashed past the block is a second caller on a session the pool has already lent to
  somebody else, which is I2 broken from outside the pool, and it cannot be prevented by asking. So
  the session is made to stop working instead, loudly, at the point of misuse.
- **`moveOnto`** is the rename sequence, and `clearTheWay` and `ensureDirectory` are the two places
  a failure means "the state you wanted is already the state that exists". Both swallow exactly one
  class and re-raise everything else after looking.

**What the server actually does about an occupied rename target, measured rather than assumed.**
Spec 5.2 says rename "uses the `posix-rename@openssh.com` extension when the server advertises it",
which reads as something the adapter arranges. It is not: **JSch sends the extension by itself**
whenever the server advertised it, and the embedded MINA SSHD advertises it. So a rename onto an
occupied path against that server **succeeds and destroys the old file**, and reports success.

That makes `Overwrite.REFUSE` unenforceable at the server, and it is the finding this ticket turned
on. Refusing is therefore the connector's own decision, taken before the request goes out, on both
operations: a look and then the request. A writer arriving between the two still wins, and on a
server without the extension the request itself is refused as well, which closes the race there and
only there. The first draft left refusing to the server and passed against the fake, which is a
server without the extension; only the embedded-server test caught it.

**Acceptance:**

- *upload streams a local file to the remote path with overwrite flag* -
  `SftpWritePathTest.an upload puts the local file where it was told to`, `.an upload told to
  replace writes over what was there`, and `.an upload that was told not to replace sends nothing at
  an occupied path`, which asserts no write was sent at all rather than only that the call failed.
  Against a real server: `WritePathAgainstServerTest.a real upload puts the bytes on the server and
  a download brings them back` and `.a real upload told not to replace leaves the file that is
  already there`.
- *rename with overwrite = true tries the rename, and on failure deletes the target then renames
  again; embedded-server test covers a pre-existing target* - `SftpWritePathTest.a rename told to
  replace clears the target and sends the rename again` pins the sequence request by request
  (`Rename`, `Delete`, `Rename`), which is the only place it can be pinned, because the embedded
  server has the extension and does it in one. The pre-existing target is covered against the real
  server twice: `.a real rename told to replace lands on a target that was already there` and
  `.a real rename told not to replace leaves the target that was already there`, the second being
  the measurement above written as a test.
- *delete, mkdir with parents, exists round-trip against the embedded server* -
  `WritePathAgainstServerTest.delete, mkdir with parents and exists round trip against a real
  server`, plus `.mkdir under a parent that is not there is refused rather than invented`. Against
  the fake: `.a delete removes the file, and says so when there was nothing to remove`, `.mkdir with
  parents creates the whole path, and a second run is content with what it finds`, `.mkdir creates
  one directory unless it is asked to fill in the path above it`, and `.mkdir over a file is refused
  rather than treated as already done`.
- *withSession runs the block on one lease and releases it on every exit path* -
  `SftpWritePathTest.withSession keeps one session for the whole block`, which asks the pool from
  *inside* the block how many sessions are out and gets one, and `.withSession gives the session
  back whether the block returns, throws or is cancelled`, which runs all three and asks the pool
  after each. `.a session kept past the block it was given to refuses to work` covers the other half
  of the contract. Against a real server: `.withSession runs a whole sequence against a real server
  on one session`.
- *Progress entry appended* - this.

Two more things the write path gives the tickets after it, each with its own test: a rename whose
source is gone reports the *source* as missing and touches nothing (`.a rename whose source is gone
reports the source and touches nothing`, and the same against a real server), and a rename refused
while nothing is at the target is passed on rather than met with a delete.

**What T11 can rely on to tell a landed rename from an absent target.** The failure classes a
`rename` can raise are disjoint on exactly the question I11 asks:

- **`NoSuchFile`, naming the source.** The source is not there. Either it never was, or an earlier
  attempt at this same rename already landed. That is the signal to go and stat the target, and I11
  is the rule for reading the answer.
- **`ServerFailure`.** The server refused the request, or the connector refused it on the caller's
  behalf. The source is still where it was.

The discrimination holds because of one deliberate choice: **a missing path reported by `rename` is
always the source, never the target.** The delete inside the replace sequence swallows `NoSuchFile`,
because a target that was already gone is the state the replacement wanted, and letting it out would
hand T11 a "the source may have landed" signal about a path that was never the source. The expected
size I11 compares against is not this layer's to know - the source layer holds the `RemoteFile` it
listed - so `rename` takes no size parameter and T11 supplies one.

**Deviations:**

1. **The transport's write operation is `writeFrom(path, source)` where spec 5.1 names `openWrite`,
   and this ticket could not amend the spec.** It is the same argument spec 5.1 already makes for
   `readTo`: a stream the caller pumps puts every blocking socket write on whatever thread the
   caller happens to be on, and spec 3.3 requires them all on the bounded dispatcher. Handing the
   transport a source keeps the whole transfer inside one call on that dispatcher. Spec 5.1 was
   amended for the read side and needs the same sentence for the write side; T7's scope boundary
   allowed `progress.md` and not `spec.md`, so it is recorded here and listed as an open seam.
2. **`SftpConnection` is split into `SftpSession` plus `close()`, and `withSession` takes
   `suspend SftpSession.() -> T` where spec 6.1 abridges it as `suspend Connection.() -> T`.** Same
   scope note as above. The reason is in the concepts section: handing a pooled session's `close()`
   to a caller is a foot-gun the pool cannot survive, and the split removes it rather than
   documenting it. Additive for every existing implementor.
3. **`Overwrite` is an enum where spec 8.2 and spec 12 both spell it as a boolean
   (`move("temp/", overwrite = true)`).** A boolean cannot carry what the concepts section says the
   value means, and a reader of `rename(from, to, true)` learns nothing about the gap. Ticket 10
   builds `Move(target, overwrite)` and should take this type rather than a boolean.
4. **The replace sequence looks at the target before deleting it, where spec 8.2 says "on failure
   delete the target then rename again".** A server refuses a rename it cannot do at all - across
   filesystems, which is spec 8.2's own next paragraph and scenario S6 - with the same generic
   status it refuses an occupied target with. Looking first means a refusal that was never about the
   target is passed on as the server gave it, instead of being met with a pointless delete and a
   second rename whose failure is the one the caller would then see. It does not prevent the loss in
   the case where the target *is* occupied and the rename still cannot be done; nothing on this
   protocol does, which is what the startup probe exists for.
5. **`Overwrite.REFUSE` over an occupied path raises `ServerFailure` with SSH_FX_FAILURE, and this
   is the reading in this ticket I would most like ruled on.** The class is right about the session
   and about the message, and it is the same class and code a server without the extension answers
   with itself, so a caller handles one and handles both. It is wrong about the disposition: spec
   10.2 retries every recoverable failure and counts it against the breaker, and a deterministic
   policy refusal should be neither. Nothing retries yet so nothing misbehaves today, but from T11
   this costs three attempts and a breaker failure per call. The disposition that fits already
   exists - `FAIL_THE_ATTEMPT` - and no class carries it for this. Adding one is the maintainer's
   pen, which is why this is recorded and raised rather than done: it is C7's shape exactly, and
   it is on the open-seams table against T11.
6. **`mkdir` is idempotent whatever `parents` says.** Spec 6.1 puts "AlreadyExists counts as
   success" in the *retry* column, which is T11's. It is here as well because `parents = true`
   cannot work otherwise - every ancestor of a path is normally already there - and because a
   startup that creates the folders it needs has to be able to run twice. `parents` therefore
   controls only whether missing *ancestors* are created, not what an existing directory means.
   A file found where a directory was wanted is still a failure.
7. **`upload` writes straight to the target rather than through a temporary name.** Writing to a
   temporary name and renaming it into place would make an upload atomic at the target and would
   match spec 16.1's own recommended convention - but it would put a rename *inside* the upload,
   and a retry after a lost reply on that rename would rewrite the temporary file and then be
   refused by a target that its own first attempt had already created. That is exactly the phantom
   failure D10 exists to avoid, and it is why spec 6.1's retry note for upload says "Restart; remote
   partial is overwritten". A caller that wants the atomic version composes it from `upload` and
   `rename`, which is what `WritePathAgainstServerTest.withSession runs a whole sequence` does.
8. **No new configuration knob**, so the standing rule about knobs landing in the DSL with
   build-time validation has nothing to apply to.
9. **Two review findings were declined.** `BorrowedSession` being eight delegating one-liners was
   called a middle man; the delegation is the price of the revocation, and the revocation is the
   answer to the question this ticket was asked about `withSession`. And `FakeSftpTransport`
   spelling status code 4 and its refusal message itself, rather than sharing the client's, stays
   separate on T6's precedent: sharing means making something public across a module boundary to
   serve a fake, and a fake that agreed with the code under test by construction would prove less.
10. **Size.** About 573 lines that are neither blank nor comment, roughly 390 of them tests, inside
    the 200-600 budget on the measure the earlier entries used. Four checkboxes, one new domain
    type, four new transport operations and twenty-five tests.

**For the next ticket:**

- **Refusing an overwrite is the connector's decision and cannot be given back to the server.** JSch
  uses `posix-rename@openssh.com` on its own whenever the server advertised it, and such a server
  replaces an occupied target and reports success. Any later code that sends a bare rename and reads
  the answer is trusting a behaviour that half of the servers in scope do not have.
- **Read deviation 5 before starting T11.** A policy refusal currently arrives as a retryable,
  breaker-counted failure. It needs a ruling, not a workaround.
- **`abort()` is still absent from `SftpSession`**, and is T8's with the rest of the cancellation
  ladder. Note that it belongs on `SftpConnection` rather than on `SftpSession`: it destroys the
  session, so it is the pool's to call and not a borrower's.
- **Nothing retries yet.** `upload`, `rename`, `delete` and `mkdir` each make one attempt on one
  lease and `Attempt.number` stays 1. Each is shaped so a retry wraps the whole operation: an upload
  restarts from zero over the top of what it left, a mkdir finds its directories already there, and
  a rename reports a missing *source* when its own earlier attempt landed.
- **`withSession` holds a session for the length of the block**, and there are only ever `maxSize`
  of them. Anything that reaches for it to run a long sequence is taking a session out of a pool of
  five for the duration; the single-operation methods are the default for a reason.
- **`FakeSftpTransport` now writes, renames, deletes and creates directories**, and it is a server
  *without* the POSIX rename extension: a rename onto an occupied path is refused with status 4.
  That is the harder server and the one whose sequence is worth staging. A test that needs the
  extension's behaviour wants the embedded server.
- **An `InputStream` handed to `writeFrom` is left open**, like the sink `readTo` writes to. On
  Windows a stream left open on a `@TempDir` file fails the whole test class at teardown with
  "Failed to delete temp directory", not at the test that leaked it.

---

## T8: Cancellation ladder: cooperative abort, keepalive floor, forced disconnect

**Built:** cancelling a coroutine now reaches the thread it left behind. A download or a listing
whose caller has gone away stops itself within a chunk of bytes and its session is handed straight
to the next caller; a call nothing gentler will unblock is cut apart after `cancelGrace` and its
entry ends `Closed`. `abort()` joined the transport seam, `cancelGrace` joined the DSL, and
`socketTimeout` left it.

A separate first commit applied coordinator decision C8: a refused overwrite is `OverwriteRefused`
rather than `ServerFailure`, with its row added to T2's `FailureModelTest` and T7's two refusal
assertions retargeted. T7's open question is closed and its seam is struck through above.

**Concepts named:**

- **`CancellationLadder`** (`pool`) is where the ticket's design went, and the thing it is named
  after is not a mechanism but a *question*: a call that has stopped stopped in one of several
  ways, and only one of them cost the session its life. Nobody at a call site can answer that -
  a client operation knows it was cancelled and nothing else - so the answer is worked out in one
  place and the session's fate follows from it. Its whole surface is `carry(entry) { ... }`, and
  what is behind it is the waiting, the cutting, and the reading of what the stopped call died of.
- **The rungs, and which of them the connector actually climbs.** Spec 5.3 lists three tiers, and
  writing them down as three things this class does turned out to be wrong twice over. The
  cooperative rung needs *nothing* from the ladder: the monitor and the selector both watch the
  same cancellation that summoned it, so it is already armed by the time anything here runs. The
  keepalive rung is not a rung at all but a **floor** - it ends a blocked read whether or not
  anybody cancelled anything, which is why it also answers the ticket's third checkbox with no
  cancellation anywhere in the test. Only the third is something the connector does. So the class
  waits, and then cuts, and its documentation is most of it.
- **`StopWhenNobodyIsWaiting`** (`transport.jsch`) is the cooperative rung, and it is a
  `SftpProgressMonitor` that reads the caller's `Job` rather than a flag somebody has to set.
  There is nothing to arm and nothing to remember to disarm: the cancellation arrives on a
  different thread from the transfer, and a job is already the thing both threads agree on.
- **`PoolEntry.cutLoose()`** puts the destroying and the recording of it in one method, because
  they are one decision. A session cut apart and not recorded goes back on the shelf and is handed
  to somebody, and the record is the only trace the cutting leaves - a cancellation says a caller
  stopped waiting and nothing whatever about a session.
- **`abort()` on `SftpConnection`, not `SftpSession`**, following T7. It is not `close()` in a
  hurry: `close()` is the orderly hang-up on an idle session by whoever owns it, and this is called
  from a different thread while a call is in flight, leaves the session unusable on purpose, and
  must never touch the bounded IO dispatcher - the moment it is worth aborting anything is the
  moment every thread there may already be blocked.
- **`Disposition.ACCEPT_THE_REFUSAL`** is the seventh disposition, added by the C8 pre-task.
  `FAIL_THE_ATTEMPT` would have kept the session too, but by way of `LeaseFate.NONE_HELD`, which
  claims there was no session - and there was.
- **Two testkit fault hooks.** `LoopbackConnectProxy.holdAfter(bytes) { }` stops a transfer at a
  byte count of the test's choosing and `resume()` lets the *same* transfer carry on, which is a
  different fault from `stall()` and differs in exactly the bytes behind it: a stall throws them
  away, a hold leaves them queued where they were. `onNextClientRequest { }` fires when the client
  has put a request on the wire, which on a stalled tunnel is the only moment a test can act on
  with confidence - the thread that sent it is committed to waiting for an answer that is never
  coming.

**What was decided about `socketTimeout`, and why it was removed.**

The seam left by T2's measurement and spec D26 was: make it mean something, or take it out. It is
out, and the DSL is one knob shorter.

The case for keeping it was in spec 5.3 itself - "the knob stays in the DSL because it is what a
reader reaches for" - and the way to honour that would have been to spend it as
`serverAliveCountMax`, deriving the number of unanswered probes from
`socketTimeout / keepAlive`. That was written and then reverted, for three reasons.

1. **It cannot be honoured as written.** The library gives up after a whole number of unanswered
   probes at the keepalive interval, so any bound is a multiple of `keepAlive`. A duration knob
   whose value is silently rounded to a multiple of a *different* knob is not one knob but half of
   two, and changing `keepAlive` would then quietly change how many probes a deployment gets.
2. **Its name is a lie in this library, and that is the exact lie D26 exists to end.** There is no
   separately settable socket read timeout, because `serverAliveInterval` *is* the socket read
   timeout. Keeping the name while giving it a different job preserves the misreading rather than
   ending it - the next reader still believes there is a socket timeout, and is still wrong.
3. **It would have overturned an earlier ticket's premise.** T2's `S2_` test shortens `keepAlive`
   alone because `keepAlive` is the bound; under the derivation the same test would have taken
   sixty seconds instead of four, and fixing that means editing a test whose whole subject is the
   measurement this ticket is built on.

What a reader reaches for now is `keepAlive`, whose documentation says outright that twice its
value is the bound on a hung server and the number to size against an SLA. The adapter pins
`serverAliveCountMax = 1` rather than inheriting the library's default, because that bound is a
promise this connector makes and not one it should inherit from a dependency's next release. If a
deployment ever needs the bound tuned independently of how often a session speaks, the honest knob
is a count of probes spelled as a count, and nothing needs it today.

**Acceptance:**

- *Cancelling a download mid-transfer returns within cancelGrace, the session is validated and
  returned to the pool, no partial file remains* - `CancellationLadderTest.a download cancelled in
  mid transfer stops itself, leaves nothing behind, and keeps its session`. The transfer is held
  mid-file by the tunnel, cancelled, and let go again so the next chunk of bytes carries the news;
  the join is asserted inside one second against a three-second grace, which is what proves nothing
  was cut apart. `validationBypass` is zero for that test, so the session is not merely on the shelf
  afterwards - it has answered the server since.
- *Cancelling a listing stops the selector; the session is reused for the next operation* -
  `.a listing its consumer walked away from leaves the session fit for the next operation`, which
  asserts `sftp_pool_created_total` is still 1 after the listing and a later `exists` on the same
  pool.
- *A server-side stall raises SessionLost, poisons the lease, evicts the entry* - `.a server that
  goes quiet ends the call itself, and the session goes with it`. **Read against the keepalive, not
  `socketTimeout`**, which is spec 5.3 as amended by D26 and is the ticket checkbox this entry
  deviates from in wording only.
- *A call stuck past cancelGrace is force-disconnected; the blocked thread returns and the entry is
  Closed* - `.a call nothing else unblocks is cut loose after the grace, and its entry ends closed`,
  which reads the entry's own state flow and asserts `EntryState.Closed`.
- *Progress entry appended* - this.

Three tests beyond the four checkboxes, each closing something the checkboxes did not name:
`I13_no partial file survives a transfer the pool had to cut apart` is the arm of I13 T6 could not
prove because there was no abort yet to prove it with; `.a cancelled call the keepalive ends inside
the grace still costs the session` is deviation 3's case; and `.a borrow cancelled while the pool is
proving a session is cut loose as well` covers the validation round trip, which is a blocking call
made before any lease exists and was outside the ladder in the first draft.

**How each rung is enforced, not merely asserted.** Six breaks were staged, and each turned exactly
the right test - and only that test - red.

- **The cooperative rung** is the monitor answering `caller.isActive`. Making `count()` return
  `true` always fails the download test on the bytes the server was made to send: that count is the
  only place a transfer that stopped early and one that ran to the end of an eight-megabyte file
  differ from outside, which is why the test asserts on it rather than on the download returning.
- **The forced rung** is `entry.cutLoose()`. Removing the `abort()` inside it does not fail the
  forced test, it hangs it for the full fifteen seconds the test allows - which is the honest
  symptom, because without the cut nothing ends that call until the keepalive does a minute later.
- **The record of the cut** is the other half of `cutLoose()`. Aborting without setting the flag
  fails the forced test at the entry's state: a session cut apart and unrecorded goes back on the
  shelf as `Idle`.
- **The cancellation branch in `releaseAfter`** is what stops a cancelled operation costing a
  handshake. Reverting it to poison every cancellation fails both cooperative tests, which see a
  second session dialled.
- **The keepalive-inside-the-grace branch** is what stops a *dead* session going back. Removing it
  fails only its own test.
- **The ladder around the validation round trip** likewise: unwrapping it hangs only the borrow
  test, for the full fifteen seconds.

**What the forced tier measures.** Against the embedded server through a stalled tunnel, with a
300 ms grace, the blocked thread comes back in well under a second from the cancellation - the
assertion band is 300 ms to 6 s and four consecutive runs sat at the bottom of it. Closing the
socket is what does it: `Session.disconnect()` returned promptly rather than blocking on the
stalled socket it was hanging up on, which was the risk worth measuring before relying on it. The
keepalive floor, measured with a 400 ms interval, ends a stalled read in under two seconds, which
is the two intervals `serverAliveCountMax = 1` buys.

**Deviations:**

1. **`socketTimeout` is removed from the DSL, and spec 5.2, 5.3, 12 and 17.2's S2 still name it.**
   The removal is inside the grant C6 gave this ticket; the four passages are not this ticket's to
   edit. **For the maintainer:** 5.2's `Session.setTimeout(socketTimeout)` is now
   `setServerAliveCountMax(1)`; 5.3's "the knob stays in the DSL because it is what a reader
   reaches for" is the sentence this ticket appeals against, with the three reasons above; 12's
   pool block loses `socketTimeout` and gains nothing, since `cancelGrace` was already listed
   there; and S2's "Server stalls past `socketTimeout`" should read "past the keepalive ladder",
   which is what 5.3 already says in prose.
2. **Two earlier tickets' test files were edited, both mechanically forced by the removal.**
   T1's `ConnectorDslTest` used `socketTimeout = 45.seconds` as its example of a pool knob reaching
   the built configuration; it now uses `cancelGrace = 45.seconds`, which is the same assertion
   about a different knob and the substitution the coordinator's brief anticipated. T2's
   `JschErrorMappingTest` config helper set `socketTimeout = BRIEF` alongside `connectTimeout` and
   `keepAlive`; that line is deleted. Nothing was weakened: no assertion changed meaning and
   `keepAlive = BRIEF`, which is what actually bounds anything there, is untouched. T2's KDoc at
   that test still says "the clock is `keepAlive`, not `socketTimeout`" - still true, and now naming
   a knob that no longer exists. Left alone rather than reworded, because it explains a measurement
   and is not this ticket's prose to change.
3. **`withLease` runs its block in a child coroutine, under a `supervisorScope`.** It has to run
   it as a child: a caller blocked inside JSch cannot observe its own cancellation, so somebody has
   to be watching from outside, and there is nowhere else that holds both the lease and the grace.
   The supervisor part is load-bearing rather than incidental - a cut session raises a lost
   connection, and under a plain `coroutineScope` that failure wins the race against the
   cancellation, so a caller that merely changed its mind is told the network broke and
   `CancellationException` is effectively swallowed. It was found by a test, not by reading.
4. **`readTo` and `writeFrom` end with `ensureActive()`, and no test covers it directly.** A
   transfer the monitor stopped has delivered less than the file holds, so without it the staging
   area reports `IncompleteTransfer` - "a file changed size underneath you" - for what was a
   cancellation. It is not separately observable from above because the ladder drops a cancelled
   call's outcome by design, so the two look identical to a caller. It stays because the transport
   is the layer that knows which of the two happened, and reporting the wrong one is a landmine for
   the first caller that reaches `readTo` outside a lease.
5. **The ladder does not wrap `dial()`.** A cancelled caller waiting on a connect is bounded by
   `connectTimeout`, which the library applies itself, and an entry mid-dial has no session to
   abort. `proves()` is wrapped, because it has both.
6. **`sftp_pool_evicted_total{reason=poisoned}` is what a cut session is counted as.** Spec 13
   fixes five labels and the ground rules forbid a sixth, so a session destroyed to rescue a thread
   shares a label with one that failed. The eviction is honest - the session really is unusable -
   but a dashboard cannot tell "the server poisoned it" from "we cut it loose", and the WARN line
   is the only place that distinction lives.
7. **Size.** 379 lines that are neither blank nor comment, 236 of them the one test file. Inside
   the budget, but the seven tests are most of it, and three of them were added after review found
   that the four checkboxes covered neither I13's own wording, nor the keepalive-during-grace case,
   nor the validation round trip.

**For the next ticket:**

- **T13 inherits the ladder for its forced phase.** Spec 11.2 step 4 is "remaining leases are
  aborted, which unblocks their threads", and that is `entry.cutLoose()` - it marks and aborts
  together, so a lease released afterwards is evicted without shutdown having to say so. What T13
  still owns is the *reason*: `Retirement.SHUTDOWN` has no producer, and a session cut during a
  drain is currently counted as `poisoned` like any other.
- **I9 is `drainTimeout + cancelGrace`, and the second half now exists.** The grace is the only
  thing bounding a blocked call, so a drain is bounded by `drainTimeout` plus one grace per lease
  that has to be cut - not one grace overall, if the cuts are sequential. T13 should cut them in
  parallel or say why not.
- **Anything that adds a blocking transport call to a path outside `withLease` is outside the
  ladder.** Two such paths exist and both are handled - `dial()` by `connectTimeout`, `proves()` by
  being wrapped. A third would be unbounded on a hung server except by the keepalive floor, and
  nothing would fail to say so.
- **`Lease.entry` is now `internal`**, so the ladder can be handed the entry rather than the lease.
  A borrower still receives a full `SftpConnection` from `Lease.connection` and can therefore call
  `abort()` on it, which T7 said should be the pool's alone. `withSession` is protected by
  `BorrowedSession`; a direct `withLease` caller is not. Worth closing when something needs it.
- **`cancelGrace` defaults to five seconds and is the only new knob.** It is what a cancelled
  caller waits before its session is destroyed, so shortening it trades handshakes for
  responsiveness; the tests use 300 ms.
- **The keepalive interval bounds the key exchange, and a warm-up connection must use the *shipped*
  interval.** T5's note says to warm the JVM's first key exchange with a throwaway connection; what
  it does not say, and what cost an hour here, is that the throwaway must not itself use the
  shortened interval - otherwise the warm-up is the cold handshake it exists to absorb, and fails
  with `timeout in waiting for rekeying process.` The failure is intermittent and looks like a
  connect bug.
- **`FakeSftpTransport` now records an `Abort`** and hangs up on the session without going through
  the `answer` hook, because a real abort is called while another thread is stuck and so cannot be
  given anything to wait for.

---

## T9: Startup sequence and probe

**Built:** the connector is a thing now. `SftpConnector.start(config)` takes a configuration and
hands back something running: it asks the server whether the configuration describes anything the
server can actually do, and if it does, it launches the pool's housekeeper into a scope of its own
and returns. The largest open seam on the list is closed - `SftpPool.housekeep()` has a production
caller, so lifetime eviction, idle eviction, leak reporting and `minIdle` all do something for the
first time. The polling block gained the five knobs the checks read, with build-time validation,
and the testkit gained a server whose second root is on another filesystem.

**Concepts named:**

- **`SftpConnector`** (`sftp.connector`) is the first thing in this build with a *lifecycle* rather
  than behaviour. Everything before it did something when called; this one is started, keeps
  running while nobody is asking it for anything, and will one day be stopped. It exists rather
  than being an assembly a caller writes by hand for exactly one reason: the pool looks after
  itself only while something runs its housekeeper, and a pool that launched that coroutine in its
  own constructor would be a pool nothing could stop. So the scope belongs to the object with the
  life, and the object with the life is this.
- **`backgroundWork`** is that scope, narrowed to the one thing anyone outside needs from it: a
  `Job` to cancel. Handing over the `CoroutineScope` would let a caller launch into the connector's
  own supervisor, which is not something anyone should be able to do by accident; a `Job` can only
  be watched and cancelled. It is where the phased shutdown will end and is deliberately not that
  shutdown - its documentation says outright that nothing is drained and no session is hung up on.
- **`StartupProbe`** is the deep module, and its whole surface is `run()`. Behind it are three
  kinds of check against two kinds of path, and the design work went into the fact that **the
  message is the deliverable**. A probe that reports "start-up failed" has done all the work and
  thrown away the only reason for doing it, so every check names the path it was looking at, what
  it was trying, and what to change - `checking(trying, remedy) { }` is the shape that makes
  writing one without a remedy impossible rather than merely unlikely.
- **`PostAction`** (`config`) is spec 8.1's `Move` / `Delete` / `Noop` as a sealed set, and
  `Move.targetUnder(directory)` is where a relative target becomes a path. That method is the whole
  reason the type is not a string: a target of `temp/` is a different folder for each watched
  directory, and the probe, the validator and T10's executor all have to agree about which.
- **`EmbeddedSftpServer.start(separateFilesystemAt = ...)`** is a second root that renames cannot
  cross. It is a fault hook because a test cannot mount a second disk, and it is faithful because
  what a client sees is exactly what a real boundary looks like from a client: two ordinary
  folders, two ordinary listings, two ordinary stats, and one rename that fails with a status
  carrying no information at all.

**What a resolved path knows that a configured one does not.** Two of the checks exist because of
this, and both were found by review rather than by writing them.

- Spec 12's rule is "action targets are not equal to the watched directory", and comparing the
  configured strings enforces it only in the spelling somebody happened to use. `directories("drop")`
  with `onAck = move("/home/etl/drop")` is the same folder twice and is not the same string twice,
  and the connector would then have handed the same file to every poll it ever ran while succeeding
  at every step. The builder cannot know; the probe has the server's own answer, so the comparison
  is made there as well, against the resolved path.
- `move(".")` escapes it in the other direction: it resolves onto the watched directory and is
  never equal to it as a string. It is refused at build time for naming no folder, which is what it
  does - along with `""`, `"/"` and `".."`.

**Measured: `realpath` does not check that anything is there.** Against MINA SSHD 2.19.0, resolving
a path that leads nowhere and resolving one that leads to a file both succeed and return the
canonical name. So spec 11.1's "realpath of each watched directory" cannot be the whole check, and
the probe follows it with a `stat` that insists on a directory. Both tests for it go red - and only
those two - when that second half is removed, which is how the measurement was taken.

**Acceptance:**

- *Configuration validation failures surface as `ConfigurationError` before any connection is
  opened* - structurally, and then tested. `SftpConnectorConfig` has one producer, `sftpConnector { }`,
  which raises every fault it can decide on its own from a module that has no transport in it; the
  new rules are `ConnectorDslTest.an action target that is the watched directory itself is refused`
  and `.a move target starting with a slash is that path, and any other is under the directory it
  came from`. `SftpConnectorTest.an action that files a message back where it came from is refused
  before there is a connector` states it from the connector's own side.
- *Probe: realpath of each watched directory; mkdir of action targets when `createActionTargets`;
  marker rename into each target and back; `startupProbe = false` skips the marker rename* -
  `StartupAgainstServerTest`: `.a watched directory that is not there stops the connector from
  starting`, `.a watched directory that is a file stops the connector from starting`, `.the
  connector makes the folder its actions move files into, and leaves nothing else behind`, `.a
  folder the connector was told not to create stops it when nobody has created it`, and
  `.startupProbe off skips the marker rename and starts anyway`.
- *A cross-filesystem action target fails startup with `ConfigurationError` (S6)* -
  `S6_a move target on another filesystem stops the connector from starting`. See below.
- *`minIdle` fill runs in the background; the connector is usable before it completes* -
  `SftpConnectorTest.the pool fills to its minimum in the background, and the connector works
  before it has`. It asserts one session the instant `start` returns - the one the checks
  borrowed, one short of the minimum - answers a `client.exists` at that moment, and finds two
  spares after one housekeeping interval of virtual time.
- *Progress entry appended* - this.

Three tests beyond the checkboxes: `.an action target the server resolves onto the watched
directory stops the connector`, `.a connector that has started once starts again over what it
left` (the ordinary production case - every restart after the first finds the folder already
there), and `SftpConnectorTest.a start-up that was refused starts no housekeeper`.

**How S6 is enforced, not merely asserted.** Removing the marker rename fails `S6_` and nothing
else, which is the whole argument for the rename in one line: every other check passes against a
cross-filesystem target. The folder is there, the listing works, the stat works, and the connector
would have run happily until the first ack. Removing the resolved-path comparison in `prepare`
likewise fails only `.an action target the server resolves onto the watched directory stops the
connector`.

**What S6 stages.** The server serves one root holding `drop/` and `elsewhere/`, and is told that
`elsewhere` is on another filesystem. Its SFTP subsystem gets an accessor that refuses any rename
crossing that line with `AtomicMoveNotSupportedException` - the JDK's own name for what a kernel
answers a `rename(2)` between two mounts with - and MINA maps an `IOException` it has no other
mapping for to `SSH_FX_FAILURE`, which is exactly the featureless status D19 is about. Nothing else
about the folder differs: `mkdir` creates it, `stat` reports it as a directory, and only the move
fails. The test asserts the refusal names both paths and says "same filesystem", and then that
neither directory holds anything afterwards - a start-up that refuses and leaves a file on somebody
else's server is a start-up nobody will let run twice.

**Deviations:**

1. **`polling { directories(...) }` is a new knob that spec 12's DSL block does not have.** Spec
   11.1 step 2 asks for a check of "each watched directory" and spec 12's validation rules compare
   action targets against "the watched directory", so both already assume the configuration names
   them; `watch(dir, every)` takes a directory at call time, which is too late for a start-up
   check. The alternative was a parameter on `start`, which would put a value nothing validated
   outside the one type that is validated. **For the maintainer:** spec 12's block needs the line.
2. **`move(target, overwrite)` takes T7's `Overwrite` enum where spec 12 writes
   `move("temp/", overwrite = true)`.** This is T7 deviation 3 being honoured - a boolean cannot
   carry what replacing means on this protocol, and T7 asked that the ticket building `Move` take
   the type. It defaults to `REFUSE`, matching `SftpClient.rename`.
3. **The `minIdle` fill happens on the housekeeper's first round, one `housekeepingInterval` after
   start, rather than immediately.** Spec 11.1 step 3 asks that the fill be in the background and
   that readiness not wait for it, and both hold; spec 4.5 defines topping up as one of the things
   the housekeeper does every round. Making it immediate means the housekeeper sweeping before its
   first delay, which is a change to T5's function whose timing three of T5's tests are written
   against. A pool that is cold for thirty seconds works - it pays for one handshake - so the trade
   was not worth taking an earlier ticket's tests apart for.
4. **`config` now imports `sftp.connector.client.Overwrite`.** The same shape as T4 deviation 1
   (`error` importing `pool.PoolStats`) and accepted on the same grounds: the alternative is a
   second overwrite-shaped type in `config` meaning exactly what the first one means. Inside one
   module. Appealable, and the appeal would have to say what the second type buys.
5. **A start-up the probe refuses leaves the session it borrowed open.** The pool has no `close()`
   until T13, and half of one built here would be a seam nobody had designed - T1 and T3's
   precedent is absent rather than stubbed. The cost is one socket and a reader thread per refused
   start, which in production is a process that does not start anyway. On the open-seams table
   against T13, whose `close()` is what `start` should call on its own failure path.
6. **The `AcceptAll` warning is not repeated at start-up, which T1 deviation 2 left for this
   ticket to decide.** Spec 5.2 says the warning is logged "at startup", and T1 logs it while the
   configuration is built. Declined because in every real arrangement the two moments are the same
   moment - `sftpConnector { }` and `SftpConnector.start` are consecutive statements - so a second
   line would be a duplicate rather than a fact. If the Quarkus adapter ever builds configurations
   long before it starts connectors from them, this is worth revisiting there, where the gap would
   actually exist.
7. **`makeDirectory` was extracted from `SftpClient.mkdir` as an `internal` extension on
   `SftpSession`.** The probe needs the idempotent create-with-parents on a session it is holding,
   and `mkdir` had it locked inside a lease. No behaviour changed and T7's sixteen write-path tests
   pass untouched.
8. **Size.** About 470 lines that are neither blank nor comment across two new main files, four
   modified ones and two new test files, roughly 210 of them tests. Inside the 200-600 budget.

**For the next ticket:**

- **The housekeeper is launched by `SftpConnector.start`, after the checks and into a
  `SupervisorJob` scope of the connector's own. T13 stops it by cancelling
  `SftpConnector.backgroundWork`,** which is that scope's job and the only handle on it. T12's
  watchers belong in the same scope, which is why it is named for background work rather than for
  housekeeping.
- **`start` is where a failed start-up has to give things back.** Today it cannot, because there is
  nothing to give them back to. Once `close()` exists, `start` should call it before rethrowing.
- **`PollingConfig.actionTargetsUnder(directory)` is the one place a relative move target becomes a
  path,** and T10's lister has to exclude exactly those folders under recursion. Working out where a
  moved file went any other way is how two parts of the connector come to disagree about which
  folder is the temp folder.
- **`PostAction` is configured and only half read.** The probe reads `Move` and nothing reads
  `Delete` or `Move.overwrite`; T10's ack and nack are what make the sealed `when` exhaustive.
- **The probe runs on one session and every step of it is a `checking(trying, remedy) { }`.** A
  check added without a remedy does not compile, which is deliberate: the reason this class exists
  is the sentence it prints, not the round trips it makes.
- **`realpath` proves nothing about a path existing** - measured above. Any later code that treats a
  successful `realpath` as evidence the path is there is wrong on this server and on OpenSSH.
- **`EmbeddedSftpServer.start(separateFilesystemAt = "elsewhere")`** is testkit main source and
  available to any ticket that needs a move the server will not make. It is also the only way to
  stage a `ServerFailure` from a real server that is not about an occupied target.
- **A connector that watches nothing opens no session at start-up.** Every earlier ticket's test
  configuration names no `directories`, which is why nothing started dialling when this landed.

---

## T10: Poll with ack, nack, readiness and in-flight backpressure

**Built:** the hourly use case works end to end. `sftp.connector.source` exists, and
`SftpConnector.source.poll(dir)` is a cold flow that lists a watched directory once, holds back
files that are still being written, hands every ready file to the consumer as a `FileSeen` with
`ack`, `nack` and `download` on it, and ends with a `PollCompleted` that counts what it saw. An ack
runs the configured action - move with its overwrite policy, delete, or nothing - so a file lands in
`temp/` when the consumer says it is done. A file that vanished between listing and download comes
back as gone rather than as a failure. A file the consumer still holds is never handed over again by
any poll, the listing waits when `maxInFlight` files are out, and a collection that ends without
answering gives every place back. The polling block gained four knobs with build-time validation,
the four readiness checks and their `+` composition ship, and the four meters spec 13 names for the
source are published.

**Concepts named:**

- **`InFlightSet`** (`source`, internal) is the deep module, and its whole surface is `holds`,
  `admit` and the slot `admit` hands back. Three promises live behind it and nowhere else: a file in
  the set is not handed over again by any poll, the set holds at most `maxInFlight` files and a
  poll wanting one more waits for room, and every file comes back exactly once. Membership and
  exclusion are decided under one plain lock that nothing slow is ever taken inside; waiting for
  room is a `Semaphore` taken *before* the lock, and the consumer's work - the move, the delete -
  runs long after the lock was released. `admit` checks membership twice on purpose: once before
  the wait, so a duplicate never queues for room it will not use, and once under the lock after it,
  because a poll running alongside may have taken the same file while this one waited. The second
  check is I7 under a `PROCEED` overlap and the first cannot replace it - a test proves that by
  removing only the second.
- **`InFlightSlot`** is one file's place and the once-only guard. *Settling* and *releasing* are two
  steps: settling is the decision, taken first and atomically, so the second of two competing calls
  learns it lost and does nothing; the action then runs with the file still in the set, so an
  overlapping poll cannot hand it over while it is half moved; only then does the place go back.
  That ordering is I12, and it is also why an ack whose move fails leaves the file where it was
  and counts nothing - the file is re-listed next poll.
- **`Settlement`** (`ACK`, `NACK`, `CANCELLED`, `GONE`) is the closed set of ways a file leaves
  the set, and three of its labels are the `sftp_ack_total{outcome}` tags spec 13 fixes. `GONE` is
  the fourth because nobody answered: it is counted with the poll's files, as
  `sftp_poll_files{state=gone}`, not with the consumer's answers.
- **`SftpEvent`** is spec 7.1's four events for this ticket. `FileSeen` carries `ack()`, `nack(reason,
  redeliver)` and `download(localTarget)` as methods on the event rather than closures in fields;
  to a caller they read the same, and the once-only guard lives in the slot the event holds rather
  than in three captured lambdas. `PollSkipped` and `PollFailed` are absent rather than stubbed,
  following T1, and are T12's to add - the compiler names every consumer's `when` when they land.
- **`ReadinessCheck`** is a suspending `fun interface` over `(file, ctx)`, where `ReadinessContext`
  offers exactly what spec 7.5 says - `stat` on a session of the check's own, and the clock - and
  nothing else. `Readiness` is `Ready`, `NotReady(reason)` or `Skip`, and `Skip` has a producer:
  `MarkerFile` skips the markers themselves, so a directory full of `.done` files does not read as
  a directory full of stuck uploads. `AllOf` is the composite and `+` builds one, flattening.
- **`SftpSource.FileHandling`** is the per-directory half of an answer. The directory is the one
  thing an ack needs that the event does not carry: a relative target is a different folder under
  each watched directory, and it is resolved through `PollingConfig.actionTargetsUnder` - the same
  call the probe and the lister make, which is what stops three parts of the connector disagreeing
  about which folder `temp/` is.
- **`SourceMeters`** owns the four names and lives beside the source, for the reason `PoolMeters`
  and `ClientMeters` live beside theirs. The `result` label rule was one private function in
  `ClientMeters`; it is now `resultLabelOf` in the same file, shared by both timers rather than
  copied.

**Acceptance:**

- *poll returns a cold Flow of the sealed events PollStarted, FileSeen, FileGone, PollCompleted* -
  `SftpSourceTest.a poll is cold, and reports the listing as events`, which asserts no listing was
  sent before collection and pins the first and last events by value; `.a file gone at download
  time is reported, and needs no answer` for `FileGone`, including that it follows its `FileSeen`.
- *ack runs the ack action (Move with overwrite, Delete, Noop) and releases the slot; nack runs the
  nack action, releases the slot, and redelivers on a later poll unless redeliver = false* -
  `.an ack moves the file into its folder and gives the place back` runs the full replace sequence
  against the fake (a server without the rename extension, target occupied); `.a delete action
  removes the file, and a noop leaves it where it was`; `.a nacked file is handed over again on a
  later poll unless told otherwise`.
- *I12, I8, I7* - `I12_ack and nack are each accepted once per file`, `I8_cancelling a collector
  with unacked files gives every place back`, `I7_a file in flight is not handed over by any poll`,
  and `I7_a file two waiting polls both want is handed over once`. See below.
- *Readiness interface plus SizeStable, MinAge, MarkerFile, AllOf; default SizeStable(2, 10s) +
  MinAge(1m); not-ready files counted in PollCompleted* - `ReadinessTest`, four tests on fixed
  clocks; `SftpSourceTest.a file that is not ready is counted and looked at again next poll`;
  the default is pinned by `ConnectorDslTest.a poll that could hand over nothing is refused, and
  the defaults are the documented heuristic`, added to T1's file, additive only.
- *Action targets inside the watched directory are excluded from listing, also under recursive* -
  `.the folders actions move files into are left out of a recursive walk`: `temp/` and `failed/`
  under the watched directory are configured as the two targets, both hold files, and neither file
  is handed over while a file in an unrelated subdirectory is.
- *S5, S7, S12 against the embedded server* - `SourceAgainstServerTest`, through a started
  connector so the folder the start-up made is the folder the ack moves into: `S5_a file removed
  between the listing and the download is gone, not failed`, `S7_an ack without a download runs
  the move and transfers nothing` (the staging directory is asserted empty, which is the only way
  "no transfer" is visible from outside a real server), and `S12_a file listed again while in
  flight is handed over once`, staged as two concurrent collections by hand since overlap is T12's.
- *Meters sftp_poll_seconds, sftp_poll_files{state}, sftp_inflight, sftp_ack_total{outcome}* -
  `.the source publishes what a dashboard needs to watch a directory`, plus the gauge read in
  nearly every other test, since `sftp_inflight` is how a test sees the internal set at all.
- *Progress entry appended* - this.

Three tests beyond the checkboxes: `.the listing waits when maxInFlight files are out, and moves
on when one comes back` is the backpressure itself, proved by looking rather than waiting; `.a
consumer whose block throws gives every place back as well` is I8's other half, found by review;
and `.a poll of a directory the connector was not configured for is refused at the call`.

**How I7, I8 and I12 are enforced, not merely asserted.** Each was checked by breaking the set and
watching the right test - and only the right test - go red.

- **I7** is the membership check under `InFlightSet`'s lock. Removing every membership check fails
  `I7_a file in flight is not handed over by any poll`, `S12_`, and the redeliver-for-good test,
  because exclusion is the same check. Removing only the check *under the lock*, leaving the one
  before the wait, fails exactly `I7_a file two waiting polls both want is handed over once` - two
  polls waiting on a full set for the same file, room coming free twice - and nothing else.
- **I8** is the `catch (Throwable)` around the whole poll body withdrawing every slot it handed
  over. Replacing the withdrawal with a no-op fails `I8_` at "places still taken after the cancel"
  and `S12_` at its final in-flight count, which is I8 seen from the server side.
- **I12** is `InFlightSlot.settle`'s compare-and-set. Making it always succeed fails `I12_` alone,
  and with the honest symptom: the set itself refuses the second release, "the number of released
  permits cannot be greater than 16".

**How the locking works, and what runs outside it.** One plain lock guards two hash sets; every
section under it is a membership test or an insert or a remove, and none suspends. The semaphore is
acquired before the lock and released after it. `settle` is a compare-and-set with no lock at all.
Everything that touches a server - the readiness check's `stat`, the download, the move, the delete
- runs on a session of its own with nothing held. The listing's own session is held for the length
of the listing, which is spec 7.1's design: when the poll is waiting for room the lister is waiting
on the consumer. For a directory of fewer than the channel's 64 buffered entries the listing has
already finished and given its session back by then.

**Deviations:**

1. **`SizeStable` observes across polls, where spec 7.5 says "inside one poll" - please rule.**
   The coordinator's brief for this ticket steered this way: "`SizeStable` needs to remember what
   it saw last tick, which means it has state across polls and needs the injected `Clock`". The
   argument for it is in the class's own documentation - inside one poll means waiting `interval`
   per file, in turn, while holding the listing's session, so a hundred new files make a poll take
   a quarter of an hour. The cost, which the review found and this entry must state plainly: on
   the hourly pipeline the shipped default is ready on the *second* poll, which is an hour of
   latency per file where the spec's wording is ten seconds. The two readings need a decision, and
   it is tier 2 - spec 7.5's row and the default in spec 12. Options are to keep this and say so in
   7.5, or to make the observations concurrent inside one poll, which is a design the ticket did
   not have room for. What it remembers is bounded (10,000 files, oldest forgotten first, and a
   forgotten file merely costs one more poll), so the memory is not the problem.
2. **`FileSeen.download()` is a method on the event, which spec 7.1 does not list.** D17 says the
   download is a separate call and the consumer chooses when; it still is, and the consumer still
   may. But `FileGone` has to be produced by *something* that saw the download hit `NoSuchFile`,
   and the consumer's own `client.download` cannot tell the source. So the event offers the download
   that knows, returning null for gone - the same shape as `stat` returning null for a path that is
   not there - and releasing the place on the spot. `FileGone` is emitted when the download happened
   inside the collect block, which the after-`emit` check sees; a download after the poll has ended
   has no poll to speak, and gets the null and the counter. On the open-seams table.
3. **A collection that ends abnormally withdraws its files as `cancelled`, without running the nack
   action.** Spec 7.2 says cancellation "is treated as nack with redelivery"; here it is treated as
   redelivery. Running the nack action - by default nothing, but configurably a move to `failed/` -
   for files the consumer never looked at would file every unprocessed message as a failure on
   every shutdown, and it would be I/O inside a cancelled coroutine. The counter has its own
   `cancelled` label in spec 13, which reads as the same distinction. Review also widened *which*
   endings withdraw: originally only cancellation, now any - a consumer's block throwing, or the
   listing failing mid-way - because a place nobody will ever give back is capacity lost until
   restart. A consumer that stored events from a poll that failed and acks them later finds the ack
   ignored and the file handed over again next poll: at-least-once, and the application's ledger
   is what deduplicates (D14).
4. **`poll(dir)` refuses a directory the configuration does not name**, with
   `IllegalArgumentException` at the call. Only configured directories were checked at start-up
   and only their action folders were created, so a poll of any other would fail at the first ack
   an hour later. Not in the spec; the same reasoning as T9's `directories(...)` knob.
5. **`maxFilesPerPoll` and `recursive` are built; `sortBy` is not.** Spec 7.4 names all three and
   the ticket names none; the first two cost a parameter each and the walk needed `recursive` to
   mean anything. `sortBy` needs materialisation and a design, and nothing asked for it. The walk
   descends after finishing each directory's listing rather than as subdirectories are found, so it
   holds one session at a time however deep the tree.
6. **`SftpClient.list` gained `withDirectories: Boolean = false`.** T6's method, additive, every
   existing caller named its arguments, and its sixteen tests pass untouched. The alternative was a
   second listing mechanism, which T6's note said not to build.
7. **`nack(reason: Throwable, ...)`.** Spec 7.2 does not type `reason`. T12's `consume` nacks when
   the consumer's block throws, so the thing it has in hand is a throwable; a manual caller wraps a
   sentence in one. It is logged at WARN with the file and whether it will be seen again.
8. **Readiness constructor faults are `ConfigurationError` but not aggregated.** `sizeStable(0, ...)`
   raises at the moment the polling block runs, so an operator with that fault and another hears
   about them one at a time, where the builder reports everything else at once. On the open-seams
   table; the fix is the builder holding a description and constructing late.
9. **The pool needs at least two sessions for a poll whose readiness check stats.** The listing
   holds one and `ReadinessContext.stat` takes another, by spec 6.2. On a pool of one the stat
   waits `acquireTimeout` and fails with `PoolExhausted`. D21's five, "leaving one for the lister",
   already assumes this; it is written down here because a test with `maxSize = 1` would find it
   the hard way.
10. **`RenameClaim`, `ackWait` and `SeenRepository` are not built.** The first proves nothing on
    Linux by spec 7.5's own row and is spec 14.2's seam; the other two are off by default and not
    in this ticket.
11. **Three review findings were declined.** The cap applied twice - `maxEntries` on each listing
    and `take` on the walk - stays: the first is what T6's S11 pins at the server, the second is the
    only total under recursion, and they are one knob. `SftpSource`'s constructor defaults for the
    registry and the clock follow `SftpPool` and `SftpClient`. And `SourceMeters` keying its file
    counters by the tag string is four lines shorter than four named fields and reads the same.
12. **Size.** About 320 lines of main source and 420 of tests that are neither blank nor comment,
    plus 65 in modified files, against a 200-600 budget. Over the top of it, and the honest reading
    is that this slice is large: eight checkboxes, three invariants, three scenarios, four readiness
    checks, four knobs and 23 tests. Nothing here looked like it would get simpler for being
    smaller, but the next ticket should not read this as slack.

**For the next ticket:**

- **T11: do not retry `NoSuchFile` from `download`.** The conversion to gone sits in
  `SftpSource.FileHandling.download`, *outside* `client.download`, which is where the retry ladder
  will go. Spec 6.1's download row says a retry restarts from zero into a fresh `.part` file, and
  nothing about that cures a file that is not there. If the predicate retries it, S5 costs three
  attempts and a breaker failure, and T2's warning about a directory another system writes into
  opening the breaker comes true. On the open-seams table.
- **T12: `consume` must catch the block's exception and nack, not let it out of `collect`.** A
  throwable escaping the collect block now ends the poll and withdraws every unanswered file as
  `cancelled` - correct for a consumer that has died, wrong for one file that failed to parse.
- **T12: `PollSkipped` and `PollFailed` join the sealed interface**, and the tick counter is per
  source, so a watch's ticks continue the numbering its polls use. `FileHandling` is built per
  `poll(dir)` call and is cheap. S12 under a real `PROCEED` overlap is already the set's promise;
  the staged test in `SourceAgainstServerTest` is the same interleaving by hand.
- **T13: a poll waiting on its consumer holds the listing's session** for directories longer than
  the channel buffer, and cancelling the collector releases it through the cooperative tier - the
  selector answers `STOP` when the consumer is gone. The drain should expect a lease held by a poll
  and cancel the watchers before waiting for leases, which is spec 11.2's order anyway.
- **`sftp_inflight` is the only window onto the set from outside `core`.** `InFlightSet` is
  internal; tests read the gauge. A test that needs the set's contents rather than its size has to
  add a seam.
- **The readiness checks are `synchronized` where they keep memory,** because two polls of
  different directories share one `SizeStable` instance through the configuration. A check added
  later with memory of its own needs the same.
- **A suspending `fun interface` works in Kotlin 2.2:** `ReadinessCheck { _, _ -> Ready }` is how
  every test here says "always ready".
- **`FakeSftpTransport` needed nothing new.** `file`, `directory` and `remove` between polls, and
  its recorded `Rename`/`Delete` calls, staged everything this ticket asked.

## Corrections before T11

Two targeted fixes applied as two commits after T10, on the coordinator's instruction (C10 for the
first, C11 for the second). Neither is a ticket. 177 tests green afterwards (46 core, 131 testkit),
up from 169.

**Fix 1 - a listed name that escapes the staging directory is refused** (commit
`1403ccd`). `SftpClient.download` with no explicit `localTarget` now resolves the listed name
against the staging directory in one private place, `stagingTargetFor`, and refuses unless the
normalised result still starts with the staging directory *and* still ends in exactly the listed
name, and the name holds no backslash. A name the filesystem cannot spell (`InvalidPathException`)
is refused the same way. The refusal is `UnsafeFileName`, a new top-level class beside
`OverwriteRefused` on `ACCEPT_THE_REFUSAL`: no retry, breaker untouched, no session borrowed. The
message names the remote path and the staging directory. A caller passing its own target is not
guarded. `localTarget` became `Path? = null`, which let `SftpSource.FileHandling.download` pass the
consumer's choice straight through. The red run on Windows wrote `evil.csv` two directories above
the temp staging directory, into `%LOCALAPPDATA%`; that is the defect, demonstrated. The seam row
is struck through above. Tests: four in `SftpClientTest` (`..`, `..\..\evil.csv`, `C:evil`, and a
plain name), plus `UnsafeFileName`'s row in `FailureModelTest`'s exhaustive `when` - the one
earlier-ticket test touched by this fix, and only because the `when` will not compile without it.

**Fix 2 - `SizeStable` observes inside one poll, batched** (D36). `ReadinessCheck` keeps its single
abstract per-file `check(file, ctx)` - every `ReadinessCheck { _, _ -> Ready }` in T10's tests
compiles unchanged - and gains a non-abstract `check(files, ctx): Map<RemoteFile, Readiness>` that
defaults to asking per file. `SizeStable` overrides the batch form: stat every candidate, `delay`
one `interval`, stat again, `checks - 1` times; a single file is a batch of one. `AllOf` overrides
it so each check is asked only about the files every earlier check let through. `MinAge` and
`MarkerFile` are untouched, and `+` still flattens. The across-poll memory, its cap, its
`synchronized` and its use of the clock are gone. `SftpSource.poll` is now three phases: collect
the listing's candidates (bounded by `maxFilesPerPoll`), which returns the listing's session; run
readiness over the batch; emit.

*Earlier tests retargeted, as C11 mandates:* `ReadinessTest`'s
`a size is stable once it has held still across the interval, and a change starts over` asserted
the across-poll semantics with three fixed clocks and is replaced by
`a batch is stated twice one interval apart, and only the size that moved is not ready` (three
files, one grows during the wait, virtual time advances by exactly one interval for the batch),
plus `a size check over a batch remembers nothing between calls` and
`a composite over a batch asks each check only about the files still ready`. `SftpSourceTest`
gained `a poll pays the size check's interval once for all of its files` and
`the listing's session is back in the pool before any readiness check runs` (asserting
`pool.stats().inUse == 0` from inside a check), and its pool became a field so the second could
read it. Every other T10 test is unchanged and green.

**For T11 and later:**

- **A poll no longer needs a pool of two.** T10's deviation 9 is void: the listing is finished and
  its session returned before any check stats.
- **A poll waiting on its consumer no longer holds the listing's session** - the listing runs to
  `maxFilesPerPoll` before the first `FileSeen`, and it is the *emitting* that suspends on
  `maxInFlight`. T10's note to T13 about the drain expecting a lease held by a waiting poll is
  therefore weaker than written; a poll holds a session only while listing or while a check stats.
  The price is `maxFilesPerPoll` `RemoteFile` objects held per poll, which is what that cap is for.
- **`SizeStable` stats one file at a time**, `checks` round trips per file per poll, each on its
  own lease. A thousand candidates is two thousand stats. It is marked `ponytail:` in the source;
  a fan-out bounded by the pool is the upgrade if it ever shows on `sftp_poll_seconds`.
- **The `delay` inside `SizeStable` is on the poll's coroutine**, so cancelling the collector
  cancels the wait, and under `runTest` it is virtual time.
- **T10's note that checks with memory must be `synchronized` no longer applies to anything
  shipped**; a custom check that keeps memory across polls still needs it, for the same reason.

---

## R1: Fable review of the pool and the ladder (T3-T5, T8)

A review-and-fix session under C12, on `claude-fable-5-1`, of code it did not write: `SessionRegistry`,
`SftpPool`, `PoolEntry`, `CancellationLadder`, `PoolMeters`, the JSch adapter's `connect`, `abort` and
progress monitor, and the tests under `pool/` and `CancellationLadderTest`. 177 tests were green at the
start and 180 are green at the end (46 core, 134 testkit). Three findings were fixed in three commits,
each with the invariant it restores in its message; the rest are recorded here. No earlier test was
edited.

The method was the one the brief asks for: trace every exit path of `acquire`, `withLease`, `proves`,
`giveBack` and `sweep` for the permit and the session; walk every ordering of a cancellation against
the ladder; then check each belief the T3-T8 entries record against the mechanism it rests on. Two of
the three fixes are in places every earlier test walked past - because the fake transport answers a
connect on the caller's own coroutine, and because nothing had yet cancelled the housekeeper.

**Findings, by severity.**

*High.*

1. **A session that finished its handshake into a cancelled caller was left running, with the pool
   never told** - `JschTransport.connect`, commit `f92310b`. T4 closed the gap between `connect()`
   returning and `registry.filled`, and its test proves it with the fake. The real adapter runs the
   handshake inside `withContext(io)`, and a `withContext` that switches dispatchers hands its result
   back through a *cancellable* resume on the caller's dispatcher: when the caller's job is cancelled
   by then, the value is replaced with the `CancellationException`. Interleaving: `acquire` → `dial` →
   `session.connect` on the IO thread; the caller is cancelled (a consumer walking away, a
   `withTimeout`, a shutdown); the handshake finishes anyway; the pool's `catch` sees an entry with
   `connection == null`, discards it with nothing to close, and gives the permit back. I4 held and the
   session leaked: a socket, JSch's reader thread, and a server-side session the keepalive keeps alive
   for the life of the process. Reproduced by `LadderReviewTest.I4_a session that finishes its
   handshake into a cancelled caller is hung up on, not left running`, which lands the cancellation on
   the first bytes the client sends through the tunnel and then asks the server how many sessions it
   holds - red for the full five-second bound before the fix. The fix keeps hold of the session on the
   producing side and hangs up on it when the scope drops it; the transport's contract - a session you
   own, or a throw and you own nothing - is unchanged. **The first attempt, `withContext(io +
   NonCancellable)`, was tried and did not work**: the drop is at the delivery, not in the block, so
   `NonCancellable` inside the switch protects nothing. That is now a seams row, because T12 and T13
   will both write code of this shape. Two corrections from the self-review followed in
   `49bc341`: the hang-up on the orphan runs through the error mapper, so a hang-up that
   failed would have replaced the `CancellationException` with a mapped failure - it is now caught
   and warned about, the way `SftpPool.close` does it; and the commit's `(I4)` overstates - the permit
   came back, and what leaked was the session, which is T4's own checkbox ("releases the permit and
   closes the half-open entry") rather than the numbered invariant. The test lost its `I4_` prefix
   for the same reason.
2. **A housekeeping round cancelled between deciding and doing stranded what it had decided** -
   `SftpPool.sweep`, commit `48d0d0f`. `SessionRegistry.sweep` retires sessions and reserves room for
   spares under the lock, and returns; `SftpPool.sweep` then closed and dialled with nothing between
   the two that survived a cancellation - which is what T13's shutdown will do to the housekeeper.
   Cancelled while hanging up on the first retired session (the fake's `close` hook, or in production
   `registry.closed` waiting for a contended lock), the rest of the round's list was dropped, and that
   list was the last reference to those connections: sockets and reader threads for the life of the
   process. Cancelled while dialling the first spare, every spare after it stayed registered as
   `Connecting` with its permit taken - I1's bound eaten from the inside, `sftp_pool_active` counting
   sessions that do not exist, and invisible to leak detection, which watches only the states a caller
   holds. Two tests in `PoolReviewTest` on virtual time: `I4_a housekeeper cancelled while opening
   spares gives back every room the round reserved` (one entry stranded before, the pool fills to its
   size after) and `a housekeeper cancelled while hanging up on one retired session still hangs up on
   the rest` (two sessions left open before). Retired sessions are now closed under `NonCancellable`
   and every reserved entry is dialled or given back in a `finally`. The self-review found the
   retired loop sitting outside that `try`, so an `Error` out of a hang-up (which `SftpPool.close`
   deliberately lets through) would still have stranded the reserved entries; `9eb4962` moves it
   inside. On the label: the stranded entries never took the pool past `maxSize`, so `(I1)` in the
   commit is loose - they ate the bound from inside it; the invariant restored is I4 for the
   permits, and I9's "leaves every entry `Closed`" for the retired list.

*Medium.*

3. **A permit granted at the instant its waiter is cancelled was lost** - `SftpPool.admit`, commit
   `37d4597`. **Reasoned, not reproduced.** T3's note that `Semaphore.acquire()` is
   cancellation-safe is true of the semaphore: it gives the permit back if the waiter is cancelled
   while suspended, and again if the cancellation is seen when the granted continuation is dispatched.
   What it does not cover is the scope around it. `withTimeoutOrNull { capacity.acquire() }` completes
   its block *after* the permit is in hand, and if the caller's cancellation lands between the
   dispatched resume's activity check and the block's completion - or, on the fast path, between
   `tryAcquire` failing and `acquire` taking a permit freed a moment later by another thread - the
   scope finalises as cancelled and throws the permit away with its result. `admit` runs before
   `acquire`'s `try`, so nothing downstream frees it. The window is a few instructions wide and needs a
   release and a cancel from two other threads to land inside it, which no scheduler a test controls
   can order; a stress test would be probabilistic and was not written. The fix is the smallest that is
   obviously right: a flag set inside the block the moment the permit is held, read on the way out
   instead of the wait's own answer, and `freeRoom()` on the throwing path when it is set. A waiter
   granted at the last instant of its timeout is now served rather than turned away, which is also
   right. Covered for non-regression by T4's `a caller that cannot be served is turned away`, `I4_`'s
   timed-out path, and the pending-gauge test.

*Low, noted, not changed.*

4. **A cancelled acquire throws away a healthy session.** `acquire`'s `catch` discards the claimed
   entry as `POISONED` on every path, including a `Reuse`, a proved `Prove`, and a dial that landed. The
   session is fine in all three; the cost is a handshake per cancelled borrow, not a leak. Handing the
   entry back to the shelf instead would need to exclude the dial that never landed (no connection) and
   the validation the ladder cut (`unfitAfterCancelling`), and T4's `a session that opens into a
   cancelled caller is closed rather than left running` pins `Connect, Close` for the filled case. Left
   as it is, recorded as the price it is.
5. **`abort()` may block the aborting thread on a wedged peer.** From memory of the library rather than
   its source (no sources jar locally, so not verified against the pinned fork): `Session.disconnect`
   disconnects each channel first, which sends a channel-close packet, and only then closes the socket.
   On a peer whose receive window is shut and whose TCP send buffer is already full, that write blocks
   with no timeout, and the ladder's `NonCancellable` cut waits behind it. T8 measured the common case
   - a stalled proxy - returning in under a second, which is the case that matters; the pathological
   one has no fix inside the library's API. Separately, `abort()` runs on the caller's thread by design;
   on an event-loop host that is a blocking socket close on the loop, which T14 should know.
6. **`PoolMeters` on a shared host registry.** Registering a gauge whose id already exists returns the
   existing gauge, so a second `SftpPool` for the same endpoint on one `MeterRegistry` keeps reading the
   first pool's `lastCount`. Nothing throws; the numbers lie. T14's binding is where this is decided.
7. **`reason=poisoned` for a cut session.** The seams row stands; noted, not touched.
8. **`Lease.connection` reaches `abort()` and `close()`.** Decided: left, with the reason. Closing it is
   narrowing the property to `SftpSession` - one word - plus one type argument in T3's `I2_` test, which
   builds a `mutableSetOf<SftpConnection>()` from it. Nothing reaches the hole today: every production
   borrower goes through `SftpClient`, and `withSession` is guarded by `BorrowedSession`. T13 has to
   reshape `Lease` to cut leases during the drain and is the ticket with cause to narrow it in the same
   change; doing it here would be an edit to an earlier test for a hole nothing exercises.

**Beliefs from the T3-T8 entries, checked against the mechanism.**

- *T3, I1/I2/I5 mechanisms* - confirmed. I5 in particular: the registry is handed no transport,
  `sweep(takeRoom)` runs `Semaphore.tryAcquire`, which is a CAS loop with no suspension and no
  re-entry, and nothing else under the lock touches a meter or a socket. `lastCount` is a volatile
  read plus an atomic read; no gauge path can reach the mutex. `PoolMeters` registration cannot throw
  on a fresh registry, and on a shared one degrades as in finding 6.
- *T3, "`Semaphore.acquire()` is cancellation-safe, so a `withTimeoutOrNull` around it cannot leak"* -
  **falsified** (finding 3). The semaphore protects its own suspension; the scope does not protect its
  value.
- *T4, the after-connect gap is closed* - confirmed for the pool, **falsified as complete** (finding
  1): the same gap one layer down, and the fake cannot show it.
- *T4, `freeRoom()` is the only release path* - confirmed on every exit traced: success, a returning
  failure, a poisoning failure, an unclassified error, cancellation before the block runs, during it,
  at the instant it completes, during `proves`, during `proved`'s lock, a failed dial, a cancelled dial,
  the housekeeper's failed and cancelled dials, and now finding 3's path. The release-once guard on
  `Lease` means a `release()` that throws and is followed by `releaseAfter` in `withLease`'s catch frees
  exactly one permit.
- *T4, `NONE_HELD` keeps the session* - confirmed, and the reasoning stands.
- *T5, `sweep(takeRoom)` is safe by type* - confirmed, see above.
- *T5, `entries.size < maxSize` bounds the top-up* - confirmed, jointly with the permit. The two views
  do disagree: an entry retired but not yet freed is in neither `entries` nor the free permits, and an
  idle entry is in `entries` and holds no permit. Every disagreement makes the top-up more
  conservative, never less. Lifetime eviction on release against the housekeeper: no double retire,
  because `sweep` sees only idle entries and `handBack` decides under the same lock.
- *T5, nothing collects `state`* - still true (grepped). The hazard is also smaller than the seams row
  says: an `Unconfined` collector runs on the setter's stack only until its first suspension, and a
  re-entrant `stats()` from it suspends on the mutex rather than deadlocking, so the cost is latency in
  the critical section, not correctness. Still theoretical; the row stays.
- *T8, `supervisorScope` is load-bearing* - confirmed by mechanism: a job that is cancelling and whose
  block throws a non-cancellation exception finalises with that exception as the root cause, so under
  `coroutineScope` the cut session's `SessionLost` would become the scope's own. Every ordering was
  walked: cancellation before the block runs (the child never starts; the session goes back `Idle`);
  during a transfer the monitor stops (the child ends with the `CancellationException`, `Idle`);
  during a call the keepalive ends inside the grace (`SessionLost` is the child's root cause,
  `unfitAfterCancelling`, `POISONED`); after the grace (`cutLoose`, the scope waits for the child to
  return from the closed socket, `POISONED`); and after the keepalive had already ended the read
  (`await` throws `SessionLost` without suspending, the caller is told the session died, `EVICTED`).
  Every reported exception is the truthful one for its ordering.
- *T8 deviation 4, `ensureActive()` at the end of `transferring`* - confirmed harmless; nothing between
  the monitor's stop and it can throw first, since JSch returns normally from a monitor-stopped
  transfer. It is also, as it turns out, redundant: a `withContext` whose job was cancelled completes
  with the `CancellationException` even when its block returns normally. Left in.
- *T8 deviation 5, the ladder does not wrap `dial`* - confirmed as the right call; finding 1 is what
  was actually missing on that path.

**Seams.** Two rows added above: the dispatcher-switch drop, and "cancelled is not proof it did not
land". Closed none. The `Lease.connection` row is answered by finding 8 and stays for T13.

**For T13 (shutdown) and T12 (watch).** After finding 2, cancelling the housekeeper waits for the
closes of the sessions the current round had retired; they run on the IO dispatcher, so a drain
should cut blocked leases first or in parallel, or the housekeeper's cancel queues behind them. After
finding 1, a cancelled top-up dial closes its own session in the transport and the pool sees a dial
that never landed; nothing is parked by a cancelled housekeeper. Every `withContext(dispatcher)` that
produces something owned is the seams row's shape.

---

## R2: Fable review of the write path and compensation (T7)

A review-and-fix session under C12, on `claude-fable-5-1`, of code it did not write: `SftpClient`'s
write operations and `moveOnto`, `Overwrite`, `BorrowedSession`, the write operations of
`JschConnection`, and the tests under `client/` and `WritePathAgainstServerTest`. 180 tests were
green at the start and 189 are green at the end (46 core, 143 testkit). Five findings were fixed in
five commits, each with the invariant or the truth it restores in its message; the rest are recorded
here. No earlier test was edited. Every finding has a failing test in `WritePathReviewTest`, run red
before its fix; the extension and the SSH library's own behaviour were reproduced against the
embedded server, because the fake is a server without the extension that takes every path literally.

The method was the one the brief asks for: trace every path through `moveOnto` and `clearTheWay` under
both policies against a server with and without the extension, asking at each request what the server
holds afterwards and what the caller is told; read the SSH library's source (the sources jar was
fetched for this) for what it does with a path before it is sent; walk `withSession`'s loan against a
block that launches work it does not wait for; and check each promise the T7 entry makes to T11
against the mechanism it rests on.

**Findings, by severity.**

*High.*

1. **JSch reads a path as a pattern, so a rename or an upload could land on a neighbour and a delete
   could remove every file that matched** - `JschTransport.kt`, every path-taking operation, commit
   `ed5af6f`. `ChannelSftp.rename`, `rm`, `put`, `get`, `stat` and `ls` treat `*` and `?` in the last
   component as wildcards and list the directory to resolve them (`glob_remote`), with a backslash as
   the escape, stripped on the way out; `mkdir` and `realpath` send the path raw. Interleaving: a
   `rename(from, "/drop/temp/l*.csv", REPLACE)` had JSch list `/drop/temp`, match `ledger-old.csv`,
   and send a posix-rename onto *that*, which the server replaced and reported as success;
   `upload(..., "/drop/l*.csv", REPLACE)` did the same to `ledger.csv`; `delete("/drop/*.csv")` sent one
   remove per file that matched, and removed both. A name the server listed is untrusted input and a
   POSIX name may hold either character, so the source's own ack action could do this to a neighbour.
   Reproduced by the three `..names one file..`/`..lands on the name it was given..` tests against the
   embedded server (legal neighbour names, a wildcard in the requested path; the literal name itself
   cannot exist on the Windows host, so the assertion is that the neighbour is untouched, which holds
   on every host). Fix: `literally()` escapes `\`, `*` and `?` at the one place the library is handed a
   path, for exactly the operations that unquote it. The read path (`list`, `stat`, `readTo`) had the
   same defect - a download of a listed `a?.csv` could fetch a different file - and is covered by the
   same fix, which is why it sits in the transport and not in the client.
2. **`REPLACE` deleted a healthy target when the rename could never have landed** -
   `SftpClient.moveOnto`, commit `86c7a28`. Look-before-delete (T7 deviation 4) reads a refusal with an
   occupied target as "the target is in the way". S6's case with a file at the target: rename refused
   (other filesystem), look says occupied, delete the healthy target, rename refused again,
   `ServerFailure` passed on with nothing said about the file that is gone. Reproduced against the
   embedded server with `separateFilesystemAt`: `/other/ledger.csv` was gone after the refused rename.
   On a server that advertised the POSIX rename extension a refusal is never about occupancy, because
   that server replaces without being asked - so `SftpSession` gained `renameReplaces`, read from the
   handshake by the JSch adapter (`getExtension("posix-rename@openssh.com") == "1"`, the same test the
   library applies before choosing the request) and `false` in the fake, and on such a server the
   refusal is passed on as given. A seam by the codebase-design skill's test: two adapters answer it
   differently. On a server without the extension the target is cleared only when it is a file: a
   directory there is not what replacing a file means, and against the fake, which does not know a
   directory from a file when deleting, the old sequence deleted the directory and put the file in
   its place (`..refused by a directory at the target..`). The residual loss on a non-extension server
   with a file at the target stands (finding 5, seams row).
3. **A rename's "no such file" was reported against the source when it was about the target** -
   `SftpClient.moveOnto`, commit `3724927`. T7's promise to T11 - "a missing path reported by `rename`
   is always the source, never the target" - was true of the connector's own delete and false of the
   server: it answers `SSH_FX_NO_SUCH_FILE` when the target's directory does not exist, and the
   transport builds the attempt on `from`. Interleaving for I11: reply lost, retry on a fresh session,
   `NoSuchFile` naming the source, stat the target, nothing there, report the source gone - while it
   sat where it always was. Reproduced against the embedded server under both policies
   (`..names the target as missing..`). Fix: on that answer the source is looked at; still there, and
   the failure names `to` on `attempt.path`, keeping the server's words in the message. The
   discrimination T11 reads is now on the class *and* on the path it names (below).

*Medium.*

4. **A call still in flight when the `withSession` block ended kept using a session the pool had
   re-lent** - `BorrowedSession`, commit `6663d44`. The loan was revoked by a flag read at the start of
   each call, so a call that had passed the check and was on the wire when the block returned - one
   the block launched and did not wait for - ran on after `withLease` released the lease: two callers
   on one channel, I2 broken from outside the pool. Reproduced against the fake on virtual time
   (`I2_a call still in flight..`): `withSession` returned with the escaped `realpath` still parked
   inside the transport and `stats().inUse == 0`. Fix: calls are taken one at a time under a mutex and
   the revocation takes the same lock, so the loan cannot end while a call is in flight and no call can
   start once it has; `withSession` ends the loan under `NonCancellable`, because a cancelled block is
   the likeliest to have left a call behind. The wait is bounded by the ladder when the caller is
   cancelled and by the keepalive floor otherwise - the bound every call already has. Cost: a call made
   from inside another call's callback (a stat from a listing's entry callback) now waits on the lock
   instead of corrupting the channel's stream; neither was ever going to work. `close()` and `abort()`
   are confirmed unreachable through the borrowed session: it implements `SftpSession` only and holds
   the connection as one, so no cast reaches them.
5. **After clearing the target, a second refusal said nothing about it** - `SftpClient.moveOnto`,
   commit `ed63778`, raised by the spec-axis self-review. On a server without the extension the
   sequence cannot tell an occupied target from a rename the server could never do, so a refusal with a
   file at the target still clears the file and is refused again; spec 8.2 mandates the sequence. The
   second `ServerFailure` read as "the source is still where it was" and let a caller take it for
   "nothing changed". It now says the target was cleared and holds nothing. Proved red against the fake.

*Low, noted, not changed.*

6. **A local I/O failure inside a transfer is `SessionLost`.** JSch wraps an `IOException` from the
   caller's stream into its status exception with the generic code and the exception as cause, and the
   mapper reads that shape - correctly for the wire - as the connection breaking. A full local disk
   under a download poisons a healthy session and retries on a fresh one. Mapper territory; seams row.
7. **A local failure inside a lease evicts a healthy session.** `upload` opens the local file and
   `download` the partial inside `withLease`, so a `java.nio` exception there is unclassified and
   `releaseAfter` poisons. R1 finding 4's price, not a lie; seams row.
8. **`writeFrom` leaves the caller's stream open.** Documented at the interface, mirrors `readTo`, and
   `upload` closes its own with `use`. A contract, not a footgun; T7's note about `@TempDir` teardown
   on Windows is the cost of getting it wrong in a test and stands.
9. **`literally()` is a name that needs its KDoc** (standards-axis, judgement call). Left.

**Beliefs from the T7 entry, checked against the mechanism.**

- *"A missing path reported by `rename` is always the source, never the target"* - **falsified**
  (finding 3). True of the connector's delete, false of the server.
- *"`ServerFailure` means the source is still where it was"* - confirmed on every path, including the
  landed-then-cancelled orderings: the ladder and every dispatcher switch back replace a landed
  call's outcome with the `CancellationException`, never with a failure class, so no path reports a
  landed rename as `ServerFailure`. What `ServerFailure` did not say was whether the *target* is still
  where it was (finding 5).
- *"Refusing is the connector's decision; the window is the look"* - confirmed. On `upload` and
  `rename`, explicit and relative targets alike (JSch resolves both the stat and the request against
  the same working directory), the window is between the stat's reply and the request's arrival and
  nothing wider. Through the glob (finding 1) a wildcard target made the look answer for a neighbour,
  which was a false refusal rather than a bypass. No bypass found.
- *"The delete inside the replace sequence swallows `NoSuchFile`"* - confirmed, and still the right
  call: it keeps the connector's own clearing out of the signal T11 reads.
- *"`REPLACE` is look, delete, rename, with a gap"* - confirmed as the sequence on a server without
  the extension, and now *only* there. Enumerated at each of the three points: a writer between
  refusal and look wins and is replaced (that is what `REPLACE` means); a lost reply on the delete
  retries into either a clear target or the same sequence; a lost reply on the second rename retries
  into `NoSuchFile` naming the source, which I11 resolves; a cancellation between delete and rename
  leaves the target empty and the source in place, and the caller is told "cancelled", which R1's row
  says is the truthful answer. The one lie was finding 5.
- *"`upload` writes straight to the target; a retry restarts over the top"* - confirmed for `REPLACE`.
  **Under `REFUSE` the retry is refused by its own success** - seams row, below, for T11. The same is
  true of `rename` under `REFUSE`, which T7 did not say.
- *"`mkdir` is idempotent whatever `parents` says"* - confirmed, now against the real server too
  (`mkdir twice against a real server..`).
- *"`withSession` revokes the loan when the block returns"* - **falsified as stated** (finding 4); it
  now ends when the last call on it does, which is the only version that keeps I2.
- *Every JSch write call goes through `translating` on the bounded dispatcher while holding a pool
  place* - confirmed by reading every call site: `rename`, `delete`, `mkdir` are `withContext(io) {
  translating {..} }`, `writeFrom` goes through `transferring`, and every client operation borrows
  through `withLease` first. `renameReplaces` is a field set at construction, no call.

**Seams.** Four rows added above: `REFUSE` retried after a lost reply refuses itself; the residual
loss on a non-extension server; the mapper's reading of a local I/O failure; a local failure inside a
lease. The R1 row "a cancelled `withLease` is not proof the operation did not land" stays, owner T11,
with its consequence spelled out below. Closed none.

**Spec findings, raised and not applied** (scope was `progress.md`, C9's protocol): spec 6.1's rename
row should read "on `NoSuchFile` *naming the source* after a retry, stat `to`"; spec 6.1's
`withSession` sentence "revoked when the block returns" should say the loan ends when the last call on
it does; spec 8.2's "on failure delete the target then rename again" should say the target is cleared
only on a server without the extension and only when it is a file, and that on such a server a refusal
after the clearing leaves the target empty; and spec 5.2 should note that the SSH library reads `*`
and `?` in a path as a pattern and that the adapter escapes them. `SftpSession.renameReplaces` is a
transport-interface addition that spec 5.1's list of operations does not name.

**What T11 may rely on for I11 - the discriminator, operation by operation.**

- **`rename(from, to, policy)`**, after any attempt whose outcome was lost (`SessionLost`,
  `OperationTimeout` from the time limiter, or a `CancellationException` T11 chooses to retry after):
  - `NoSuchFile` with `attempt.path == from`: the source is not there. Either it never was or an
    earlier attempt landed. Stat `to`: present with the expected size, the move landed and the call
    succeeds (I11); absent, the file is at neither place and `NoSuchFile` is the truth.
  - `NoSuchFile` with `attempt.path == to`: the source is still there and the target's location does
    not exist. Nothing landed; the failure is deterministic and a fresh session will not change it.
  - `ServerFailure`: the source is still where it was. If the message says the target was cleared,
    the target is gone as well (a non-extension server only). Deterministic.
  - `OverwriteRefused` **on a retry**: the look found something at `to` *before* the request went
    out, and one of the things it can find is the earlier attempt's own landed file. T11 must not
    read it as final on a retry: stat `to` and apply I11, or - the cleaner shape - decide the policy
    once before the first attempt and send every attempt as a replacement.
  - `PermissionDenied`: source still where it was, nothing landed.
  - The `CancellationException` case: no information at all, by R1's row. Landed or not are both
    possible, including for the second rename of the replace sequence. Treat as "reply lost".
  - The expected size is the source layer's `RemoteFile.size`, as T7 said; `rename` takes none.
- **`delete(path)`**: `NoSuchFile` after a retry is success. With finding 1 a path is literal, so
  that answer is about this path and no other.
- **`upload(local, remote, policy)`**: an attempt that landed and lost its reply leaves a whole file
  at `remote`; one cut short leaves a partial one. "My upload landed" is `stat(remote).size` equal to
  the local size - the same size discriminator as I11; there is no digest on the write path and
  nothing to compare one against without downloading. Under `REPLACE` a retry restarts over the top
  and needs no discrimination. Under `REFUSE` the retry is refused by its own file (seams row): the
  look must run once, before the first attempt, and the write is a replacement on every attempt.
- **`mkdir`**: retry blindly; a directory already there is the outcome.
- **`withSession`**: no retry, as the spec says; and the loan ends when the last call on it ends, so
  a block that launched work and did not wait for it delays its own lease's return rather than
  handing the pool a session that is still in use.

---

## T11: Resilience and transparent reconnect

Built on `claude-fable-5-1`. 189 tests were green at the start and 216 are green at the end (50 core,
166 testkit): 4 new in `ResilienceDslTest`, 18 in `RetrySemanticsTest`, 5 in
`ResilienceAgainstServerTest`. One earlier test's *setup* was touched and is declared under
Deviations; no assertion was changed. The self-review (standards and spec axes, two sub-agents)
found two things that were fixed before the commit and are folded in below: the breaker's clock
*can* be injected, so S3 runs on virtual time and a settable clock rather than a real wait; and a
retry under `REFUSE` that sent its rename without looking first could, on a server with the POSIX
rename extension, move a file the uploader had since put at the source over the one that had
already landed - so a retry after a try that reached the server now looks for its landed file
before it sends anything.

**Built:** a session that dies under an operation, a proxy that refuses for a while, or a server
that stalls no longer reaches the caller as a failure unless the budget is spent or the failure is
final. Every client operation runs through `sftp.connector.resilience.Resilience`, which is
`Retry( CircuitBreaker( TransferLimit( TimeLimiter( withLease { op } ) ) ) )`, outermost first,
each try on a fresh lease. The retry, breaker and time limiter are Resilience4j's; the transfer
limit is a coroutine semaphore (Deviation 1). A `resilience { }` block in the DSL carries
`retry { maxAttempts; backoff = exponential(initial, max, jitter) }`, `circuitBreaker {
failureRateThreshold; slidingWindow; waitInOpen }`, `bulkhead { maxConcurrentTransfers }`,
`operationTimeout` and `transferTimeout`, all validated at build time. `sftp_retry_total{op}` and
`sftp_breaker_state` are published under the endpoint. `OperationTimeout` has a producer.
`Attempt.number` counts the try, and a full pool names the operation that was queued for it.

**Concepts named:**

- **A try knows three things the operation does not** - which number it is, whether an earlier try
  *reached the server*, and what proves that try landed - and those live in one place per
  operation. **`RenameTries`** (`client/Compensation.kt`) holds all three for a rename: the size the
  file had before the first request (from the caller's listing, or measured once when the caller has
  none), whether a try before this one sent the request, and whether the target has already been
  looked at and found free. A missing source after a try that reached the server sends it to look at
  the target: a file of the expected size there is the landed rename and the call succeeds (I11); no
  file, or a file of another size, and the missing source is the truth. The look comes *before*
  the retry's own rename as well as after a missing-source answer, because on a server that
  replaces without being asked a rename sent regardless would move whatever the uploader had since
  put at the source over the file that had already landed. `upload` and `delete` hold
  the one fact each of them needs as state captured across tries in the operation itself: `upload`
  that its look ran, `delete` that a try reached the server. `mkdir` and `download` need none -
  a directory already there was the outcome, and a download restarts into a fresh partial.
- **"Decided once."** Under `REFUSE` the look at the target runs on the first try that gets to
  look, and never again: a later try would find the earlier try's own landed file there and refuse
  the operation for it - a phantom failure with the one disposition that says never retry (R2's
  seams row). Every later try is sent as a plain request against a target already known to be the
  connector's to take; under `REPLACE` the sequence is the same one T7 built, on every try.
- **"Reached the server", not "failed how".** The fact I11 rests on is whether a request went out,
  which is recorded before the request is sent rather than inferred from the class of what came
  back. A `ConnectFailed` on the first try therefore does not make a missing source on the second
  look like a landed rename - nothing was sent - and a cancelled try, had one been retried, would
  count the same as a lost reply (R1's row: "a cancelled `withLease` is not proof the operation did
  not land"). Nothing retries after a cancellation, because the caller is gone.
- **`CurrentAttempt`** (`error`) is a coroutine-context element carrying the try. The transport and
  the pool know what they were asked and not how many times; the retry loop puts the try in the
  context and `Attempt.inside(endpoint, op, path)` reads the number off it, so every failure raised
  anywhere under a try names it, and `PoolExhausted` names the caller's operation and path rather
  than the pool's own `acquire`. The fake transport does the same, so a test can assert `attempt=3`.
- **The retry predicate reads the `Disposition` and makes one reading** (Deviation 2): a try is
  repeated only when its failure is `RETRY_ON_A_FRESH_SESSION` - the wire produced it and the reply
  was lost. A failure the server *answered* (`NoSuchFile`, `ServerFailure`, `PermissionDenied`) is the
  server's decision about that request, the same server asked again a second later decides the
  same, and by the time it arrives at the retry the operation has already said what it means to it.
  Its retry is the next tick, which is the source's business. The breaker predicate reads
  `countsAgainstTheBreaker` with one exclusion, `NoSuchFile` (Deviation 3).
- **The seams.** `Resilience` is the one place the order lives; its interface is two methods,
  `attempting(operation, path, transfer, unhurried, stillWorthRetrying, block)` and `once(operation,
  block)` for `withSession`, which the breaker guards and nothing else touches. The client's
  operations are the per-operation semantics and nothing about retrying; the transport and the pool
  are unchanged except for reading the try off the context. `Compensation.kt` took `moveOnto`,
  `renameNamingWhatIsMissing`, `clearTheWay`, `entryAt` and `refuse` out of `SftpClient` unchanged,
  because `RenameTries` is the thing that composes them now.

**Acceptance:**

- *Retry, CircuitBreaker, Bulkhead, TimeLimiter wrap every client operation, outermost first in
  that order.* `Resilience.attempting` is the order, read top to bottom. `RetrySemanticsTest.an open
  breaker fails a call fast, before any session is asked for` proves the retry sees the breaker
  (`CircuitOpen` ends the loop, no dial); `a try that runs out of time is reported as a timeout and
  tried again` proves the time limiter sits inside the retry and the breaker; `transfers past the
  limit wait for one to finish, and a waiter that gives up frees its place` proves the transfer
  limit and its cancellation. The bulkhead is not Resilience4j's - Deviation 1.
- *Only recoverable errors are retried and counted by the breaker; fatal errors short-circuit and
  are not counted.* `a failure whose disposition says never is not tried again and not counted`
  (`AuthenticationFailed` and `OverwriteRefused` each against a breaker of one call that a single
  counted failure would open; `PermissionDenied` is not retried, and *is* counted, as its
  disposition says); `S10_` against the real server. `a failure names
  which try it was, and a full pool names the operation that was queued for it` proves
  `PoolExhausted` is not retried and carries the caller's attempt. What "recoverable" means to the
  retry is narrower than spec 10.2's table - Deviation 2.
- *Per-operation semantics.* `I11_a rename retried after a lost reply reports success when the
  target holds the expected size`, `I11_a stranger's file at the target is not taken for the landed
  one`, `I11_a rename with no size given measures the source once before sending anything`, `a
  refusing rename asks about the target once, so its retry is not refused by its own landed file`;
  `a delete retried after a lost reply reads a missing path as the delete having worked` and `a
  delete of a path that is not there is reported on the first try and not tried again`; `a mkdir
  retried after a lost reply finds its directory there and is content`; `a download that loses its
  session starts over into a fresh partial file on a new one` and `a download of a file that is gone
  is not tried again and not held against the server`; `an upload under a refusing policy asks once,
  so its retry writes over what it left`; `a listing is tried again only while nothing has been
  handed on`. On the wire: `I11_a rename whose reply is lost on the wire is retried and reports the
  move that landed` - the proxy passes the rename on and stalls, the keepalive ends the call, the
  retry finds the file at the target with the size it had.
- *Breaker open makes acquire fail fast with `CircuitOpen`.* `RetrySemanticsTest.S3_an open breaker
  fails a call fast without a session, and a probe after the wait closes it` (no `Connect` recorded
  while open; the wait in open passes on the test scheduler's clock and the probe closes it) and
  `ResilienceAgainstServerTest.S3_` (no CONNECT reaches the proxy while open; the message names
  `op=exists`; the wait passes on a settable clock the client was handed).
- *S1, S2, S3, S10.* `ResilienceAgainstServerTest`: `S1_a session killed mid-download is replaced
  and the download completes, with the file seen once` (through `SftpSource.poll`: one `FileSeen`,
  the whole file, one retry, one poisoned eviction, two sessions opened); `S2_a stall past the
  keepalive poisons the session and the call is retried on a fresh one` (the breaker's count is
  made visible by a window of two: one failure and the retry's success open it); `S3_the breaker
  opens on failed dials, refuses without dialling, and closes on a successful probe` (and its
  virtual-time twin in `RetrySemanticsTest`); `S10_a wrong
  password is refused once, never retried, and never held against the server` (the server counts
  the passwords offered; the client's one call offers exactly one connect's worth).
- *Meters.* `the meters the ticket names are published under the endpoint`, and every retry test
  reads `sftp_retry_total{op}`; every breaker test reads `sftp_breaker_state`.
- *Progress entry appended.* This.

**Deviations:**

1. **The transfer limit is `kotlinx.coroutines.sync.Semaphore`, not Resilience4j's `Bulkhead`.**
   Read from the library's Kotlin module: `Bulkhead.executeSuspendFunction` takes its permit inside
   `withContext(Dispatchers.IO)` whenever a wait is configured, and only then enters its `try` - so a
   caller cancelled at the switch back holds a permit nothing releases, the exact shape of R1
   finding 1 and a transfer slot lost until restart; with no wait configured it blocks nothing but
   turns the fifth concurrent download away on the spot with `BulkheadFullException`, which has no
   class in the hierarchy and would need one. A suspending semaphore is cancelled cleanly, a transfer
   beyond the limit waits its turn (bounded by the transfers ahead of it, each on the clock), and it
   is, literally, the coroutine-compatible semaphore bulkhead spec 9 describes. `resilience4j-bulkhead`
   is not a dependency. The spec's order and the DSL knob are unchanged.
2. **Within one call, only a failure the wire produced is retried.** Spec 10.2 has "Recoverable, no
   poison: retry yes"; here `NoSuchFile` and `ServerFailure` are not retried inside the call. Three
   reasons, in order of weight: the coordinator's instruction that `NoSuchFile` from `download` is
   never retried, which the class's `Disposition` (`RETRY_ON_THIS_SESSION`, pinned by T2's
   `FailureModelTest`) contradicts, so the predicate could not simply read it; T7's two tests
   `a rename refused while nothing is at the target is passed on rather than met with a delete`
   (one rename sent) and R2's `a refusal after the target was cleared says so` (the "cleared"
   wording reaching the caller), which a retried `ServerFailure` breaks - a second try of the
   replace sequence finds the target already cleared and reports a plain refusal, rewriting R2
   finding 5's truth; and `PermissionDenied`'s own reasoning in T2 ("retrying in a hundred
   milliseconds asks the same question of the same unchanged server"), which applies to every
   answer the server gives. The next tick is the retry for those, and T12 owns it. **For the
   maintainer:** spec 10.2's two "no poison" rows and `NoSuchFile`'s disposition want reconciling
   with 6.1's "per-operation meaning"; the cleanest shape is a disposition for "the server
   answered: report it, keep the session, no immediate retry", which touches T2's test and was not
   done here.
3. **`NoSuchFile` is not counted against the breaker**, though its disposition says it is. The
   coordinator's instruction, and T2's own warning that a directory another system moves files out
   of would otherwise open the breaker on its own. `ServerFailure` *is* still counted, as its
   disposition says; on a server without the POSIX rename extension every refused overwrite counts,
   which C4 called wrong for poisoning and is arguably wrong for counting too. Raised, not changed.
4. **Time limiter outside `withLease`, so the wait for a session is on the clock**, as the
   coordinator specified and for the reason the ladder gives: a timeout that fires inside the lease
   cancels a coroutine whose blocking call the ladder is not watching, and is then bounded only by
   the keepalive floor. The cost is that a caller queued longer than the operation timeout is
   reported as `OperationTimeout` - poisoning, counted - rather than `PoolExhausted`. The DSL
   refuses that configuration: `operationTimeout` and `transferTimeout` must be longer than
   `acquireTimeout` (defaults 1 min and 15 min against 30 s).
5. **A listing is retried only until its first entry has been handed on.** Spec 6.1 says "blind
   retry"; starting a listing over after entries went out would hand them on twice, and
   remembering which ones would cost the memory the flow exists not to spend (S11). A listing that
   dies mid-way fails the poll; the next tick lists again.
6. **`rename` takes `expectedSize: Long? = null`.** I11 needs the size and spec 6.1's signature has
   none; the source passes `file.size`, a caller without it costs one `stat` before the first
   request. `StartupProbe` is unaffected (it works inside `withSession`).
7. **Two knobs the spec's DSL example does not show:** `operationTimeout` and `transferTimeout`,
   directly on the `resilience` block. Spec 9 says "per-operation timeout" without a knob; one
   clock for a round trip and one for anything whose length the other end decides (a file's size,
   a listing consumer's pace) was the smallest honest version.
8. **`bulkhead { maxConcurrentTransfers }` unset means four, or the pool's size when smaller.**
   Spec 12 validates `maxConcurrentTransfers <= maxSize`; a fixed default of four made every
   existing test config with a pool of one or two refuse to build. Set explicitly, the rule holds.
9. **One earlier test's setup was touched** (C5's shape, declared): `CancellationLadderTest.a
   server that goes quiet ends the call itself, and the session goes with it` now builds its
   config with `resilience { retry { maxAttempts = 1 } }`. Its assertion - the stalled call ends
   with `SessionLost` after the keepalive, the session is gone - is unchanged; with retries the
   stalled call would be tried again on a fresh tunnel and succeed, which is S2 and is proved
   separately. Nothing else in the earlier suites needed anything: `LoopbackConnectProxy.stall()`
   now stalls only the tunnels that exist (a retry's fresh tunnel relays), and
   `onNextClientRequest` fires after the bytes were passed on, both of which every earlier use
   tolerates.
10. **`SftpClient` takes a `Clock`** (default the system's; `SftpConnector.start` passes its own)
    for the breaker's wait in open, through `CircuitBreakerConfig.Builder.clock`. The first draft
    waited real time in S3 on the belief that the library offered no clock seam; the standards
    review read the jar and found one. Backoffs are on virtual time everywhere the fake is used;
    the server scenarios run with a one-millisecond backoff and no test waits for a clock.

**Seams.** Closed above, struck through: `Attempt.number`/`acquire`; `OperationTimeout` has no
producer; `NoSuchFile` from `download` turned into `FileGone` outside the client; a `REFUSE`
retried after a lost reply refused by its own success; and R1's "a cancelled `withLease` is not
proof the operation did not land" (owner T11 - the compensation reads "reached the server", not
the failure). Added: `OperationTimeout`'s `EVICTED` is advisory, `sftp_breaker_state` on a shared
registry, the `REFUSE` window under retry, `ServerFailure` counting against the breaker.

**For the next ticket (T12, watch/consume; T13, shutdown):**

- `CircuitOpen` and `PoolExhausted` reach the source with the caller's operation in them; `watch`
  maps `CircuitOpen` to `PollSkipped(BreakerOpen)` per its disposition (`REPORT_A_SKIP`) and
  nothing here does that yet.
- The retry's `delay` is cancellable; a shutdown that cancels a poll mid-backoff sees a
  `CancellationException` from inside `Resilience.attempting` and no further try.
- A `PermissionDenied` is not retried here: the next tick is its retry, and T12 decides what
  "waits a full tick" means for a file whose ack action was refused on permissions.
- Every `Resilience` is per `SftpClient`, and its breaker gauge registers under the endpoint; a
  second client for the same endpoint on one registry reads the first's breaker (R1 finding 6).
- Under `REFUSE`, a retry after a lost reply looks for its own landed file first and, finding
  none, sends a plain rename against a target found free on the first try; on a server with the
  POSIX rename extension a stranger's file that arrived at the target during the backoff is then
  replaced. The window is the backoff rather than the look, and it is the same race T7 accepted,
  widened; recorded as a seams row.

---

## T12: Watch ticker, overlap policy and error policy

Built on `claude-fable-5-1`. 216 tests were green at the start and 231 are green at the end (51 core,
180 testkit): 11 in `SftpWatchTest`, 2 in `WatchAgainstServerTest`, 1 each added to `SftpConnectorTest`
and `ConnectorDslTest`, and `FailureModelTest` reshaped by the pre-task (one row test replaced, one
added; the count there is unchanged). The self-review (standards and spec axes, two sub-agents) found
four things that were fixed before the commit and are folded in below: the server demo asserted tick
*numbers* on a real interval, which a slow ack could shift by one skipped tick; a tick failing in the
instant its collector left would have reached the thread's default handler rather than a log line; a
cancellation thrown *inside* a tick by nobody - a custom check letting its own `withTimeout` escape -
would have ended the tick without a word where a poll would have thrown it; and S12 had no test under
a real `PROCEED` overlap.

**Pre-task, commit `65a967c` - ruling C13 / spec D41 applied.** `RETRY_ON_THE_NEXT_TICK` is now the
disposition for every failure the *server answered*: `NoSuchFile`, `PermissionDenied`, `ServerFailure`.
It keeps the session, is not retried inside the call, and no longer counts against the breaker.
`RETRY_ON_THIS_SESSION` had no class left on it and is deleted rather than kept as a name nothing lands
on. The line between the two retry dispositions is now `Recoverable.poisons` alone - a session is
untrustworthy exactly when the wire failed - so `PermissionDenied` lost its override. `Resilience`'s two
predicates read the answers with no class list: `retry == IMMEDIATELY` and `countsAgainstTheBreaker`;
the `!is NoSuchFile` exclusion T11 carried is gone. *Earlier tests changed, as C13 mandates:*
`FailureModelTest.a recoverable failure that does not poison keeps the session it was using` (pinned
`NoSuchFile` to `RETRY_ON_THIS_SESSION`, counted, retried immediately) is replaced by `.a failure the
server answered keeps its session, waits a full tick, and is not held against the server` over the three
classes; `.a permission refusal waits a full tick rather than asking again immediately` is replaced by
`.a short transfer is retried on a fresh session and counted, like any failure of the wire`, since the
permission row is now inside the first; and `rowOf` puts the three on one row. No other earlier test
asserted a server-answered failure as counted - T11's `a failure whose disposition says never is not
tried again and not counted` asserts `PermissionDenied` is not retried and never asserted the breaker,
and every JSch-mapping assertion of `countsAgainstTheBreaker` is on a wire failure.

**Built:** the pipeline shape spec 7.1 promised. `source.watch(dir, every)` is a cold flow that polls
when collected and again every interval for as long as the collector keeps collecting, reports a tick
that failed as `PollFailed` and a tick that sent nothing as `PollSkipped`, and ends only on a failure no
later tick could survive - which is I10, read off the failure's own `Disposition.watch` and never off its
class. `source.consume(dir, every) { file -> }` is the ordinary pipeline over it: the block per file,
acked when it returns, nacked when it throws, and neither ending the watch. The overlap policy is a
knob, `polling { overlap = OverlapPolicy.SKIP }`, and `SftpEvent` gained the two events T10 left out.
The ticker runs in the connector's own scope, so cancelling `backgroundWork` ends every watch.

**Concepts named:**

- **A tick is a number taken when the interval comes round, whether or not anything runs for it.**
  T10's per-source counter is the only counter: `poll(dir)` takes a number at collection and the ticker
  takes one per interval, so a watch's ticks continue its polls' numbering (the first test proves poll
  1, watch 2, 3). The number is taken *before* the overlap decision, which is why a skipped tick has one.
  `poll` is now `flow { emitAll(tickOf(dir, ticks.incrementAndGet())) }` and `tickOf(dir, tick)` is
  T10's body unchanged with the number passed in; that is the whole refactor of `poll`.
- **The ticker is a producer in the connector's scope; the watch is a collector of its channel.** The
  seam that answers the brief's "how does `watch` get into the connector's scope without handing the
  scope out": `SftpSource` takes a `CoroutineScope` at construction (the connector passes its own; a
  source built alone gets `CoroutineScope(SupervisorJob())`, which nothing stops and whose watches end
  when their collectors do), and `watch` runs `background.produce { tickEvery(...) }` and consumes the
  channel into its own `emit`. A rendezvous channel, so backpressure is suspension exactly as for `poll`:
  a tick's `send(FileSeen)` waits for the collector to take it. Each tick that runs is a `launch` inside
  the producer, so the ticker is free to come round again while a tick works - which is what makes
  overlap a thing that can happen at all. Cancelling the collector cancels the channel, which cancels
  the producer and every tick under it, and the tick's own `catch` (T10's) withdraws what it had handed
  over. Cancelling the connector's scope closes the channel with a cancellation; the watch tells that
  apart from its own by asking its own job (`ensureActive`, T11's shape) and ends *normally*, logging
  that the connector stopped it. A fatal in any tick fails the producer, which closes the channel with
  the error, cancels the sibling ticks under `PROCEED`, and reaches the collector as the thrown error.
- **The watch claim.** `watching`, a synchronised set of directories; `add` under its lock is the whole
  decision, and a second collector of the same directory is refused with `IllegalStateException` naming
  the directory and the reason (one consumer per directory is what the in-flight set's promise rests on).
  Released in the collector's `finally`, so a watch that ended any way gives the directory back.
- **`OverlapPolicy`** (`config`): `SKIP` (default) and `PROCEED`, as spec 12 spells them. The decision
  is made by the ticker alone - one coroutine, so no lock - from whether the latest tick's job is still
  active. Under `SKIP` the ticker sends `PollSkipped(tick, OVERLAP)` and launches nothing; under
  `PROCEED` it launches alongside, and the in-flight set is what keeps S12 (proved under a real overlap
  now, with `maxInFlight = 1` so the first tick is still waiting for room when the second lists).
- **`SkipCause`** (`source`): `OVERLAP`, `BREAKER_OPEN`. `PollFailed(tick, error: SftpException)` is
  typed, so a consumer can read `error.disposition` rather than the class.
- **The error policy is one `catch` operator, `reportingFailures`,** and it is where I10 lives. It sees
  only the tick's own failures - the `catch` operator passes a downstream failure and the tick's own
  cancellation untouched - and does what `disposition.watch` says: `REPORT_THE_FAILURE` emits
  `PollFailed`, `REPORT_A_SKIP` emits `PollSkipped(BREAKER_OPEN)`, `STOP` rethrows. Nothing sorts
  classes; `PoolExhausted` (S4), `CircuitOpen` (S3's shape), `PermissionDenied` (D20) and
  `AuthenticationFailed` (S10) each arrive by their disposition. Two things that are not
  `SftpException`s are decided here as well, and both end the watch, because no tick survives a bug by
  waiting: an unclassified exception (logged at ERROR and rethrown), and a `CancellationException`
  raised while the tick itself is active - a check that let its own `withTimeout` out - which is turned
  into an `IllegalStateException` naming the tick and carrying the timeout as cause, because rethrown as
  a cancellation it would have ended the tick's coroutine silently and the watch would have carried on
  with a tick missing. That is not wrapping a cancellation: the tick was not being cancelled, which is
  what `ensureActive` establishes first.
- **`consume`'s two answers.** The block's exception is the nack, but this collector's own cancellation
  is not the block failing (`ensureActive` before the nack, so a `withTimeout` the block set for itself
  *is* a nack, and a shutdown is not). An ack or nack *action* that fails with a connector failure is
  logged and the pipeline goes on - the file is still where it was and the next tick hands it over again
  - unless the failure is fatal; an action failing with anything else is a bug and ends the pipeline.

**Acceptance:**

- *watch(dir, every) repeats poll on a ticker driven by the injected clock* -
  `SftpWatchTest.a watch polls when collected and again every interval, numbering its ticks after the
  source's polls`: poll takes tick 1, the watch starts on tick 2 the instant it is collected, tick 3
  after one interval of virtual time, three listings. See deviation 2 for "driven by the injected clock".
- *OverlapPolicy SKIP emits PollSkipped(Overlap) while a tick runs (S8); PROCEED runs a second tick
  alongside* - `S8_under SKIP a tick that comes round while the last is still running is skipped, and no
  second listing is sent` (the first listing never answers; two intervals later the events are
  `PollStarted(1)`, `PollSkipped(2, OVERLAP)`, `PollSkipped(3, OVERLAP)` and one listing was sent);
  `under PROCEED a tick that comes round while the last is still running runs alongside it` (two
  `PollStarted`, two listings); `S12_a file listed again by a tick running alongside is handed over
  once`. Mutating the ticker to ignore the policy fails S8 and nothing else.
- *Recoverable errors emit PollFailed and the flow continues; fatal errors terminate the flow with the
  error (I10); PoolExhausted emits PollFailed (S4); breaker open emits PollSkipped(BreakerOpen)* -
  `I10_a recoverable failure is reported and the watch goes on, and a fatal failure ends it with the
  error` (a listing refused on permissions is `PollFailed(1)`, tick 2 lists and completes, a rejected
  password on tick 3 ends the collect with `AuthenticationFailed` and no listing follows); `S4_a full
  pool fails the tick, and the watch continues` (one session, held by somebody else past the acquire
  timeout on virtual time; `PollFailed(1, PoolExhausted)`, then `PollCompleted(2)` once it is given
  back); `an open breaker skips the tick` (a breaker of one call opened by a lost session;
  `PollFailed(1, SessionLost)` then `PollSkipped(2, BREAKER_OPEN)` and one listing in total). Mutating
  `STOP` to report-and-continue fails I10 and nothing else on virtual time - and hangs the wrong-password
  server demo, which is the same fact seen from a real interval.
- *A second watch on the same directory of the same connector is rejected at call time* - `a second
  watch of the same directory is refused until the first has ended`. See deviation 1 for "call time".
- *consume(dir, every) { } acks on normal return and nacks on exception* - `consume acks a file its block
  returns from, nacks one it throws on, and goes on`: the acked file is deleted by its action and not
  seen again, the nacked one is counted as a nack and handed over again next tick, the pipeline is
  still running when it is, and nothing is left in flight.
- *Demo against the embedded server: the watch survives a server restart between ticks and terminates on
  a wrong password* - `WatchAgainstServerTest.the watch survives a server restart between ticks` (a
  started connector; after the first tick's file is acked every server session is cut and a second file
  is written; the next tick is `PollFailed(SessionLost)` because the pool, told to skip validation,
  handed it the dead session and the retry budget is one; the tick after lists on a fresh session and
  the second file is acked into `temp/`; asserted as an order of events, not tick numbers) and `a wrong
  password ends the watch with the rejection, and no tick asks again` (the collect ends with
  `AuthenticationFailed`, no `PollFailed` was ever emitted, the server counted one connect's worth of
  passwords, the breaker of one call is still closed).
- *Progress entry appended* - this.

Four tests beyond the checkboxes: `a cancellation that is nobody's, let out of a check, ends the watch as
a bug rather than silently`; `a collector whose block throws ends the watch with its own exception and
gives every place back` (what `consume` exists to prevent, seen from `watch`, and I8 for a watch);
`SftpConnectorTest.stopping the connector's background work ends a watch normally in its collector`
(the T13 seam: `backgroundWork.cancelAndJoin()`, and the collector is completed and not cancelled);
`ConnectorDslTest.a tick that comes round while the last is running is skipped by default, and may be
told to proceed`.

**Deviations:**

1. **A second watch is refused when its flow is collected, not when `watch()` is called.** Spec 7.6 and
   the ticket say "at call time". `watch` returns a cold flow, and nothing has happened at the call: a
   claim taken there would be held by a flow that is built and never collected, for ever, and a flow
   object collected twice - ordinary Kotlin - would hold no claim the second time. So the claim is the
   first thing the collection does, before any listing, and the refusal is an exception out of
   `collect` rather than an event - which, from the caller's `watch(dir).collect { }`, is the same
   statement. **For the maintainer:** spec 7.6's "at call time" should read "when collected".
2. **The ticker is `delay(every)` on the scope's scheduler, and reads no `Clock`.** The ticket says
   "driven by the injected clock". A coroutine cannot sleep on a `java.time.Clock`; the injected clock
   is what timestamps are read from, and under `runTest` the scheduler's time and the injected
   `virtualClock()` are the same time, which is the only sense in which the checkbox can be tested and
   is how every test here runs. The interval is counted from each tick's launch (or from the skipped
   tick's event being taken), so the schedule drifts by however long the ticker's own `send` waits;
   on the seams table.
3. **A watch the connector stops ends normally.** Spec 7.1 says a watch "never terminates on
   recoverable errors" and 11.2 says "watchers are cancelled"; neither says what the collector sees.
   Chosen: the flow completes, with an INFO line, because the collector did nothing wrong and is not
   the thing being stopped, and a `CancellationException` thrown into a live collector is a trap - a
   `launch` would end silently, a `runBlocking` would throw. The cost is on the seams table: a consumer
   that loops on `watch` after the connector stopped gets another normal end at once, until T13's
   `Closing` state refuses the claim.
4. **`SftpSource` takes a `CoroutineScope`, and `SftpConnector.start` makes its scope before the probe.**
   The scope is a name and a job and costs nothing until something is launched, and the housekeeper is
   still launched only after the checks pass - T9's `a start-up that was refused starts no housekeeper`
   is unchanged and green. The parameter is defaulted, so T11's `SftpSource(client, config, meters)` in
   `ResilienceAgainstServerTest` builds unchanged. Whether the watchers should sit under a supervisor of
   their own beneath the connector's, so T13 can cancel them before the housekeeper, is T13's to decide:
   it can pass `scope + SupervisorJob(scope.job)` here and cancel that.
5. **A tick that fails at the instant its collector leaves is logged, not reported.** The producer's
   channel is already cancelled by then, so the failure cannot be delivered and, under a supervisor,
   nobody else handles it; a `CoroutineExceptionHandler` on the producer turns what would have been the
   thread's default handler into a WARN line. Reasoned from `ProducerCoroutine.onCancelled`, not
   reproduced: the window is a channel cancel racing a tick's throw.
6. **`ResultLabel` and `sftp_poll_seconds` are untouched.** A failed tick is already timed and labelled
   by T10's `timingPoll` (`recoverable` for everything a watch survives, `fatal` for what ends it); a
   tick skipped for overlap runs nothing and records nothing, which is consistent with spec 13 naming
   no skip counter. No new meter.
7. **Size.** About 135 lines of main source that are neither blank nor comment across five files (all
   but ~15 in `SftpSource`), about 360 in the two new test files and 30 in three earlier ones. Inside
   the budget.

**Seams.** Closed above, struck through: `ServerFailure` counts against the breaker. Added: `consume`
re-runs its block for a file whose ack keeps being refused; a watch on a stopped connector ends normally
and again on restart; the ticker's own `send` under `SKIP` delays it.

**For the next ticket (T13, shutdown):**

- **The watchers are `produce` coroutines in the scope `SftpConnector.start` hands `SftpSource`.**
  Today that is the connector's own scope, beside the housekeeper. Spec 11.2 cancels watchers in step 2
  and stops the housekeeper in step 5, so T13 will want them separable: a child supervisor for the
  source is one line at the `SftpSource` call, and the housekeeper's `launch` should keep its `Job`.
- **Cancelling a watcher ends its collector's flow normally,** and the running tick's `catch` withdraws
  every unanswered file as `cancelled` (T10's mechanism, unchanged). An ack the consumer is in the middle
  of runs on the *consumer's* coroutine, which is not cancelled: it will hit step 1's fast-failing
  acquire, and `consume` will log that the answer could not be carried out. That is the "unacked files
  are treated as nacks" of 11.2, with the file re-listed on the next start.
- **A watch claimed after the connector is closing should be refused at the claim** - `watching.add`
  in `watch` is the place, and the state to read is T13's. Without it a looping consumer spins (seams
  row).
- **The `Executor has been shut down` lines in the test log are MINA's own,** printed on the server's
  own threads when a server is stopped with a client session still open, and predate this ticket
  (they appear under T11's `ResilienceAgainstServerTest` as well). T13's `close()` hanging up the
  pool's sessions before the server stops is what will make them go away.
- **`SftpEvent`'s `when` is now six-armed** and every consumer's exhaustive `when` over it was named by
  the compiler when the two events landed; nothing in `core` or `testkit` switched on it exhaustively.
- **`readiness` checks that time themselves out must catch their own timeout.** A `withTimeout` let
  out of a check ends the watch as a bug (named in the exception); `withTimeoutOrNull` and `NotReady`
  is the shape.

## T13: Graceful shutdown

Built on `claude-fable-5-1`. 231 tests were green at the start and 241 are green at the end (52 core,
189 testkit): 6 in `PoolShutdownTest`, 1 in `ShutdownAgainstServerTest`, 2 added to `SftpConnectorTest`,
1 added to `ConnectorDslTest`. The self-review (standards and spec axes, two sub-agents) found five things
that were changed before the commit and are folded in below: `close()` cut the held sessions twice, once in
its `try` and once in its `finally`, and could be entered by an already-cancelled caller and throw before it
had stopped lending; `proved` was the one registry mutation that ignored whether the entry was still the
registry's; a `Retired` was hand-built in the pool for a dial that landed late, which made the count of
sessions still being hung up on balance by luck; the attempt a refused caller carries was looked up in two
places; and the orderly hang-ups at the end ran one after another on a dispatcher no wider than the pool.

**Built:** the connector can be stopped. `SftpConnector.close()` is one suspend call, and it is the whole of
what a host's shutdown hook needs: from its first instant no session is lent, no watch is claimed and no
housekeeping round is decided; the watchers are cancelled and every file a running tick had handed over goes
back as cancelled; the sessions out on lease are given `drainTimeout` to come back on their own; what is
still out is cut apart, all at once, and given one `cancelGrace` to hand back through the failure the cut
raises; and everything the pool holds is hung up on, in parallel, with every entry ending `Closed` and every
one counted as `sftp_pool_evicted_total{reason=shutdown}`. `start` calls it before rethrowing a refusal.
`drainTimeout` joined the DSL, `PoolExhausted` learned to say the pool is closing, and `Lease.connection` is
an `SftpSession`.

**Concepts named:**

- **The pool's closing state lives in the registry, and the registry decides everything that reads it under
  its lock.** This is where the ticket's design went. There is no `Closing` object and no flag each party
  reads at a moment of its own: `SessionRegistry.closing` is set once by `beginClosing()`, and the four
  decisions that depend on it are the registry's own methods - `checkOut` answers null, `handBack` retires
  with `SHUTDOWN` whatever the caller had to say, `sweep` decides an empty round, and `filled` hands back
  the session of a dial nobody was left to receive. Because the refusal in `checkOut` is taken under the
  same lock the drain counts under, an entry cannot appear that the drain has not seen. The pool reads the
  flag without the lock in exactly two places, both for speed and never for the decision: at the door of
  `acquire`, so a caller does not queue for a session that is never coming, and after a dial or a proof
  lands, so a session opened for a caller during the drain goes the way of everything else the pool holds.
- **`Closed` is terminal.** `PoolEntry.moveTo` no longer leaves it. A shutdown writes off an entry whose
  holder has not come back, and that holder may still be on its way to handing it back or to reporting that
  its dial landed; whatever it reports, the entry stays finished. With that, `handBack` and `filled` need
  only ask whether the entry is still in `entries` - "already retired by somebody else" is a state only a
  shutdown produces - and `closed()` said twice of one entry is nothing.
- **Quiet** is what the drain waits for: every entry on the shelf, and every retired session hung up on.
  The second half is `retiring`, a count kept in the registry, because a retired session has left
  `entries` and the drain would otherwise return while one was still being closed under `NonCancellable`
  on another coroutine, leaving an entry that ends `Closed` a moment after "every entry ends `Closed`" was
  said. Every `Retired` is made by the registry, and the count moves only in `retire` and `closed`.
- **The drain polls; nothing signals from under the lock.** `settled(within)` looks every 20 ms, one
  uncontended lock per look, under `withTimeoutOrNull`. A `CompletableDeferred` or a `StateFlow` completed
  from `recount()` would be the seams-table hazard the maintainer flagged - something resumed while the
  registry's lock is held - and a shutdown is not where 20 ms matters.
- **The force is `cutLoose()`, at once, and the grace is one, shared.** T8 asked for the cuts to be made
  in parallel or for a reason why not; the reason is that a cut does not wait for anything - it is
  `abort()`, a socket close on the calling thread, measured by T8 at well under a second - so making them
  one after another *is* making them at once, and the grace is not the ladder's grace-then-cut per lease
  but one wait for all the cut threads to come back, which they do concurrently on their own coroutines.
  The drain timeout was every lease's grace already. The hang-ups that follow are the part that can wait
  on a peer (R1 finding 5), so those run side by side under `coroutineScope`.
- **`close()` cannot be cancelled, and need not be.** It is bounded by construction, and a pool left
  half-closed is sockets and threads for the life of the process. `withContext(NonCancellable)` around the
  whole of it also covers a caller that was cancelled before it called: the registry's lock, the delays and
  the timeouts all still work under it, which `I9_` proves on virtual time.
- **The source reads its scope, not a flag.** A watch claimed after `close()` is refused because the scope
  its ticker would run in is no longer active - the fact that governs a watch, and the first thing `close()`
  makes true. T12 asked for "T13's state" to be read at the claim; this is that state, seen from where a
  watch lives, and it also covers a scope stopped by `backgroundWork.cancel()` alone.

**Acceptance:**

- *`close()` is a suspend function with the phases Closing, cancel watchers, drain, force, stop
  housekeeper* - `SftpConnector.close()` is `scope.cancel()` then `pool.close()`, and `SftpPool.close()` is
  `beginClosing`, `settled(drainTimeout)`, `cutEverythingHeld`, `settled(cancelGrace)`, `closeEverything`.
  See deviation 1 for the order.
- *During Closing, acquire fails fast with `PoolExhausted(closing = true)`* -
  `PoolShutdownTest.during closing an acquire fails at once with PoolExhausted closing`: a caller arriving
  after the pool started closing is refused with no virtual time spent; one queued before it started is
  refused the moment room comes free, not at its acquire timeout. Seen through the client in
  `SftpConnectorTest.close ends a watch normally, gives every unanswered file back, and refuses what comes
  after` (`client.exists` after close), and through a retry in S9.
- *Unacked files at shutdown are treated as nacks with redelivery* - the same connector test: a tick caught
  mid-handover, one file with the consumer and one waiting to be handed over, both given back as
  `cancelled` with `sftp_inflight` at zero, and the collector ending normally once the consumer lets go.
  T10's mechanism, proved under a real close. See the seams row for a tick that had already finished.
- *I9* - `I9_close returns within the drain plus one grace and leaves every entry closed`: three entries -
  parked, handed back a second into the drain, and blocked on a read the fake never answers. `close()` is
  still running at the end of the drain, the blocked one has been cut, and at exactly
  `drainTimeout + cancelGrace` `close()` has returned with all three `Closed`, no session open, three
  counted `shutdown`. The blocked thread then comes back and hands back an entry the pool has finished
  with, and nothing is counted twice.
- *S9* - `ShutdownAgainstServerTest.S9_closing during a download leaves no partial file, releases the
  lease, and returns within the bound`. See below.
- *`sftp_pool_evicted_total{reason=shutdown}` counts closed entries* - every test above reads it;
  `a session handed back during the drain is retired as shutdown, and close returns once the last is back`
  pins that a session handed back *poisoned* during the drain is `shutdown` and not `poisoned`.
- *Progress entry appended* - this.

Three tests beyond the checkboxes: `a dial that lands into a closing pool is hung up on, and its caller
refused` and `a dial that lands after the pool gave up waiting for it is hung up on all the same` are the
two orderings of a handshake against a close, the second being the leak-shaped one (an entry written off,
then a session arriving for it); `a closing pool is not kept in shape` is the housekeeper opening nothing
for a shelf about to be cleared. `SftpConnectorTest.a start-up that was refused leaves no session open`
closes T9's seam, and `ConnectorDslTest.a drain no longer than the grace it is followed by is refused, and
the default is` is the knob.

**How I9 is enforced, not merely asserted.** The bound is two `withTimeoutOrNull`s and nothing else waits:
the drain, and then the grace for what was cut. Removing the cut fails `I9_` at the fake's `Abort`
assertion and nowhere else: the bound and the counts still hold on the fake, whose hang-up is instant,
because the write-off then hangs up on a session nobody cut - against the server that hang-up would queue
behind the blocked thread on a dispatcher of width one, which is why the cut comes first. Removing the
grace wait fails `I9_` at "returned before the cut had its grace", and only that. Removing
`retiring` from `isQuiet` is invisible to the fake, whose hang-ups are instant; it is there for the real
transport, where a hang-up is a round trip under `NonCancellable` on another coroutine.

**What S9 measured.** A started connector through the tunnel, one session, a real download held
mid-file at 64 KiB of eight mebibytes and never let go, `drainTimeout = 1 s`, `cancelGrace = 300 ms`.
`close()` returned in about a second: the drain ran out, the cut landed, the blocked thread came back with
"inputstream is closed", the lease was handed back as `shutdown`, and the pool was quiet well inside the
grace. The download's own coroutine then retried - after a 100 ms backoff, on the closing pool - and ended
with `PoolExhausted(closing = true)`, which is where an operation in progress at shutdown ends; the
partial file was removed by the first try's cleanup, the pool holds nothing, and the server saw its
session go. The assertion band is `[drainTimeout, drainTimeout + cancelGrace + 2 s]`, the two seconds being
for the closes on a loaded machine and not part of the bound.

**Deviations:**

1. **The housekeeper is cancelled with the watchers, before the pool stops lending, rather than last.**
   Spec 11.2 cancels watchers in step 2 and stops the housekeeper in step 5, and T12 suggested a child
   supervisor so the two could be told apart. Neither is needed: a closing pool refuses the housekeeper its
   rounds, so there is nothing for it to outlive the drain for, and R1's concern - its cancel queueing
   behind the closes of a round in progress - is moot because nothing joins it. Cancelling the scope before
   `beginClosing` rather than after opens a window of microseconds in which an ack in flight may still
   borrow a session; that ack then lands rather than being refused, which is a file that does not have to
   be handed over again after restart. One line either way; this is the one with less machinery.
2. **`close()` does not join the connector's scope.** A tick's `withdraw` after its cancellation, or a
   housekeeper mid-hang-up, may still be running for a moment after `close()` returns. Both are memory
   work or work already under `NonCancellable`; joining them would put an unbounded wait back into a call
   whose point is its bound, in the one case (R1 finding 5, a peer that will not take the close) that no
   timeout can shorten.
3. **`drainTimeout` must be longer than `cancelGrace`,** as the coordinator's brief asked. The spec review
   noted that `drain = 1 s, grace = 5 s` - cut fast, let the cut unblock - is a configuration somebody
   could want and is now refused. Recorded; the rule is the brief's, and a deployment that wants it can
   argue the point with a use.
4. **Two earlier tests were edited, both mechanically forced.** T3's `I2_an entry is handed to at most one
   lease at a time` built a `mutableSetOf<SftpConnection>()` from `lease.connection`; it is
   `mutableSetOf<SftpSession>()`, the narrowing R1 finding 8 described and the shape C5 allows. T1's
   `a described connector becomes an immutable configuration` set `cancelGrace = 45.seconds` against the
   default drain of thirty and would have been refused by deviation 3's rule; it now also sets
   `drainTimeout = 2.minutes` and asserts it. Nothing was weakened.
5. **A session cut by the drain is counted `shutdown`, not `poisoned`.** The two T8 rows about a cut
   session counting as `poisoned` still stand for a cut the ladder makes outside a drain; inside one, the
   reason a session is leaving is that the pool is closing, and the label says so.
6. **`SftpConnector` carries `config.name`** for its two log lines. A scalar on the constructor, noted by
   the review; passing the whole configuration for a name would be the larger thing.
7. **`SftpConnector.close()` is called twice harmlessly, not refused.** The second `scope.cancel()` is a
   no-op and the second `pool.close()` finds an empty registry and returns without waiting.
8. **Size.** About 115 lines of main source that are neither blank nor comment across eight files, and
   about 320 of tests, two new files and three earlier ones. Inside the budget.

**Seams.** Closed above, struck through: a refused start-up leaves its sessions open;
`Retirement.SHUTDOWN` has no producer; a watch on a stopped connector ends normally and again on restart;
`Lease.connection` reaches `abort()` (both rows). The `withContext(dispatcher)` row stays as the standing
rule it is; `close()` creates nothing inside a dispatcher switch - the decision of what to close is taken
under the registry's lock and the hang-ups run on the transport's own path. Added: a file held from a tick
that already completed stays in flight across `close()`; a failing assertion before `close()` in a
`runTest` with a started connector hangs rather than fails.

**For the next tickets:**

- **T14 (Quarkus): `SftpConnector.close()` is the one call**, `runBlocking` under a timeout no shorter
  than `drainTimeout + cancelGrace` of the connector's own configuration - the call is bounded by that and
  cannot be cancelled, so a shorter timeout on the `runBlocking` would only make the hook return early
  while the close goes on. `abort()` runs on the closing thread (R1 finding 5), so an event-loop host should
  call `close()` from a worker thread. `drainTimeout` is a pool knob and belongs in the pool block of the
  config mapping. The "dispatcher closes" of spec 11.2 step 5 has nothing behind it: the transport's
  dispatcher is a view over the shared IO pool.
- **T15 (acceptance): I9 and S9 are `PoolShutdownTest.I9_` and `ShutdownAgainstServerTest.S9_`.** The
  `Executor has been shut down` lines T12 saw in the log are gone from the tests that close the connector
  before the server, as predicted; a test that still stops the server with a started connector will still
  see them. A pool used without a connector needs `pool.close()` at the end of its test to leave no session
  open; the earlier tests do not, and the fake makes that harmless.
- **`SessionRegistry.filled` returns the session to hang up on** when the pool has written the entry off.
  Anything that dials outside `SftpPool.dial` must do the same with what it gets back, or the session
  leaks exactly the way R1 finding 1 described.
- **`PoolEntry.moveTo` never leaves `Closed`.** A later ticket that wants to reuse an entry object
  cannot; entries are made, not recycled.
- **`isQuiet` counts on `retiring`**, which every `Retired` from `retire()` raises and `closed()` lowers.
  A `Retired` made anywhere but the registry would drift it and a drain would then wait its full bound.
- **A `runTest` that starts a connector and then fails an assertion hangs** (seams row). Declare the
  readiness the test needs, and assert after `close()` where possible.

---

## T14: Quarkus adapter

Built on `claude-opus-5[1m]`. 241 tests were green at the start and 247 are green at the end
(52 core, 189 testkit, 6 quarkus): 5 in `SftpConnectorPropertiesTest` and 1 `@QuarkusTest` in
`SftpConnectorAdapterTest`. Nothing in `core` or `testkit` was touched, so the 241 are the same
241. The self-review (standards and spec axes, two sub-agents) found three things that were
changed before the commit and are folded in below: `hostKey.known-hosts` was declared in the
mapping and never read, so a strict policy silently dropped the file it was given; the Prometheus
registry was a compile-scope dependency, which would have put it on the classpath of every host
that wanted a connector; and the `quarkus-maven-plugin` was binding a `build` goal a library
module has nothing to package.

**Built:** a Quarkus host now gets a connector out of its application properties. Put
`sftp.connector.*` in `application.properties`, inject `SftpConnector`, and the module does the
rest: reads the properties, puts them to the DSL, hands the host's own `MeterRegistry` to the
pool so the pool's gauges land in whatever the host scrapes, starts the connector with the
application, and closes it on the shutdown event within the bound the connector promises. The
core learned nothing about Quarkus: `sftpconnector/quarkus` is a new module depending on
`sftpconnector-core`, and `core`'s `ArchitectureTest` passes unchanged.

**Concepts named:**

- **The properties carry values and decide nothing.** `SftpConnectorProperties` is a
  `@ConfigMapping` in which *every* leaf is `Optional`, including the ones the connector cannot
  run without. A property nobody set arrives as absent rather than as a substitute the operator
  never chose, and `toConnectorConfig()` then simply does not mention it, so the builder's own
  default stands. There is one definition of what a valid configuration is and it is the
  builder's; the mapping never has a second opinion. What this buys is the thing the builder was
  built for: an operator with several faults in one properties file hears about all of them in one
  `ConfigurationError` and corrects them in one pass. `SftpConnectorPropertiesTest.every fault in
  one properties file is reported together` is that claim with six faults in it.
- **The consequence, which is the rule for the next knob:** an absent property must leave the
  builder's default in place, never restate it. A default written twice is a default that will
  one day disagree with itself, and the copy in the adapter would be the one no test covers.
  `a property nobody set keeps the connector's own default` is the guard on that.
- **A knob is one value even when it is three properties.** The backoff curve is `initial`, `max`
  and `jitter`, and naming any one of them replaces the whole curve, with the two nobody named
  coming from the builder's own curve. Readiness is the same shape read the other way: naming any
  check replaces the connector's default heuristic outright rather than adding to it, because a
  deployment that says "ready when the marker is there" means that and not "that as well as a
  minute of age". Several named checks compose, and all of them must pass.
- **One connector per application, by construction rather than by guard.** This is the ruling on
  the two open seams that were routed here. One `sftp.connector` prefix means one configuration
  means one produced bean, so a second connector for the same endpoint on one registry cannot
  come from this module, and nothing was written to refuse a shape the module cannot make. The
  colliding gauges are untouched and the seams row now records what a host that builds a second
  connector by hand is taking on.
- **The close is dispatched, and its timeout is an assertion rather than a cut.** Closing is
  bounded by `drainTimeout + cancelGrace` and cannot be cancelled, so `withTimeoutOrNull` around
  it cannot make the hook return sooner. What it is for is the log line underneath: if it ever
  fires, the close overran its own bound, which is a fault worth a WARN. The work runs under
  `runBlocking(Dispatchers.IO)` rather than on the thread that delivered the event, because
  cutting a session loose is a blocking socket close made on whichever thread is doing the
  closing (R1 finding 5).

**Acceptance:**

- *Separate module depending on core; CDI producer builds the connector through the DSL from
  mapped configuration* - `sftpconnector/quarkus` (`sftpconnector-quarkus`) is in the
  aggregator's `<modules>` and depends on `sftpconnector-core`, `quarkus-arc` and
  `quarkus-micrometer`. `SftpConnectorLifecycle.connector` is the producer, and it starts what
  `toConnectorConfig()` built. Proved end to end by the `@QuarkusTest` below, which injects the
  connector and polls with it.
- *Shutdown event calls `close()` under `drainTimeout`* - `SftpConnectorLifecycle.stop` observes
  `ShutdownEvent` and calls `close()` under `runBlocking(Dispatchers.IO)` inside
  `withTimeoutOrNull(drainTimeout + cancelGrace + 5s)`, per spec 11.2's last paragraph and T13's
  note. The `@QuarkusTest` calls it and then finds the pool holding nothing, at least one entry
  counted `sftp_pool_evicted_total{reason=shutdown}`, and `client.exists` refused with
  `PoolExhausted`.
- *Host `MeterRegistry` is bound; the pool gauges appear in the host's metrics endpoint* - the
  same test reads `sftp_pool_active`, `_idle` and `_pending` off the injected `MeterRegistry`
  under the endpoint tag, finds `sftp_pool_idle` at 1 after the poll handed its session back, and
  then asserts all three appear in `PrometheusMeterRegistry.scrape()` - which is the endpoint's
  own content rather than a registration somewhere.
- *A Quarkus test boots against the embedded server, polls once, shuts down cleanly* -
  `SftpConnectorAdapterTest.a host gets a connector from its properties, polls with it, and closes
  it on shutdown`. See below.
- *ArchUnit in core still passes: no Quarkus import in core* - `core`'s 52 tests, including
  `ArchitectureTest`, are green and untouched. `core` gained no dependency.
- *Progress entry appended* - this.

**What the `@QuarkusTest` measured.** `EmbeddedSftpServerResource` starts a real MINA SSHD before
Quarkus reads configuration - it has to be a `QuarkusTestResourceLifecycleManager`, because the
port is chosen by the operating system and the connector is built and started while the
application boots, long before a test method runs - and hands back the address, the credential and
a staging directory as configuration overrides. The application then starts a connector against
it, whose start-up probe made `/drop/done`. The test writes `vendor.csv` and its `.ready` marker,
polls once, receives exactly one `FileSeen` (the marker is skipped, not handed over), acks it, and
finds the file at `/drop/done/vendor.csv`. Readiness is the marker rather than the default
heuristic precisely so there is no clock in the test. The gauges are then read, the shutdown
observer is called with a `ShutdownEvent`, and the pool is empty with nothing staged and no
partial file. Calling the observer a second time finds nothing to do, which is what the
application's own shutdown event does a moment later. The core's slf4j `DEBUG` line from
`StartupProbe` appears in Quarkus's log format in that run, which is spec 3.2's D3 working with no
configuration.

**Deviations:**

1. **Three dependency versions are pinned in the module's own `pom.xml`, not the parent's.** The
   brief asks for versions in the parent. These cannot go there: they exist *because* this module
   imports `quarkus-bom`, whose managed versions beat the reactor's for this module only.
   `micrometer-core` is held to the platform's 1.13.7 because the platform manages
   `micrometer-commons` and the registry implementations but not the core, so the module otherwise
   resolves a 1.17 core beside a 1.13 commons - measured, it is what the first run produced. The
   connector touches only `MeterRegistry`, `Counter`, `Gauge` and `Timer`, unchanged since
   Micrometer 1.0, so converging on the older is safe. `sshd-common` is pinned to 2.19.0 for the
   mirror-image reason - the platform manages the common and not the core, and the first test run
   failed with `NoSuchMethodError: ValidateUtils.hasContent` from a 2.19 core on a 2.12 common.
   `quarkus.version` is 3.17.5, matching `etl-host`, which is the only Quarkus this repository has
   resolved. **Debt:** `sshd-common 2.19.0` now sits beside the parent's `sshd-core`/`sshd-sftp`
   2.19.0 literals, so an SSHD bump is two edits or the split-version failure comes back. Repaid by
   converting the parent's literals to an `${sshd.version}` property, which T1 deviation 3 already
   asked for and which this ticket's scope (parent `<modules>` and `<dependencyManagement>` only)
   did not allow.
2. **The `AcceptAll` warning is still not repeated at start-up.** T9 deviation 6 left this to be
   decided here, on the grounds that a host which builds configurations long before starting
   connectors from them is where a second line would earn its place. This host is not that host:
   the configuration is built in `SftpConnectorLifecycle`'s constructor and the connector is
   started by the producer method on that same bean, which runs as the bean is created. The two
   moments are microseconds apart in the same start-up, and the warning is in the log either way.
   Declined again. The shape that would change it is a host holding configurations in a registry
   and starting connectors from them on demand - and it would then be the *starting* that needs
   the line, not the building.
3. **`HostKeyPolicy.Strict` with no known-hosts file named reaches the transport as the empty
   path.** The adapter validates nothing by design, and the builder does not check that a
   known-hosts file exists, so a strict policy with no file is refused by the first handshake
   rather than at build time. The empty path is deliberate: standing in a location nobody named -
   the account's own `~/.ssh/known_hosts` - is the one substitution that could silently succeed
   against the wrong recorded key. On the seams table below.
4. **The Prometheus registry is a test-scope dependency, and `quarkus-micrometer` is the
   compile-scope one.** Spec 3.2 lists "micrometer" among the module's dependencies; a registry
   *implementation* is not that. `quarkus-micrometer` alone binds a composite registry with no
   children on which every gauge is a silent no-op, so the module's own test needs a real one -
   but shipping it would put Prometheus on the classpath of every host that wanted an SFTP
   connector.
5. **The `quarkus-maven-plugin` binds `generate-code` and `generate-code-tests`, not `build`.**
   This module is a library a host depends on, not an application, so there is no runner to
   package; the plugin is here for the application model the `@QuarkusTest` boots against. The
   suite and `mvn package` are both green without the `build` goal.
6. **The shutdown observer keeps the started connector in a field rather than injecting it.** An
   observer that took `SftpConnector` as an injection point would, after a start-up that had
   already failed, ask the container to start a second connector while the application was
   stopping.
7. **The adapter's enums restate two of core's sealed types.** `HostKeyKind` and `ActionKind` are
   the spellings a properties file can use for `HostKeyPolicy` and `PostAction`; a properties file
   cannot name a Kotlin type. A new policy or action in core means editing an enum and one `when`
   here, which the compiler names.
8. **Size.** 244 lines of main source that are neither blank nor comment across two files, and 220
   of tests across three, plus a 141-line pom. Inside the 200-600 budget.

**Seams.** Closed above, struck through: the breaker and `sftp_breaker_state` being per
`SftpClient`, which is the same ruling as R1 finding 6's `PoolMeters`. Added to the table above: a
second connector for one endpoint on one registry reads the first one's numbers, which is now the
maintainer's and needs a change to spec 13's meter identity. Added below: the strict host-key
policy that cannot check its own file.

| Seam | Left by | Owner | What happens if it is forgotten |
|---|---|---|---|
| `HostKeyPolicy.Strict` does not check that its known-hosts file exists | T14 | Whoever next touches DSL validation | A strict policy with a mistyped or missing path is refused by the first handshake with the SSH library's message rather than at build time with the connector's. The builder is where the check belongs, beside the staging-directory check that already lives there |
| ~~`connection is closed by foreign host` on `connect` is not in the error table~~ | T15 | ~~Whoever next touches the mapper (T2's table)~~ | **Closed by T16 (`bc39171`).** Partition row P4 reproduces it on every run: a proxy disabled behind Docker's port publisher accepts the connection and closes it before the version line, and P5 produced it twenty-two times in a minute. The row maps it to `ConnectFailed`; `JschErrorMapperTest` has the case |

**For the next tickets:**

- **T15 (acceptance): the adapter's test is `SftpConnectorAdapterTest`, and it is one test on
  purpose.** Its last act stops the connector, so a second test method in that class would run
  against a stopped one. A `@QuarkusTest` that wants a live connector needs its own class, and the
  application boot it pays for is about five seconds.
- **A `@QuarkusTest` cannot observe the real `ShutdownEvent`,** which fires after the last test.
  Calling `SftpConnectorLifecycle.stop(ShutdownEvent())` is how the shutdown path is exercised
  deterministically, and the real event afterwards finds nothing running and returns.
- **The properties are `Optional` all the way down, and that is load-bearing.** A knob given a
  `@WithDefault` in the mapping is a default that has left the builder, and the two will drift.
  The only `@WithDefault` in the mapping is `readiness.size-stable-interval`, which is inert
  unless `size-stable-checks` is also set.
- **Durations in the mapping are `java.time.Duration`,** converted with `toKotlinDuration()`.
  Quarkus parses `30s` and `4m`; plain SmallRye, which is what the unit test builds, parses only
  ISO-8601, so a `@WithDefault` duration has to be written `PT10S`.
- **`quarkus-micrometer` alone is a composite registry with no children,** on which every gauge is
  a silent no-op. Any later test that reads a meter in a Quarkus host needs a registry
  implementation on its own test classpath, or it goes green against nothing.

---

## T15: Acceptance run: full scenario table and Toxiproxy tier

**Built:** the scenario table as one named suite; the one scenario that had no end-to-end proof;
the partition tier's base and its first two cases against a real Toxiproxy container; the Sec 16
re-check below. No production code changed.

- `AcceptanceScenarios` (testkit) - a JUnit platform `@Suite` that selects S1 to S12 by class
  and method. `mvn -Dtest=AcceptanceScenarios` runs exactly the twelve, verified: `Tests run: 12`.
  It selects, it does not copy: every scenario stays in the class the ticket that built it left it
  in, and runs there on every build. The suite itself runs only by name, because surefire's default
  includes do not match it - by design, or the twelve would run twice per build.
- `AcceptanceScenariosTest` - keeps the list honest on every build: the suite is a suite, every
  selected method exists and is a `@Test`, the IDs are exactly 1 to 12 each once, and no selection
  is from a transport-layer class (`JschErrorMappingTest`, `JschTransportTest`).
- `WatchUnderOpenBreakerTest.S3_` - **the one new end-to-end proof.** S3's expected column is a
  sequence of ticks: `PollSkipped(BreakerOpen)` each tick until the half-open probe succeeds.
  The breaker was proven against a real server through the client (`ResilienceAgainstServerTest.S3_`)
  and one skipped tick on the fake (`SftpWatchTest`), but no test showed a watch skipping every
  tick through the wait *and* the first tick after it being let through as the probe that closes
  the breaker. Fake transport, virtual time, one test; the probe's tick number is not asserted
  because Resilience4j leaves open strictly after the wait, so the assertion is on the shape.
- `partition/PartitionTest` - the base T16's matrix extends. Interface: `withPartitionedClient
  (extra) { client, partition -> }` and `untilEvictedAsPoisoned(n)`, plus the meter readers.
  `Partition` carries `server`, `tunnel`, the Toxiproxy `proxy` handle (T16's rows each name a
  toxic, so a wrapper per toxic would be a pass-through), `globalRequestsHeard`, `drop(directions)`,
  `heal()` and `healOnceNoticed()`, which is the whole of a row's recovery: wait for the pool to
  write the session off, heal, return how long the noticing took counted from `drop`. A row asserts
  `sftp_error_unmapped_total` is zero beside the poisoned eviction, which is what says the loss was
  classified rather than caught by the unknown-wording default. Behind it: one container per JVM started lazily after the first host port is
  exposed, `host.testcontainers.internal` as the upstream, a proxy per topology on the container's
  8666 deleted on the way out, the warm handshake at the shipped keepalive, `pool.close()`.
- `partition/HalfOpenPartitionTest` - **P1** (`timeout` toxic, `timeout=0`, both directions,
  mid-download) and the reply stall the ticket names (the proxy swallows replies under a live
  request; the request direction is proven open by the keepalives the server heard).

**Concepts named:** *Partition* - the network between client and server as a test can damage and
repair it; the seam is `withPartitionedClient`, which is the only thing a matrix row touches.
*Heal window* - the retry backoff in the partition base is the time a test has, after the pool has
written a session off, to repair the network before the fresh dial (see deviation 2). *End-to-end
layer* - a scenario counts when the assertion is on what the connector did (client, source, watch),
not what the adapter raised; the fake transport on virtual time is that layer for tick sequences.

**Acceptance:**

- [x] One suite covers S1 to S12, each named by its ID - `AcceptanceScenarios`, 12 tests run by
  name; `AcceptanceScenariosTest` (3 tests) proves the coverage on every build.

  | ID | Class and method | Layer |
  |---|---|---|
  | S1 | `ResilienceAgainstServerTest.S1_` | embedded server through a tunnel, `SftpSource.poll` + `download` |
  | S2 | `ResilienceAgainstServerTest.S2_` | embedded server, `SftpClient` with retry (the transport-layer S2 in `JschErrorMappingTest` is not counted) |
  | S3 | `WatchUnderOpenBreakerTest.S3_` **(new)** | fake transport, virtual time, `SftpSource.watch` |
  | S4 | `SftpWatchTest.S4_` | fake transport, virtual time, `watch` over a real pool |
  | S5 | `SourceAgainstServerTest.S5_` | embedded server, started `SftpConnector` |
  | S6 | `StartupAgainstServerTest.S6_` | embedded server, `SftpConnector.start` |
  | S7 | `SourceAgainstServerTest.S7_` | embedded server, started `SftpConnector` |
  | S8 | `SftpWatchTest.S8_` | fake transport, virtual time, `watch` |
  | S9 | `ShutdownAgainstServerTest.S9_` | embedded server through a tunnel, `SftpConnector.close` |
  | S10 | `ResilienceAgainstServerTest.S10_` | embedded server, `SftpClient` (the "watch terminates" half is `WatchAgainstServerTest.a wrong password ends the watch...`, unnamed; the transport-layer S10 is not counted) |
  | S11 | `ReadPathAgainstServerTest.S11_` | embedded server, `SftpClient.list` |
  | S12 | `SftpWatchTest.S12_` | fake transport, a real `PROCEED` overlap (the two-polls version against the server is `SourceAgainstServerTest.S12_`, not selected: one per ID) |

  S4, S8 and S12 are on the fake deliberately: spec Sec 17 layer 1 names overlap, pool invariants
  and source events as the fake's, and a real server could prove a tick sequence only with a timer.
- [x] Toxiproxy tier for half-open connection and proxy stall - `HalfOpenPartitionTest`, 2 tests,
  **ran here against Docker Desktop** (Testcontainers 2.0.5, `ghcr.io/shopify/toxiproxy:2.9.0`),
  three consecutive green runs. The skip when Docker is absent is `assumeTrue` in the base's
  `@BeforeAll` with a message that begins `SKIPPED, NOT PASSED`. **The skip path could not be
  exercised on this machine:** Testcontainers falls back through every client strategy, and the
  Docker named pipe is always reachable here, so `DOCKER_HOST` pointed at a dead port still found
  Docker. The coordinator should run the class once on a machine without Docker and read the message.
- [x] Sec 16 open items re-checked - the subsection below. One measurement contradicts spec 17.3
  P1's bound; recorded there and raised, `spec.md` untouched.
- [x] Progress entry appended.

**Deviations:**

1. **S3 is proven on the fake, not against the embedded server.** The ticket says the table runs
   "against the embedded server"; spec Sec 17 puts tick sequences on layer 1, and the spec wins.
   The breaker's real-server proof is still `ResilienceAgainstServerTest.S3_`, and T16's P5 is the
   breaker cycling under a watch on a real network.
2. **The partition base retries with a fixed 250 ms backoff, not 1 ms.** The first P1 run had two
   retries: the fresh dial went out 1 ms after the eviction, its handshake was in progress under the
   toxics when the healer removed them, and Toxiproxy ended that connection - JSch reported
   `connection is closed by foreign host`. That retry was the toxic's, not the partition's. A
   half-open connection is the old flow dead and a new one working, so the base gives the test the
   backoff as the window in which to heal. T16's reset rows need the same window: `reset_peer` stays
   armed for new connections too.
3. **`Partition.heal()` removes toxics and does not re-enable the proxy.** Re-enabling goes through
   the server's update path, which can restart the listener and cut every connection through it;
   P4 and P5 call `disable()`/`enable()` on the handle themselves.

**Spec Sec 16 re-check:**

- **Item 3 (JSch wording) - closed by T2, stands.** One new wording surfaced here that is not in
  T2's table: `connection is closed by foreign host`, `op=connect`, which is what a connection cut
  during the version exchange produces. The mapper's default for an unknown wording - retryable,
  session discarded - did the right thing, and the WARN line asked for the table entry. Recorded in
  the seams table for whoever next touches the mapper.
- **Item 2 (temp folder ownership) - closed by T9's code, for both ownership models.**
  `createActionTargets = true` (the default) has the probe `mkdir -p` the target and, when the
  account may not, refuses start-up with the remedy "set it false and have it created upstream";
  `false` has the probe insist the folder is there and refuse with "createActionTargets is off"
  when it is not. Either way the marker rename then proves the move into it. Tests: `StartupAgainstServerTest`
  (`the connector makes the folder...`, `a folder the connector was told not to create stops it...`,
  `a connector that has started once starts again over what it left`). The one path without a test
  is the refused `mkdir` itself, because the embedded server's virtual filesystem cannot deny one;
  it is the same `checking` wrapper the tested refusals go through.
- **Item 1 (producer-side completeness) - cannot be closed by code; here is what the default does.**
  The shipped default is `sizeStable(2, 10.seconds) + minAge(1.minutes)`, batched per D36: the poll
  stats every candidate, waits ten seconds once with the listing's session released, stats again,
  and hands over a file whose size did not change and whose mtime is at least a minute old.
  - *Protects against:* an upload still writing at any steady rate (its size moves within ten
    seconds, or its mtime is under a minute old); a file that appeared in the last minute; a
    pre-allocated file still being filled (each write moves the mtime).
  - *Does not protect against:* an uploader paused for more than a minute mid-file - a network
    stall, a retry backoff, a client-side flow-control pause - because a paused file has a stable
    size and an old mtime, which is exactly what a finished file has; an uploader that writes in
    bursts more than a minute apart; a server clock behind the connector's, which makes every file
    look older than it is by the skew (`MinAge` compares the server's mtime to the connector's clock).
    SFTP mtime has one-second resolution, which is noise beside a one-minute floor.
  - *The question for the upstream team, precisely:* "Do you upload under a temporary name and
    rename when finished, or write a marker file next to the finished file? If neither, what is the
    longest pause your uploader can take in the middle of one file, and how far can your server's
    clock be from ours?" A rename or marker convention makes readiness deterministic
    (`markerFile(suffix)` ships); an answer to the pause question sizes `minAge`.

**Measurements** (recorded, not asserted beyond a bound; D35):

| Case | `keepAlive` | Session written off after | Bound asserted |
|---|---|---|---|
| P1, both directions dropped mid-download | 500 ms | 1046, 1048, 1052 ms | `< 2 x keepAlive + keepAlive` |
| Reply stall under a live request | 500 ms | 1024, 1025, 1027 ms | same |

**A measurement contradicts spec 17.3 P1's "`SessionLost` within `2 x keepAlive`":** the session
is written off `2 x keepAlive` *plus* 25 to 50 ms, every run, and cannot be sooner. The mark is
taken the moment the toxics are in place; in P1 the 64 KB the tunnel was holding lands on the
client in the same instant, on the relay thread that took the mark, and the stall case holds no
bytes at all and shows the same excess. Two intervals is when JSch's keepalive gives up, counted
from the last byte the client received; the disconnect, the ladder and the eviction follow.
Measured from the partition, the bound is not met and never will be. **Proposed decision entry:** *"Spec 17.3's `within 2 x keepAlive` is JSch's give-up point,
measured from the last byte received; the connector's write-off lands a few tens of milliseconds
after it. A partition test asserts `2 x keepAlive` plus one keepalive of grace and records the
number."* The tests assert that today; the spec wording is the coordinator's to change.

**For the next ticket (T16):**

- **Extend `PartitionTest`; do not copy it.** A row is `withPartitionedClient(extra) { client,
  partition -> ... }`, faults through `partition.proxy.toxics()` (or `drop`), recovery through
  `partition.healOnceNoticed()`, or `heal()` after `untilEvictedAsPoisoned(n)` for a row that
  counts differently. The 250 ms backoff is the heal window; a
  row that arms `reset_peer` and heals after the first reset relies on it.
- **`heal()` does not re-enable a disabled proxy** (deviation 3); P4 and P5 own `disable()`/`enable()`,
  and should expect every connection through the proxy to be cut by `enable()`.
- **A `Partition` is a bare `SftpPool` and `SftpClient`, not a started connector.** A row that acks
  with `move("temp/")` creates the folder itself (P1 does); a row that needs the housekeeper or
  `close()` (P6) should start an `SftpConnector` over the same config and its own scope, as
  `ShutdownAgainstServerTest` does, and `withPartitionedClient` will still close the pool it made.
- **One container per JVM, one proxy at a time on port 8666**, named `sftp-<n>`; the topology
  deletes its proxy on the way out. Two topologies alive at once would need a second listen port
  from the container's range.
- **`Testcontainers.exposeHostPorts(port)` for every new server**, which the base does; the first
  call is what lets the container start with `host.testcontainers.internal` at all, and later
  calls add ports to the running forwarder.
- **The reactor is longer now:** S11 alone is fifty seconds, and the partition class is eight
  seconds plus the container's first start.

---

## T16: Pressure: randomized adversary, model checking, partition matrix, soak

Built on `claude-fable-5-1`. 253 tests were green at the start and 263 are green at the end
(55 core - 53 plus the two model checks in their own execution - 202 testkit, 6 quarkus), with two opt-in groups outside the count.
One defect was found and fixed in its own commit; two production refactors were made for the model
checker and are declared below; no earlier test was weakened. The harness found four flaws in its own
model before it found one in the connector, and each is described because the next reader will meet
the same shapes.

**Built:**

- **Tier A** - `pressure/AdversaryTest` and `pressure/Adversary` (testkit). A real `SftpConnector`
  over `FakeSftpTransport`, the network played from the sequence's own `Random` through the fake's
  one hook: stall (10 ms, 1 s, 30 s - the last past the operation timeout), lose the reply, land a
  rename and then lose its reply (the I11 case, done to the fake's contents by the hook), refuse with
  `ServerFailure`, invent a `NoSuchFile` on a stat, rename or delete, an unknown wording, a refused or
  rejected connect, and a killed session that answers nothing again - including a call already on the
  wire. Eleven ops: `poll`, `ack`, `nack(redeliver)`, `download`, `cancelCollector`, `killSession`,
  `advanceClock`, `addFile`, `blockTarget` (a stranger's file at the ack target, for `OverwriteRefused`),
  `removeFile`, `growFile`; one sequence in five closes the connector mid-way with whatever is in
  flight. 5000 sequences x 40 ops on virtual time, **52.6 s wall**; the model is the server as two
  maps of names and the files the consumer holds; a failure is rerun on ever shorter prefixes and
  rethrown with the seed, a replay line and the full operation log. Checked after every op: I1
  (`total <= maxSize`), I7/I8 (the `sftp_inflight` gauge against the model, and no file handed over
  while a held copy is still in flight), `sftp_pool_leak_total` zero, and the breaker: a window of one
  makes "the breaker never counts a `Fatal`, an `OverwriteRefused` or an answered refusal" exact -
  whenever no counted-class fault has happened since it last closed, it reads closed. At quiescence:
  I4 (the pool fills to `maxSize` and the next caller is refused with `PoolExhausted`), no orphan
  session (`openSessions == total`), I13, and I15 as a ledger - the fake's every file against the
  model's. At the end: I9 on virtual time, `openSessions == 0`, `total == 0`, `created_total ==
  evicted_total` (every session opened left for a reason), no `.part`. The leak run: 50,000 ops on
  one seed, sessions 2 -> 2, threads 4 -> 4, post-GC heap 11 MB -> 18 MB alone and 19 -> 28 MB inside the reactor (the
  harness's own operation log is most of that), all inside the band.
- **Tier B** - `InFlightSetLincheckTest` and `SessionRegistryLincheckTest` (core), Lincheck 3.7 in a
  surefire execution of its own with a 600 s fork timeout, `ModelCheckingOptions` only; 7 s and 30 s
  per build. Lincheck 3.7 carries no JUnit 4, so there was nothing to exclude.
- **Tier C** - `partition/PartitionMatrixTest`, P2 to P6 on T15's base, plus `Partition.damage` (any
  toxic, with the mark the recovery is measured from). Green against Docker Desktop.
- **Tier D** - `pressure/SoakTest`, gated on `-Dsftp.soak.minutes=N` through a surefire profile
  (`@Tag("soak")`, excluded by default); run once here at N=5, numbers below.
- **Measurements** - `pressure/MeasurementsTest` and the matrix's `measure_` row, `@Tag("measure")`,
  run with `-Dsftp.measure`; observations only, in the table below.

**Concepts named:** *Adversary* - the network as a seeded random plays it; its seam is the fake's
hook, and its one addition is a per-operation log (`OpLog`, a coroutine-context element) of what it
did to the wire, because the model that judges an outcome cannot see the wire. *Ledger* - the fake's
contents against the model's, the whole of I15 in one equality. *Landed* - a rename the server
carried out before its reply was lost; the model learns of it at the instant it lands, exactly as it
learns of a listing, and the ack that follows may then honestly fail. *Lock body* - the
non-suspending core of a lock-guarded structure, which is what a model checker can check.

**Acceptance:**

- [x] Tier A, the seeded fault model - `AdversaryTest.model_thousands of random sequences...`, 5000 x
  40, 52.6 s; shrinking is `runShrinking`, exercised four times on the way (below).
- [x] Tier A invariants after every op - `World.checkStep`; the quiescent ones in `checkQuiescent`,
  because an entry being hung up on mid-op is neither leaked nor orphaned, and a `.part` mid-download
  is the design. Deviation 1 says what runs where.
- [x] Tier A leak check - `AdversaryTest.leak_fifty thousand ops...`, band asserted on all three.
- [x] Tier B - both classes green; the approach, and why it is the lock body, in deviation 2.
- [x] Tier C - one named test per row P2-P6, each asserting the disposition, the counter and the
  recovery time; skip on no Docker is the base's `assumeTrue` from T15.
- [x] Tier D - `SoakTest`, N=5 run below; CSVs in `sftpconnector/testkit/target/soak/`.
- [x] Degradation recorded - the measurements table below.
- [x] One defect, one commit, the row in its message (`bc39171`, P4). Spec contradictions raised
  below, `spec.md` untouched.
- [x] This entry.

**Defect found and fixed:**

1. **`connection is closed by foreign host` at `connect` was `Unknown`** - `JschErrorMapper`, commit
   `bc39171`, found by P4 and reproduced by P5 twenty-two times in a minute. A proxy disabled behind
   Docker's port publisher accepts the TCP connection and closes it before the version line, and
   JSch reports that from `Session.connect`. The default for an unknown wording - retry, discard the
   session - was right by accident, and `sftp_error_unmapped_total` counted every dial. T15 saw the
   wording once from a toxic removed under a handshake and left it on the seams table; that row is
   closed. Mapped to `ConnectFailed`; `JschErrorMapperTest` has the case, run red first. The same
   shape exists in production: a load balancer with nothing behind it.

**What the harness found in itself first** (each a false alarm whose fix made the check exact):

- A 30 s stall's wire fault was consumed by the check before its timeout fired, and the breaker was
  then "seen closed" with the fault still in flight. The flag clears only at quiescence.
- With a breaker window of 2, an old counted failure still in the window opened the breaker on a
  later *success*, so "closed means no fault since" was unsound. A window of one makes it exact.
- The network moved a file whose reply it then lost, and the world grew the same name at the source
  before the ack finished; the model's "unknown location" resolution found it at both places. The
  model now learns of a landed move at the instant it lands (`Adversary.onLanded`).
- The client buffers a listing and the tick checks the in-flight set only as it drains that buffer,
  so an ack completing in between makes a file both listed and free. The listing expectation is now:
  nothing handed over that was not listed, everything listed and free at listing time handed over,
  and I7 asserted at the instant of each handover instead.

**Deviations:**

1. **The quiescent invariants run at quiescence, not after every op.** I4's fill-and-refuse probe,
   `openSessions == total`, I13 and the I15 ledger are checked when the op's tail settled the world
   (every op has a random tail of 0, 100 ms or 5 min 31 s; two in four are the long one). Mid-op, a
   session being hung up on is open in the transport and gone from the registry, and a `.part` under
   a download is the design; asserting either there would assert a transient. I1, I7/I8, the leak
   counter and the breaker run after every op.
2. **Tier B checks the lock bodies, not the suspending operations, and two production lines moved
   for it.** `InFlightSet.admit` is not linearizable by design: a duplicate that passes the first look
   while another poll admits the same file waits for room it will then not use, which no sequential
   run can produce and which `SftpSourceTest.I7_a file two waiting polls both want...` proves
   directly. The checker reported that first, then its own verifier failed with a null pointer on the
   suspending wrapper. So `InFlightSet`'s lock bodies are now `enter`/`exit`, non-suspending and
   `internal`, and those are what the model check covers (I7's double entry, I8's exit, the
   for-good exclusion); the room the semaphore keeps is the library's. `SessionRegistry`'s `HOLDABLE`
   became an `EnumSet`, because a model checker that makes identity hashes deterministic cannot find
   an enum constant in a `HashSet` built before instrumentation (`hash(InUse) = 0` in the trace). The
   registry run checks its single-lock calls (`checkOut`, `stats`, `beginClosing`, `closeEverything`):
   its multi-call sequences - check out, dial, fill; hand back, hang up, close - are outside the lock
   by design (I5), their intermediate states are observable on purpose, and a wrapper that composed
   them was reported for exactly that. Those sequences are Tier A's and R1's. Not the coroutines
   mismatch the ticket anticipated: suspend operations ran; the shape of the structures was the limit.
3. **The world sets `maxLifetimeJitter = 0.0` and `jitter = false`.** `SessionRegistry.lifetime()` uses
   an unseeded `kotlin.random.Random` and the retry's jitter is Resilience4j's own; either would make a
   seed not replay. Seams row added.
4. **The directory is kept under forty files.** `SftpClient.list` hands entries on with
   `trySendBlocking`, which parks the calling thread once the channel's buffer is full; the fake answers
   on the caller's own coroutine, so on the one test thread a listing over the buffer would deadlock
   the test. Production runs the listing on the IO dispatcher and is unaffected; S11 covers 100k
   against the real server.
5. **P3 holds the reply with a `latency` toxic before the request goes out.** A `reset_peer` toxic
   fires on the next bytes through its direction; the first staging (a tunnel stall plus a reset) left
   nothing flowing through the proxy in either direction, and the rename sat until the operation
   timeout. With the reply held for 500 ms inside the proxy, the reset added behind it the moment
   the request has passed the tunnel is what the reply runs into, and the server heard the rename
   with certainty. The retry's log line reads "is at /drop/temp/a.csv with the expected 15 bytes, so
   the rename whose reply was lost had landed; reported as success". Not asserted on P1/P2: the class
   of the first try's failure, which is not observable from outside a successful retry; it is proved by
   exclusion (evicted as poisoned, nothing unmapped, and in P2 noticed inside the reset's time).
6. **Tier D's recovery bound adds the tick interval and the breaker's wait in open** to the ticket's
   `2 x keepAlive + max backoff`: a watch recovers on its next tick, and a breaker that opened lets
   nothing through until its wait has run. Proposed decision entry below. The N=5 run never needed
   either: every recovery was under 1.11 s against a bound of 8 s.
7. **`FakeSftpTransport` gained `snapshot()` and `bytesAt()`**, so a model can be checked against the
   truth without a listing and the hook can move a file the way the server would.

**Spec findings raised, not applied** (C9's protocol):

- **I15's "no landed move is reported as failed" holds only while a retry is permitted.** A rename
  that landed and lost its reply is rescued by the retry's look at the target (I11); when that retry
  is refused - the budget spent, the breaker open - the ack must report failure while the file is at
  the target, and the file is then neither redelivered nor acked. The adversary models the move at the
  instant it lands and asserts I11 only when a retry's stat saw the landed file. *Proposed:* "I15's
  phantom-failure clause is bounded by the retry budget and the breaker; a lost reply on the last
  permitted try is reported as failure, and the consumer's WARN line is the only record."
- **Tier D's `2 x keepAlive + max backoff`** omits the watch's tick and the breaker's wait in open.
  *Proposed:* "Recovery is measured heal-to-next-`PollCompleted` and bounded by `2 x keepAlive + max
  backoff + interval + waitInOpen`."
- **`InFlightSet`'s class comment says a duplicate never queues for room it will not use.** It does
  when the duplicate arrived between the two looks; the outcome is still once, never twice.
  *Proposed:* the comment says "seldom", and spec 7.3 says the second look is what keeps the promise.

**Measurements** (D35; recorded, not asserted):

| What | Setting | Observed |
|---|---|---|
| Tier A wall time | 5000 x 40 ops, virtual time | 52.6 s |
| Tier A leak run | 50,000 ops, one seed | sessions 2 -> 2, threads flat (4 -> 4 alone, 29 -> 29 inside the reactor), post-GC heap +7 to +9 MB |
| Tier B per build | model checking, 10 iterations x 500 | `InFlightSet` 7 s, `SessionRegistry` 30 s |
| P2 reset mid-download | `keepAlive` 500 ms | session written off 26 ms after the reset |
| P3 reply held then reset | reply held 500 ms | written off 542 ms after the mark; one retry; success once |
| P4 proxy down, then back | breaker 3/50 %, `waitInOpen` 1 s, tick 300 ms | `PollFailed(ConnectFailed)`, breaker 2, then `PollCompleted` 1344 ms after `enable()`, 5 ticks skipped |
| P5 flapping 3 s for 60 s | `maxSize` 2, tick 1 s | 61 ticks: 30 completed, 1 failed (`ConnectFailed`), 30 skipped; breaker 0 -> 2 -> 1 -> 0 seen; sessions never above 2; 10 opened, 9 evicted poisoned |
| P6 close under a two-way drop | `drainTimeout` 1 s, `cancelGrace` 300 ms | closed in 1045 ms; `.part` gone; one `reason=shutdown` |
| Acquire wait, `maxSize` 3, 30 rounds per caller | concurrency 1 / 2 / 3 / 4 / 5 | p50 34 / 31 / 27 / 274 / 398 us; p99 79 / 61 / 75 / 1104 / 839 us |
| Three concurrent 100k listings | embedded server | 100,000 each in 1 m 44 s; post-GC heap 9 MB before, 38 MB peak used, 11 MB after |
| One stat under each toxic, one attempt | `keepAlive` 500 ms | none: ok 37 ms; latency 200 ms: ok 208 ms; reset_peer: `SessionLost` 20 ms; timeout 0 both ways: `SessionLost` 1011 ms |
| Soak, N=5 | `maxSize` 2, `keepAlive` 500 ms, tick 1 s, a file every 500 ms, a fault every 2-10 s | 42 faults (stall, kill, refuse); 585 produced, 585 delivered, none twice; threads 36 -> 37; post-GC heap 11.26 -> 11.45 MB (46 KB/min); `created_total` 26 against the sessions killed; breaker 0 at every sample; recovery 47-1106 ms, stalls slowest (2 x keepAlive), refusals fastest |

**For the next ticket (T17, the deep review):**

- Read `AdversaryTest`'s KDoc and the four self-findings above before reading the model; each one is a
  timing fact about the connector that a reviewer will otherwise rediscover as a "bug".
- The replay line in a failure is `runSequence(seed=..., ops=..., closeAt=...)`; call it from a
  scratch test. The op log names every fault the network injected, per operation.
- The adversary never faults `Close` or `Abort` (a real hang-up closes the socket whatever the peer
  does), never injects `NoSuchFile` on a read, and only cuts a call the fake is stalling; a killed
  session otherwise fails its calls at their next hook. A lost reply on `delete` is not simulated (no
  delete action in the world). Those are the places a reviewer would widen it.
- The Lincheck wrappers are deliberately narrow; deviation 2 says what a wider one reports and why.
- `PartitionMatrixTest.measure_...` and `MeasurementsTest` are `@Tag("measure")`; `SoakTest` is
  `@Tag("soak")`. The default reactor excludes both groups through `sftp.excludedGroups`.
- The reactor is longer again: Tier A adds a minute, Tier B forty seconds, the matrix about two and a
  half minutes with P5's sixty.
