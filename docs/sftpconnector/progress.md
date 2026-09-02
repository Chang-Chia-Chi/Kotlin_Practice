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

### Open seams - things deferred, and who picks them up

Coordinator-maintained. A ticket that closes one strikes it through in its own entry; a ticket
that adds one appends a row. These are the things most likely to be lost between sessions,
because each was correctly deferred by the ticket that found it.

| Seam | Left by | Owner | What happens if it is forgotten |
|---|---|---|---|
| `SftpPool.housekeep()` has no production caller | T5 | Whichever ticket builds the connector's `CoroutineScope` - T9 startup is the natural home, T13 owns cancelling it | The housekeeper never runs: no lifetime eviction, no idle eviction, no leak reports, and `minIdle` is a knob with no effect. The most consequential open seam on this list |
| `socketTimeout` is dead configuration | T2 measurement, spec D26 | T8 | A knob that reads as the bound on a hung server and bounds nothing. T8 either makes it mean something or removes it |
| `HostKeyPolicy.Fingerprint(sha256)` unimplemented | T1 | The first ticket needing fingerprint pinning | Two of spec 5.2's three policies ship. Kotlin's exhaustive `when` names every site when it is added, so this cannot rot silently |
| `sftp_pool_leak_total` registers on first use | T5 | The ticket that next revisits T4's exact-meters assertion | No series on a dashboard until the first leak, so an alert must treat absent as zero |
| `Attempt.number` is always 1; the pool names its own operation `acquire` | T2, T4 | T11, which owns retries and is the layer that knows which try it is | Log lines and metrics attribute a caller's failure to the pool rather than to the operation that failed |
| `Retirement.SHUTDOWN` has no producer | T5 | T13 | `sftp_pool_evicted_total{reason=shutdown}` never appears |
| `OperationTimeout` has no producer | T2 | T11's time limiter | A failure class in the hierarchy that nothing raises |
| `MutableStateFlow.value` can resume an undispatched collector under the registry lock | flagged by the maintainer | Any ticket that collects `PoolEntry.state`/`Lease.state` | Foreign code runs inside a critical section. Still theoretical: T5 confirmed nothing collects either, both read `state.value` |

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
