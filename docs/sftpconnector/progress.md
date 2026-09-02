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
