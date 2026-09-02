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
