# SFTP Connector - Standing Brief for Implementers

Read this before your ticket. It holds the decisions that are the same for every ticket, so
each session does not re-invent them. Your ticket file holds what is different.

## Authority order

1. `docs/sftpconnector/spec.md` - the design. Wins over the ticket when they disagree.
2. Your ticket in `.scratch/sftp-connector/issues/` - the slice of the spec you build now.
3. This brief - layout, stack and conventions the coordinator fixed.
4. `docs/sftpconnector/progress.md` - what earlier tickets did, and every deviation they took.
   A deviation recorded there overrides the spec for the code that already exists.

If reality contradicts the spec, stop and report to the coordinator rather than deciding
alone. Update the document first, code second.

## Module layout

`sftpconnector/` is an aggregator (`packaging` pom). Three modules under it:

| Module | Artifact | Contains | Depends on |
|---|---|---|---|
| `sftpconnector/core` | `sftpconnector-core` | transport interface, JSch adapter, pool, client, source, resilience, DSL, errors, metrics | kotlin-stdlib, kotlinx-coroutines, mwiede JSch, resilience4j-kotlin, micrometer-core, slf4j-api |
| `sftpconnector/testkit` | `sftpconnector-testkit` | embedded Apache MINA SSHD with fault hooks, and the scripted fake transport | `sftpconnector-core`, Apache MINA SSHD |
| `sftpconnector/quarkus` | `sftpconnector-quarkus` | CDI producer, config mapping, shutdown hook, registry binding (ticket 14 creates it) | `sftpconnector-core`, Quarkus |

**Where tests live.** `testkit` depends on `core`, so `core`'s tests cannot use `testkit`.
Therefore:

- `core/src/test` - only tests that need neither the fake transport nor a server: ArchUnit
  boundary rules, DSL build-time validation, error-table unit tests.
- `testkit/src/test` - everything else. Pool invariants against the fake transport, client,
  source, resilience, shutdown, and every embedded-server test.

This is decision C1 in `progress.md`. Where a ticket says "fake transport in the testkit" and
also asks for a pool test, both land in `testkit` - that is this layout, not a deviation.

Base package: `sftp.connector`. Sub-packages follow the spec's layers: `transport`,
`transport.jsch`, `pool`, `client`, `source`, `resilience`, `config`, `error`, `metrics`.

## Stack (fixed, do not substitute)

- Kotlin 2.2.0, `kotlinx-coroutines-core`. JVM target **17** - the host runs JDK 17, so no
  virtual threads (spec D4). The parent pom compiles with JDK 21; set `jvmTarget`/`release`
  to 17 in the module, and fix the stale `1.8` the scaffold pom currently carries.
- JSch: the **mwiede** fork (`com.github.mwiede:jsch`), not `com.jcraft:jsch`. jcraft 0.1.55
  has no rsa-sha2 signatures.
- Resilience4j (`resilience4j-kotlin` plus the modules you need).
- Micrometer `micrometer-core`. Logging `org.slf4j` - **not** JBoss Logging and not
  `io.quarkus.logging.Log`. Spec Sec 3.2 (D3) fixes slf4j for this connector, which keeps
  `core` free of any framework.
- Tests: JUnit 5 + Mockito (`mockito-core`, `mockito-kotlin`) + AssertJ, and
  `kotlinx-coroutines-test` for virtual time. Apache MINA SSHD for the embedded server.
- Quarkus only in `sftpconnector/quarkus`. ArchUnit fails the build if `core` imports it.
- No database in this design. JDBI is available in the repo but nothing here needs it - do
  not add persistence.

Declare versions in the **parent** `pom.xml` `dependencyManagement` and reference them
without a version in the modules, the way the existing modules do.

## Ground rules (from every ticket, repeated once here)

- Implement only your ticket. A stub throwing `NotImplementedError` is the correct
  placeholder for a later ticket's seam.
- Roughly 200-600 lines including tests. Judge size by whether the design stays simple, not
  by the line count alone - but a ticket that has clearly outgrown its slice is a stop-and-report.
- **No `Thread.sleep` in tests.** Determinism comes from injected `java.time.Clock`,
  `kotlinx-coroutines-test` virtual time, and declared hook points.
- Invariant tests are named `I<n>_<description>`; scenario tests are named by their `S<n>` ID.
- Every new configuration knob lands in the DSL block for its area, with build-time validation.
- Every new meter uses the exact name from spec Sec 13. No new metric names.
- Never weaken or modify a test an earlier ticket wrote. A failing earlier test means your
  change is wrong - stop and report.
- `CancellationException` is never wrapped, never swallowed.

## Comments and messages carry reasons, not citations

Spec section numbers live in `docs/**` and nowhere else. A log line, exception message, HTTP
response, comment or KDoc states its reason in its own words - "(spec 10.1)" tells an
operator nothing. A comment must read as a complete thought with no document open.

Two things that look like citations and stay: ticket and finding names (T3, S7) point at
`progress.md` history, and invariant names (I1, I14) are the invariants' own identifiers.

## How to work the ticket

You are running the `implement` workflow. In order:

1. **Model the domain first.** Use the `mattpocock-skills:domain-modeling` and
   `mattpocock-skills:codebase-design` skills before writing code. Name the concepts your
   ticket introduces, and design deep modules - a narrow interface over real substance. A
   shallow pass-through wrapper, an interface with one implementation that exists only to
   look layered, or a class that is a bag of getters is a design failure, not a style
   preference. Say in your report which concepts you named and where the seams went.
2. **TDD at the pre-agreed seams.** Write the failing test, then the code. The acceptance
   checkboxes on your ticket are the tests.
3. Typecheck and run your module's tests often; run the full reactor once at the end.
4. Self-review with `mattpocock-skills:code-review` before you report.
5. Append your `progress.md` entry (template at the top of that file).
6. Commit to the current branch with a message naming the ticket.

## Build commands

```bash
mvn -q -pl sftpconnector/core -am test        # one module and its deps
mvn -q -pl sftpconnector/testkit -am test
mvn -q test                                   # full reactor, once, at the end
```

## Report back

Finish with: what you built, the concepts you named and the seams you chose, which
acceptance boxes are green with the evidence, any deviation you recorded and why, and
anything the next ticket must know.
