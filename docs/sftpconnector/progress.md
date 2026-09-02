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
