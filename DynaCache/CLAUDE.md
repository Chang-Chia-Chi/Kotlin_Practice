# DynaCache

A Dynamo-style AP distributed cache in Kotlin speaking the Redis wire protocol, plus a
Raft-backed CP subsystem (MicroRaft) for linearizable primitives on the `cp:*` namespace.
A learning project built ticket by ticket by subagents under an orchestrator.

## Documents of authority

All under the parent repository's `docs/dynamiccache/`:

- `design-spec.md` (AP engine: C1 to C15, I1 to I12) and `design-spec-cp.md` (CP: C16 to C23,
  I13 to I22). The spec wins over a ticket unless `progress.md` records a deviation.
- `plan.md` (ground rules, module graph, seams, ticket DAG, model routing, orchestrator
  protocol) and `plans/p1..p5` (one entry per ticket).
- `progress.md`: one entry per finished ticket.
- Tickets: `.scratch/dynacache/issues/NN-<slug>.md` in the parent repository.

In this repository: `CONTEXT.md` is the glossary (use its words in code and tests);
`docs/adr/` holds the architecture decisions.

## How work happens

The orchestrator writes no code. Every ticket runs in a fresh subagent that follows the Matt
Pocock `implement` shape: `tdd` at the seams the plan entry names (red before green, one
vertical slice at a time), `codebase-design` vocabulary for any new interface, a
`code-review` pass, then a commit. See `plan.md` section 6.

## Module graph (Maven-enforced)

```
dynacache-engine   kotlin-stdlib only; JDK executors allowed; no I/O, no coroutines
dynacache-cluster  engine + kotlinx-coroutines + grpc-kotlin + protobuf
dynacache-cp       cluster + MicroRaft                     (from ticket 38)
dynacache-server   cp (cluster until ticket 38) + Netty + LuaJ
```

## Tech stack

Kotlin 2.2, JDK 21, Maven. Netty for RESP2, gRPC-Kotlin and protobuf between nodes, LuaJ for
`EVAL`, MicroRaft for consensus. No framework.

## Tests

JUnit 5 + Mockito only. No AssertJ, MockK, Kotest or Hamcrest. Mocks only at true boundaries
(sockets, clock, randomness, filesystem). No sleeps: time is an injected `java.time.Clock`.
Spec-named tests keep the spec's name; constraint tests are `C<n>_<description>`, invariant
tests `I<n>_<description>`.

## Build

```bash
export JAVA_HOME=/c/Users/maxch/.jdks/openjdk-22.0.1
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o package
/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn -o test -pl dynacache-engine
```

`protoc` (run by the protobuf Maven plugin in `dynacache-cluster`) cannot open paths that
contain the non-ASCII `文件` directory, and Maven canonicalizes junctions, so a build in this
checkout fails at protobuf generation. Build from a git worktree of the repository on an ASCII
path instead, for example `git worktree add -b <name> /c/Users/maxch/kp-verify misc/ai_gen`
and then `mvn -o -f DynaCache/pom.xml clean package` from that worktree.

## Git

This directory is an ordinary part of the `Kotlin_Practice` repository (branch `misc/ai_gen`);
its history arrived through `git subtree add` from the `dynacache` branch, which remains as a
backup of the standalone period. Code, docs and tickets are committed together in this one
repository.
