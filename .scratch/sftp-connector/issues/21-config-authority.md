# 21: A config the connector sees has always passed `build()`

**What to build:** `SftpConnectorConfig` and its nested config classes have public constructors,
so the DSL's validation is a convention. The one consumer resizes a validated config with
`copy(pool = pool.copy(maxSize = ...), resilience = resilience.copy(...))` after the fact
(`shuttle/src/main/kotlin/infra/shuttle/quarkus/ShuttleHost.kt:413-416`), re-deriving
`minIdle <= maxSize` by hand and bypassing `maxConcurrentTransfers <= maxSize`. Make the
compiler enforce what spec 12 states.

**Blocked by:** T17 lens 3 committed on `misc/ai_gen` (it touches `ConnectorDsl.kt`)

**Model:** Opus 5 - well-specified, single-threaded

**Status:** done

**Spec changes this ticket applies first:**

- 12: one sentence - the config types are produced only by the DSL and cannot be constructed or
  copied outside the connector; a host that needs to size a pool from its own numbers passes
  them into the DSL blocks.

- [x] Every config data class in `sftp.connector.config` gets an `internal` constructor and
      `@ConsistentCopyVisibility` (Kotlin 2.2 otherwise leaves `copy()` public and warns);
      `sftpConnector { }` remains the only producer. Any connector test that constructed one
      directly goes through the DSL instead - record each in the progress entry
- [x] Shuttle compiles: `ShuttleHost.sized()` is replaced by two parameters on
      `infra.shuttle.sftp.sftpConnectorConfig(store, poll, algorithm, resolve, sessions, transfers)`
      that land in `pool { maxSize; minIdle = minOf(...) }` and `bulkhead { maxConcurrentTransfers }`.
      This is the whole of the shuttle change in this ticket - no other shuttle edit; the
      Shuttle orchestrator has been told and ticket 31 there does the rest
- [x] Test in `ConnectorDslTest`: `transfers > sessions` passed through the DSL is refused at
      build time with the existing rule's message (the rule `sized()` used to bypass)
- [x] Full reactor green, shuttle included
- [x] Because this ticket touches `shuttle/`: run shuttle's default tier in the worktree (`mvn -B -o -q -pl shuttle test`, about 90 s) and put the counts in the progress entry. `ShuttleQuarkusTest` may fail on port 8081 and `ShuttleHostTest`'s two readiness cases may flake under parallel builds until shuttle ticket 30 lands; rerun those alone and say so
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen`; modify only `sftpconnector/`, `docs/sftpconnector/`, and the two shuttle files named.
