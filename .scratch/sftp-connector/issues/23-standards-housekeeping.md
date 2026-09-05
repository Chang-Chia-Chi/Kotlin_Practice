# 23: Standards housekeeping from the whole-module review

**What to build:** The findings of a Fable 5.1 standards pass over the whole module against the
implementer brief and a smell baseline, batched into one ticket because each is small and none
changes behaviour. Every item names its lines as of `6dcda68`; re-locate after lens 3 lands.

**Blocked by:** 24 (deleting the Quarkus module removes two of the findings), and 22

**Model:** Opus 5

**Status:** done

**Spec changes this ticket applies first** (spec-only findings; code stays):

- 5.3, cancellation tiers table: the cooperative-tier row says "returned to the pool after
  validation". The code shelves the session idle with no probe and revalidates only after
  `validationBypass` elapses (`SftpPool.kt:527-529`, `SessionRegistry.kt:111`). Amend the row to
  say what the code does and why it is safe: the cooperative stop drains the pipelined reads, so
  the session is as sound as any other idle one.
- 7.2 and 14.3: `ackWait` is "not built", not "off by default"; there is no knob. Same wording
  fix 06's S2 made for `SeenRepository`.
- 8.2: action targets are relative to the watched directory; the automatic exclusion from the
  listing compares spellings, so an absolute target under a relative watch is re-listed. One
  sentence.
- Progress open-seams table: append a row "Meters are declared in five files and `source` reads
  a result label from `client`" - left by this review, owner "whoever next revisits spec 13",
  consequence "the closed list of what the connector publishes is not in one place".
- Progress T12 entry, Deviations: record that the stray-timeout guard at `SftpSource.kt:254-258`
  rethrows a `CancellationException` as the cause of an `IllegalStateException`, why (a timeout
  that was not the tick's own must not be mistaken for the tick being cancelled), and that this is
  the one accepted exception to "never wrapped".

Hard violations of the brief:

- [x] Spec-section citations removed from code and tests, each replaced by the reason in its own
      words: `config/ConnectorDsl.kt:73`, `resilience/Resilience.kt:229`, `StartupProbe.kt:219`,
      `testkit/.../SftpConnectorTest.kt:188-189`, `testkit/.../source/SftpWatchTest.kt:314`,
      `testkit/.../pressure/AdversaryTest.kt:74`; then `grep -rn "spec [0-9]" sftpconnector/`
      finds nothing outside `docs/`
- [x] `testkit/.../testkit/JschTransportTest.kt:29, 40, 61, 85`: `runBlocking<Unit>`; count the
      tests the class reports before and after

Judgement calls accepted:

- [x] Duplicate `SftpSession.entryAt`: `StartupProbe.kt:186` deleted, the `internal` one in
      `client/Compensation.kt:152` used
- [x] `JschTransport.kt:169-242`: the six `withContext(io) { errors.translating(Attempt.inside(
      endpoint, op, path)) { ... } }` bodies become one private `call(op, path) { }` helper
- [x] `SftpClient.kt:147-293`: the operation name is spelled once per operation, not once for
      `meters.timing` and again for `resilience.attempting`
- [x] `SftpPool.acquire()` (`:123`), `Lease.release()` and `releaseAfter()` (`:511, :528`) become
      `internal`; production goes through `withLease`; tests in `testkit` are in the same module
      group only if the build allows - if `internal` is not visible from `testkit`, report and
      leave public with a KDoc line saying `withLease` is the interface
- [x] `SftpPool.close()` (shutdown, `:302`) and the private `close(connection, entry)` (`:455`)
      no longer share a name; `RenameTries.attempt(session, attempt: Attempt)` (`Compensation.kt:44`)
      renamed so the verb and the noun differ
- [x] Full reactor green; no test modified except the `runBlocking<Unit>` change
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen`; modify only `sftpconnector/` and `docs/sftpconnector/`.
