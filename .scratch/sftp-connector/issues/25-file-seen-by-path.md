# 25: A listed file can be fetched by its path while it is in flight

**What to build:** After ticket 31, shuttle's `SftpPollSource` keeps one last table: path to
`FileSeen`, because a pipeline that starts from a stored row knows the path it wants and the only
way to download, ack or nack is through the `FileSeen` the watch handed over earlier. The in-flight
set already holds exactly that mapping (T19 keys exclusivity on the path). Give it a query: the
source answers the `FileSeen` for a path currently in flight, or null, and shuttle deletes its
table. Shuttle ticket 31's progress entry asks for this in so many words.

**Blocked by:** None (the batch 18 to 24 is merged and pushed at `fff5688`)

**Model:** Fable 5.1 - a read of the in-flight set under its lock, and a `FileSeen` that outlives
the tick that made it

**Status:** done

**Spec changes this ticket applies first:**

- 7.1 or 7.3: one paragraph. A watch (or the source it belongs to) answers "the `FileSeen` at this
  path, if one is in flight"; the answer is the same handle the watch emitted, so acking through
  it is idempotent with acking the emitted one, and a path not in flight answers null. Say why: a
  consumer that resumes work from its own durable record has a path and nothing else, and must not
  keep a second ledger to get back to the file.

- [x] `InFlightSet` answers the slot at a path under its own lock, in one call
- [x] `SftpSource` exposes the lookup on the handle a `watch` gives its collector (or on the source
      with the directory), returning the exact `FileSeen` instance handed over, so `download`, `ack`
      and `nack` on it behave as on the original; decide the placement in the ticket's report with
      the codebase-design vocabulary and record it
- [x] Test: a file handed over on tick 1 is answered by path on tick 2; after ack it answers null;
      a path never listed answers null
- [x] Test: acking through the looked-up handle and then through the emitted one is one ack and one
      "already settled" WARN, not two actions
- [x] Test: after the watch ends (T19's give-back) the lookup answers null and a late ack through a
      previously looked-up handle is "already settled"
- [x] `InFlightSetLincheckTest` passes unmodified; every earlier test unmodified
- [x] Shuttle untouched (its deletion of the handle table is a shuttle ticket); confirm it compiles
      and its default tier passes against the reinstalled connector
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen` after `git reset --hard misc/ai_gen`; never `git stash`; modify only
`sftpconnector/` and `docs/sftpconnector/`.
