# 27: Say what a poll's listing now costs

**What to build:** Documentation only, no behaviour change. Ticket 26 moved a ceiling and did not
write it down.

`maxFilesPerPoll` used to bound two different things at once: how many entries a listing *examines*
and how many it *keeps*. The name filter runs inside the listing's own per-entry filter, ahead of
the count, so a turned-away entry costs no budget place - which is the whole point - and the first
of those two bounds is gone. A polled directory holding two hundred thousand excluded names is now
walked in full on every tick, and the listing runs on the `transferTimeout` clock rather than the
round-trip one, so the tick's cost is the size of the directory rather than the size of the budget.

This is inherent to the feature and not a defect: the files a route wants cannot be found without
looking past the ones it does not. It is also strictly better than what it replaces - a bounded
scan of junk beats a budget permanently starved by junk. But the failure mode moved rather than
disappeared, from "the wanted file is never reached" to "the listing times out", and an operator
sizing `transferTimeout` now sizes it against the directory. Nothing in the spec says so, and the
spec is where this project keeps its ceilings.

No new code is needed to watch it. `sftp_poll_files{state=filtered}` counts what the filter turned
away and `{state=seen}` counts what reached the walk, and they are disjoint, so a tick examines
`seen + filtered` entries and the ratio between them is the pressure on this ceiling.

**Blocked by:** None - ticket 26 is merged at `0697506a`

**Model:** Opus 5 - documentation, one paragraph and one row

**Status:** ready-for-agent

- [ ] Spec 7.4 gains the ceiling in the section that already describes the filter: what
      `maxFilesPerPoll` bounds after this change and what it no longer bounds, that the listing is
      on the `transferTimeout` clock, and that `filtered` and `seen` are disjoint so their sum is
      what a tick examined
- [ ] A row in the open-seams table in `docs/sftpconnector/progress.md`, owned by whoever first
      polls a directory whose excluded names outnumber the budget by orders of magnitude, saying
      what they would reach for: not a cap on entries examined, which is the starvation ticket 26
      removed, but either an upstream that stages outside the polled directory (the standing
      recommendation, spec 14 and the two-phase upload row) or a listing that asks the server to do
      the matching, which is a protocol question and not a knob
- [ ] No behaviour change, no new test, and the 313 tests still pass unchanged
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen` after `git reset --hard misc/ai_gen`; never `git stash`; modify only
`sftpconnector/` and `docs/sftpconnector/`.
