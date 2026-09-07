# 26: A poll takes only the names the route asked for

**What to build:** A poll declares the file names it wants and the names it will not take, as two
regular expressions, and both are decided *before the entry costs anything*: one that does not pass
takes no place in `maxFilesPerPoll`, is never stated by a readiness check, never enters the
in-flight set and is never downloaded.

This is the class of file `onReject` cannot reach. A file that never becomes ready is never handed
over, so it is never answered, so no action can take it out of the directory - and it keeps a
listing place for as long as the connector runs. An upstream that stages under a temporary name in
the polled directory produces exactly that file, and so does a dead upload's remains. The failure
`onReject` was written for - the listing budget going on files nothing will ever do anything with,
and files that arrive after them never being reached - holds unchanged for them.

A name filter and a readiness check answer different questions, and the spec should say so: a
readiness check asks whether a file is *finished*, a filter asks whether it is this route's file at
all. Keeping `.tmp` out with `MarkerFile` works but leaves it permanently `NotReady`, which is the
budget problem wearing a different hat.

**Blocked by:** None (can start immediately)

**Model:** Opus 5 - well-specified, single-threaded, one decision inside the walk

**Status:** ready-for-agent

**Spec changes this ticket applies first:**

- 7.4: a paragraph for the two patterns. Include first, then exclude; both unset means every entry
  is a candidate, which is today's behaviour. **Full-string match**, not a partial one, with the
  reason: an exclude matched partially makes `.` eat the whole directory. **Include applies to
  files only; exclude applies to files and to directories**, with the reason: a `recursive` walk
  under an include of `data_.*\.csv` would otherwise descend into nothing, because no directory
  name is a csv - and exclude reaching directories is what keeps the walk out of a staging folder
  inside a watched tree. Say that the decision happens before the listing budget is taken, which is
  the whole point of the feature and the one thing a later refactor must not move.

- 7.5: one sentence saying a name filter is not a readiness check and pointing at 7.4, so a reader
  looking for "how do I keep `.tmp` out" finds the filter rather than reaching for `MarkerFile`.

- [ ] Two patterns on the polling configuration, compiled when the configuration is built: a pattern
      that does not parse is a `ConfigurationError` aggregated with the builder's other faults, not
      a surprise at the first poll
- [ ] Both unset behaves exactly as before this ticket, so an existing configuration is untouched
- [ ] The decision is made in the walk **before the listing budget is taken**, and a test proves it
      rather than asserting it in prose: a directory holding many unwanted entries and one wanted
      file, `maxFilesPerPoll` set below the unwanted count, and the wanted file is still handed over
- [ ] Full-string match, with a test that names the trap: an exclude of `.*\.tmp` turns away
      `data.tmp` and leaves `data.tmp.csv` alone
- [ ] Include applies to files only; a test proves a `recursive` walk still descends with an include
      set that no directory name could match
- [ ] Exclude applies to directories too; a test proves a staging subdirectory inside a watched tree
      is not descended into under `recursive = true`
- [ ] A skipped entry is counted where an operator can see it, so a pattern that turns away
      everything is not a route that stops silently - the failure mode `onReject`'s entry describes
- [ ] The probe-marker exclusion in the walk stays exactly as it is: unconditional, separate, and
      not folded into the new mechanism. It is the connector's own bookkeeping, not configuration
- [ ] Test: an upstream's temporary file in the polled directory is never handed over, is never
      stated by a readiness check, and is not counted in `PollCompleted.notReady`
- [ ] KDoc says the patterns are operator-supplied while the names they are matched against come
      from the server, so a pattern that backtracks catastrophically is the operator's own foot
- [ ] Shuttle untouched - its declaration is shuttle ticket 48; confirm shuttle compiles against the
      reinstalled connector and its default tier passes
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
