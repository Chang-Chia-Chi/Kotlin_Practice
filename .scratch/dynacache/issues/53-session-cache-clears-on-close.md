# 53: A connection's session cache clears on CLOSE

**What to build:** A client that closes its CP session and creates another on the same
connection gets a fresh session, and every later CP verb on that connection uses it. Today
the connection handler memoises the first session it created and keeps handing it out after
`CP.SESSION.CLOSE`, so `CREATE` returns the closed id and every following verb answers
`-NOSESSION` until the client reconnects. After this ticket the handler forgets its cached
session when that session is closed through it, and also when the CP engine answers
`-NOSESSION` for it (the session lapsed), so the next `CREATE` starts clean.

**Blocked by:** None (can start immediately)

**Nature:** session lifecycle, CP spec 4 (Opus)

**Status:** ready-for-agent

- [ ] `session_create_after_close_returns_a_new_session`: CREATE, CLOSE, CREATE on one
      connection yields two different ids and the second is usable
- [ ] `session_verbs_after_close_use_the_new_session`: a lock taken after the second CREATE
      is held by the new session, and STATE reports it
- [ ] `session_lapse_clears_the_cache`: after the session lapses at a TTL tick, the next verb
      answers `-NOSESSION` once and the next CREATE succeeds
- [ ] Every existing session and P5 acceptance test passes
- [ ] Progress entry appended

A red test for this exists in the review worktree `kp-wt/review` under the server module's
test tree (`BugHuntCpCompatTest`); reuse it if present, otherwise rewrite it from the first
criterion. Ground rules for every ticket: implement only this ticket; 200 to 600 lines
including tests; JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected
Clock; spec-named tests keep their names, constraint tests `C<n>_<description>`, invariant
tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before
green, one slice at a time, `codebase-design` vocabulary for any new interface, and a
`code-review` self-pass before the commit; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec-cp.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
