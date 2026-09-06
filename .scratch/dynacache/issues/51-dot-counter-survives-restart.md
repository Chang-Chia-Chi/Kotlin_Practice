# 51: A node's dot counter survives restart

**What to build:** A coordinator that restarts never hands out a dot it handed out before, so
its first write after the restart is new to every replica and an acknowledged write is never
silently dropped. Today the counter starts from nothing on every start; the first
post-restart write to a key reuses its old dot, a replica still holding that dot treats the
new value as a duplicate, keeps its old value, and still acks toward W, so the write lives
on the coordinator alone until read repair reverts it. After this ticket the node persists a
reserved ceiling for its counter before it uses any dot below it (a block at a time, so the
write path pays nothing per write) and starts above the last persisted ceiling on restart.
The cluster module does no file I/O (plan 2.2): the persistence is an adapter in the engine's
persist package or the node's data directory handled by the server's wiring. Persisting the
full version table is ticket 67; this ticket closes the data loss on its own.

**Blocked by:** None (can start immediately)

**Nature:** causal ordering, C2 and I2 (Fable)

**Status:** ready-for-agent

- [ ] `C2_dot_counter_never_reuses_a_dot_across_restart`: hand out dots, restart the counter
      from its persisted state, and every new dot is above every old one, including when the
      process died between the last persisted ceiling and the last dot used
- [ ] `I2_acknowledged_write_survives_coordinator_restart`: write a key twice on node A,
      restart A's replication layer over the same engine state, write a third value on A with
      W acks, quorum-read from B; the third value is returned and survives read repair
- [ ] `dvv_no_counter_reuse` passes and is extended across a restart
- [ ] Every existing replication, hint, read-repair and convergence test passes
- [ ] Progress entry appended

A red test for this exists in the review worktree `kp-wt/review` under the cluster module's
test tree (`BugHuntReplicationTest`); reuse it if present, otherwise rewrite it from the
second criterion. Ground rules for every ticket: implement only this ticket; 200 to 600 lines
including tests; JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected
Clock; spec-named tests keep their names, constraint tests `C<n>_<description>`, invariant
tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before
green, one slice at a time, `codebase-design` vocabulary for any new interface, and a
`code-review` self-pass before the commit; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's
entry is docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only DynaCache/ and,
when a measurement forces it, docs/dynamiccache/.
