# 79: Fan-out runs the partition groups concurrently

**What to build:** A ten-key command costs one executor hop of latency, not ten in a row. Today
`CommandEngine.fanOut` chains one `thenCompose` per partition group and `Router` does the same
on its multi-partition path, so the groups run strictly one after another and pipelining cannot
overlap them; `MSET` pipelines at 1.3x against Redis's 8.6x. After this ticket both submit every
group at once and assemble the reply with `allOf` in argument order; the reply bytes are
identical to before, ADR 0002's per-key non-atomicity is unchanged, and no thread or executor is
added (ADR 0001). Not in this ticket: a bulkhead or bounded queue on fan-out. Each partition is
already one single-thread executor, which is the bulkhead between partitions, and `allOf` adds no
thread and no queue; whether the per-partition queue needs a bound is answered by measuring
queue depth and p99 under `-P 16`, and if that pass shows latency growing with in-flight count it
becomes its own ticket. Source: benchmark anomaly 4.

**Blocked by:** None (can start immediately)

**Nature:** command dispatch, ADR 0001, ADR 0002, the T73 engine seam (Opus)

**Status:** done (DynaCache f39f1dd0, measured d91efbc8, merged into misc/ai_gen)

- [x] `fan_out_submits_every_group_before_any_completes`: with partition doubles that hold their
      futures, every group has been submitted before the first one is completed
- [x] `fan_out_reply_preserves_argument_order`: keys spread across partitions come back in the
      order given, for `MGET`, `MSET` and `DEL`
- [x] `fan_out_one_failed_group_fails_the_command_and_settles_the_rest`: the command's future
      completes exceptionally only after every group has settled
- [x] `Router`'s multi-partition path gets the same treatment and its existing tests pass
- [x] Before/after `-r 100000 -t mset,mget` plain and `-P 16`, plus an `MGET` key-count sweep at
      2, 8, 16 and 64 keys, with p99 next to p50, in
      `docs/dynamiccache/benchmarks/<date>-t79-fan-out.md`
- [x] Progress entry written

Ground rules for every ticket: implement only this ticket; JUnit 5 + Mockito only, no AssertJ or
MockK; no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time, `codebase-design` vocabulary for any new
interface, and a `code-review` self-pass before the commit; append a progress entry describing
what was done and every deviation. The spec is docs/dynamiccache/design-spec.md, the plan is
docs/dynamiccache/plan.md and this ticket's entry is docs/dynamiccache/plans/p7-performance.md;
the spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only DynaCache/ and docs/dynamiccache/benchmarks/. The before pass is taken on
the base commit and the after pass on the ticket head with `DynaCache/bench/single-node.sh` and
its quiet gate on; numbers are recorded as measured, never rounded up; a gain smaller than the
run-to-run noise is recorded as such and the code change is not landed.
