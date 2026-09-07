# 78: Group-commit WAL with a short fsync deadline

**What to build:** A durable write costs one fsync per batch, not one second per write and not
a fresh buffer per batch. Today `WalWriter` under `EVERY_SECOND` parks every waiter until the
next one-second tick, so a client's writes complete at exactly clients-per-second (measured 53.30
requests per second, p50 1014 ms), and under every policy `writeBatch` allocates a
`ByteBuffer` sized to the batch and copies every record before any waiter completes, which is
why writes pipeline at a third of Redis's gain. After this ticket the log keeps reply-after-durable
(C14) and gains a `GROUP_COMMIT(deadline)` policy: a batch is written and forced as soon as the
writer is free, and a batch that stays open is forced when its oldest waiter has aged past the
deadline on the injected Clock, whichever comes first; the writer reuses one growable buffer.
`EVERY_SECOND`, `NEVER` and `ALWAYS` keep their behaviour. The change lives in the writer's
fsync and batching policy only: the entry layout, the record format (T33) and `CommandCodec` are
untouched, because T67 changes those in parallel. Source: benchmark anomalies 1 and 3.

**Blocked by:** None (can start immediately)

**Nature:** concurrent durability protocol, C14, spec 2.8 (Fable)

**Status:** done (DynaCache 7924af40, measured be637b07, merged into misc/ai_gen)

- [x] `C14_group_commit_replies_only_after_fsync`: through a sink double, no write's future
      completes before the force that covers it is observed
- [x] `group_commit_forces_at_the_deadline_when_the_batch_stays_open`: one lonely write is forced
      when the injected clock passes the deadline and not before
- [x] `group_commit_forces_once_per_batch_not_per_write`: fifty writes submitted together cause
      one force
- [x] Every existing WAL, recovery, rotate and P4 acceptance test passes unchanged; the buffer
      reuse is covered by the existing batch tests
- [x] Before/after `-t set,incr,hset,zadd` plain and `-P 16` under `NEVER` (buffer reuse alone),
      one pipelined pass with no data directory (the log's share, stated separately), and a
      `DURABILITY_REQUESTS=20000` `SET` pass under `GROUP_COMMIT` at 2 ms next to
      `EVERY_SECOND`'s 53.30, all in `docs/dynamiccache/benchmarks/<date>-t78-group-commit.md`
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
