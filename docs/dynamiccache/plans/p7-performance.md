# DynaCache P7 - Measured performance fixes (T77 to T79, T85, T86)

Companion to `../plan.md`. Source: the single-node benchmark of 2026-09-06
(`../benchmarks/2026-09-06-single-node.md`, T47), whose four anomalies each name a code path.
Three become tickets here; the fourth (writes waiting a full second under `EVERY_SECOND`) is
folded into T78 because the same change answers both. Specs: `../design-spec.md` 2.7, 2.8, 5.4,
5.5; C14; I6, I12. Glossary `DynaCache/CONTEXT.md`; ADRs `DynaCache/docs/adr/`.

**Goal:** remove the three per-command costs the benchmark isolated, without moving any
contract: reply-after-durable (C14) stands, engine-owned executors (ADR 0001) stand, multi-key
commands stay non-atomic (ADR 0002). Each ticket carries its own before-and-after pass from
`DynaCache/bench/single-node.sh`, and a ticket whose measured gain is smaller than the
run-to-run noise records that and lands nothing.

**Rule for this phase:** every ticket lands a measurement in `../benchmarks/` beside its code.
The numbers are taken with the script's quiet gate on (`QUIET_BUDGET` raised if the machine
is busy) and are recorded as measured, never rounded up. The before pass is taken on the
ticket's base commit, the after pass on the ticket's head, same machine, same session.

**Ticket DAG:** none of the three blocks another; they touch disjoint files
(`PartitionStore`/`Value` for T77, `persist/Wal.kt` for T78, `CommandEngine.fanOut` and
`Router` for T79). All three can run in parallel worktrees.

---

### T77 - List accounting is incremental

- **Goal:** anomaly 2. `PartitionStore.charge` calls `Value.approximateBytes()` after every
  keyed command, which for a `List` walks every element; list commands run at 0.4 to 15 percent
  of Redis and pipelining does not help them. After this ticket each aggregate value carries
  its own running byte total, maintained by a delta at every mutation site, and the store's
  recharge is O(1) for every value kind.
- **Blocked by:** none.
- **Fixed contracts:** spec 2.7 (memory accounting), 5.4, 5.5; I6; the `PartitionStore`
  interface T72 introduced (its shape stays; only the cost of `charge` changes).
- **Acceptance:** `I6_used_bytes_equals_sum_of_entries_after_any_sequence` (T72's invariant
  test, unchanged, still green); `list_charge_is_constant_in_list_length` (a 1-element and a
  100,000-element list cost the same number of element visits to recharge, counted through a
  test double, not timed); before/after `-t lpush,rpush,lpop,rpop,lrange` plain and `-P 16`
  passes recorded in `../benchmarks/<date>-t77-list-accounting.md`.
- **Model:** Opus. **Size:** small to medium (200 to 500 lines).

### T78 - Group-commit WAL with a short fsync deadline

- **Goal:** anomalies 1 and 3. Under `EVERY_SECOND` every write waits for the next one-second
  tick (53 requests per second); under `NEVER` every batch allocates a fresh `ByteBuffer`,
  copies each record and writes before completing any waiter, so writes pipeline at a third of
  Redis's gain. After this ticket the log keeps reply-after-durable (C14) and gains a
  `GROUP_COMMIT(deadline)` policy: waiters are forced as soon as the current batch is written
  or when the oldest waiter has aged past the deadline (milliseconds, injected Clock), whichever
  is first; the writer reuses a growable buffer instead of allocating per batch. `EVERY_SECOND`
  stays as it is for anyone who wants it; `NEVER` and `ALWAYS` are untouched.
- **Blocked by:** none.
- **Fixed contracts:** C14 (a reply is sent only after the entry is durable under the policy
  in force); spec 2.8 fsync policies and recovery sequence; the WAL record format (T33) and
  `WalWriter`'s public surface beyond the new policy.
- **Acceptance:** `C14_group_commit_replies_only_after_fsync` (a write's future does not
  complete before the sink's force is observed, through a sink double); `group_commit_forces_at_
  the_deadline_when_the_batch_stays_open` (one lonely write is forced when the injected clock
  passes the deadline, never earlier); `group_commit_forces_once_per_batch_not_per_write`
  (fifty pipelined writes cause one force); every existing WAL, recovery and P4 acceptance test
  green. Before/after: `-t set,incr,hset,zadd` plain and `-P 16` under `NEVER` (buffer reuse
  only), plus a `DURABILITY_REQUESTS=20000` `SET` pass under `GROUP_COMMIT(2ms)` next to the
  53.30 of `EVERY_SECOND`, in `../benchmarks/<date>-t78-group-commit.md`. The report states
  the log's share separately: one pipelined pass with no data directory at all.
- **Model:** Fable (fsync ordering, waiter interleavings, the flushing flag).
- **Size:** medium (300 to 600 lines).

### T79 - Fan-out runs the partition groups concurrently

- **Goal:** anomaly 4. `CommandEngine.fanOut` chains one `thenCompose` per partition group, so
  a ten-key `MSET` is ten executor hops one after another; `Router` does the same for its
  multi-partition path. After this ticket both submit every group at once and assemble the
  reply with `allOf` in key order; the per-group ordering and non-atomicity of ADR 0002 are
  unchanged, and the reply is byte-identical to before.
- **Blocked by:** none.
- **Fixed contracts:** ADR 0001 (no new executor, no new thread); ADR 0002 (multi-key
  non-atomic, per-key results in argument order); the `CommandEngine` seam as narrowed by T73.
- **Acceptance:** `fan_out_submits_every_group_before_any_completes` (with partition doubles
  that hold their futures, all groups have been submitted before the first is completed);
  `fan_out_reply_preserves_argument_order` (keys spread over partitions come back in the
  order given); `fan_out_one_failed_group_fails_the_command_and_settles_the_rest` (the
  command's future completes exceptionally only after every group has settled, so no
  partition task is left running against a reply already sent); before/after `-r 100000 -t
  mset,mget` plain and `-P 16`, and a key-count sweep (`MGET` at 2, 8, 16, 64 keys) in
  `../benchmarks/<date>-t79-fan-out.md`.
- **Not in this ticket:** a bulkhead or bounded queue on fan-out. Each partition is already
  one single-thread executor, which is the bulkhead between partitions; `allOf` adds no thread
  and no new queue. Whether the per-partition executor queue needs a bound (backpressure under
  a pipelined flood) is a separate question answered by measuring queue depth, not by this
  change; if T79's `-P 16` pass shows p99 latency growing with in-flight count, that becomes
  its own ticket.
- **Model:** Opus. **Size:** small (150 to 350 lines).

---

## Addendum: finding from the fix loop (T85)

T78's interleaving review found one pre-existing shutdown edge while adding the group-commit
deadline. It is its own ticket, blocked by T78 because the 2 ms cadence is what makes it likely. Numbered 85
because the other session committed T80 to T84 while this was still a draft.

### T85 - Shutdown drains the log instead of interrupting it

- **Goal:** `DynaCacheServer.close` calls `scheduler.shutdownNow()`, which interrupts the tick
  thread; an interrupt inside `FileChannel.force` closes the channel and throws, so the waiters
  that force covered fail. They fail correctly, since their bytes were not durable; the fault is
  that a clean shutdown should have made them durable. The shutdown hook closes the server before
  the snapshot engine, so the final save's rotate runs after the scheduler is down and meets the
  channel the interrupt closed. Nothing acknowledged is lost, so this is shutdown correctness,
  not a durability hole.
- **Deliverables:** close stops the scheduler, waits a bounded grace for the force in flight,
  forces whatever is still parked, then interrupts; hitting the bound is reported, not silent.
- **Blocked by:** T78.
- **Fixed contracts:** C14; spec 2.8; the shutdown snapshot still saves.
- **Acceptance:** `close_completes_the_force_in_flight_rather_than_interrupting_it`,
  `close_forces_whatever_is_still_parked_before_it_returns`,
  `close_past_its_grace_bound_reports_and_returns`.
- **Model:** Opus. Section 4 routes a ticket of this shape to Fable, but Fable ran out of usage
  credits on this account on 2026-09-07 and its agents die within two minutes of spawning having
  done nothing. Any ticket picking this up records the swap as a deviation. The same applies to
  T78, already relaunched on Opus after losing its first agent this way.
  **Size:** small to medium (200 to 500 lines).


### T86 - Replace the contended single-node baseline

- **Goal:** the published baseline (`../benchmarks/2026-09-06-single-node.md`) is provisional:
  every table was taken while another session built, and its own variance section records the
  same pass moving by a factor of two. It is now stale twice, contended and describing a tree
  three fixes behind. Handed over by the other orchestrator session, which owns neither the
  benchmark area nor the three fixes.
- **Deliverables:** the whole suite rerun on a quiet machine with T77, T78 and T79 in; a new
  dated report; a pointer at the top of the old one; and a line per original anomaly saying
  whether it closed, by which ticket, and what it costs now.
- **Blocked by:** T77, T78, T79. It measures the tree with all three in, which is the number
  worth publishing; folding it into any one of them would measure a tree nobody will run.
- **Fixed contracts:** none; measurement only, no change under `src/main`.
- **Acceptance:** the four original anomalies each answered, an anomaly that did not improve as
  its ticket predicted reported with the prediction quoted, and the load recorded per pass.
- **Window:** the longest of the phase, 35 to 40 minutes, and the one where the disk must be
  quiet too, not only the CPU. The other session clears the machine rather than pausing dispatch.
- **Model:** Opus. **Size:** measurement only.
