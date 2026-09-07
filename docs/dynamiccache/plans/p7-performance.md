# DynaCache P7 - Measured performance fixes (T77 to T79)

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
