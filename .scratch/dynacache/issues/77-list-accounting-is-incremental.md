# 77: List accounting is incremental

**What to build:** A list command costs the same whether the list holds one element or a
hundred thousand. Today `PartitionStore` recharges the touched entry after every keyed command
by calling `Value.approximateBytes()`, which for a `List` walks every element (and for a
`Hash` or `ZSet` every field or member), so `RPUSH` on a 200,000-element list measured 0.4
percent of Redis pipelined and the four list tests trace a U shape that follows list length, not
the command. After this ticket every aggregate value carries a running byte total maintained by a
delta at each mutation site, `approximateBytes()` returns that total, and the store's recharge
is O(1) for every value kind. The `PartitionStore` interface from T72 keeps its shape; only the
cost of the charge changes. Source: benchmark anomaly 2, `docs/dynamiccache/benchmarks/2026-09-06-single-node.md`.

**Blocked by:** None (can start immediately)

**Nature:** memory accounting, spec 2.7, 5.4, 5.5, I6 (Opus)

**Status:** done (DynaCache bf7212d6, measured 18d6c291, merged into misc/ai_gen)

- [x] `list_charge_is_constant_in_list_length`: recharging a 1-element and a 100,000-element
      list visits the same number of elements, counted through a test double or a counter, never
      timed; the same for a hash and a sorted set
- [x] `I6_used_bytes_equals_sum_of_entries_after_any_sequence` (T72's invariant test) stays green
      unchanged, and a new seeded sequence that pushes, pops, trims and sets ranges on lists keeps
      the running total equal to a from-scratch recount after every step
- [x] Every existing engine, eviction, expiry and P1 acceptance test passes unchanged
- [x] Before/after `-t lpush,rpush,lpop,rpop,lrange` plain and `-P 16` passes recorded in
      `docs/dynamiccache/benchmarks/<date>-t77-list-accounting.md`, with the machine load the
      script recorded per pass
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
