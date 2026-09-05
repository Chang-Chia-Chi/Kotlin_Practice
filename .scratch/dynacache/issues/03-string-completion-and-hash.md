# 03: String completion and Hash

**What to build:** The String command set of spec 2.1 is complete (`INCR`, `DECR`, `INCRBY`,
`DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET`) and the Hash type exists with all ten commands
(`HGET`, `HSET`, `HDEL`, `HGETALL`, `HMGET`, `HMSET`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`).
The engine fans a multi-key command out to the partitions involved and joins the replies in
argument order, with no atomicity across partitions (ADR 0002); multi-key `DEL` and `EXISTS`
take the same path.

**Blocked by:** 02 (Engine walking skeleton)

**Nature:** command semantics (Opus)

**Status:** ready-for-agent

- [ ] `string_incr_atomic` (11 from "10", error on non-integer, 1 on missing)
- [ ] `hash_field_independence`, `hash_getall_complete`
- [ ] `mget_spans_partitions`: keys on two partitions, one array in argument order with nil for missing
- [ ] `mget_across_partitions_is_not_atomic`: a write parked between two partitions of one `MGET` is visible in the result (pins ADR 0002)
- [ ] Every reply matches the Redis shape for that command
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p1-data-engine.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit code
there and docs in the parent.
