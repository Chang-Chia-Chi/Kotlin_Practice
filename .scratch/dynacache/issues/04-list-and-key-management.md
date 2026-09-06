# 04: List and key management, WRONGTYPE

**What to build:** The List type with all nine commands (`LPUSH`, `RPUSH`, `LPOP`, `RPOP`,
`LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LREM`) on a deque with O(1) both ends; the Server commands
`DBSIZE`, `FLUSHDB`, `KEYS` (glob), `RANDOMKEY`, `COMMAND` and `INFO` (both minimal); and the
type check that makes a wrong-type command fail with `-WRONGTYPE` before touching the entry.

**Blocked by:** 02 (Engine walking skeleton)

**Nature:** command semantics and C13 (Opus)

**Status:** done (DynaCache 950a1a1, merged into misc/ai_gen)

- [x] `list_push_pop_order`, `list_lrange_bounds`
- [x] `wrongtype_rejected`, `C13_wrongtype_leaves_value_intact`
- [x] `KEYS` glob matches `*`, `?` and `[...]`; `DBSIZE` and `FLUSHDB` cover every partition
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p1-data-engine.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
