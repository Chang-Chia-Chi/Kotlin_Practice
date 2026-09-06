# 16: P1 acceptance

**What to build:** The single-node part of spec 9 as one acceptance class against an
unmodified Jedis (test scope of the server module only): start a node on an ephemeral port,
run `SET foo bar EX 60` and `GET`, the leaderboard `ZADD` and `ZRANGE WITHSCORES`, the counter
`EVAL`, a `SCAN` loop over 1,000 keys, a `MULTI/EXEC`, and a TTL that fires through the real
scheduler (awaited with a deadline, never a fixed sleep); record a `redis-cli` transcript in
the progress log.

**Blocked by:** 04 (List), 05 (Hash table and SCAN), 07 (Sorted Set), 11 (TinyLFU), 15 (Lua)

**Nature:** acceptance (Opus)

**Status:** done (DynaCache ba09459, merged into misc/ai_gen)

- [x] `P1_acceptance_redis_client_unmodified` green
- [x] Every test of tickets 02 to 15 green in the same run
- [x] `redis-cli` transcript in the progress entry
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
