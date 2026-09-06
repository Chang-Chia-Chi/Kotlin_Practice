# 47: Single-node benchmark with redis-benchmark

**What to build:** A repeatable measurement, not a feature: a script starts one DynaCache node
in single-node mode, runs the official `redis-benchmark` against it (from the `redis:7` Docker
image, since no Redis binary is installed on this machine), and writes the numbers to
`docs/dynamiccache/benchmarks/<date>-single-node.md` together with the machine, the JVM, the
node's arguments, and a reading of where the time goes against the two ceilings the code marks
(sequential multi-key fan-out in `ApEngine.fanOut`, the per-command `approximateBytes` recount).
No production code changes in this ticket; a hot spot found here becomes its own ticket.

**Blocked by:** 16 (P1 acceptance)

**Nature:** measurement (Opus)

**Status:** done (8509c654, merged; numbers provisional, quiet rerun is a follow-up)

- [x] `DynaCache/bench/single-node.sh` (Git Bash) starts a node on an ephemeral or fixed port with a data dir under `%TEMP%`, waits for `PING`, runs `redis-benchmark` for the commands DynaCache supports (`PING_INLINE`, `PING_MBULK`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE_100`, `HSET`, `ZADD`, `MSET`) at `-c 50 -n 100000 -d 3`, then again with `-P 16` (pipelined) and once with `-d 1024`, and stops the node
- [x] The same run against a real `redis:7` container on this machine, same flags, so every DynaCache number sits next to a Redis number in the same table
- [x] `docs/dynamiccache/benchmarks/<date>-single-node.md`: the tables, the environment, and one paragraph per anomaly (any command below 20 percent of Redis, any command whose pipelined gain is far below Redis's) naming the code path
- [x] Every unsupported `redis-benchmark` test (`SADD`, `SPOP`, `LPUSH` variants DynaCache lacks) is listed as skipped with the reason, not silently absent
- [x] Progress entry written

Ground rules for this ticket: measurement only; no change under `DynaCache/*/src/main`; if a
run needs a code change to be fair (a missing command, a crash under load), stop, record it as
a finding, and report; JUnit is not involved; nothing is installed on the machine (Docker must
already work, otherwise stop and report); numbers are recorded as measured, never rounded up;
the script must be re-runnable by a human. The spec is docs/dynamiccache/design-spec.md, the
plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p1-data-engine.md (Measurement addendum).
