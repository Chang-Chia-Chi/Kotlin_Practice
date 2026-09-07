# T78 - Group-commit WAL, before and after

Measured by `DynaCache/bench/single-node.sh` with `BENCH_PLAN=t78`. Every number is a value
`redis-benchmark --csv` printed; nothing is rounded, averaged or adjusted.

## What is compared

The two parents of one merge, whose trees differ by this ticket alone:

| | commit |
|---|---|
| before | the `misc/ai_gen` head this branch merged (filled in with the pass) |
| after | this branch's merge commit (filled in with the pass) |

Not the ticket's base `ca75415f`, which stopped being the right baseline when the branch merged:
a delta against it would carry another session's versioned store, inbound loop and narrowed
engine seam as well as this change. The parents of the merge isolate this ticket against the code
that ships. Progress entry deviation 0.

**Two samples per side, not one.** T79 measured its read side once, saw a 2 to 15 percent gain
that looked real, sampled again and found the noise band on the same binary was 1 to 32 percent.
The apparent win was nothing. So the noise band is established first and any delta inside it is
reported as a null.

## Predictions

**Written before any pass ran, and committed before the machine was touched, so that a reader can
tell a prediction from a rationalisation.**

1. **The `NEVER` before-and-after will probably come back a null.** It measures the reused batch
   buffer alone, which removes one allocation and one copy per batch from a path whose other end
   is a file write. A saving that small, against a syscall, is the kind the run-to-run band
   swallows. If it is swallowed, that is the result and nothing is landed on the strength of it.
   If it is instead a real gain, the reasoning above is wrong and the report has to say why, which
   is a finding of its own.
2. **`GROUP_COMMIT` will land between 20,000 and 26,000 requests per second, bound by the network
   rather than by the disk.** `EVERY_SECOND`'s 53.30 is arithmetic, not a slow path: fifty clients
   over a one-second fsync interval, p50 1014.783 ms being one interval. The same arithmetic at
   2 ms gives a ceiling of 25,000, but that ceiling sits above the other constraint in the path:
   plain `SET` under `NEVER` measured about 26,500 on 2026-09-06, and that number is the Docker
   round trip, not the log. So at a 2 ms deadline the fsync stops being what binds and the round
   trip takes over, putting `GROUP_COMMIT` near the `NEVER` rate rather than near 25,000 or near
   53. Each way it can fail says something. Far below the band, and the force path costs more than
   the arithmetic allows, which is a finding. At or above the `NEVER` rate, and durability is not
   costing what reply-after-durable implies, so the next thing to check is whether the force
   really covers its waiters. Landing in the band is two independent constraints agreeing, which
   is the strongest form of the result.
3. **The no-data-directory pass states the log's share separately.** A node started with no data
   directory has no log at all, so the gap between it and the `NEVER` node is everything the log
   costs a pipelined write, of which the buffer is one part.

## Environment

To be filled from `environment.txt` and `load.txt` with the passes: every pass records the other
Java process count and the CPU idle it ran under, and a pass taken under contention is marked as
such rather than quietly believed.

## Pass 1: NEVER, plain and pipelined, `-t set,incr,hset,zadd`

To be filled.

## Pass 2: pipelined with no data directory

To be filled.

## Pass 3: durability, `SET` under `EVERY_SECOND` and `GROUP_COMMIT`

To be filled.

## Result

To be filled, against the predictions above.
