# T78 - Group-commit WAL, before and after

Measured by `DynaCache/bench/single-node.sh` with `SECTIONS=t78`. Every number is a value
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
2. **Two ratios, both from this session, both against `NEVER` measured beside them:
   `GROUP_COMMIT` over `NEVER` close to 1.0 and inside the measured noise band, and
   `EVERY_SECOND` over `NEVER` near 0.002.** Ratios rather than rates, because the Docker round
   trip, the client, the machine and the day cancel in a ratio and dominate a rate. Whatever the
   network costs, it costs both sides of the same ratio equally, so a ratio near one says the
   fsync has stopped binding, which is this ticket's actual mechanical claim; no absolute number
   can say that, because it cannot separate the disk from the wire. The arithmetic behind the
   second: `EVERY_SECOND` is fifty clients over a one-second interval, which is the 53.30 and the
   p50 of 1014.783 ms already measured, against a `NEVER` rate of tens of thousands. The only
   difference between the two ratios is the deadline, 1000 ms against 2 ms. Each way the first
   can fail says something. Well below one, and the force path costs more than the arithmetic
   allows, which is a finding. At or above one, and durability is not costing what
   reply-after-durable implies, so the next thing to check is whether the force really covers its
   waiters. No external number is used, and the 26,500 of 2026-09-06 anchors nothing: it was
   taken under contention and is provisional.
3. **At one client the same ratio will be far below one, and that pass is what tests the
   deadline.** At fifty clients the two ceilings are a few percent apart, 25,000 from the 2 ms
   arithmetic and about 26,000 from the round trip, so a ratio near one cannot separate "the
   deadline has stopped binding" from "it binds at almost exactly the network rate by
   coincidence". Prediction 2 alone cannot fail informatively. A second deadline value would not
   separate them either: at fifty clients batches form continuously and the writer is free
   constantly, so the force fires on the batch and the deadline is nearly irrelevant by
   construction. Sweeping it there measures the wrong thing and returns a null meaning "wrong
   experiment". The deadline exists for the lonely writer, where nothing else can fire the force,
   so the concurrency changes instead: `-c 1`, where the arithmetic is one client per 2 ms, about
   500 per second, against `NEVER` at one client bound by the round trip at several thousand. The
   pair is the characterisation. Near one at fifty clients, the deadline does not bind; far below
   one at one client, it does. The single-client pass is there for that, not a stray
   low-concurrency data point.
4. **The no-data-directory pass states the log's share separately.** A node started with no data
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

## Pass 3: durability, `SET` under `NEVER`, `EVERY_SECOND` and `GROUP_COMMIT`, at 50 and 1 client

Three policies, one node shape, one window. `NEVER` and `GROUP_COMMIT` run at the same request
count, which is what makes their ratio the clean one.

`EVERY_SECOND` runs at 500 where the other two run at 20,000, so its ratio is computed across
different counts. That is legitimate because its rate is not a measurement of the engine but
arithmetic: fifty clients divided by a one-second interval, with a p50 of one interval. A
quantity fixed by a clock does not sharpen with more samples the way a noisy one does, and 500
requests already spans nine or ten intervals; 20,000 would buy the same number after six minutes.

Asserting that is not showing it, so it is checked rather than assumed: `EVERY_SECOND` runs twice,
at 500 and 1500, about nine and twenty-eight seconds. Two rates that agree demonstrate the
interval bound and the mismatched ratio stands. Two that disagree mean the rate is not purely
interval-bound, which is worth more than the six minutes, and then the matched pass is worth
running.

To be filled.

## Result

To be filled, against the predictions above.
