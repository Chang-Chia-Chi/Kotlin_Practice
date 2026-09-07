# T78 - Group-commit WAL, before and after

Measured by `DynaCache/bench/single-node.sh` with `SECTIONS=t78`. Every number is a value
`redis-benchmark --csv` printed; nothing is rounded, averaged or adjusted.

**The ratios are the result. The absolute rates are provisional.** Every pass ran on a machine
that could not pass the quiet gate, and says so in its own row. Both sides were measured minutes
apart in one window under the same recorded load, which is what makes a ratio survive here where
a rate does not.

## What is compared

The two parents of one merge, whose trees differ by this ticket alone:

| | commit |
|---|---|
| before | `d24c4699`, the `misc/ai_gen` head this branch merged |
| after | `1add9a48`, this branch, whose only engine difference from the before tree is this ticket |

Not the ticket's base `ca75415f`, which stopped being the right baseline when the branch merged:
a delta against it would carry another session's versioned store, inbound loop and narrowed
engine seam as well as this change. The parents of the merge isolate this ticket against the code
that ships. Progress entry deviation 0.

**Two samples per side, not one.** T79 measured its read side once, saw a 2 to 15 percent gain
that looked real, sampled again and found the noise band on the same binary was 1 to 32 percent.
The apparent win was nothing. So the passes run A, A, B, B; the within-side spread is the noise
band and is reported separately from the between-side difference; anything inside the band is a
null; and if the two before samples straddle the two after samples the result is a null whatever
the means say.

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

   **The single-client p50 decides between the two ways that ratio can come back near one**, where
   the rate cannot. A writer that genuinely waits for the deadline has a p50 of at least the
   deadline, about 2 ms plus the round trip. A writer whose force fires at once, because the
   post-batch check in `flushIfIdle` finds the previous batch already past its deadline, never
   waits and has the round trip alone, well under a millisecond at one client. Those are far
   apart. So the p50 is recorded beside the rate for both policies and the report says which of
   the two the latency says it is: a rate near the round trip with a p50 near the round trip is
   the deadline binding at zero, a bug in the force path and its own ticket; a rate near the round
   trip with a p50 above 2 ms is stranger still and worth stopping for.
4. **The no-data-directory pass states the log's share separately.** A node started with no data
   directory has no log at all, so the gap between it and the `NEVER` node is everything the log
   costs a pipelined write, of which the buffer is one part.

## Environment

| | |
|---|---|
| CPU | AMD Ryzen 7 7840HS, 8 cores / 16 threads |
| OS | Microsoft Windows 11 Home 10.0.26200 |
| JVM | OpenJDK 22.0.1, default heap and GC |
| Docker | Docker Desktop, engine 29.7.2 |
| Client | `redis-benchmark` from `redis:7`, `sha256:71da9275c5f3fcb97d0fa0c8c5b36cc995327265420f17a04bfd544f458059f7` |
| before run | 2026-09-07T12:57:20Z, commit `d24c4699` |
| after run | 2026-09-07T13:11:08Z, commit `1add9a48` |

**Every pass ran contended and every pass says so.** Load recorded per pass: one other Java
process throughout, an IntelliJ Maven daemon, and CPU idle between 63 and 97 percent, most passes
above 92. Docker Desktop was also running a three-container kind cluster. The gate's floor is 70
percent idle with an allowance of one Java process, so every pass is stamped
`NO_TAKEN_UNDER_CONTENTION`: the machine cannot pass that gate unless the user shuts down their
own tooling, which is not this ticket's to require.

Run with `QUIET_BUDGET` at 45 seconds, not the default 600. The gate waits that long per pass
before giving up and running anyway, so on a busy machine the default costs ten minutes a pass to
reach a number that is stamped contended regardless. A first attempt at this measurement spent
ten minutes in the gate and took no reading at all. A minute catches a machine that is briefly
busy and hands the window back while it is still worth having. Recommended for whoever measures
next.

## Pass 1: NEVER, plain and pipelined, `-t set,incr,hset,zadd`

Plain, requests per second:

| test | before A | before B | after A | after B | before spread | before to after |
|---|---|---|---|---|---|---|
| SET | 19190.17 | 24050.02 | 24201.36 | 23923.45 | 25.3% | +11.3% |
| INCR | 20470.83 | 25753.29 | 25746.65 | 25131.94 | 25.8% | +10.2% |
| HSET | 21992.52 | 27777.78 | 26990.55 | 27225.70 | 26.3% | +8.9% |
| ZADD | 21519.26 | 27647.22 | 27925.16 | 28082.00 | 28.5% | +14.0% |

Pipelined `-P 16`, requests per second:

| test | before A | before B | after A | after B | before spread | before to after |
|---|---|---|---|---|---|---|
| SET | 161550.89 | 149476.83 | 151057.41 | 146627.56 | 8.1% | -4.3% |
| INCR | 165562.92 | 167785.23 | 166944.92 | 160513.64 | 1.3% | -1.8% |
| HSET | 162601.62 | 164473.69 | 164744.64 | 162866.44 | 1.2% | +0.1% |
| ZADD | 154320.98 | 156250.00 | 156250.00 | 153374.23 | 1.2% | -0.5% |

**Null.** The plain pass's own before-side spread is 25 to 28 percent against a before-to-after
difference of 9 to 14 percent, so the difference is inside the band by a factor of two. The
before samples do not straddle the after samples, but only because before A alone is low: it ran
first, on a cold JVM and a cold page cache, immediately after a Maven build. The pipelined pass
settles it. Its band is 1.2 percent, and there the change measures between -4.3 and +0.1 percent,
with the four tests disagreeing on the sign.

This is prediction 1 landing. The reused buffer removes one allocation and one copy per batch
from a path that ends in a file write, and that is not visible against a syscall. Nothing is
landed on the strength of it. The buffer reuse stays because it is strictly less work, not
because it was measured to be faster.

## Pass 2: pipelined with no data directory

| test | before A | before B | after A | after B |
|---|---|---|---|---|
| SET | 152207.00 | 147275.41 | 151285.92 | 158227.84 |
| INCR | 183486.23 | 185873.61 | 177935.95 | 189753.31 |
| HSET | 189753.31 | 206611.58 | 196850.39 | 198019.80 |
| ZADD | 263852.25 | 268817.19 | 289855.06 | 297619.06 |

Null between the trees, as it must be: this node has no log, so nothing in this ticket can reach
it. The pass is here for the other reason.

**The log's share, from the after tree.** `SET` runs at 151286 with no data directory against
151057 with one, which is no difference at all. `ZADD` runs at 289855 against 156250, which is
the log costing 46 percent of the command. So the log's share is not one number: it is per
command, and it is largest for the commands whose in-memory work is smallest.

## Pass 3: durability, `SET` under `NEVER`, `EVERY_SECOND` and `GROUP_COMMIT`, at 50 and 1 client

All from the after tree, one window, minutes apart.

| policy | clients | requests | rps A | rps B | p50 A ms | p50 B ms |
|---|---|---|---|---|---|---|
| `NEVER` | 50 | 20000 | 15037.59 | 14630.58 | 2.487 | 2.735 |
| `GROUP_COMMIT` | 50 | 20000 | 3188.27 | 2931.69 | 15.511 | 17.647 |
| `EVERY_SECOND` | 50 | 500 | 50.17 | 49.43 | 1019.391 | 1013.759 |
| `EVERY_SECOND` | 50 | 1500 | 49.60 | 49.46 | 1017.343 | 1016.319 |
| `NEVER` | 1 | 5000 | 1991.24 | 1984.91 | 0.455 | 0.463 |
| `GROUP_COMMIT` | 1 | 5000 | 63.27 | 63.24 | 15.791 | 15.807 |

Ratios, means of the two samples:

| ratio | measured | predicted |
|---|---|---|
| `GROUP_COMMIT` / `NEVER`, 50 clients | 0.206 | ~1.0, inside the band |
| `EVERY_SECOND` / `NEVER`, 50 clients | 0.00336 | ~0.002 |
| `GROUP_COMMIT` / `NEVER`, 1 client | 0.0318 | far below 1 |
| `GROUP_COMMIT` / `EVERY_SECOND`, 50 clients | 61.3 | - |

`EVERY_SECOND` at 500 and at 1500 requests measured 50.17, 49.43 against 49.60, 49.46, agreeing
within 1.5 percent, which is inside the band. Its rate is bound by the fsync interval and not by
the length of the pass, so computing its ratio across two request counts is sound, as the method
claimed in advance.

## Result

**Prediction 1 landed. The buffer reuse is a null**, reported as the prediction landing rather
than as an absence of a result.

**Prediction 2 failed, in the direction the report had already named as a finding.**
`GROUP_COMMIT` over `NEVER` at fifty clients is 0.206, not near one. The prediction said what
that would mean: "the force path costs more than the arithmetic allows, which is a finding". It
is a finding, and it is not the force path.

**The deadline is not 2 ms. It is one platform timer tick, about 15.9 ms.** Both rates are
exactly clients divided by that tick:

| | measured | clients / 15.86 ms |
|---|---|---|
| 1 client | 63.26 | 63.05 |
| 50 clients | 3059.98 | 3152.6 |

and the single-client p50, 15.791 and 15.807 ms, is that tick plus the round trip. That is
prediction 3's p50 discriminator answering: the writer genuinely waits, so the deadline does
bind. It binds at 15.9 ms rather than at the 2 ms it is configured with.

The cause was measured, not assumed. A `ScheduledExecutorService` on this machine, asked for a
2 ms fixed delay, fires with a median gap of **15.860 ms**: 127 ticks in two seconds, minimum
15.225, maximum 16.712. That is the Windows default timer resolution of 15.625 ms. The log's
deadline is driven by that scheduler, so no deadline shorter than a platform tick can be
delivered through it. The 2 ms is what the policy asks for; 15.9 ms is what the platform grants.

**What the ticket set out to do, it did.** Reply-after-durable holds and a durable write costs
one fsync per batch rather than one second per write: `GROUP_COMMIT` answers 61 times faster than
`EVERY_SECOND` on the same node, 3060 against 49.9, with C14 intact. Anomaly 1 of the 2026-09-06
report is closed. What it does not do is reach `NEVER`, and the 2 ms in the ticket is not
deliverable through a scheduled executor on this platform.

**Follow-up, its own ticket rather than this one.** Making the deadline mean what it says needs
the force off the platform scheduler: a parking thread on `LockSupport.parkNanos`, which has
resolution the scheduler lacks, or forcing from the flush path aggressively enough that a
scheduled tick is only the idle-writer backstop. Both are design changes. Until one lands, the
honest description of `GROUP_COMMIT` is one fsync per platform timer tick, and its 2 ms constant
should say so.
