# T77 - List accounting is incremental, before and after

Measured 2026-09-07 by `DynaCache/bench/single-node.sh`. Every number below is a value
`redis-benchmark --csv` printed; nothing is rounded, averaged across runs or adjusted.

The change under test: every aggregate value now carries the running byte total its own
mutations book, so `Value.approximateBytes()` is a read and `PartitionStore`'s recharge after
every keyed command is O(1) for every kind. Before it, a `List` walked every element on that
recharge. This is anomaly 2 of `2026-09-06-single-node.md`.

| | |
|---|---|
| Before | `ca75415f`, the ticket's base |
| After | `bf7212d6`, the ticket's head |
| Flags | `-c 50 -n 100000 -d 3 -t lpush,rpush,lpop,rpop,lrange`, plain and `-P 16` |
| Node | `dynacache 6390 16 %TEMP%\...\data NEVER`, single-node, 16 partitions |
| CPU | AMD Ryzen 7 7840HS, 8 cores / 16 threads |
| JVM | OpenJDK 22.0.1, default heap and GC |
| Docker | engine 29.7.2, `redis:7` at `sha256:71da9275c5f3fcb97d0fa0c8c5b36cc995327265420f17a04bfd544f458059f7` |
| Before run started | 2026-09-07T09:17:47Z |
| After run started | 2026-09-07T11:12:33Z |

Both sides ran on the same machine in the same reserved window, same client, same container,
same flags, one after the other with nothing else running. `-t lrange` selects the LPUSH that
fills `mylist` and then LRANGE at 100, 300, 500 and 600 elements.

## Machine load at each pass

The script gates every pass on a quiet machine and records what it saw:

| Pass | Foreign JVMs | CPU idle | Gate |
|---|---|---|---|
| before, plain | 3 | 95% | NO_TAKEN_UNDER_CONTENTION |
| before, pipelined | 1 | 92% | NO_TAKEN_UNDER_CONTENTION |
| after, plain | 1 | 97% | NO_TAKEN_UNDER_CONTENTION |
| after, pipelined | 1 | 90% | NO_TAKEN_UNDER_CONTENTION |

**The gate label is the gate being strict, not the machine being busy.** `wait_for_quiet`
requires that no `java.exe` but our own node exists at all, and this machine runs a resident
IntelliJ Maven JVM that never exits. Measured while these passes were arranged, that process
used 0.09 CPU seconds in 12 seconds, under one percent of one core. The gate has no way to
say "idle foreign process", so it counts it and reports NO. The CPU-idle half of the gate is
the half that carries information here, and it read 90 to 97 percent on all four passes.

This is not a clean gate and is not claimed as one. It is also not the situation the
`2026-09-06` report is marked provisional for: that run carried three other Java processes at
25 percent CPU idle, actively building. These four passes ran at 90 percent idle or better.

## The anomaly reproduces on the before pass

Before anything else, the before pass reproduces anomaly 2's U shape exactly. `redis-benchmark`
runs the four list tests back to back on one key, so each test meets `mylist` at a known length:
LPUSH grows it 0 to 100,000, RPUSH 100,000 to 200,000, LPOP drains it 200,000 to 100,000, and
RPOP 100,000 to 0.

| Test | `mylist` averages | Before rps |
|---|---|---|
| LPUSH | 50,000 | 3819.56 |
| RPUSH | 150,000 | 1373.21 |
| LPOP | 150,000 | 2745.97 |
| RPOP | 50,000 | 12087.51 |

The two tests that run on the long list are the two slow ones and the two that run on the short
list are the two fast ones, with no overlap between the groups. RPUSH on the longest list is the
slowest at 1373.21 and RPOP on the shortest is the fastest at 12087.51, a spread of 8.80x
across four commands that each do O(1) work at an end of a deque. The anomaly is real and it
reproduces on a quiet machine.

## Pass 1: plain

`-c 50 -n 100000 -d 3`

| Test | Before rps | Before p50 ms | After rps | After p50 ms | After / before |
|---|---|---|---|---|---|
| LPUSH | 3819.56 | 7.047 | 21777.00 | 1.855 | 5.70x |
| RPUSH | 1373.21 | 32.079 | 25239.78 | 1.711 | 18.38x |
| LPOP | 2745.97 | 10.175 | 23849.27 | 1.759 | 8.69x |
| RPOP | 12087.51 | 3.975 | 23397.29 | 1.751 | 1.94x |
| LPUSH (LRANGE fill) | 7370.83 | 6.431 | 23116.04 | 1.759 | 3.14x |
| LRANGE_100 | 4095.17 | 10.079 | 21677.87 | 1.703 | 5.29x |
| LRANGE_300 | 6702.86 | 6.743 | 12835.32 | 2.231 | 1.91x |
| LRANGE_500 | 6449.12 | 7.119 | 9611.69 | 2.871 | 1.49x |
| LRANGE_600 | 6339.14 | 7.247 | 8216.25 | 3.143 | 1.30x |

## Pass 2: pipelined

`-c 50 -n 100000 -d 3 -P 16`

| Test | Before rps | Before p50 ms | After rps | After p50 ms | After / before |
|---|---|---|---|---|---|
| LPUSH | 3305.57 | 228.095 | 143061.52 | 5.119 | 43.28x |
| RPUSH | 1803.56 | 445.183 | 145348.83 | 5.255 | 80.59x |
| LPOP | 2619.24 | 224.127 | 151285.92 | 5.007 | 57.76x |
| RPOP | 5548.16 | 140.287 | 153139.36 | 4.975 | 27.60x |
| LPUSH (LRANGE fill) | 3627.00 | 203.007 | 149253.73 | 5.143 | 41.15x |
| LRANGE_100 | 4017.03 | 194.303 | 75930.14 | 3.343 | 18.90x |
| LRANGE_300 | 3389.37 | 228.607 | 27270.25 | 6.783 | 8.05x |
| LRANGE_500 | 3159.26 | 242.047 | 16181.23 | 12.143 | 5.12x |
| LRANGE_600 | 3050.08 | 245.887 | 13555.65 | 14.183 | 4.44x |

RPUSH's pipelined p50 fell from 445.183 ms to 5.255 ms.

## What the numbers say

**The convergence is the evidence, not the multiplier.** A multiplier only says the code got
faster; it does not say the recount was the reason. What says that is the shape. Before, the
four list tests spread over a factor of 8.80 in the plain pass and 3.08 pipelined, and the
ordering was explained entirely by how long `mylist` was at that moment. After, the same four
sit between 21777.00 and 25239.78 plain, every one within 7.6 percent of their mean, and
between 143061.52 and 153139.36 pipelined, within 3.5 percent of theirs. The ordering has not
merely weakened: it has stopped tracking length altogether. RPUSH and LPOP, the two tests that
run on the 150,000-element list, are now the fastest and the second fastest of the four. A
per-command cost proportional to list length cannot produce that, and its absence is what the
change was for.

**The anomaly is closed, not reduced.** Pipelined, the list commands now sit at 143,062 to
153,139 requests per second. In the `2026-09-06` report the same commands pipelined at 1351.39
to 3467.89, while `SET` in that report's pipelined pass reached 145348.83 and `HSET` 130890.05.
The list commands have moved from a band of their own into the band the string and hash
commands were already in, which is where a command whose accounting is O(1) belongs. There is
no longer a list-shaped hole in the pipelined pass.

**The remaining gap to Redis is anomaly 3, not this one.** Against the pipelined Redis figures
the `2026-09-06` report recorded, RPUSH has moved from 0.5 percent of Redis to 40 percent. The
40 percent that remains is the per-command write-ahead log: DynaCache writes a WAL record for
every mutating command even under `NEVER`, where Redis in its default configuration writes
nothing, and 40 percent is where `SET`, `HSET` and `ZADD` already sat in that report for the
same reason. That is anomaly 3 and it is T78's ticket. Nobody should re-open T77 looking for
it. Those Redis figures were taken under contention and the report carrying them is marked
provisional, so read the three orders of magnitude and not the exact percentage; Redis was not
re-measured today.

**The LRANGE slope survives, and it should.** After the change, LRANGE_600 runs at 38 percent
of LRANGE_100 plain and 18 percent pipelined: six hundred elements cost about five times what a
hundred do in the pipelined pass. That is the command doing what was asked of it. `LRANGE 0 599`
copies six hundred elements into a reply and writes six times as many bytes to the socket as
`LRANGE 0 99` does, so its cost is proportional to the reply, not to the key.

The accounting slope is the one that disappeared, and the before pass shows it by having no
payload slope at all. Before this change every LRANGE also walked the whole 100,000-element key
to recharge it, and that walk swamped the reply completely: LRANGE_300, LRANGE_500 and
LRANGE_600 came in at 6702.86, 6449.12 and 6339.14, within 6 percent of each other while
carrying two hundred, four hundred and five hundred more elements per reply. LRANGE_600 was
even faster than LRANGE_100. After the change the four order themselves by reply size, cleanly,
because reply size is the only thing left that separates them. A future reader should not chase
the remaining slope; it is the payload.

## Reproducing

```bash
export JAVA_HOME=/c/Users/maxch/.jdks/openjdk-22.0.1
TESTS=lpush,rpush,lpop,rpop,lrange PASSES=plain,pipelined SECTIONS=dynacache \
  QUIET_BUDGET=60 BENCH_OUT=$TEMP/dynacache-bench-t77-before \
  bash DynaCache/bench/single-node.sh
```

`TESTS`, `PASSES` and `SECTIONS` are knobs this ticket added to the script. It had no way to
select which tests, which of the four passes, or which of the four sections ran, so a ticket
measuring one code path had to pay for the whole suite against both engines. All three default
to the full suite, so a release measurement is unchanged. The script builds the jars it finds
missing; both sides here were built with `mvn -o clean package -DskipTests` from their own
commit before the run.
