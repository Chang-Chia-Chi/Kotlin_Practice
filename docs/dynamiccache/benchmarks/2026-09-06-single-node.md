# T47 - Single-node benchmark, DynaCache against redis:7

> **PROVISIONAL - EVERY TABLE BELOW WAS TAKEN UNDER CONTENTION.** Another session was running
> Maven builds and test suites on this machine throughout. That was not known when these passes
> ran, and the load was not recorded per pass. Sampled afterwards, the machine was carrying
> three other Java processes at 25 percent CPU idle. It is the most likely explanation for the
> factor-of-two run-to-run spread described at the end. The script now gates every pass on a
> quiet machine and records what it saw, and these tables are to be replaced by a run taken in
> a quiet window. Read the ratios and the four anomalies, which held across all three runs; do
> not quote a single absolute number from here.

Measured 2026-09-06 by `DynaCache/bench/single-node.sh`. Every number below is a value
`redis-benchmark --csv` printed; nothing is rounded, averaged across runs or adjusted.

## Environment

| | |
|---|---|
| CPU | AMD Ryzen 7 7840HS, 8 cores / 16 threads |
| RAM | 15.3 GB |
| OS | Microsoft Windows 11 Home 10.0.26200 |
| JVM | OpenJDK 22.0.1, default heap and GC |
| Docker | Docker Desktop 4.89.0, engine 29.7.2 |
| Benchmark client | `redis-benchmark` from `redis:7` |
| `redis:7` digest | `sha256:71da9275c5f3fcb97d0fa0c8c5b36cc995327265420f17a04bfd544f458059f7` |
| DynaCache commit | `8a7214e231cc7356d0ab2f5da7ac3c8eb39b0c98` |
| Run started | 2026-09-06T10:46:30Z |

DynaCache node arguments:

```
dynacache 6390 16 %TEMP%\dynacache-bench\data NEVER
```

That is single-node mode: 16 partitions, a throwaway data directory, no `--peers`, no CP group.
The redis:7 container is `docker run -d --name dynacache-bench-redis -p 6391:6379 redis:7`,
its own default configuration.

Both servers are reached from inside a `redis:7` container, DynaCache through
`host.docker.internal` and Redis through its published port, so each measurement crosses the
Docker network boundary exactly once. The plain and 1 KB passes are dominated by that
round trip: Redis answers every command in those two passes at 19 to 24 thousand requests per
second regardless of which command it is, which is the round trip's ceiling and not Redis's.
The pipelined pass is where the two engines are actually compared.

### Why the node runs with fsync NEVER

The ticket asks for `EVERY_SECOND`. DynaCache answers a write only once its write-ahead log
entry is durable (C14), so under `EVERY_SECOND` every write waits for the next second's fsync.
Measured, that is 53.30 requests per second with a p50 of 1014.783 ms, so a 100,000-request SET
pass would take about half an hour and every write test together would take most of a day. The
redis:7 container runs with no append-only file and never makes a reply wait for the disk, so
`NEVER` is the setting that compares like with like. `EVERY_SECOND`'s cost is measured on its
own below and is the first finding.

DynaCache still writes a WAL record for every mutating command under `NEVER`; it just does not
force it. Redis in its default configuration writes nothing per command. That asymmetry is
against DynaCache and is not corrected for.

## Skipped tests

| Test | Why |
|---|---|
| `SADD` | DynaCache has no Set type; `CommandParser` has no `sadd` |
| `SPOP` | same |
| `ZPOPMIN` | DynaCache has `ZADD`, `ZREM`, `ZRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZSCORE`, `ZCARD`, `ZINCRBY`, `ZSCAN`, but no `zpopmin` |
| `XADD` | DynaCache has no Stream type; no `xadd` |
| `LRANGE_300`, `LRANGE_500`, `LRANGE_600` | Supported by DynaCache. Left out by the ticket's test list, not by a missing command |

Everything else `redis-benchmark` offers is measured. `LPUSH (needed to benchmark LRANGE)` is
the fill pass `redis-benchmark` runs before `LRANGE_100`; it is a real measurement and is kept.

Every DynaCache pass printed `WARNING: Could not fetch server CONFIG` on stderr, and no Redis
pass did. `redis-benchmark` asks for `CONFIG GET` once at startup to report the server's
persistence settings; DynaCache's parser has no `config`, so it answers an error and the client
carries on. It changes no measurement. Nothing else appeared on stderr in any pass, and neither
node log contains an exception: no crash, no hang and no error reply under load.

## Pass 1: plain

`-c 50 -n 100000 -d 3`

| Test | DynaCache rps | DynaCache p50 ms | redis:7 rps | redis:7 p50 ms | DynaCache as % of Redis |
|---|---|---|---|---|---|
| PING_INLINE | 25176.23 | 1.679 | 23702.30 | 1.911 | 106% |
| PING_MBULK | 26730.82 | 1.655 | 24461.84 | 1.863 | 109% |
| SET | 26483.05 | 1.647 | 23702.30 | 1.919 | 112% |
| GET | 27654.87 | 1.583 | 24189.65 | 1.887 | 114% |
| INCR | 27225.70 | 1.607 | 23917.72 | 1.895 | 114% |
| LPUSH | 2842.93 | 17.199 | 23551.58 | 1.935 | 12% |
| RPUSH | 1072.78 | 43.743 | 22841.48 | 1.975 | 5% |
| LPOP | 1284.11 | 33.343 | 22573.37 | 1.991 | 6% |
| RPOP | 3618.08 | 14.831 | 23832.22 | 1.911 | 15% |
| HSET | 27487.63 | 1.599 | 21963.54 | 2.015 | 125% |
| ZADD | 25367.83 | 1.647 | 22031.29 | 2.015 | 115% |
| LPUSH (LRANGE fill) | 4785.83 | 7.967 | 19175.46 | 2.263 | 25% |
| LRANGE_100 | 3998.56 | 12.215 | 19241.87 | 2.143 | 21% |
| MSET (10 keys) | 13877.33 | 3.423 | 20559.21 | 2.111 | 67% |

## Pass 2: pipelined

`-c 50 -n 100000 -d 3 -P 16`

| Test | DynaCache rps | DynaCache p50 ms | redis:7 rps | redis:7 p50 ms | DynaCache as % of Redis |
|---|---|---|---|---|---|
| PING_INLINE | 295858.00 | 2.031 | 328947.38 | 2.127 | 90% |
| PING_MBULK | 377358.50 | 1.655 | 313479.62 | 2.175 | 120% |
| SET | 145348.83 | 5.207 | 313479.62 | 2.287 | 46% |
| GET | 389105.06 | 1.607 | 320512.81 | 2.239 | 121% |
| INCR | 139275.77 | 5.479 | 294117.66 | 2.383 | 47% |
| LPUSH | 2046.20 | 376.831 | 298507.47 | 2.287 | 0.7% |
| RPUSH | 1351.39 | 552.959 | 362318.84 | 1.983 | 0.4% |
| LPOP | 2013.00 | 285.183 | 336700.34 | 2.103 | 0.6% |
| RPOP | 3467.89 | 220.543 | 320512.81 | 2.255 | 1.1% |
| HSET | 130890.05 | 5.767 | 320512.81 | 2.207 | 41% |
| ZADD | 124069.48 | 6.127 | 342465.75 | 2.071 | 36% |
| LPUSH (LRANGE fill) | 1891.75 | 371.199 | 316455.69 | 2.207 | 0.6% |
| LRANGE_100 | 2046.20 | 365.567 | 70472.16 | 3.903 | 2.9% |
| MSET (10 keys) | 17540.78 | 43.999 | 176991.16 | 4.231 | 10% |

Pipelining gain, each engine against its own plain pass:

| Test | DynaCache | redis:7 |
|---|---|---|
| PING_INLINE | 11.8x | 13.9x |
| PING_MBULK | 14.1x | 12.8x |
| SET | 5.5x | 13.2x |
| GET | 14.1x | 13.2x |
| INCR | 5.1x | 12.3x |
| LPUSH | 0.7x | 12.7x |
| RPUSH | 1.3x | 15.9x |
| LPOP | 1.6x | 14.9x |
| RPOP | 1.0x | 13.4x |
| HSET | 4.8x | 14.6x |
| ZADD | 4.9x | 15.5x |
| LRANGE_100 | 0.5x | 3.7x |
| MSET (10 keys) | 1.3x | 8.6x |

The list rows of this table mix two effects and should not be read as a pipelining result on
their own: `mylist` is already about 200,000 elements long when the pipelined pass starts,
because the node is not restarted between passes. Redis is treated identically, which is what
makes the side-by-side columns fair, but DynaCache's own plain-to-pipelined ratio for a list
command compares two different list lengths.

## Pass 3: 1 KB values

`-c 50 -n 100000 -d 1024`

| Test | DynaCache rps | DynaCache p50 ms | redis:7 rps | redis:7 p50 ms | DynaCache as % of Redis |
|---|---|---|---|---|---|
| PING_INLINE | 22163.12 | 1.847 | 22114.11 | 2.023 | 100% |
| PING_MBULK | 20429.01 | 1.935 | 20733.98 | 2.127 | 99% |
| SET | 20933.64 | 1.879 | 20601.56 | 2.127 | 102% |
| GET | 21824.53 | 1.823 | 22182.79 | 2.015 | 98% |
| INCR | 21491.51 | 1.855 | 23299.16 | 1.935 | 92% |
| LPUSH | 901.05 | 52.703 | 22629.55 | 1.975 | 4% |
| RPUSH | 456.37 | 108.479 | 22306.49 | 1.999 | 2% |
| LPOP | 507.04 | 94.783 | 22376.37 | 1.991 | 2% |
| RPOP | 1225.04 | 41.471 | 22441.65 | 1.983 | 5% |
| HSET | 24078.98 | 1.695 | 22851.92 | 1.959 | 105% |
| ZADD | 23320.89 | 1.783 | 21781.75 | 2.031 | 107% |
| LPUSH (LRANGE fill) | 1106.77 | 39.487 | 20855.06 | 2.103 | 5% |
| LRANGE_100 | 514.55 | 89.471 | 673.27 | 16.831 | 76% |
| MSET (10 keys) | 8664.76 | 5.559 | 8301.51 | 5.479 | 104% |

`LRANGE_100` and `MSET` collapse on both engines here: each reply or request carries about
100 KB and 10 KB of payload, and both engines end up bandwidth-bound at the same place.

## Pass 4: spread keyspace

`-c 50 -n 100000 -d 3 -r 100000`, tests `set,get,incr,mset`.

| Test | DynaCache rps | DynaCache p50 ms | redis:7 rps | redis:7 p50 ms | DynaCache as % of Redis |
|---|---|---|---|---|---|
| SET | 26673.78 | 1.647 | 21992.52 | 2.023 | 121% |
| GET | 27886.22 | 1.607 | 21630.97 | 2.055 | 129% |
| INCR | 27480.08 | 1.631 | 18079.91 | 2.383 | 152% |
| MSET (10 keys) | 21584.29 | 1.887 | 18218.26 | 2.359 | 118% |

This pass exists because `redis-benchmark` leaves the string `__rand_int__` in the command
literally unless `-r` is given. Without `-r`, every `SET`, `GET` and `INCR` in passes 1 to 3
names the single key `key:__rand_int__`, and `MSET (10 keys)` names that same key ten times.
Confirmed by running a short benchmark against a `redis:7` container and reading the keyspace
back: four keys exist afterwards, `myhash` holds one field and `myzset` one member. So passes 1
to 3 never cross a partition boundary and never exercise multi-key fan-out. `-r 100000` is what
puts an `MSET`'s ten keys on up to ten partitions.

## Durability: fsync EVERY_SECOND

`-c 50 -n 500 -d 3 -t set` against a node started with `EVERY_SECOND`:

| Test | rps | avg ms | p50 ms | max ms |
|---|---|---|---|---|
| SET | 53.30 | 937.833 | 1014.783 | 1029.119 |

## List-length confirmation

Four identical `-c 50 -n 20000 -d 3 -t lpush` passes against one fresh node, all pushing onto
the same `mylist`:

| Round | mylist reaches | rps | p50 ms |
|---|---|---|---|
| 1 | 20,000 | 12650.22 | 3.615 |
| 2 | 40,000 | 7122.51 | 6.759 |
| 3 | 60,000 | 3855.79 | 12.895 |
| 4 | 80,000 | 4901.96 | 9.935 |

---

# Anomalies

## 1. Every write waits a full second under EVERY_SECOND

`SET` measured 53.30 requests per second with a p50 of 1014.783 ms under `EVERY_SECOND`, against
26483.05 under `NEVER` on the same node arguments otherwise. 53 is 50 clients divided by one
second, and the p50 is one fsync interval. The number is not a slowdown of the write path; it is
the write path waiting.

The path is `Partition.execute`, which hands every mutating command to the log hook and adds the
returned future to the task's `durable`, and `Partition.task`, which completes the command's
future only after `durable` does. Under `EVERY_SECOND`, `WalWriter.writeBatch` in
`dynacache-engine/.../persist/Wal.kt` puts the waiter on `awaitingFsync` instead of completing
it, and `forceAwaiting` releases the whole set once per tick. So a connection's next write cannot
start until the previous one's second has elapsed, and throughput is exactly clients per fsync
interval no matter how fast the engine is.

Redis's own `appendfsync everysec` does not do this: it acknowledges the write immediately and
fsyncs in the background, trading a one-second window of acknowledged-but-unsynced writes for
throughput. DynaCache's C14 is the stricter contract and the measured cost of that strictness is
three orders of magnitude. A follow-up ticket would measure a group-commit variant: keep
reply-after-durable, but force on a short deadline (one to five milliseconds) or as soon as a
batch is ready, rather than on a one-second tick. The question that ticket answers is what fsync
deadline buys back most of `NEVER`'s throughput on this disk.

## 2. List commands fall to 0.4 to 15 percent of Redis, and pipelining does not help them

The list tests are the only ones below 20 percent of Redis in any pass. In the plain pass
`RPUSH` reached 1072.78 against Redis's 22841.48, which is 5 percent; pipelined, `RPUSH` reached
1351.39 against 362318.84, which is 0.4 percent, and its p50 was 552.959 ms.

The cause is `Partition.account`, called from `Partition.execute` after every keyed command,
which calls `Value.approximateBytes()`. For `Value.List` that is `items.sumOf { it.size + 16 }`
over the whole `ArrayDeque`. The push and pop themselves are O(1) at both ends of an
`ArrayDeque`, so the only length-dependent work in the command is the recount, and it runs on
reads as well as writes.

The evidence is the shape of the four list tests inside one pass. `redis-benchmark` runs them
back to back on one key: `LPUSH` grows `mylist` from 0 to 100,000, `RPUSH` from 100,000 to
200,000, `LPOP` drains it from 200,000 to 100,000 and `RPOP` from 100,000 to 0. The plain-pass
rates were 2842.93, 1072.78, 1284.11 and 3618.08. That is a U shape that tracks the average
length of the key during each test, and it separates length from the command: `LPUSH` and `RPOP`
are the two tests that run on a short list and they are the two fast ones, even though one is a
write and the other a read-and-write at opposite ends. The 1 KB pass is the same shape one level
lower, with `RPUSH` at 456.37, because a longer element list is a longer walk.

The dedicated confirmation passes point the same way but do not close the case on their own:
12650.22, 7122.51, 3855.79, then 4901.96. The first three fall as the key grows; the fourth rose
in this run and rose in a second run of the same script, and one run each is not enough to say
why. A follow-up ticket would repeat those four rounds with more rounds and a warm JVM.

The upgrade path is the one the `ponytail:` comment on `approximateBytes` already names: thread
per-element size deltas through the mutation sites so the entry's charge is maintained
incrementally and `account` becomes O(1). The follow-up ticket would measure `RPUSH` on a
200,000-element list before and after that change; the prediction from these numbers is that it
moves from 1072.78 to somewhere near `HSET`'s 27487.63, since a hash of one field is what the
same code path costs when the aggregate is small.

## 3. Writes pipeline at about a third of Redis's gain; reads pipeline like Redis

`GET` gained 14.1x from `-P 16` and Redis's `GET` gained 13.2x, so DynaCache's read path takes
pipelining as well as Redis's does and ends the pipelined pass ahead of it, 389105.06 against
320512.81. Every write is different: `SET` gained 5.5x, `INCR` 5.1x, `HSET` 4.8x and `ZADD` 4.9x,
where Redis gained 13.2x, 12.3x, 14.6x and 15.5x. The gap is only on commands that mutate.

The difference between the two paths is the write-ahead log. `Partition.execute` calls the log
hook for every mutating command and the reply waits on `durable`; a read never enters that code.
Under `NEVER`, `WalWriter.writeBatch` still allocates a `ByteBuffer` the size of the batch,
copies each record into it and calls `sink.write` before completing the waiters, so each pipelined
write carries a serialization and a file write that the equivalent read does not, and the
redis:7 container it is measured against has no append-only file at all. Pipelining removes the
network round trip from both engines and leaves whatever per-command work remains, which is why
the gap only appears once the round trip is gone.

A follow-up ticket would measure the same pipelined pass against a node started with no data
directory, which takes the log out entirely, and against one with a data directory. The
difference between those two numbers is the log's share, and the rest is DynaCache's own
per-command cost: the executor hop into the partition and the callback back onto the Netty event
loop, which is two thread handoffs per command where Redis has none.

## 4. MSET's fan-out is not the ceiling it was expected to be

The plan named `ApEngine.fanOut`'s sequential per-partition chaining as a ceiling to measure.
It is real code: `fanOut` groups the command's keys by partition and chains one
`thenCompose` per group, so ten keys on ten partitions are ten executor tasks one after
another rather than ten in parallel. The measurement does not show it as the dominant cost.

With the keyspace spread (`-r 100000`), where an `MSET`'s ten keys genuinely land on up to ten
partitions, `MSET` reached 21584.29 against `SET`'s 26673.78 on the same pass, so ten keys
across partitions cost about 19 percent more per command than one key. Without `-r`, where all
ten keys are the same key and the whole command runs as a single `submitAll` on one partition,
`MSET` reached only 13877.33 against `SET`'s 26483.05. Spreading the keys made the command
faster, not slower: with fifty clients all naming one key, one of the sixteen partition threads
does all the work, and that contention costs more than the sequential chain does.

So the sequential chain is worth about 19 percent at ten keys and is not why `MSET` pipelines at
1.3x against Redis's 8.6x; that is anomaly 3's write path plus the chain's ten serialized
executor hops per command, which pipelining cannot overlap because they are chained. A follow-up
ticket would replace the `thenCompose` chain with `allOf` over the groups and re-run pass 4, and
would sweep the key count (`MGET` at 2, 8, 16 and 64 keys with `-r`) so the chain's cost is
measured as a function of how many partitions a command spans rather than at the one point
`redis-benchmark` happens to offer.

## Run-to-run variance

The script was run three times while this ticket was worked. Absolute numbers moved by up to
a factor of two between runs on the plain pass, in both directions and on both engines; the
first run's DynaCache plain `SET` was 14122.30 where this run's was 26483.05. The shape did not
move: the list tests were the slowest in every run, the U shape across the four list tests
appeared in every run, the pipelined write gain stayed near a third of Redis's in every run, and
the `EVERY_SECOND` SET rate was 50.47, 49.51 and 53.30. Treat a single number here as good to
about a factor of two and the ratios as the result.

The cause is known and the omission is mine. Another orchestrator session was running Maven
builds and test suites in the `kp-wt/t48` to `kp-wt/t51` worktrees on this machine during all
three runs. The ticket asked for the number of other Java processes and the CPU idle percentage
to be recorded per pass, and neither was: no pass in these three runs has a load reading behind
it, so no table here can be said to have been taken on a quiet machine. Sampled after the runs,
the machine was carrying three other Java processes at 25 percent CPU idle. The follow-up is a
rerun in a quiet window, which the script now supports: it waits for ten seconds in a row with
no `java.exe` but its own node and at least 70 percent CPU idle before each pass, and writes the
other-Java count and CPU idle it saw for every pass to `load.txt`.

## Reproducing

```bash
export JAVA_HOME=/c/Users/maxch/.jdks/openjdk-22.0.1
bash DynaCache/bench/single-node.sh
```

It builds if the jars are missing, starts and stops the node and the container itself, writes
every CSV to `%TEMP%\dynacache-bench\`, and exits non-zero if any pass fails. It takes about
35 minutes. Nothing but Docker images is installed.
