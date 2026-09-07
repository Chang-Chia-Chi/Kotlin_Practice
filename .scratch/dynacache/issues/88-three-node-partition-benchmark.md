# 88: Three-node benchmark across a real network partition

**What to build:** The measurement this project has never taken. Everything measured so far is one
node on one machine: T47's baseline and all three P7 fixes. The whole Dynamo apparatus the spec
exists to teach — quorum reads and writes, sloppy quorum, hinted handoff, read repair, dotted
version vectors, Merkle anti-entropy — has only ever run under in-process tests against a
simulated transport (`InProcessCluster`, `ChaosDriver`). Nothing has partitioned a cluster of real
processes talking gRPC over real sockets while a client was writing to it. After this ticket a
script starts three nodes, drives load through `redis-benchmark`, severs one node from the other
two mid-run, keeps writing to both sides, heals the split, and reports what availability cost,
what the split cost in latency and throughput, and what convergence cost after the heal, with the
protocol work counted and not only timed.

This is a measurement ticket. No production code changes. A defect found here becomes its own
ticket, and finding one is a likely outcome rather than a failure.

**Blocked by:** None (T77, T78 and T79 are merged; this measures the tree as it stands)

**Nature:** measurement, spec 3 (quorum), 4 (membership and failure), 6 (anti-entropy), C7 to C13
(Opus)

**Status:** ready-for-agent

## How to partition three processes without admin rights

The mechanism is the first thing to get right and the existing command line already affords it.
A node's listening port and the port its peers dial are separate: `--grpc=<port>` sets where it
listens, and the address in `--peers` is what everyone dials. So start each node listening on a
private port and advertise a *forwarder* port instead, with a small TCP relay in
`DynaCache/bench/` joining the two. Killing a relay severs that link, restarting it heals, and
each direction can be cut independently, which is what makes a genuine one-node-cut-off partition
possible rather than a node kill.

```
node A: dynacache 6390 16 <dirA> NEVER --node=a --grpc=7001 --peers=a=127.0.0.1:8001,b=...,c=...
relay:  8001 -> 7001
```

Do not reach for Toxiproxy (it is on the plan's do-not-build list), Windows Firewall rules (they
need administrator), or a node kill (a kill is a different fault: the node loses its socket state
and rejoins clean, whereas a partitioned node keeps running and keeps accepting writes, which is
the case the AP design exists for). If the relay approach fails on this machine, stop and report
rather than substituting a kill and calling it a partition.

## What to measure

- [ ] **Replication cost, no fault.** Three nodes at the default `--quorum=3/2/2` against the
      single-node numbers on the same machine and in the same window: `SET`, `GET`, `INCR` plain
      and `-P 16`. This is what quorum replication costs when nothing is wrong, and it is the
      denominator for everything below
- [ ] **Availability during the split.** Sever one node from the other two while load runs.
      Measure, on the majority side, throughput and latency during the split against the same
      pass with no fault; and on the minority side, what a client connected to the isolated node
      gets for reads and for writes. State plainly whether the minority side serves writes, and
      if it does, by what rule (sloppy quorum with hinted handoff, or a `W` that its own view
      still satisfies). This is the central question: what an AP cache actually gives you when
      the network breaks
- [ ] **Conflict and convergence.** Write different values to the same key on both sides during
      the split, heal, and measure how long until both sides agree, and by what path: read repair
      on the next read, anti-entropy in the background, or hinted handoff replay. Report the
      convergence *time* and, more importantly, the *counts*: hinted handoffs replayed, Merkle
      rounds run, leaves shipped, keys repaired. T84 made the tree exchange countable; use it
- [ ] **Nothing acknowledged is lost.** Every write that received `+OK` on either side is present
      or properly superseded after the heal. A lost acknowledged write is a serious defect and
      the run should stop and report it rather than continue to the next pass
- [ ] `docs/dynamiccache/benchmarks/<date>-three-node-partition.md`: the tables, the environment,
      the partition mechanism, and one paragraph per finding naming the code path
- [ ] Progress entry written

## Method rules, learned in P7 and not optional here

- **Count the work, not only the time.** Convergence is a protocol question, so the strongest
  assertions are counts: round trips, leaves shipped, handoffs replayed. Those hold on any
  machine. Timings need a noise band and conditions attached, so take both but let the counts
  carry the conclusions
- **Two samples per side before believing any delta**, with the within-side spread reported as
  the measured noise band. Anything inside the band is a null and is reported as a null
- **Record the load per pass**, both the foreign Java count and CPU idle. The gate's "quiet" label
  reads NO on this machine because it allows zero foreign JVMs and a resident IDE daemon cannot
  exit; say so rather than implying contention. With the other orchestrator session's builds
  parked the real floor is 78 to 96 percent idle
- **Intermittent load is worse than heavy constant load**, because it lands across one pass and
  not another and a before/after design cannot tell it from the effect. Coordinate a parked
  window for the whole run
- **Set `QUIET_BUDGET=60`** so a dirty machine reports within a minute instead of parking for ten
- **Never park waiting on a background job.** Run every pass in the foreground with a long
  timeout, or poll for the file it writes. Background completion notices are not delivered here,
  and a parked agent inside a reserved window spends someone else's resource as well as its own

Ground rules for this ticket: measurement only; no change under `DynaCache/*/src/main`; the relay
and the driver script live in `DynaCache/bench/`; if a run needs a production change to be fair or
possible, stop, record it as a finding, and report; JUnit is not involved; numbers are recorded as
measured, never rounded up; the script must be re-runnable by a human. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p8-cluster-measurement.md.
