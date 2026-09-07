# DynaCache P8 - Cluster measurement under fault (T88)

Companion to `../plan.md`. One ticket to begin with, because its findings are what will decide
whether there are more; every phase so far has grown by roughly a third from what the work itself
turned up, and a first measurement of untested machinery is the likeliest place for that to
happen again.

**Why this phase exists.** Everything measured to date is a single node: T47's baseline and the
three P7 fixes. The distributed half of the system — quorum reads and writes, sloppy quorum,
hinted handoff, read repair, dotted version vectors, Merkle anti-entropy — has only ever run
under in-process tests against a simulated transport. Those tests are good and they pass, but
they prove the logic composes, not that the system behaves when real processes lose sight of each
other while a client is writing. That is the claim the AP design makes and the one thing nobody
has checked.

**What the phase is not.** It is not a performance phase. P7 asked how fast the engine is; this
asks what the cluster does when the network breaks, which is a correctness and availability
question with numbers attached. A finding here is more likely to be "the minority side does
something surprising" than "this is slower than expected".

**Method inherited from P7, and treated as settled rather than re-argued:** count the work rather
than timing it wherever a claim is about protocol behaviour; two samples per side before believing
any delta, with the within-side spread as the measured noise band; ratios rather than absolute
rates, since the machine and the network cancel in a ratio; the load recorded on every pass; a
parked window coordinated with the other orchestrator session, because intermittent load is worse
than heavy constant load; and no agent ever parks waiting on a background job.

---

### T88 - Three-node benchmark across a real network partition

- **Goal:** start three nodes as real processes on real sockets, drive load through
  `redis-benchmark`, sever one from the other two mid-run, write to both sides, heal, and report
  what availability, latency and convergence actually cost.
- **Blocked by:** none.
- **Fixed contracts:** none changed. Measurement only, no production code, spec 3, 4 and 6 and
  C7 to C13 are what is being observed rather than modified.
- **Partition mechanism:** the existing command line already separates a node's listening port
  (`--grpc`) from the address its peers dial (`--peers`), so each node listens privately and
  advertises a small TCP relay in `DynaCache/bench/`. Killing a relay severs a link, restarting it
  heals, and each direction is independent. Not Toxiproxy (do-not-build list), not firewall rules
  (need administrator), and emphatically not a node kill, which is a different fault: a killed
  node loses its socket state and rejoins clean, while a partitioned node keeps running and keeps
  accepting writes, which is the case the whole design exists for.
- **Acceptance:** replication cost with no fault against the single-node numbers; availability and
  latency on both sides during the split, saying plainly whether the minority side serves writes
  and under what rule; convergence after the heal reported as counts (handoffs replayed, Merkle
  rounds, leaves shipped, keys repaired) as well as time; and no acknowledged write lost.
- **Model:** Opus. **Size:** measurement only; the relay and driver script may reach 400 lines.

**The result that would matter most.** If a write acknowledged on one side of the split is absent
after the heal, that is a serious defect and the run stops there and reports it. Everything else
in this ticket is characterisation; that one is a bug hunt with a benchmark attached.
