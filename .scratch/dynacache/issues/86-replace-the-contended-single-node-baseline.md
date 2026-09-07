# 86: Replace the contended single-node baseline

**What to build:** One published baseline for a single DynaCache node against `redis:7`, under load
conditions that are recorded rather than assumed, describing the tree people will actually run. The current
`docs/dynamiccache/benchmarks/2026-09-06-single-node.md` carries a PROVISIONAL banner because
every table in it was taken while another session ran Maven builds; its own variance section
records the same pass moving by a factor of two between runs. Its four anomalies each became a
ticket (T77 list accounting, T78 group commit, T79 fan-out), so the numbers are now stale twice
over: contended, and describing a tree three fixes behind. After this ticket a new dated report
holds the whole suite measured with T77, T78 and T79 in, under load conditions that are recorded
rather than assumed, the old report keeps
its file but gains a line at the top pointing at the new one as its replacement, and the anomaly
sections that the three tickets closed say what closed them and what the number is now.

**Blocked by:** 77, 78, 79 (all three must be merged; this measures the tree with them in)

**Before starting:** ask the user whether they will quit IntelliJ and stop the kind cluster for
about forty minutes. That is the only way to a clean absolute baseline on this machine, and it is
their call, not the ticket's. Either answer is workable; the acceptance list covers both.

**Nature:** measurement (Opus)

**Status:** ready-for-agent (T77, T78 and T79 are all merged; needs the user's answer on clearing the machine before it starts)

- [ ] The full `DynaCache/bench/single-node.sh` run at its release defaults, both targets, every
      pass the 2026-09-06 report covers, so the two are comparable table for table
- [ ] The load conditions handled honestly, which on this machine means NOT waiting for a quiet
      one. Measured 2026-09-07 with both orchestrator sessions fully stopped, the floor is still
      an IntelliJ Maven daemon, a Kotlin compile daemon and a three-container kind Kubernetes
      cluster, with CPU idle sampled between 23 and 52 percent against the gate's floor of 70.
      The gate cannot pass unless the user shuts down their own tooling, which is not this
      ticket's to require. So: ask first whether the user wants to clear it for forty minutes,
      and if they do, take the clean absolute baseline. If they do not, run under the real load
      with the gate relaxed, `QUIET_BUDGET=60` so a dirty machine reports in a minute rather
      than parking for ten, and the Java count and CPU idle recorded on every pass
- [ ] **Under load, the DynaCache-to-Redis ratio is the result and the absolute rates are
      provisional.** That ratio is the report's actual subject and it is robust here for the
      same reason T78's ratios were: both engines are measured minutes apart in one window, so
      the daemons, the cluster and the machine cancel where they would wreck a rate. Say this at
      the top rather than in a footnote, and give each pass's load beside its numbers so a
      reader can see both sides ran under the same conditions
- [ ] Two samples per side wherever a conclusion rests on a difference, with the within-side
      spread reported as the measured noise band; anything inside the band is a null. The 2026-09-06
      report's own factor-of-two variance is the evidence for why one sample is not enough
- [ ] `docs/dynamiccache/benchmarks/<date>-single-node.md`: the tables, the environment, and one
      paragraph per remaining anomaly naming its code path, in the shape of the 2026-09-06 report
- [ ] Each of the four original anomalies gets a line saying whether it is closed, by which
      ticket, and what the command costs now; an anomaly that did NOT improve as its ticket
      predicted is reported as such, with the prediction quoted
- [ ] The 2026-09-06 report gains a pointer at the top to this one and keeps its provisional
      banner; it is not deleted, because its contended numbers are the evidence for why a
      measurement needs a noise band
- [ ] Progress entry written

Ground rules for this ticket: measurement only; no change under `DynaCache/*/src/main`; a hot
spot found here becomes its own ticket rather than a fix in this one; JUnit is not involved;
numbers are recorded as measured, never rounded up, and a pass that had to run contended says so
in its own row rather than in a footnote. **Do not park waiting on a background job:** run every
pass in the foreground with a long timeout, or poll for the CSV the pass writes, and never end a
turn with "waiting for the pass". This run is 35 to 40 minutes on a window another session is
holding open, so a parked agent spends a reserved resource, not just its own time. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p7-performance.md.
