# 86: Replace the contended single-node baseline

**What to build:** One published baseline for a single DynaCache node against `redis:7`, taken on
a quiet machine, describing the tree that people will actually run. The current
`docs/dynamiccache/benchmarks/2026-09-06-single-node.md` carries a PROVISIONAL banner because
every table in it was taken while another session ran Maven builds; its own variance section
records the same pass moving by a factor of two between runs. Its four anomalies each became a
ticket (T77 list accounting, T78 group commit, T79 fan-out), so the numbers are now stale twice
over: contended, and describing a tree three fixes behind. After this ticket a new dated report
holds the whole suite measured on a quiet machine with T77, T78 and T79 in, the old report keeps
its file but gains a line at the top pointing at the new one as its replacement, and the anomaly
sections that the three tickets closed say what closed them and what the number is now.

**Blocked by:** 77, 78, 79 (all three must be merged; this measures the tree with them in)

**Nature:** measurement (Opus)

**Status:** blocked

- [ ] The full `DynaCache/bench/single-node.sh` run at its release defaults, both targets, every
      pass the 2026-09-06 report covers, so the two are comparable table for table
- [ ] A genuinely quiet machine: the other orchestrator session clears the machine rather than
      only pausing dispatch, the disk is quiet as well as the CPU (no worktree creation, no
      Maven), and the load the gate saw is recorded per pass. If the resident IDE JVM still
      makes the gate read `NO`, say so and give the CPU idle figure rather than claiming a clean
      gate or dropping the caveat
- [ ] `docs/dynamiccache/benchmarks/<date>-single-node.md`: the tables, the environment, and one
      paragraph per remaining anomaly naming its code path, in the shape of the 2026-09-06 report
- [ ] Each of the four original anomalies gets a line saying whether it is closed, by which
      ticket, and what the command costs now; an anomaly that did NOT improve as its ticket
      predicted is reported as such, with the prediction quoted
- [ ] The 2026-09-06 report gains a pointer at the top to this one and keeps its provisional
      banner; it is not deleted, because its contended numbers are the evidence for why the
      quiet gate exists
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
