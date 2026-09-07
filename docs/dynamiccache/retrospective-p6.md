# P6 retrospective: the review fixes, and what the fix loop cost

Written 2026-09-07, at `81fef2c4`, by the orchestrating session. Companion to
`plans/p6-review-fixes.md` (the ticket entries) and `progress.md` (one entry per ticket).
P7's own account is in `plans/p7-performance.md` and the three reports under `benchmarks/`.

## What P6 was

A four-axis review of DynaCache at `adf9d349` on 2026-09-06: standards, spec, architecture, and
a bug hunt in which every finding had to be proved by a failing test before it counted. It
produced seven confirmed bugs, five standards violations, six spec divergences, four missing
spec items, and eight architecture candidates.

All of it is closed. The phase ran from T48 to T84, plus the three P7 performance tickets from
the other session, and finished at 527 tests green with 84 of 87 tickets done. The three open
tickets are P7 follow-ups, none blocked on this work.

## What the review found, and what the loop found on top

The review's own list was 22 items. The fix loop then found eight more while working, which is
the number worth remembering: **more than a third of what P6 closed was discovered by fixing,
not by reviewing.**

- T55, moving snapshot parts behind a persist adapter, found three: the live write-ahead log
  continuing *inside* a snapshot part so an aborted set deleted acknowledged writes (T74), a
  wire-supplied snapshot id reaching the filesystem unchecked (T75), and a restore of a missing
  id silently emptying a node (T76).
- T75 in turn found that an ordinary `IOException` out of the cut still killed the node, which
  became T80.
- T66, deciding the conflict rule in one place, found that doing so faithfully made a tombstone
  defend a deleted key, which broke anti-entropy's resurrection and closed two deviations
  recorded years-of-tickets earlier (T28 and T30).
- T81 found that single-node startup on a fresh path had never worked, because only the cluster
  path created its data directory.

None of these was visible to a reviewer reading the code. Each surfaced because someone changed
the code next to it and the tests moved.

## Three lessons worth keeping

### The merge cost of a wave is set when the tickets are cut

T84 crossed seven intervening tickets and hours of tree movement with no conflict at all,
because it owned anti-entropy, the Merkle tree and the cluster proto outright. T80 shared one
method *name* with T82 and produced a merge that compiled, passed the entire suite including
every test T80 wrote, and shipped with its own availability fix bypassed at its only production
call site. Same wave, same merge machinery, opposite outcomes.

The mechanism is worth stating because it will recur. T82 had deleted a pass-through alias,
correctly, and repointed its caller. T80 gave that same name real behaviour. Git conflicted on
the method, which got resolved by hand, and auto-merged the call site silently, because a
deletion leaves no text to conflict with: the loud half gets attention and the dangerous half
does not. A ticket's tests call the method it added, by name, and nothing asserts *who calls
it*, so the bypass lived in a caller belonging to neither ticket's test surface.

So: verifying on the merged tree is necessary but not sufficient. A green build proves the tree
is consistent, not that the new behaviour is reachable. Every merge involving a renamed,
deleted or newly-meaningful symbol now gets its call sites checked by hand. Where two tickets
must share a symbol, sequence them **and say in the ticket that the sequence is merge-avoidance
rather than a functional gate** — an undocumented ordering constraint is indistinguishable from
an oversight, and the person most likely to remove it is someone trying to be helpful.

### Where a change is about how much work happens, count the work

T84's claim is that a range of 300 keys with one divergent key ships 16 leaves instead of 300,
at a cost of 32 hashes and two extra round trips. Those are counts, so the test asserts them
directly, and the claim is true on a loaded laptop, on a quiet server, and on hardware nobody
has bought yet.

Every timing measured in P7 needed a paragraph of conditions and a noise band beneath it, and
was still only as good as the machine it was taken on. That is not a criticism of the timings:
whether group commit at a 2 ms deadline earns its `fsync` is not a fact about a protocol and no
count can answer it. The rule is only that you should ask which kind of question you have. The
failure is not measuring time; it is measuring time when a count was available.

### Measure the machine rather than reasoning about it

A benchmark window failed outright because "agents already running can finish" and "the machine
is quiet" are not the same statement: an agent in flight is a loop of build, read, edit, build.
Worse, the instinct that a small intermittent load is safer than a large constant one is exactly
backwards for a before-and-after design. A constant background sits on both sides of a ratio and
the noise band prices it; a four-minute build landing across one pair of samples and not the
other is indistinguishable from the effect, and it fails by producing a *plausible* number.

What eventually settled it was measurement rather than argument, and it found that the floor was
neither session: this machine carries an IDE daemon, a Kotlin compile daemon and a three-node
Kubernetes cluster, holding CPU idle between 23 and 52 percent against a gate that wanted 70. No
amount of standing down could have reached it. The gate was relaxed and the phase leaned on
ratios instead.

## What went wrong

- **A tier vanished mid-wave.** Fable ran out of credits; three agents died within two minutes
  having done nothing. T67, T80, T84 and P7's T85 are interleaving- and protocol-class tickets
  carried by Opus. This is recorded under the routing table in `plan.md` rather than only here,
  so a reader who consults the routing meets the exception.
- **Agents parked on notifications that never arrive.** Five agents idled waiting for a
  background build to announce itself; each build had finished green minutes earlier. One agent
  died on a session limit while parked and lost its entire uncommitted diff, which a fresh agent
  had to redo. Briefs now say to poll, and to commit the moment the code compiles.
- **A benchmark window was wasted**, as above, for fifteen minutes. Cheap next to the
  alternative, which was a report full of numbers taken across another session's builds with
  nothing in the numbers to say so.

## One correction to the joint headline

P7 is often summarised as four benchmark anomalies found and fixed. Three were closed with
measurements beside the code. The fourth was never a ceiling: the plan named the sequential
fan-out chain as a cost to remove, and measuring it showed the chain was worth about nineteen
percent while the pass was actually dominated by fifty clients contending on one partition,
because the benchmark sent a literal key. The chain was removed anyway, correctly. But that
anomaly was a hypothesis the measurement retired, not a defect it repaired, and the phase should
say so. (Correction supplied by the session that ran P7.)

## Still open

The three P7 follow-ups: the shutdown drain (T85), the platform-timer fix (T87, which T78's own
measurement uncovered when a 2 ms deadline turned out to fire on a 15.6 ms Windows tick), and
the baseline rerun (T86), which needs a decision about clearing the machine rather than any
engineering.
