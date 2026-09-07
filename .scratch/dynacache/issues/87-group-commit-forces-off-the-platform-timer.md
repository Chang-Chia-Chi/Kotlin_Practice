# 87: Group commit forces off the platform timer, not on it

**What to build:** A `GROUP_COMMIT` deadline that means what it says. T78 shipped the policy with
a 2 ms deadline and measured it at one fsync per platform timer tick instead: a
`ScheduledExecutorService` asked for a 2 ms fixed delay fires on this machine with a median gap of
15.860 ms, the Windows default timer resolution of 15.625 (measured directly, 127 ticks in two
seconds, min 15.225, max 16.712). Every observed rate falls out of that one number rather than out
of the deadline: clients divided by 15.86 ms predicts 63.05 requests per second at one client
against 63.26 measured, and 3153 at fifty clients against 3060 measured, and the single-client p50
of 15.79 ms confirms the writer genuinely waits a tick rather than a deadline. So durable writes
run at about a fifth of no-sync throughput where the arithmetic says they should be near parity.
After this ticket the deadline is delivered by something with sub-tick resolution, the platform
scheduler is at most an idle backstop, and the measured ratio of `GROUP_COMMIT` to `NEVER` at fifty
clients moves from 0.206 towards 1.0.

Two shapes are worth trying, in this order. Force from the flush path whenever the batch is
already past its deadline, so that under load the tick is never what fires the force and the
scheduler only covers a genuinely idle writer. If that is not enough, park a dedicated thread on
`LockSupport.parkNanos`, which does not go through the timer wheel and reaches sub-millisecond
resolution. Do not reach for `Thread.sleep` or a busy-wait loop; the first has the same tick
problem and the second burns a core to save a millisecond.

**A constraint from T78 that shapes the fix.** A parking thread is a thread, and the engine owns
none by rule (ADR 0001), so it belongs to the server exactly as the current schedule does. It must
also stay on the same thread as the checkpoint rotate, or the interleaving T78 closed reopens: a
force running while `rotate` sees a clear flag and an empty queue can fsync the new sink for bytes
in the closed one. That constraint is the whole reason the present design is a scheduled task
rather than a timer of its own, so a fix that adds an independent timer thread is not a fix.

**Blocked by:** 78 (Group-commit WAL with a short fsync deadline)

**Nature:** concurrent durability protocol, C14, spec 2.8 (Opus; plan 4 routes this to Fable,
which is out of usage credits on this account as of 2026-09-07 — record the swap as a deviation)

**Status:** ready-for-agent

- [ ] `group_commit_deadline_is_not_bounded_by_the_platform_tick`: with an injected clock and a
      sink double, a batch left open is forced at the configured deadline and not at the next
      platform tick; the test asserts the force ordering against the clock, never a wall-clock
      duration
- [ ] Under load the force is driven by the flush path rather than the scheduler: a test shows
      that with writers arriving continuously, no force is attributable to the timer
- [ ] `C14_group_commit_replies_only_after_fsync` and every existing WAL, recovery, rotate and P4
      acceptance test pass unchanged; the reply-after-durable contract is not weakened to get the
      throughput
- [ ] Before/after measurement in the shape T78 established, and reported the same way: the
      ratios are the result and the absolute rates provisional, two samples per side with the
      within-side spread as the noise band, before/before/after/after with the straddle rule, and
      the single-client p50 recorded beside the rate as the discriminator between a deadline that
      binds and one that fires on a tick. Recorded in
      `docs/dynamiccache/benchmarks/<date>-t87-group-commit-timer.md`
- [ ] Until this lands, `GROUP_COMMIT`'s documentation says plainly that its effective deadline is
      one platform timer tick on Windows, so nobody reads the 2 ms as a promise. If this ticket
      succeeds, that note is removed in the same commit
- [ ] Progress entry written

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock, and note that
this ticket is about real timer behaviour but its tests must still be deterministic: measure the
platform's tick in the benchmark, never in a unit test; spec-named tests keep their names,
constraint tests `C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at
the seams the plan entry names, red before green, one slice at a time, `codebase-design`
vocabulary for any new interface, and a `code-review` self-pass before the commit; append a
progress entry describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p7-performance.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and
docs/dynamiccache/benchmarks/.

**Measurement conditions on this machine, learned the hard way in T78:** a genuinely quiet machine
is not available; the floor is an IDE Maven daemon, a Kotlin compile daemon and a three-container
kind cluster. With the other orchestrator session's builds parked that floor is 78 to 96 percent
CPU idle, which is fine. With its builds running it is 23 to 52 percent, and an intermittent build
is far worse than a heavy constant load, because it lands across one pair of samples and not the
other and a before/after design cannot tell it from the effect. Coordinate a parked window, set
`QUIET_BUDGET=60` so a dirty machine reports in a minute rather than parking for ten, and record
the load on every pass.
