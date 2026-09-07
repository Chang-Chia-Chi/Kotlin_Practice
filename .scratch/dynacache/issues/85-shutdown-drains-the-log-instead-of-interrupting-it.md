# 85: Shutdown drains the log instead of interrupting it

**What to build:** Closing a node completes every acknowledged write's fsync instead of
interrupting it. Today `DynaCacheServer.close` calls `scheduler.shutdownNow()`, which interrupts
the scheduler thread; when that thread is inside `FileChannel.force`, the interrupt closes the
channel and throws `ClosedByInterruptException`, so every waiter that force was covering
completes exceptionally. Those waiters fail correctly, because their bytes were not durable; what
is wrong is that a clean shutdown should have made them durable instead of failing them. The
ordering is what makes this reachable: the shutdown hook closes the server first and the snapshot
engine second, so the final save and its rotate run after the scheduler is already down, and that
rotate's fsync meets a channel the interrupt has closed. A grace before the interrupt fixes both
halves. Nothing acknowledged is lost (a failed waiter was never answered `+OK`, and the shutdown
snapshot still saves), so this is a shutdown-correctness bug rather than a durability hole. It
predates T78, under `EVERY_SECOND`, but T78's `GROUP_COMMIT` runs the tick at a 2 ms cadence,
which makes the thread far likelier to be inside an fsync when the hook fires. After this ticket
close asks the scheduler to stop, waits for the current force to finish within a bounded grace,
forces whatever is still parked, and only then interrupts; a close that hits the grace bound says
so in the log rather than failing silently.

The shape of the fix is fixed by one detail: `ClosedByInterruptException` does not merely fail the
force in flight, it closes the channel, so the sink is dead for every later use and not only for
the rotate that tripped over it. Forcing again after the interrupt therefore cannot work. The fix
is "never interrupt a live sink", not "force once more afterwards", which is why the grace has to
come before the interrupt rather than a retry after it. Nothing else needs that sink today,
because no command is in flight by then, but a fix built as a retry would be built on sand.
Found by T78's interleaving review.

**Blocked by:** 78 (Group-commit WAL with a short fsync deadline)

**Nature:** shutdown ordering and durability edges, C14, spec 2.8 (Opus; plan 4 routes this to Fable, which is out of usage credits on this account as of 2026-09-07 — record the swap as a deviation)

**Status:** ready-for-agent

- [ ] `close_completes_the_force_in_flight_rather_than_interrupting_it`: with a sink double whose
      force blocks until released, a close started during a force does not interrupt it, and the
      waiters that force covers complete normally
- [ ] `close_forces_whatever_is_still_parked_before_it_returns`: writes parked under
      `GROUP_COMMIT` and not yet forced are durable after close returns
- [ ] `close_past_its_grace_bound_reports_and_returns`: a force that never finishes does not hang
      close for ever; the bound is on the injected Clock, never a real sleep
- [ ] The shutdown snapshot still saves, and every existing shutdown, rotate, recovery and P4
      acceptance test passes unchanged
- [ ] Progress entry written

Ground rules for every ticket: implement only this ticket; 200 to 500 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green, one
slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry describing what was done and every
deviation. The spec is docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md
and this ticket's entry is docs/dynamiccache/plans/p7-performance.md; the spec wins over this
ticket when they disagree, unless the progress log records a deliberate deviation. Modify only
DynaCache/.
