# 04: Watchdog and purge

**What to build:** the layer becomes self-healing and stops growing without bound. Every
PENDING row eventually reaches a terminal status without anyone intervening, and storage
stays inside a fixed retention window.

The watchdog takes any PENDING row older than the timeout T, verifies its inventory against
what is actually in MinIO, and conditionally flips it to COMPLETE or FAILED. T comes from
the worst-case upload time on the real MinIO link - which ticket 01 did NOT measure, since
there was no MinIO link available to it. Spec 18.6 item 3 is still open and measuring it is
this ticket's first task. Ticket 01 sized the payload (~14 MB per 1M-row table) and nothing
more; deriving T from its export duration would be wrong by orders of magnitude. An uploader still working while the
watchdog wakes up is not a hazard to design around: both go through the conditional
transitions from ticket 02, so exactly one wins and the other learns it changed nothing.
This is the single recovery path that both crashes and clean shutdowns feed into.

Purge enforces retention. The window is fixed and sized to the slowest ETL cadence plus
margin, not "keep latest only" — an ETL slower than the archive cadence would otherwise
full-compare on every run, which is correct but wasteful and hides the archiver breaking.
On top of the window sits an unconditional keep-newest-COMPLETE rule, so a broken archiver
that stops publishing can never have its last good baseline purged out from under the
consumers (D34). Reclaiming a version means mark, then delete objects per its inventory,
then delete the row: objects first, so a dangling object without a covering row stays
impossible. FAILED versions are cleaned the same way.

A staleness alert fires when the newest COMPLETE checkpoint ages past a threshold. It is
purely operational — diffs stay correct while the archiver is down, they just over-report,
which D25's idempotent consumers absorb.

The thing this ticket must not grow is an orphan sweep. D33's ordering makes a dangling
object unreachable by construction, so the code asserts that rather than scanning for it.

**Blocked by:** 03.

**Status:** ready-for-agent

- [ ] A PENDING row older than T is resolved to COMPLETE when its inventory verifies, FAILED when it does not
- [ ] The watchdog timeout T is derived from ticket 01's measurement and its rationale is recorded
- [ ] Every crash injected in ticket 03's matrix converges to a terminal status within two passes
- [ ] Purge deletes objects before the row, for both expired and FAILED versions
- [ ] Keep-newest-COMPLETE survives a retention window in which every version is expired
- [ ] Staleness alert fires when the newest COMPLETE checkpoint exceeds its age threshold
- [ ] No LIST-based orphan sweep exists anywhere in the codebase; the dangling-object case is asserted impossible, not scanned for
