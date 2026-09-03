# 04: Route consumer and end-of-poll reconciliation

**What to build:** A route's event flow is collected end to end: files become bounded parallel pipelines under
a supervisor so one failure never cancels a sibling, poll failures and skips are counted, a
route-down event ends the collector with the route gauge at zero, and every completed poll with
a complete listing repairs any file that was moved but never recorded as ACKED by marking it
ACKED and creating its deliveries. The hook points are wired so the next ticket can stop the
pipeline anywhere.

**Blocked by:** 03 (File pipeline)

**Nature:** coroutine structure work

**Status:** ready-for-agent

- [ ] With `parallelism + 1` files scripted, at most `parallelism` pipelines run at once on the virtual clock
- [ ] A `PollFailed` never cancels a running pipeline; `PollFailed` and `PollSkipped` increment the spec Sec 13 counters
- [ ] Reconciliation marks ACKED exactly the UPLOADED rows older than the poll start and absent from a complete listing, and creates their deliveries through the same function the pipeline uses
- [ ] A truncated listing skips reconciliation and increments the skipped counter (S14)
- [ ] S16: a ledger outage for one poll nacks every file with redelivery, uploads nothing, and the next poll completes them all
- [ ] `RouteDown` ends the collector with the route gauge at zero and no pipeline cancelled
- [ ] `stuck_files` gauge refreshes at every poll completion
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
