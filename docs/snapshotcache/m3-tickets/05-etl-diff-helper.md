# 05: ETL diff helper and full-compare fallback

**What to build:** the consumer side, and the reason the other four tickets exist. An ETL
job asks "what changed since the version I last processed" and gets an exact answer that
survived the pod restarting.

The helper takes the ETL's recorded watermark, looks it up in the manifest, downloads that
one checkpoint's tables in parallel, and `FULL OUTER JOIN`s them on primary key against the
live snapshot in local DuckDB, emitting `(pk, op in I/U/D, changed_columns, current
values)`. It holds the snapshot lease for the whole diff through `withSnapshot` scoping,
so the baseline it compares against cannot shift underneath it.

There is one correctness rule here that must never be weakened, and every other decision
follows from it: **the baseline checkpoint must have been taken at or before the ETL's last
processed moment.** This is why the watermark is a version the ETL recorded, never "the
newest checkpoint available now". A checkpoint published after the last run describes state
the ETL never processed, and diffing against it silently drops every change in the gap.
Under-reporting is the failure that corrupts data quietly, so the design makes it
impossible by construction. Over-reporting is bounded by one archive interval and is safe
against D25's idempotent consumers, so the `data_as_of <= T` predicate in D35 is allowed to
err old and does. This is the standard single-baseline incremental sync shape — the ZFS
incremental send needs the common ancestor snapshot to still exist, or it falls back to a
full send.

That fallback is the second half of the ticket, and it is a first-class path rather than an
error. A watermark that is absent (a brand new ETL), purged (the job ran slower than
retention), or FAILED means the helper signals full-compare and the ETL does an anti-join
against the live snapshot — which needs nothing from this layer at all. The fallback having
to exist anyway is exactly what lets retention in ticket 04 stay a dumb fixed window
instead of a consumer registration and refcounting scheme.

The helper computes the next watermark and hands it back. It never writes it. Per-consumer
state belongs to the consumer, committed in the same transaction as the ETL's own output
(D24, D35), because a watermark committed separately from the output it describes is a
watermark that can outlive a rolled-back run.

**Blocked by:** 03 (needs published checkpoints), 04 (the purged-watermark fallback needs a real purge).

**Status:** done 2026-08-29 (`EtlDiff.kt`, `EtlDiffTest`); see the ticket-05 progress entry

- [x] E2E against real DuckDB and a fake MinIO: publish versions with known edits, and the diff yields exactly the expected I/U/D rows with correct `changed_columns`
- [x] Per-table checkpoint downloads run in parallel
- [x] The snapshot lease is held for the whole diff and released via `withSnapshot` scoping on every exit path
- [x] Absent, purged, and FAILED watermarks each return a full-compare signal to the caller rather than throwing
- [x] The helper returns the computed watermark and never writes ETL state itself
- [x] The watermark is `max(version) WHERE status='COMPLETE' AND data_as_of <= snapshot.dataAsOf`, verbatim
- [x] Property test: every injected change appears in at least one run's diff — the helper never under-reports
  (twelve rounds, fixed seed; the generator never returns a column to a previous value, which is
  the one shape where the claim does not hold - recorded as spec 18.6 item 4 and pinned by its own test)
- [x] The one-interval over-report is asserted as expected behaviour, not treated as a defect
- [x] Long-running-job race: a checkpoint published mid-run is never selected as the new watermark
