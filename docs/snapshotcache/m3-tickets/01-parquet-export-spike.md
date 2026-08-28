# 01: Parquet export spike and checkpoint sizing

**What to build:** a recorded answer to the three open items in spec 18.6, so the archiver
phase starts from measurement instead of assumption.

The load-bearing question is where a table's Parquet export actually runs. Either DuckDB
1.1.3 can `COPY (SELECT ...) TO '<file>.parquet'` on a read-only attached snapshot
connection, in which case export streams straight from the serving instance, or it cannot,
and export must stage through the public `copyOut` into the shared consumer instance
(D16) and export from there. Those are different lease-hold durations and different code,
so the archiver cannot be designed until this is settled.

Alongside it, measure a real checkpoint at ~1M rows: bytes on disk and export wall time.
That number sizes retention storage and confirms the lease-vs-K interaction is a non-issue
rather than a risk carried forward. Third, characterise worst-case upload time on the real
MinIO link, which is the input to the watchdog timeout T chosen in ticket 04.

Delivered as a thin export function plus a progress.md entry recording the decision, the
numbers, and the date. This ticket ships a decision; it does not ship the archiver.

**Blocked by:** None (can start immediately).

**Status:** done (2026-08-29), except the MinIO upload measurement — see below

- [x] The read-only-attached-connection question is answered empirically against DuckDB 1.1.3, not from documentation, and the chosen export path is implemented as one function
      — `COPY ... TO parquet` works directly; the `copyOut` staging fallback is not needed.
- [x] Checkpoint size in bytes and export duration are measured at ~1M rows and recorded
      — 14,180,166 bytes in 39/41/52 ms across three runs.
- [ ] Worst-case MinIO upload time for a checkpoint of that size is characterised
      — NOT DONE: no MinIO link in this environment. Spec 18.6 item 3 stays open and is
      ticket 04's input; the watchdog timeout T must not be derived from the export number.
- [x] progress.md records the decision, the measurements, and which spec 18.6 items are now closed
- [x] No framework source outside the export seam is modified
      — zero framework files changed; full suite 138 tests, 0 failures.
