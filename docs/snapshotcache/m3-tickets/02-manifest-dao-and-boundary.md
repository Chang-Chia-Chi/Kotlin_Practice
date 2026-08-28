# 02: Manifest DAO, version allocation, and the archive package boundary

**What to build:** the durable half of the archive layer. After this ticket an archive
version can be allocated, claimed, resolved, and found again across a pod restart, which
is the whole point of the layer (the framework persists nothing, D10).

Concretely: the `SNAPSHOT_ARCHIVE_MANIFEST` table from spec 18.2 exists, versions come
from an Oracle sequence keyed with the group, and a JDBI DAO can insert a PENDING row
carrying the full file inventory as json, conditionally transition PENDING to COMPLETE or
to FAILED, look up the newest COMPLETE version, answer the watermark query, and list
expired versions. Generation numbers are stored for diagnostics only and are never a key
(D31), because generation numbering restarts at 1 on every boot.

Two rules are load-bearing rather than incidental. Every status transition is conditional
on the current status, so two writers racing on the same row resolve to exactly one winner
and the loser learns it affected zero rows; ticket 04's watchdog depends on this and has
no other defence. And the archiver refuses to publish when `data_as_of` is not strictly
greater than the newest COMPLETE version's, which is the one place a timestamp is trusted,
so it is guarded at the point of use.

This ticket also establishes the package fence: `infra.snapshotarchive` exists, and two new
ArchUnit rules make the boundary mechanical rather than conventional. The framework must
never depend on the archive layer, and the archive layer reaches the framework only through
`api`, never through `spi`, `core`, or `duckdb`.

**Blocked by:** None (can start immediately; parallel with 01).

**Status:** done (2026-08-29) - all criteria met

- [x] Manifest DDL matches spec 18.2, with `(group_id, version)` as the primary key and version allocated by an Oracle sequence
- [x] DAO supports insert-PENDING with inventory json, conditional PENDING to COMPLETE, conditional PENDING to FAILED, newest-COMPLETE lookup, watermark query, and expired-versions query
- [x] A conditional transition from any state other than PENDING affects zero rows and reports that to its caller rather than throwing or silently succeeding
- [x] Contract test: two concurrent writers attempt the same transition, exactly one wins
- [x] Contract test: the watermark predicate is exercised at its boundaries — `data_as_of` exactly equal to T, no COMPLETE rows at all, and every COMPLETE row newer than T
- [x] Contract test: the monotonicity guard rejects a `data_as_of` regression
- [x] `ArchitectureTest` imports `infra.snapshotarchive` as well as `infra.snapshotcache` — without this the two rules below are declared but never evaluated
      — verified non-vacuous: a planted archive→duckdb reference failed the rule with 2 violations.
- [x] ArchUnit: `infra.snapshotcache..` must not depend on `infra.snapshotarchive..`
- [x] ArchUnit: `infra.snapshotarchive..` must not depend on `infra.snapshotcache.{spi,core,duckdb}..`
- [x] The test database choice (testcontainer versus an H2-compatible subset) is decided in-phase and recorded in progress.md
      — real Oracle via Testcontainers `gvenzl/oracle-free:slim-faststart`; sequences, conditional-UPDATE
      row counts and CLOB are Oracle semantics an H2 subset would only approximate.
- [x] No frozen interface, invariant, equation, or enum is changed
