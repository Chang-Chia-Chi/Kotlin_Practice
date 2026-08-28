# 03: Archiver run, scheduling, and graceful shutdown

**What to build:** the layer starts producing checkpoints on its own. An hourly run per
group takes a lease on the live snapshot, exports every table to local temp Parquet,
publishes the objects to MinIO, and leaves behind a COMPLETE manifest row that a consumer
can trust.

The step order in spec 18.3 is fixed and is the entire safety argument. The PENDING row,
carrying the complete inventory of keys, checksums, sizes, and row counts, is committed
*before* the first object is uploaded. That ordering is what makes a ghost file impossible
and is why no LIST-based orphan sweep exists anywhere in this layer (D33). A run that finds
`data_as_of` no greater than the newest COMPLETE version's skips and alerts rather than
publishing. Only after every object is uploaded and verified against the inventory does the
conditional flip to COMPLETE happen. A first run ever, and a run following a FAILED gap,
are the same run: a version is self-contained and there is no chain to stitch.

Scheduling is deliberately dull. Different groups run in parallel on a bounded executor;
the same group never runs twice at once, and a run that finds its group busy skips and logs
rather than queueing.

Shutdown is where the design earns its keep. Stop scheduling, interrupt in-flight runs,
release the lease inside the framework's bounded drain, delete the temp directory — and
deliberately leave any PENDING row untouched. Cleaning it up at shutdown would create a
second recovery path that only graceful exits exercise and only crashes need. Instead the
watchdog in ticket 04 resolves it, so crash and clean shutdown converge on one path that
gets tested every time either happens. This mirrors the framework's own no-delicate-cleanup
stance in spec 10.2.

The MinIO client lives behind a small concrete wrapper with a fake for tests. It is a
testability seam, not a new public interface; the spec 2.3 five-interface budget is a
framework budget and this layer adds nothing to it.

This ticket also owns where the Parquet export function lands. Ticket 01 settled the
statement and pinned it in `duckdb/ParquetExportSpikeTest`, but deliberately shipped no
production code, since an export function would have had no caller until now. Decide its
home against what the archiver actually needs: beside `copyOut` in
`infra.snapshotcache.duckdb` (which would need a seam on a FIXED spi interface, so a plan
amendment first), or in `infra.snapshotarchive` as a DuckDB-aware consumer, which the
public API already assumes callers are - `CopyOutSpec` takes caller SQL and a caller
connection the store ATTACHes into.

**Blocked by:** 01 (the export path must be decided before the run can be written), 02 (the run needs the DAO).

**Status:** ready-for-agent

- [ ] Happy path produces a COMPLETE version whose uploaded objects match the recorded inventory exactly
- [ ] A run never writes an object to MinIO before its covering PENDING row is committed
- [ ] A run whose `data_as_of` is not strictly greater than the newest COMPLETE version's skips and alerts
- [ ] Crash injected between every adjacent step pair leaves either no row at all, or PENDING with partial objects — never a COMPLETE row that cannot be trusted
- [ ] Per-group serialization and cross-group parallelism are asserted with the P5 hook-driver style; no `Thread.sleep` anywhere
- [ ] Shutdown mid-upload releases the lease within the drain budget and leaves no temp files
- [ ] Shutdown never resolves the run's own PENDING row
- [ ] Per-table export tasks run in parallel within a run, on the path decided in ticket 01
