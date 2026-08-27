# Checkpoint - P7 + package restructure

- Phase ID: P7 (plus an interstitial package restructure)
- Project: SimpleEtl (YAML-driven ETL framework)
- Date: 2026-08-27
- Team: engineer + sdet + reviewer, plus one independent adjudicator
- Status: PHASE COMPLETE. 300 tests, 0 failures. One review cycle.

## Layout - changed this session

Production is no longer one flat package. `infra.etl` splits four ways, enforced by ArchUnit:

| Package | Files | Rule |
|---|---|---|
| infra.etl.pipe | CanonicalType, Row, RowMapper, RowWriter, RowPipe | no task, no adapters, no org.duckdb, no snapshotcache |
| infra.etl.duckdb | DuckDbTableWriter, ScratchDb, DatasetNamer | the only package touching org.duckdb; a leaf |
| infra.etl.jdbc | JdbcWriters | a leaf, independent of duckdb |
| infra.etl.task | TaskDefinition, TaskEngine, TaskYaml, TaskFileLoader, TaskRunner, TaskScheduler, TaskAdmin | may use all of the above, and is the only package that may import infra.snapshotcache |

Seven ArchUnit rules in `infra.etl.ArchitectureTest`, every one proven able to fail by
introducing a real violation, not merely by passing on a clean tree.

The shared convention with the snapshotcache module is a decision procedure, not a fixed tree:
split by consumable unit first if there is more than one; within a unit use api / spi / core;
adapters are named for their technology and are leaves; rules are dependency sentences enforced
by ArchUnit. snapshotcache has one unit, so the procedure generates the api/spi/core/duckdb it
already has - it is unchanged.

TEST TREE: still one package, `infra.etl` plus `infra.etl.spike`. The user has since asked that
tests mirror the src layout; that move is the next task.

## Build - CHANGED

SimpleEtl now depends on the snapshotcache module (spec 7.3's cache read step, P9).

    mvn -f <repo>/pom.xml -pl SimpleEtl -am clean test              -> builds both modules
    mvn -f <repo>/pom.xml -pl snapshotcache install -DskipTests     -> then -pl SimpleEtl works alone

`-pl SimpleEtl` ALONE NOW FAILS to resolve unless snapshotcache is installed first.

Maven is NOT on PATH:
C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2024.1.4\plugins\maven\lib\maven3\bin\mvn.cmd

Two build traps found this session:

- Do NOT combine `-Dtest=X` with `-am`. The filter runs in every reactor module, and ArchUnit in
  a module whose target/classes is not yet built imports nothing and fails everything. Build the
  reactor first, then run targeted tests against it.
- Exit code 255 with truncated output is a transient stale-artifact race on this OneDrive-backed
  directory, not a failure. Re-run with `clean`.

## STANDING PRACTICE - roles, corrected this session

Only the engineer (production), the sdet (tests) and the lead (everything else) EXECUTE.
Reviewers and adjudicators READ, and end with a "For the lead to run" section naming the
mutations and measurements they want. The lead runs them and reports back.

Mutation testing remains mandatory and is the lead's. Write mutations with BINARY-mode file IO.
Do not verify a restore by md5 against a worktree snapshot - git normalises line endings between
index and worktree, so content plus a green suite is the right oracle.

## STANDING WARNING from P4

Every measurement in this project is from Windows. CI is Linux. A test whose discriminating power
comes from an OS or JVM-flag behaviour can pass on CI against the very implementation it exists to
reject.

## Open for later phases

- P8 (engineer + sdet): listener call sites live in TaskEngine.kt, the SAME file P9 edits. They
  cannot be built by two agents in parallel. Wave plan: P9 engineer + P8 sdet together, then P8
  engineer + P9 reviewer.
- P9 (engineer + reviewer): CacheCopyStep executor, currently a NotImplementedError stub at
  TaskEngine.kt:214. Calls the cache's own copyOut so no row passes through the JVM.
- P8: needs a way to reach the scratch file size for etl_scratch_file_bytes (spec 9.3).
- Host obligations from spec 8.6 remain untested here by ruling.

## Files to re-read on resume

- docs/simpleetl/spec.md sections 7.3, 9 (all), 11.2
- docs/simpleetl/plan.md P8 and P9 entries
- docs/simpleetl/progress.md P0-P7 plus the restructure interstitial
