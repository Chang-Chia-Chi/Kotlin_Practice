# Checkpoint - P4 Scratch lifecycle

- Phase ID: P4
- Project: SimpleEtl (YAML-driven ETL framework)
- Date: 2026-08-27
- Team: sdet + engineer + reviewer
- Status: PHASE COMPLETE. 160 tests, 0 failures. One review cycle. Committed.

## Files

Production, src/main/kotlin/infra/simpleetl/:
- CanonicalType.kt, Row.kt, RowMapper.kt              (P1)
- RowWriter.kt, DuckDbTableWriter.kt, JdbcWriters.kt  (P2)
- RowPipe.kt                                          (P3)
- ScratchDb.kt, DatasetNamer.kt                       (P4)

Tests: P0's six spike/*Spike, P1's six, P2's four, P3's five, P4's five
(ScratchFixtures, ScratchDbLifecycleTest, ScratchDbDeletionTest, DatasetNamerTest,
NoTempTableTest).

## Build

    mvn -f <repo>/pom.xml -pl SimpleEtl clean test    -> BUILD SUCCESS, 160/160

ALWAYS use `clean`. Maven is NOT on PATH:
C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2024.1.4\plugins\maven\lib\maven3\bin\mvn.cmd

## STANDING WARNING for every later phase

Every measurement in this project is from Windows. CI is Linux. P4 found a test that was
structurally unfalsifiable on Linux: its discriminating power came from a Windows file lock,
so on CI it would pass against the very implementation it existed to reject. When a test's
power comes from an OS behaviour, assert something platform-independent as well.

Also unconfirmed on Linux: spec 7.2's 32 GiB sizeLimit, which rests on bytes-per-value and
spill-factor ratios measured on Windows/NVMe.

## Driver facts measured in P4

- The scratch file is created at getConnection, before any statement runs.
- Windows blocks delete while any connection is open; a duplicate keeps the lock and stays
  usable after the primary closes. WINDOWS-ONLY - Linux unlinks the open file.
- The temporary catalog is per connection; a temp table on the write connection is invisible
  from a duplicate. A guard asking only one connection is a false negative.
- '512MB' reads back as 488.2 MiB; '512MiB' reads back as 512.0 MiB.

## Open for later phases

- P5 owns retry timing and which attempt number is published. DatasetNamer does not decide
  when an attempt succeeded.
- P5/P6: transform-added column silently dropped under AUTO; lands under REQUIRED.
- P5: null in JdbcSource.parameters binds as Types.OTHER; Oracle rejects it on some columns.
- P5: JdbcStatementWriter has no task-variable channel (spec 6.3); constructor amendment or
  pre-binding needed.
- P8: needs a way to reach the scratch file size for etl_scratch_file_bytes (9.3).
- JdbcStatementWriter is still unexercised by any pipe test.

## Files to re-read on resume

- docs/simpleetl/spec.md sections 3, 5.2-5.6, 6, 7.2, 10, 11.2
- docs/simpleetl/plan.md P5 entry
- docs/simpleetl/progress.md P0-P4 entries
