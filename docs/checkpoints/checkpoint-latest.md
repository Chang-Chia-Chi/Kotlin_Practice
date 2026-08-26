# Checkpoint - P5 Task model, variables, step executors

- Phase ID: P5
- Project: SimpleEtl (YAML-driven ETL framework)
- Date: 2026-08-27
- Team: sdet + engineer + reviewer, plus one independent adjudicator
- Status: PHASE COMPLETE. 209 tests, 0 failures. One review cycle (interrupted by a session
  limit and resumed). Committed.

## Files

Production, src/main/kotlin/infra/simpleetl/:
- CanonicalType.kt, Row.kt, RowMapper.kt              (P1)
- RowWriter.kt, DuckDbTableWriter.kt, JdbcWriters.kt  (P2)
- RowPipe.kt                                          (P3)
- ScratchDb.kt, DatasetNamer.kt                       (P4)
- TaskDefinition.kt, TaskEngine.kt                    (P5)

Tests: P0's six spike/*Spike, P1's six, P2's four, P3's five, P4's five, P5's eight.

## Build

    mvn -f <repo>/pom.xml -pl SimpleEtl clean test    -> BUILD SUCCESS, 209/209

ALWAYS use `clean`. Maven is NOT on PATH:
C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2024.1.4\plugins\maven\lib\maven3\bin\mvn.cmd

An exit code 255 with truncated output is a transient stale-artifact race on this
OneDrive-backed directory, not a failure. Re-run with `clean`.

## STANDING PRACTICE, adopted in P5

**Mutation-test the tests.** The reviewer introduced a regression into src/main and ran the
suite; a test whose KDoc claimed to reject that exact implementation stayed green. Reading
cannot find this class of defect. The sdet cannot do it - editing src/main is outside its
role and the permission system enforces that - so it falls to the reviewer or the lead.
Always restore and verify by checksum.

## STANDING WARNING from P4

Every measurement in this project is from Windows. CI is Linux. A test whose discriminating
power comes from an OS behaviour can pass on CI against the very implementation it exists to
reject. Assert something platform-independent as well.

## Driver facts measured in P5

- Every JDBC failure reaches Layer 2 wrapped (ResultSetException,
  UnableToExecuteStatementException, ConnectionException). A retry classifier that does not
  walk the cause chain retries NOTHING.
- A DuckDB syntax error has a NULL SQLState and is a plain java.sql.SQLException.
- PreparedBatch.add() clears bindings: bind-once-per-batch fills row 1 and NULLs the rest.
- JDBI binds an Argument value handed to bindMap directly, so Map<String, Any?> carries a
  typed null. NullArgument ships in jdbi3-core.
- JDBI rejects superfluous bindings ONLY when the statement declares no parameters at all.
- Result set metadata survives an exhausted result set, so a zero-row export knows its type.

## Open for later phases

- P6: validation rule 18 (scratch REQUIRED + retries > 0) at startup.
- P6/P7: StatementTarget happy path and non-scratch TableTarget need an Oracle-backed test.
- P7/P8: enabled, cron, logging, onSuccess, onFailure, idempotent are carried and unused.
- P8: a way to reach the scratch file size for etl_scratch_file_bytes (9.3).
- P9: CacheCopyStep executor is a NotImplementedError stub.

## Files to re-read on resume

- docs/simpleetl/spec.md sections 3, 5.5, 6, 10, 11.2
- docs/simpleetl/plan.md P6 entry
- docs/simpleetl/progress.md P0-P5 entries
