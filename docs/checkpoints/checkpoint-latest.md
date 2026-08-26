# Checkpoint - P3 RowPipe (Layer 1 complete)

- Phase ID: P3
- Project: SimpleEtl (YAML-driven ETL framework)
- Date: 2026-08-27
- Team: sdet + engineer + reviewer, plus one independent adjudicator
- Status: PHASE COMPLETE. 137 tests, 0 failures. One review cycle. Committed.
- MILESTONE: Layer 1 is finished. The snapshot cache can adopt it. Everything after this
  point is the task engine (Layer 2).

## Files

Production, src/main/kotlin/infra/simpleetl/:
- CanonicalType.kt, Row.kt, RowMapper.kt          (P1)
- RowWriter.kt, DuckDbTableWriter.kt, JdbcWriters.kt (P2)
- RowPipe.kt - JdbcSource (Handle + Jdbi forms), RowTransform, PipeResult, RowPipe (P3)

Tests, src/test/kotlin/infra/simpleetl/: P0's six spike/*Spike, P1's six, P2's four,
and P3's PipeFixtures.kt, RowPipeTest, RowPipeCommitTest, RowPipeFailureTest, RowPipeOracleTest.

## Build

    mvn -f <repo>/pom.xml -pl SimpleEtl clean test    -> BUILD SUCCESS, 137/137

ALWAYS use `clean`. Maven is NOT on PATH:
C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2024.1.4\plugins\maven\lib\maven3\bin\mvn.cmd

## Driver facts measured in P3

- Appender flush() IS the per-chunk commit: unflushed rows are invisible even to the
  appending connection; after flush they are immediately visible to a duplicate(). autoCommit
  is true by default.
- Jdbi.open(Connection) CLOSES the caller's connection - it uses a lambda ConnectionFactory
  inheriting the interface default. Only Jdbi.create(Connection) is a no-op release. Verified
  on the shipped classpath. This is why the Connection form was deleted.
- duckdb_jdbc 1.1.3 accepts setFetchSize and goes on reporting 2048. Only Oracle can assert
  the reading; assert the call on DuckDB.
- Jdbi sets no fetch size of its own, so any recorded value is the pipe's.
- Oracle's default READ COMMITTED gives statement-level consistency, so a shared-source-
  transaction test must set SERIALIZABLE or it asserts nothing.

## Open for later phases

- P5/P6: transform-added column silently dropped under AUTO; lands under REQUIRED.
- P5: null in JdbcSource.parameters binds as Types.OTHER; Oracle rejects it on some columns.
- P4: budget scratch space from P2's three retention shapes, not one number.
- JdbcStatementWriter is unexercised by any pipe test.

## Files to re-read on resume

- docs/simpleetl/spec.md sections 4.4, 4.6, 5.2, 5.5, 7.2, 9.5, 10, 11.1, 12
- docs/simpleetl/plan.md P4 entry
- docs/simpleetl/progress.md P0-P3 entries
