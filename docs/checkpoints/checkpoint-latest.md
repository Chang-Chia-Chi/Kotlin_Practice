# Checkpoint - P2 Writers

- Phase ID: P2
- Project: SimpleEtl (YAML-driven ETL framework)
- Date: 2026-08-27
- Team: sdet + engineer + reviewer, plus one independent adjudicator
- Status: PHASE COMPLETE. 115 tests, 0 failures. One review cycle. Committed.
- NOTE: the composition table marks P2 "all three, plus human review". The human review has
  NOT happened - the user instructed the lead to proceed without checking in. Flagged to the
  user at the gate; recorded here so it is not lost.

## Files

Production, src/main/kotlin/infra/simpleetl/:
- RowWriter.kt          - RowWriter interface, catalogColumns (single-schema guarded)
- DuckDbTableWriter.kt  - CreateTable, AUTO DDL with DECIMAL(p,s), positional append, 4.6 dispatch
- JdbcWriters.kt        - JdbcTableWriter, JdbcStatementWriter
- RowMapper.kt          - ColumnMeta gained precision and scale

Tests, src/test/kotlin/infra/simpleetl/:
- WriteFixtures.kt, DuckDbTableWriterAutoTest.kt, DuckDbTableWriterRequiredTest.kt, WriterOracleTest.kt

## Build

    mvn -f <repo>/pom.xml -pl SimpleEtl clean test    -> BUILD SUCCESS, 115/115

ALWAYS use `clean`. Maven is NOT on PATH:
C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2024.1.4\plugins\maven\lib\maven3\bin\mvn.cmd

## Driver facts measured in P2, all on real duckdb_jdbc 1.1.3

- Bare DECIMAL resolves to DECIMAL(18,3): 15 integer digits max, so an Oracle NUMBER(18)
  key >= 1e15 fails the append. This is why ColumnMeta was widened.
- getColumns(null, null, name, null) returns rows from EVERY schema with that table name,
  interleaved after sorting by ORDINAL_POSITION. Now guarded.
- `_` is a live wildcard in getColumns: "t_stg" matches "tXstg". The exact TABLE_NAME filter
  is load-bearing.
- DatabaseMetaData.getColumns reports nullability truthfully; ResultSetMetaData.isNullable
  does not (columnNullable for everything). Never substitute one for the other.
- Appender close(): completed rows flush; a PART-appended row discards the whole unflushed
  buffer including completed rows; an empty beginRow is harmless.
- DuckDBAppender is subclassable; DuckDBConnection is public final. No injection seam exists
  in DuckDbTableWriter, which is what makes a leak double impossible.

## Open for later phases

- P5: JdbcStatementWriter has no task-variable channel (spec 6.3). Constructor amendment or
  pre-binding needed.
- P4: budget scratch space from the three retention shapes, not one number.
- Done-when 7 partially unmet by necessity; see progress.md P2 deviation 6.

## Files to re-read on resume

- docs/simpleetl/spec.md sections 4.4, 4.6, 5.5, 7.2, 10, 11.1, 12
- docs/simpleetl/plan.md P3 entry
- docs/simpleetl/progress.md P0, P1, P2 entries
