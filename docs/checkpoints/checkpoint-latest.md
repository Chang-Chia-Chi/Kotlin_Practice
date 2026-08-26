# Checkpoint - P1 Row and type mapping

- Phase ID: P1
- Project: SimpleEtl (YAML-driven ETL framework)
- Date: 2026-08-26
- Team: sdet + engineer + reviewer (composition table: all three)
- Status: PHASE COMPLETE. Build green 79/79. Reviewer ran, REVISE/REVISE, all 5 required changes landed and verified by the lead. Committed.

## Files produced

Engineer (src/main/kotlin/infra/simpleetl/):
- CanonicalType.kt  - enum, duckDbType natural mapping, fromJdbc (spec 4.3)
- Row.kt            - immutable Row, typed accessors, with/without, internal ctor carries step
- RowMapper.kt      - ColumnMeta + RowMapper, metadata read once at construction

SDET (src/test/kotlin/infra/simpleetl/):
- DuckFixtures.kt          - SELECT-only DuckDB result sets, no table/INSERT/appender
- CanonicalTypeTest.kt     - spec 4.3 as a table; duckDbType round-trip through real DuckDB
- RowTest.kt               - spec 4.2/4.5 semantics, wrong-type message, cross-thread read
- RowMapperDuckDbTest.kt   - real duckdb_jdbc 1.1.3, 12 column types, all-NULL row
- RowMapperOracleTest.kt   - Testcontainers oracle-free, one container per class
- RowMapperErrorTest.kt    - Mockito at ResultSetMetaData/ResultSet boundary only

## Build

    mvn -f <repo>/pom.xml -pl SimpleEtl clean test    -> BUILD SUCCESS, 81/81

ALWAYS use `clean`. Without it surefire runs stale compiled probe classes from target/test-classes
that no longer exist in source; that produced three phantom passing tests in the first run.

Earlier-phase tests: P0's six *Spike files untouched, verified by mtime.
Role boundary: engineer's revision touched only CanonicalType.kt and RowMapper.kt; no test file moved.

## Reviewer findings, all fixed

Engineer: `columns` returned the live mutable key set (Row is immutable per 4.2, and P3 hands
Rows to caller-supplied transforms); duplicate column keys collapsed silently, which would
have become wrong data in P2's positional writer; a KDoc stated falsely that duckdb_jdbc has
no getBytes (it does - ojdbc is what rejects it on a BLOB).

SDET: nothing pinned the step label surviving with/without into the copy; Oracle BOOLEAN was
the one amended-4.3 row with no real-Oracle test; the new duplicate-key rejection had no test.

The reviewer also caught that CLAUDE.md had been deleted from the repo root during the
session, outside every agent's file boundary. Restored.

## Revision cycle 1 - what happened

The sdet and engineer independently reached opposite readings of spec 4.3's DATE row, and
both documented it. All 23 failures reduced to two root causes, both escalated to the user
and both ruled in the sdet's favour:

1. Types.DATE (91) -> CanonicalType.DATE, not DATETIME. Types.TIMESTAMP (93) stays DATETIME.
2. Types.BOOLEAN (16) -> CanonicalType.BOOLEAN. 4.3 had no BOOLEAN row at all.

spec.md 4.3 updated with both rows plus the rationale.

## Measured facts recorded for P2

- duckdb_jdbc 1.1.3 reports columnNullable for EVERY column, including `bigint not null`.
  Verified by the lead on the real driver. So 4.6's "NOT NULL columns keep their natural
  mapping and use the faster primitive path" is unreachable for a scratch-sourced pipe.
- Oracle folds INTEGER, SMALLINT, FLOAT into NUMBER (all Types.NUMERIC, typeName NUMBER),
  so 4.3's LONG row is unreachable from Oracle DDL and an Oracle FLOAT yields BigDecimal.
  Combined with P0's ruling, the nullable-BIGINT path is reachable only when the author
  CASTs in source SQL.
- Oracle TIMESTAMP WITH TIME ZONE cannot be read as Instant directly (ORA-17004);
  getObject(i, OffsetDateTime::class.java) works on both drivers.
- DuckDB DATE refuses LocalDateTime/Timestamp conversion entirely.

## Files to re-read on resume

- docs/simpleetl/spec.md sections 4.1 to 4.6, 11.1, 12
- docs/simpleetl/plan.md P1 and P2 entries
- docs/simpleetl/progress.md P0 entry
