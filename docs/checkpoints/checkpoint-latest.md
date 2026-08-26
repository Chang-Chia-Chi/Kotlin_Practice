# Checkpoint - P6 YAML loading and validation

- Phase ID: P6
- Project: SimpleEtl (YAML-driven ETL framework)
- Date: 2026-08-27
- Team: engineer + sdet (no reviewer, per the composition table) + one adjudicator
- Status: PHASE COMPLETE. 265 tests, 0 failures. One review cycle. Committed.

## Files

Production, src/main/kotlin/infra/simpleetl/:
- CanonicalType.kt, Row.kt, RowMapper.kt              (P1)
- RowWriter.kt, DuckDbTableWriter.kt, JdbcWriters.kt  (P2)
- RowPipe.kt                                          (P3)
- ScratchDb.kt, DatasetNamer.kt                       (P4)
- TaskDefinition.kt, TaskEngine.kt                    (P5)
- TaskYaml.kt, TaskFileLoader.kt                      (P6)

## Build

    mvn -f <repo>/pom.xml -pl SimpleEtl clean test    -> BUILD SUCCESS, 265/265

ALWAYS use `clean`. Maven is NOT on PATH:
C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2024.1.4\plugins\maven\lib\maven3\bin\mvn.cmd

Exit code 255 with truncated output is a transient stale-artifact race on this
OneDrive-backed directory, not a failure. Re-run with `clean`.

## STANDING PRACTICE: mutation-test the tests

Adopted P5, applied again in P6. Introduce a regression into src/main, run the suite, confirm
the right tests fail, restore. It has twice found what reading could not.

**Process note from P6:** write the mutation with BINARY-mode file IO. A text-mode Python
round-trip normalises mixed line endings and breaks a byte-for-byte checksum comparison, so
the restore cannot be verified by md5. Verify by re-reading the mutated line and grepping for
artifacts if that happens.

## STANDING WARNING from P4

Every measurement in this project is from Windows. CI is Linux. A test whose discriminating
power comes from an OS behaviour can pass on CI against the very implementation it exists to
reject.

## Driver and library facts measured in P6

- json_serialize_sql PARSES WITHOUT BINDING, unlike PREPARE and EXPLAIN which both bind and
  fail on a missing table. error:false = valid SELECT; "not implemented" = parsed, not a
  SELECT; "parser" = syntax error with a character offset. This is how rule 6 is enforced.
- json_serialize_sql SERIALIZES SELECT ONLY, so a CREATE TABLE yields no column list -
  regardless of IF NOT EXISTS or quoted identifiers. EXPLAIN create table emits one
  CREATE_TABLE box with no columns. PREPARE rejects DDL.
- DuckDB 1.1.3 IS cancellable: Statement.cancel() interrupts a runaway CTAS in ~200 ms, and
  set enable_external_access=false refuses read_parquet, COPY TO and ATTACH. A boot sandbox
  is containable - what defeats it is that spec 3.4's own create-index example cannot run at
  boot because its table is made by a pipe step.
- Jackson: FAIL_ON_UNKNOWN_PROPERTIES defaults TRUE on YAMLMapper; STRICT_DUPLICATE_DETECTION
  defaults FALSE (without it, a duplicate key parses silently and the second wins).
- A YAML file of only `---`, or the literal `null`, deserialises to Java null rather than
  throwing.

## Open for later phases

- P7: reload semantics; a real cron parse (rule 16 is structural).
- P7/P8: wire the loader's datasources / hooks / transforms arguments.
- P8: a way to reach the scratch file size for etl_scratch_file_bytes (9.3).
- P9: CacheCopyStep executor is a NotImplementedError stub and has no YAML schema.
- Rule 15's table half is enforced at writer open, not startup - by ruling, recorded.

## Files to re-read on resume

- docs/simpleetl/spec.md sections 8 (triggering and concurrency), 9, 10, 11.2
- docs/simpleetl/plan.md P7 entry
- docs/simpleetl/progress.md P0-P6 entries
