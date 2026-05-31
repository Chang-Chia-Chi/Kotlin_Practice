#!/usr/bin/env bats
# Boolean predicates called directly; `run` only when $output/$lines needed.
load ../helpers/setup

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh eligibility.sh capacity.sh move.sh reconcile.sh backfill.sh metrics.sh
  export NOW_OVERRIDE; NOW_OVERRIDE=$(date -u -d 2026-06-01 +%s)
  sentinel on
}
teardown() { teardown_roots; }

@test "T1-12: no migration when NAS1 at or below HIGH" {
  nas_used_pct() { echo 70; }
  make_partition 20260101 catX 1024
  migrate_run
  [ -d "$NAS1_ROOT/20260101" ]
  [ ! -L "$NAS1_ROOT/20260101" ]
}

@test "migrate_run drains oldest-first and stops below LOW" {
  # File-backed counter so it survives subshells / process-substitution.
  # First read returns HIGH (loop enters), subsequent reads return LOW (loop exits).
  nas_used_pct() {
    local n
    n=$(( $(cat "$TEST_TMP/uc" 2>/dev/null || echo 0) + 1 ))
    echo "$n" > "$TEST_TMP/uc"
    if [ "$n" -le 1 ]; then echo 85; else echo 69; fi
  }
  fits_on_nas2()          { return 0; }
  backfill_should_yield() { return 1; }
  make_partition 20260101 catX 1024
  make_partition 20260102 catX 1024
  migrate_run
  [ -L "$NAS1_ROOT/20260101" ]      # oldest migrated
  [ -d "$NAS1_ROOT/20260102" ]      # loop stopped after used dropped below LOW
  [ ! -L "$NAS1_ROOT/20260102" ]
}

@test "migrate_run sets fit-check metric when a partition does not fit" {
  nas_used_pct()          { echo 85; }
  fits_on_nas2()          { return 1; }
  backfill_should_yield() { return 1; }
  make_partition 20260101 catX 1024
  migrate_run
  grep -q "sftp_migration_nas2_fit_check_failed 1" "$METRICS_FILE"
}

@test "migrate_run yields when backfill load gate trips" {
  nas_used_pct()          { echo 85; }
  fits_on_nas2()          { return 0; }
  backfill_should_yield() { return 0; }   # always yield
  make_partition 20260101 catX 1024
  migrate_run
  [ -d "$NAS1_ROOT/20260101" ]      # not migrated
  [ ! -L "$NAS1_ROOT/20260101" ]
}

@test "migrate_run returns non-zero and emits metrics when NAS2 guard fails" {
  sentinel off
  make_partition 20260101 catX 1024
  ! migrate_run
  [ -f "$METRICS_FILE" ]            # metrics still written so dead-man's switch advances correctly
}
